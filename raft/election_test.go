package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// voteRecorder captures outbound RequestVote RPCs and answers with an error so
// the node never wins; it stays a Candidate and we can inspect what it sent.
type voteRecorder struct {
	votes chan votedFor
}

type votedFor struct {
	to   string
	args RequestVoteArgs
}

func (v *voteRecorder) RequestVote(_ context.Context, to Peer, a RequestVoteArgs) (RequestVoteReply, error) {
	v.votes <- votedFor{to.ID, a}
	return RequestVoteReply{}, errors.New("unreachable")
}

func (v *voteRecorder) AppendEntries(context.Context, Peer, AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, errors.New("unreachable")
}

func newIsolatedNode(t *testing.T) (*Node, *voteRecorder) {
	t.Helper()
	rec := &voteRecorder{votes: make(chan votedFor, 16)}
	n, err := NewNode(Config{
		ID:                 "n1",
		Peers:              []Peer{{ID: "n2", Addr: "n2:7000"}, {ID: "n3", Addr: "n3:7000"}},
		Transport:          rec,
		ApplyCh:            make(chan ApplyMsg, 8),
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	t.Cleanup(n.Stop)
	return n, rec
}

// With no heartbeat, a follower must become a candidate within the timeout
// window, bump its term, vote for itself, and ask every peer for a vote.
func TestFollowerTimesOutAndCampaigns(t *testing.T) {
	n, rec := newIsolatedNode(t)

	start := time.Now()
	got := map[string]RequestVoteArgs{}
	for len(got) < 2 {
		select {
		case v := <-rec.votes:
			got[v.to] = v.args
		case <-time.After(time.Second):
			t.Fatalf("no RequestVote broadcast after 1s; got %v", got)
		}
	}
	elapsed := time.Since(start)
	// Lower bound has slack for scheduling; upper bound is max timeout + slack.
	if elapsed < 140*time.Millisecond || elapsed > 450*time.Millisecond {
		t.Errorf("first election started after %v, want ~150-300ms", elapsed)
	}

	for peer, a := range got {
		if a.Term != 1 || a.CandidateID != "n1" {
			t.Errorf("RequestVote to %s = %+v, want term 1 from n1", peer, a)
		}
	}
	s, _ := n.Status()
	if s.State != Candidate || s.Term < 1 || s.VotedFor != "n1" {
		t.Errorf("status = %+v, want Candidate with votedFor=n1", s)
	}
}

// Valid heartbeats must keep resetting the timer: the node stays a Follower
// far longer than the maximum election timeout and never increments its term.
func TestHeartbeatsResetElectionTimer(t *testing.T) {
	n, rec := newIsolatedNode(t)

	deadline := time.Now().Add(1500 * time.Millisecond) // 5x the max timeout
	for time.Now().Before(deadline) {
		reply, err := n.AppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "n2"})
		if err != nil || !reply.Success {
			t.Fatalf("heartbeat rejected: %+v, %v", reply, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	select {
	case v := <-rec.votes:
		t.Fatalf("follower campaigned despite heartbeats: %+v", v)
	default:
	}
	s, _ := n.Status()
	if s.State != Follower || s.Term != 1 || s.LeaderID != "n2" {
		t.Errorf("status = %+v, want Follower, term 1, leader n2", s)
	}

	// Stop heartbeating: it must now time out and campaign in term 2.
	select {
	case v := <-rec.votes:
		if v.args.Term != 2 {
			t.Errorf("campaign term = %d, want 2", v.args.Term)
		}
	case <-time.After(time.Second):
		t.Fatal("follower never campaigned after heartbeats stopped")
	}
}

// Stale-term heartbeats are not "valid" and must NOT reset the timer.
func TestStaleHeartbeatDoesNotResetTimer(t *testing.T) {
	n, rec := newIsolatedNode(t)

	// Move the node to term 5 via a legitimate heartbeat, then spam term-3 ones.
	if _, err := n.AppendEntries(AppendEntriesArgs{Term: 5, LeaderID: "n2"}); err != nil {
		t.Fatal(err)
	}
	stop := time.After(600 * time.Millisecond)
	tick := time.NewTicker(40 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			reply, _ := n.AppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "old"})
			if reply.Success {
				t.Fatal("stale-term AppendEntries was accepted")
			}
			continue
		case v := <-rec.votes:
			if v.args.Term != 6 {
				t.Fatalf("campaign term = %d, want 6", v.args.Term)
			}
			return
		case <-stop:
			t.Fatal("stale heartbeats kept the election timer from firing")
		}
	}
}
