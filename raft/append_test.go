package raft

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// terms returns the term of each real log entry (index 1..last).
func terms(log []LogEntry) []uint64 {
	out := make([]uint64, 0, len(log)-1)
	for _, e := range log[1:] {
		out = append(out, e.Term)
	}
	return out
}

// entries builds AppendEntries payload: consecutive indexes starting at `from`.
func entries(from uint64, ts ...uint64) []LogEntry {
	out := make([]LogEntry, len(ts))
	for i, term := range ts {
		out[i] = LogEntry{Index: from + uint64(i), Term: term, Command: []byte{byte(from) + byte(i)}}
	}
	return out
}

func TestHandleAppendEntries(t *testing.T) {
	// Follower log in every case: terms [1,1,2,2] (lastIndex 4, lastTerm 2).
	base := []uint64{1, 1, 2, 2}

	tests := []struct {
		name        string
		args        AppendEntriesArgs
		wantSuccess bool
		wantLog     []uint64
		wantCommit  uint64
		wantCI      uint64 // expected ConflictIndex (rejections only)
		wantCT      uint64 // expected ConflictTerm  (rejections only)
	}{
		{
			name:        "heartbeat with matching prev succeeds and leaves log alone",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 2},
			wantSuccess: true, wantLog: base,
		},
		{
			name:        "heartbeat carries commit index, bounded by what was verified",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 3, PrevLogTerm: 2, LeaderCommit: 100},
			wantSuccess: true, wantLog: base, wantCommit: 3, // not 4, not 100
		},
		{
			name:        "prevLogIndex beyond our log is rejected with ConflictIndex = len+1",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 7, PrevLogTerm: 2, Entries: entries(8, 2)},
			wantSuccess: false, wantLog: base, wantCI: 5, wantCT: 0,
		},
		{
			name:        "prevLogTerm mismatch is rejected and log is untouched",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 3, Entries: entries(5, 3)},
			wantSuccess: false, wantLog: base, wantCI: 3, wantCT: 2, // term 2 starts at index 3
		},
		{
			name:        "mismatch deep in the log reports the first index of the conflicting term",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 9},
			wantSuccess: false, wantLog: base, wantCI: 1, wantCT: 1,
		},
		{
			name:        "prevLogIndex 0 always matches; identical entries change nothing",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries(1, 1, 1)},
			wantSuccess: true, wantLog: base, // suffix [2,2] is NOT truncated by a shorter, matching RPC
		},
		{
			name:        "new entries are appended",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 2, Entries: entries(5, 2, 2)},
			wantSuccess: true, wantLog: []uint64{1, 1, 2, 2, 2, 2},
		},
		{
			name:        "conflicting entry truncates the suffix and replaces it",
			args:        AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 1, Entries: entries(3, 3, 3)},
			wantSuccess: true, wantLog: []uint64{1, 1, 3, 3},
		},
		{
			name:        "overlap then extend: matching prefix kept, tail appended",
			args:        AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 1, Entries: entries(3, 2, 2, 2)},
			wantSuccess: true, wantLog: []uint64{1, 1, 2, 2, 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, st := newBareNode(t, "f", "f", "l", "x")
			setLog(f, base...)
			f.currentTerm = 2
			f.persist()

			reply := f.handleAppendEntries(tc.args)

			if reply.Success != tc.wantSuccess {
				t.Fatalf("Success = %v, want %v (reply %+v)", reply.Success, tc.wantSuccess, reply)
			}
			if got := terms(f.log); !reflect.DeepEqual(got, tc.wantLog) {
				t.Errorf("log terms = %v, want %v", got, tc.wantLog)
			}
			if f.commitIndex != tc.wantCommit {
				t.Errorf("commitIndex = %d, want %d", f.commitIndex, tc.wantCommit)
			}
			if f.lastApplied != f.commitIndex {
				t.Errorf("lastApplied = %d, want %d", f.lastApplied, f.commitIndex)
			}
			if !tc.wantSuccess && (reply.ConflictIndex != tc.wantCI || reply.ConflictTerm != tc.wantCT) {
				t.Errorf("conflict hint = (index %d, term %d), want (%d, %d)",
					reply.ConflictIndex, reply.ConflictTerm, tc.wantCI, tc.wantCT)
			}
			// The log on disk must match memory before we reply.
			if _, saved, _ := st.Load(); !reflect.DeepEqual(terms(append([]LogEntry{{}}, saved...)), tc.wantLog) {
				t.Errorf("persisted log terms = %v, want %v", terms(append([]LogEntry{{}}, saved...)), tc.wantLog)
			}
		})
	}
}

func TestAppendEntriesTermHandling(t *testing.T) {
	t.Run("stale term is rejected and does not reset the timer or learn a leader", func(t *testing.T) {
		f, _ := newBareNode(t, "f", "f", "l", "x")
		f.currentTerm = 5
		reply := f.handleAppendEntries(AppendEntriesArgs{Term: 4, LeaderID: "old"})
		if reply.Success || reply.Term != 5 || f.leaderID != "" {
			t.Errorf("reply %+v leader %q, want rejection at term 5 with no leader", reply, f.leaderID)
		}
	})

	t.Run("higher term is adopted and clears the vote", func(t *testing.T) {
		f, st := newBareNode(t, "f", "f", "l", "x")
		f.currentTerm, f.votedFor = 3, "someone"
		f.persist()
		reply := f.handleAppendEntries(AppendEntriesArgs{Term: 4, LeaderID: "l"})
		if !reply.Success || f.currentTerm != 4 || f.votedFor != "" || f.leaderID != "l" {
			t.Errorf("reply %+v term %d votedFor %q leader %q", reply, f.currentTerm, f.votedFor, f.leaderID)
		}
		if hs, _, _ := st.Load(); hs.CurrentTerm != 4 || hs.VotedFor != "" {
			t.Errorf("persisted %+v, want term 4 and no vote", hs)
		}
	})

	t.Run("candidate in the same term steps down but keeps its self-vote", func(t *testing.T) {
		f, _ := newBareNode(t, "f", "f", "l", "x")
		f.startElection() // term 1, votedFor f
		reply := f.handleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "l"})
		if !reply.Success || f.state != Follower || f.votedFor != "f" {
			t.Errorf("reply %+v state %v votedFor %q, want success/Follower/f", reply, f.state, f.votedFor)
		}
	})
}

// ---------------------------------------------------------------------------
// Leader backoff: leader.handleAppendResult <-> follower.handleAppendEntries
// ---------------------------------------------------------------------------

// appendRecorder hands the leader's outbound AppendEntries to the test, which
// plays the network and feeds replies back, one RPC at a time.
type appendRecorder struct{ ch chan AppendEntriesArgs }

func (r *appendRecorder) AppendEntries(_ context.Context, _ Peer, a AppendEntriesArgs) (AppendEntriesReply, error) {
	r.ch <- a
	return AppendEntriesReply{}, errors.New("test drives replies by hand")
}

func (r *appendRecorder) RequestVote(context.Context, Peer, RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, errors.New("unused")
}

// The follower logs below are the divergent cases (a)-(f) from Figure 7 of the
// Raft paper. The leader (term 8) has terms 1,1,1,4,4,5,5,6,6,6 plus a fresh
// term-8 no-op at index 11, with nextIndex initialised to 11 as in becomeLeader.
func TestLeaderBacksOffAndRepairsDivergentFollowers(t *testing.T) {
	leaderTerms := []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 8}

	tests := []struct {
		name     string
		follower []uint64
		maxRPCs  int // with conflict hints; naive one-step backoff needs far more
	}{
		{"a: missing one entry", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6}, 2},
		{"b: missing many entries", []uint64{1, 1, 1, 4}, 2},
		{"c: one extra uncommitted entry", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}, 1},
		{"d: extra entries from a later term", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}, 1},
		{"e: diverged at term 4", []uint64{1, 1, 1, 4, 4, 4, 4}, 3},
		{"f: long divergent history", []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}, 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &appendRecorder{ch: make(chan AppendEntriesArgs, 16)}
			leader, _ := newBareNode(t, "l", "l", "f", "x")
			leader.transport = rec
			leader.state, leader.currentTerm, leader.leaderID = Leader, 8, "l"
			setLog(leader, leaderTerms...)
			leader.log[11].Noop = true
			leader.nextIndex = map[string]uint64{"f": 11, "x": 11}
			leader.matchIndex = map[string]uint64{"f": 0, "x": 0}

			follower, st := newBareNode(t, "f", "l", "f", "x")
			setLog(follower, tc.follower...)
			follower.currentTerm = 7
			follower.persist()

			leader.replicateTo(Peer{ID: "f", Addr: "f:7000"})

			rpcs := 0
			for leader.matchIndex["f"] != leader.lastIndex() {
				if rpcs++; rpcs > 20 {
					t.Fatalf("no convergence after %d RPCs; nextIndex=%d", rpcs-1, leader.nextIndex["f"])
				}
				var args AppendEntriesArgs
				select {
				case args = <-rec.ch:
				case <-time.After(time.Second):
					t.Fatalf("leader sent nothing after RPC %d (nextIndex=%d)", rpcs-1, leader.nextIndex["f"])
				}
				before := leader.nextIndex["f"]
				reply := follower.handleAppendEntries(args)
				leader.handleAppendResult(appendResult{from: "f", term: 8, args: args, reply: reply})

				if !reply.Success && leader.nextIndex["f"] >= before {
					t.Fatalf("rejection did not decrease nextIndex (%d -> %d)", before, leader.nextIndex["f"])
				}
			}

			if got := terms(follower.log); !reflect.DeepEqual(got, leaderTerms) {
				t.Errorf("follower log = %v, want %v", got, leaderTerms)
			}
			if _, saved, _ := st.Load(); len(saved) != len(leaderTerms) {
				t.Errorf("persisted %d entries, want %d", len(saved), len(leaderTerms))
			}
			if rpcs > tc.maxRPCs {
				t.Errorf("took %d RPCs, want <= %d", rpcs, tc.maxRPCs)
			}
		})
	}
}

func TestLeaderIgnoresStaleRejectionAndStepsDownOnHigherTerm(t *testing.T) {
	rec := &appendRecorder{ch: make(chan AppendEntriesArgs, 16)}
	leader, _ := newBareNode(t, "l", "l", "f", "x")
	leader.transport = rec
	leader.state, leader.currentTerm = Leader, 8
	leader.heartbeat = time.NewTicker(time.Hour) // becomeFollower stops it, as for a real leader
	setLog(leader, 1, 1, 8)
	leader.nextIndex = map[string]uint64{"f": 2, "x": 4}
	leader.matchIndex = map[string]uint64{"f": 0, "x": 0}

	// A rejection for a probe we already moved past (prev=3 vs current nextIndex 2).
	stale := AppendEntriesArgs{Term: 8, LeaderID: "l", PrevLogIndex: 3, PrevLogTerm: 8}
	leader.handleAppendResult(appendResult{from: "f", term: 8, args: stale,
		reply: AppendEntriesReply{Term: 8, ConflictIndex: 1}})
	if leader.nextIndex["f"] != 2 {
		t.Errorf("stale rejection changed nextIndex to %d", leader.nextIndex["f"])
	}
	select {
	case a := <-rec.ch:
		t.Errorf("stale rejection triggered a resend: %+v", a)
	case <-time.After(50 * time.Millisecond):
	}

	// A reply carrying a higher term demotes the leader.
	leader.handleAppendResult(appendResult{from: "f", term: 8, args: stale,
		reply: AppendEntriesReply{Term: 9}})
	if leader.state != Follower || leader.currentTerm != 9 {
		t.Errorf("state %v term %d, want Follower in term 9", leader.state, leader.currentTerm)
	}
}
