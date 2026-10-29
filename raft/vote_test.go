package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

// deadTransport fails every RPC. The tests below drive the vote handlers
// directly on nodes that were never Start()ed, so the run loop is not running
// and the test goroutine is the sole owner of node state (just like the loop).
type deadTransport struct{}

func (deadTransport) RequestVote(context.Context, Peer, RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, errors.New("dead")
}

func (deadTransport) AppendEntries(context.Context, Peer, AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, errors.New("dead")
}

// newBareNode builds an un-started node in a cluster of ids (including itself).
func newBareNode(t *testing.T, id string, ids ...string) (*Node, *MemoryStorage) {
	t.Helper()
	var peers []Peer
	for _, o := range ids {
		if o != id {
			peers = append(peers, Peer{ID: o, Addr: o + ":7000"})
		}
	}
	st := NewMemoryStorage()
	n, err := NewNode(Config{
		ID: id, Peers: peers, Transport: deadTransport{}, Storage: st,
		ApplyCh: make(chan ApplyMsg, 8),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n, st
}

// setLog replaces the log with entries of the given terms (index = position+1).
func setLog(n *Node, terms ...uint64) {
	n.log = []LogEntry{{}}
	for i, term := range terms {
		n.log = append(n.log, LogEntry{Index: uint64(i + 1), Term: term})
	}
}

func TestHandleRequestVote(t *testing.T) {
	// The voter's log in every case is terms [1,1,2]: lastIndex=3, lastTerm=2.
	voterLog := []uint64{1, 1, 2}

	tests := []struct {
		name         string
		term         uint64 // voter's currentTerm
		votedFor     string // voter's votedFor
		args         RequestVoteArgs
		wantGrant    bool
		wantTerm     uint64
		wantVotedFor string
	}{
		{
			name: "stale term is rejected",
			term: 5, votedFor: "",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 3, LastLogTerm: 2},
			wantGrant: false, wantTerm: 5, wantVotedFor: "",
		},
		{
			name: "equal term, no prior vote, identical log: granted",
			term: 4, votedFor: "",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 3, LastLogTerm: 2},
			wantGrant: true, wantTerm: 4, wantVotedFor: "c",
		},
		{
			name: "equal term, already voted for this candidate: granted again (idempotent)",
			term: 4, votedFor: "c",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 3, LastLogTerm: 2},
			wantGrant: true, wantTerm: 4, wantVotedFor: "c",
		},
		{
			name: "equal term, already voted for someone else: rejected",
			term: 4, votedFor: "other",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 3, LastLogTerm: 2},
			wantGrant: false, wantTerm: 4, wantVotedFor: "other",
		},
		{
			name: "higher term clears an old vote, so a new candidate gets it",
			term: 3, votedFor: "other",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 3, LastLogTerm: 2},
			wantGrant: true, wantTerm: 4, wantVotedFor: "c",
		},
		{
			name: "higher term but candidate log has older last term: term adopted, vote withheld",
			term: 3, votedFor: "other",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 10, LastLogTerm: 1},
			wantGrant: false, wantTerm: 4, wantVotedFor: "",
		},
		{
			name: "same last term, candidate log shorter: rejected",
			term: 4, votedFor: "",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 2, LastLogTerm: 2},
			wantGrant: false, wantTerm: 4, wantVotedFor: "",
		},
		{
			name: "same last term, candidate log longer: granted",
			term: 4, votedFor: "",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 5, LastLogTerm: 2},
			wantGrant: true, wantTerm: 4, wantVotedFor: "c",
		},
		{
			name: "candidate last term newer beats a longer voter log",
			term: 4, votedFor: "",
			args:      RequestVoteArgs{Term: 4, CandidateID: "c", LastLogIndex: 1, LastLogTerm: 3},
			wantGrant: true, wantTerm: 4, wantVotedFor: "c",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, st := newBareNode(t, "v", "v", "c", "x")
			setLog(n, voterLog...)
			n.currentTerm, n.votedFor = tc.term, tc.votedFor
			n.persist()

			reply := n.handleRequestVote(tc.args)

			if reply.VoteGranted != tc.wantGrant {
				t.Errorf("VoteGranted = %v, want %v", reply.VoteGranted, tc.wantGrant)
			}
			if reply.Term != tc.wantTerm {
				t.Errorf("reply.Term = %d, want %d", reply.Term, tc.wantTerm)
			}
			if n.currentTerm != tc.wantTerm || n.votedFor != tc.wantVotedFor {
				t.Errorf("state = (term %d, votedFor %q), want (%d, %q)",
					n.currentTerm, n.votedFor, tc.wantTerm, tc.wantVotedFor)
			}
			// The vote must be durable before the reply is sent.
			hs, _, _ := st.Load()
			if hs.CurrentTerm != tc.wantTerm || hs.VotedFor != tc.wantVotedFor {
				t.Errorf("persisted = %+v, want term %d votedFor %q", hs, tc.wantTerm, tc.wantVotedFor)
			}
		})
	}
}

func TestLeaderStepsDownOnHigherTermVoteRequest(t *testing.T) {
	n, _ := newBareNode(t, "a", "a", "b", "c")
	n.startElection()
	n.handleVoteResult(voteResult{from: "b", term: n.currentTerm, reply: RequestVoteReply{Term: n.currentTerm, VoteGranted: true}})
	if n.state != Leader {
		t.Fatalf("setup: state = %v, want Leader", n.state)
	}

	reply := n.handleRequestVote(RequestVoteArgs{Term: n.currentTerm + 1, CandidateID: "c", LastLogIndex: 99, LastLogTerm: 99})
	if n.state != Follower || !reply.VoteGranted {
		t.Errorf("state = %v granted = %v, want Follower and granted", n.state, reply.VoteGranted)
	}
}

// ---------------------------------------------------------------------------
// 3-node split vote
// ---------------------------------------------------------------------------

// ask makes `from` send its RequestVote to `to` and feeds the reply back, as the
// run loop would. Returns whether the vote was granted.
func ask(from, to *Node) bool {
	lastIdx, lastTerm := from.lastLog()
	args := RequestVoteArgs{Term: from.currentTerm, CandidateID: from.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
	reply := to.handleRequestVote(args)
	from.handleVoteResult(voteResult{from: to.id, term: args.Term, reply: reply})
	return reply.VoteGranted
}

func leaders(nodes ...*Node) (out []string) {
	for _, n := range nodes {
		if n.state == Leader {
			out = append(out, n.id)
		}
	}
	return
}

// All three nodes time out at the same moment: each votes for itself, so every
// peer request is refused ("already voted"). Nobody reaches quorum (2 of 3).
// On the next round, a node that times out first (term 2) wins because the
// others adopt its higher term, which clears their self-votes.
func TestThreeNodeSplitVoteThenRecovery(t *testing.T) {
	ids := []string{"a", "b", "c"}
	a, _ := newBareNode(t, "a", ids...)
	b, _ := newBareNode(t, "b", ids...)
	c, _ := newBareNode(t, "c", ids...)
	all := []*Node{a, b, c}

	// Round 1: simultaneous timeouts.
	for _, n := range all {
		n.startElection()
	}
	for _, n := range all {
		if n.currentTerm != 1 || n.votedFor != n.id || n.state != Candidate {
			t.Fatalf("%s after timeout: term %d votedFor %q state %v", n.id, n.currentTerm, n.votedFor, n.state)
		}
	}
	for _, from := range all {
		for _, to := range all {
			if from != to && ask(from, to) {
				t.Errorf("%s got a vote from %s in a split round", from.id, to.id)
			}
		}
	}
	if l := leaders(all...); len(l) != 0 {
		t.Fatalf("split vote elected %v, want nobody", l)
	}
	for _, n := range all {
		if n.state != Candidate || n.currentTerm != 1 {
			t.Errorf("%s: state %v term %d, want Candidate in term 1", n.id, n.state, n.currentTerm)
		}
	}

	// Round 2: a's randomized timer fires first and it moves to term 2.
	a.startElection()
	if !ask(a, b) {
		t.Fatal("b should grant: a's term 2 is higher, which clears b's term-1 self-vote")
	}
	if a.state != Leader {
		t.Fatalf("a has 2 of 3 votes but state = %v", a.state)
	}
	if b.state != Follower || b.currentTerm != 2 || b.votedFor != "a" {
		t.Errorf("b: state %v term %d votedFor %q, want Follower/2/a", b.state, b.currentTerm, b.votedFor)
	}

	// c is still a term-1 Candidate. a's first heartbeat makes it step down.
	if c.state != Candidate || c.currentTerm != 1 {
		t.Fatalf("setup: c state %v term %d, want Candidate in term 1", c.state, c.currentTerm)
	}
	reply := c.handleAppendEntries(AppendEntriesArgs{Term: 2, LeaderID: "a"})
	if !reply.Success || c.state != Follower || c.currentTerm != 2 || c.leaderID != "a" {
		t.Errorf("c: reply %+v state %v term %d leader %q, want success/Follower/2/a",
			reply, c.state, c.currentTerm, c.leaderID)
	}

	// c's vote reply arrives late; the already-elected leader must ignore it.
	ask(a, c)
	if a.state != Leader || a.currentTerm != 2 {
		t.Errorf("a after late vote: state %v term %d, want Leader in term 2", a.state, a.currentTerm)
	}
	if l := leaders(all...); len(l) != 1 || l[0] != "a" {
		t.Errorf("leaders = %v, want exactly [a]", l)
	}
}

// Two candidates in the same term compete for one remaining voter. The voter
// can grant at most one vote per term, so there is exactly one winner.
func TestOneVotePerTerm(t *testing.T) {
	ids := []string{"a", "b", "c"}
	a, _ := newBareNode(t, "a", ids...)
	b, _ := newBareNode(t, "b", ids...)
	c, _ := newBareNode(t, "c", ids...)

	a.startElection()
	b.startElection() // same term 1; each holds only its own vote

	if !ask(a, c) {
		t.Fatal("c should vote for the first requester, a")
	}
	if ask(b, c) {
		t.Fatal("c voted twice in term 1")
	}

	if l := leaders(a, b, c); len(l) != 1 || l[0] != "a" {
		t.Errorf("leaders = %v, want exactly [a]", l)
	}
	if b.state != Candidate {
		t.Errorf("b state = %v, want Candidate (lost the race)", b.state)
	}

	// b learns of the leader in its own term and yields.
	b.handleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "a"})
	if b.state != Follower || b.leaderID != "a" {
		t.Errorf("b: state %v leader %q, want Follower following a", b.state, b.leaderID)
	}
}
