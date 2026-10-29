package raft

import (
	"reflect"
	"testing"
)

// newTestLeader returns an un-started leader in `term` whose log has the given
// entry terms. ids[0] is the leader; the rest are its peers.
func newTestLeader(t *testing.T, term uint64, logTerms []uint64, ids ...string) *Node {
	t.Helper()
	n, _ := newBareNode(t, ids[0], ids...)
	n.state, n.currentTerm, n.leaderID = Leader, term, n.id
	setLog(n, logTerms...)
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	for _, p := range n.peers {
		n.nextIndex[p.ID] = n.lastIndex() + 1
	}
	return n
}

func setMatch(n *Node, m map[string]uint64) {
	for id, idx := range m {
		n.matchIndex[id] = idx
	}
}

func TestQuorumSize(t *testing.T) {
	// cluster size -> strict majority
	want := map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 4}
	for size, q := range want {
		ids := []string{"n0"}
		for i := 1; i < size; i++ {
			ids = append(ids, "n"+string(rune('0'+i)))
		}
		n, _ := newBareNode(t, "n0", ids...)
		if got := n.quorum(); got != q {
			t.Errorf("cluster of %d: quorum = %d, want %d", size, got, q)
		}
	}
}

func TestAdvanceCommit(t *testing.T) {
	three := []string{"l", "a", "b"}
	four := []string{"l", "a", "b", "c"}
	five := []string{"l", "a", "b", "c", "d"}
	log := func(n int) []uint64 { // n entries, all from term 3
		out := make([]uint64, n)
		for i := range out {
			out[i] = 3
		}
		return out
	}

	tests := []struct {
		name  string
		ids   []string
		n     int // log length
		match map[string]uint64
		want  uint64
	}{
		{"3 nodes: nobody has replicated anything", three, 3, nil, 0},
		{"3 nodes: leader + one follower is a majority", three, 3, map[string]uint64{"a": 2}, 2},
		{"3 nodes: commits the highest index a majority holds", three, 3, map[string]uint64{"a": 3, "b": 1}, 3},
		{"3 nodes: uses the lower of two uneven followers when only that has quorum", three, 3, map[string]uint64{"a": 1, "b": 2}, 2},
		{"5 nodes: leader + one follower is NOT a majority", five, 5, map[string]uint64{"a": 5}, 0},
		{"5 nodes: leader + two followers is", five, 5, map[string]uint64{"a": 5, "b": 3}, 3},
		{"5 nodes: three-way tie at the top", five, 5, map[string]uint64{"a": 5, "b": 5, "c": 1}, 5},
		{"5 nodes: highest index with 3 holders", five, 5, map[string]uint64{"a": 4, "b": 4, "c": 2}, 4},
		{"4 nodes: 2 of 4 is not a majority", four, 4, map[string]uint64{"a": 4}, 0},
		{"4 nodes: 3 of 4 is", four, 4, map[string]uint64{"a": 4, "b": 4}, 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := newTestLeader(t, 3, log(tc.n), tc.ids...)
			setMatch(n, tc.match)
			n.advanceCommit()
			if n.commitIndex != tc.want {
				t.Errorf("commitIndex = %d, want %d", n.commitIndex, tc.want)
			}
			if n.lastApplied != n.commitIndex {
				t.Errorf("lastApplied = %d, want %d", n.lastApplied, n.commitIndex)
			}
			if got := uint64(len(n.applyQ.items)); got != tc.want {
				t.Errorf("queued %d entries for the applier, want %d", got, tc.want)
			}
		})
	}
}

// Raft paper Figure 8: an entry from an older term replicated on a majority is
// NOT committed by counting replicas. It only commits indirectly, once an entry
// from the leader's own term is committed after it.
func TestAdvanceCommitOnlyCountsCurrentTermEntries(t *testing.T) {
	n := newTestLeader(t, 3, []uint64{1, 2}, "l", "a", "b")
	setMatch(n, map[string]uint64{"a": 2, "b": 2}) // idx 2 (term 2) is on all 3 nodes

	n.advanceCommit()
	if n.commitIndex != 0 {
		t.Fatalf("committed an old-term entry by replica count: commitIndex = %d", n.commitIndex)
	}

	// The leader's no-op for its own term, not yet replicated anywhere else.
	n.log = append(n.log, LogEntry{Index: 3, Term: 3, Noop: true})
	n.advanceCommit()
	if n.commitIndex != 0 {
		t.Fatalf("committed before the current-term entry reached a majority: %d", n.commitIndex)
	}

	setMatch(n, map[string]uint64{"a": 3}) // now leader + a hold idx 3
	n.advanceCommit()
	if n.commitIndex != 3 {
		t.Fatalf("commitIndex = %d, want 3 (indexes 1-2 commit indirectly)", n.commitIndex)
	}
	var queued []uint64
	for _, m := range n.applyQ.items {
		queued = append(queued, m.Index)
	}
	if !reflect.DeepEqual(queued, []uint64{1, 2}) { // the no-op is never delivered
		t.Errorf("applier queue = %v, want [1 2]", queued)
	}
	if n.lastApplied != 3 {
		t.Errorf("lastApplied = %d, want 3", n.lastApplied)
	}
}

func TestCommitIndexNeverDecreases(t *testing.T) {
	n := newTestLeader(t, 3, []uint64{3, 3, 3}, "l", "a", "b")
	setMatch(n, map[string]uint64{"a": 3})
	n.advanceCommit()
	if n.commitIndex != 3 {
		t.Fatalf("setup: commitIndex = %d, want 3", n.commitIndex)
	}
	setMatch(n, map[string]uint64{"a": 0, "b": 0}) // e.g. matchIndex bookkeeping reset
	n.advanceCommit()
	if n.commitIndex != 3 {
		t.Errorf("commitIndex fell to %d", n.commitIndex)
	}
}

// End to end through the reply handler: success replies drive matchIndex, and a
// majority of replies drives commitIndex.
func TestCommitDrivenByFollowerReplies(t *testing.T) {
	n := newTestLeader(t, 3, []uint64{3}, "l", "a", "b")
	args := AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 0, Entries: entries(1, 3)}
	ok := AppendEntriesReply{Term: 3, Success: true}

	// A reply from a previous term of leadership must be ignored.
	n.handleAppendResult(appendResult{from: "a", term: 2, args: args, reply: AppendEntriesReply{Term: 2, Success: true}})
	if n.commitIndex != 0 || n.matchIndex["a"] != 0 {
		t.Fatalf("stale-term reply had an effect: commit %d match %d", n.commitIndex, n.matchIndex["a"])
	}

	// First follower ack makes (leader + a) = 2 of 3.
	n.handleAppendResult(appendResult{from: "a", term: 3, args: args, reply: ok})
	if n.commitIndex != 1 || n.matchIndex["a"] != 1 || n.nextIndex["a"] != 2 {
		t.Fatalf("after a's ack: commit %d match %d next %d, want 1/1/2", n.commitIndex, n.matchIndex["a"], n.nextIndex["a"])
	}

	// Duplicate / late acks are harmless and idempotent.
	n.handleAppendResult(appendResult{from: "a", term: 3, args: args, reply: ok})
	n.handleAppendResult(appendResult{from: "b", term: 3, args: args, reply: ok})
	if n.commitIndex != 1 || n.matchIndex["b"] != 1 {
		t.Errorf("commit %d match[b] %d, want 1 and 1", n.commitIndex, n.matchIndex["b"])
	}
	if len(n.applyQ.items) != 1 {
		t.Errorf("entry queued %d times, want exactly once", len(n.applyQ.items))
	}
}
