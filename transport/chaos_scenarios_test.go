package transport

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

// Every scenario below also runs under the harness's election-safety monitor:
// if any term ever had two leaders, the test fails in cleanup regardless of what
// the scenario itself asserted.

// A leader is cut off from a 3-node cluster. The majority elects a successor,
// so two nodes simultaneously believe they lead, in different terms. Only the
// newer one can commit. After the heal the stale leader steps down and its
// uncommitted write is discarded.
func TestSplitBrainAfterLeaderIsolation(t *testing.T) {
	c := newRaftCluster(t, 3)
	must(t, c.chaos.SetGlobalLatency(1*time.Millisecond, 5*time.Millisecond))

	// 1. Normal operation: a leader over a jittery network.
	old := c.waitLeader()
	oldTerm := c.status(old).Term

	// 2. Partition the leader away; the majority side elects a new one.
	c.chaos.Isolate(old)
	t.Logf("injected: %v", c.chaos.Faults())
	rest := c.others(old)
	var next string
	eventually(t, "a new leader on the majority side", func() bool {
		if l := c.leaders(rest...); len(l) == 1 && c.status(l[0]).Term > oldTerm {
			next = l[0]
			return true
		}
		return false
	})

	// 3. Split brain, as seen from outside.
	stale := c.status(old)
	if stale.State != raft.Leader || stale.Term != oldTerm {
		t.Fatalf("isolated node = %+v, want still a Leader in term %d", stale, oldTerm)
	}
	if got := c.leaders(); len(got) != 2 {
		t.Fatalf("leaders = %v, want both the stale and the new one", got)
	}
	commitBefore := stale.CommitIndex
	c.propose(old, "doomed") // accepted locally, can never reach a quorum
	idx := c.propose(next, "real")
	c.waitCommit(idx, rest...)
	time.Sleep(200 * time.Millisecond)
	if got := c.status(old).CommitIndex; got != commitBefore {
		t.Errorf("isolated leader's commitIndex moved %d -> %d without a quorum", commitBefore, got)
	}

	// 4. Heal: the stale leader sees the higher term, steps down, drops "doomed".
	c.chaos.Reconnect(old)
	lead := c.waitConverged()
	if lead == old || c.status(old).State != raft.Follower {
		t.Errorf("leader %s; old leader %+v: want the old leader demoted", lead, c.status(old))
	}
	c.waitApplied([]string{"real"}) // "doomed" is applied nowhere
}

// With 5 nodes split 3|2, the minority side can never elect a leader, so it
// churns through ever higher terms. The majority keeps working. On heal, the
// inflated terms force a fresh election (Raft without PreVote), but nothing
// committed is lost.
func TestMinorityPartitionCannotElectAndClusterRecovers(t *testing.T) {
	c := newRaftCluster(t, 5)
	must(t, c.chaos.SetGlobalLatency(1*time.Millisecond, 5*time.Millisecond))

	leader := c.waitLeader()
	others := c.others(leader)
	majority := []string{leader, others[0], others[1]}
	minority := others[2:]

	idx := c.propose(leader, "pre")
	c.waitCommit(idx)
	c.waitApplied([]string{"pre"})
	leaderTerm := c.status(leader).Term

	c.chaos.Partition(majority, minority)
	t.Logf("injected: %v", c.chaos.Faults())

	// The minority tries and fails to elect for well over several timeouts.
	for deadline := time.Now().Add(1500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if l := c.leaders(minority...); len(l) != 0 {
			t.Fatalf("minority %v elected %v without a quorum", minority, l)
		}
	}
	var maxMinorityTerm uint64
	for _, m := range minority {
		term := c.status(m).Term
		if term <= leaderTerm {
			t.Errorf("minority node %s term %d: expected it to inflate past %d by campaigning", m, term, leaderTerm)
		}
		maxMinorityTerm = max(maxMinorityTerm, term)
	}

	// The majority is unaffected and still commits.
	dIdx := c.propose(c.waitLeader(majority...), "during")
	c.waitCommit(dIdx, majority...)
	for _, m := range minority {
		if c.status(m).CommitIndex >= dIdx {
			t.Errorf("minority node %s committed index %d without a quorum", m, dIdx)
		}
	}

	// Heal.
	c.chaos.Heal()
	lead := c.waitConverged()
	if got := c.status(lead).Term; got < maxMinorityTerm {
		t.Errorf("converged on term %d, below the minority's %d", got, maxMinorityTerm)
	}
	c.waitApplied([]string{"pre", "during"}) // committed data survived, in order everywhere

	c.waitCommit(c.propose(lead, "post"))
	c.waitApplied([]string{"pre", "during", "post"})
}

// The leader's OUTBOUND links are cut, but traffic towards it still flows.
// Followers stop hearing heartbeats and elect a new leader. The old leader
// still receives (and obeys) the new leader's messages, yet its replies vanish,
// so the new leader never hears from it while it keeps applying the log.
func TestAsymmetricOutboundCutOfLeader(t *testing.T) {
	c := newRaftCluster(t, 3)
	old := c.waitLeader()
	followers := c.others(old)
	oldTerm := c.status(old).Term

	c.waitCommit(c.propose(old, "pre"))
	c.waitApplied([]string{"pre"})

	for _, f := range followers {
		c.chaos.BlockOneWay(old, f) // old -> f dropped; f -> old still open
	}
	t.Logf("injected: %v", c.chaos.Faults())

	var next string
	eventually(t, "followers to replace the deaf-mute leader", func() bool {
		l := c.leaders()
		if len(l) == 1 && l[0] != old && c.status(l[0]).Term > oldTerm {
			next = l[0]
			return true
		}
		return false
	})
	eventually(t, "the old leader to yield to the new one, whose messages it still receives", func() bool {
		s := c.status(old)
		return s.State == raft.Follower && s.Term == c.status(next).Term
	})

	// The new leader gets no acks from the old one, yet the old one applies new
	// entries, because the requests reach it and only the replies are lost.
	c.propose(next, "asym")
	c.waitApplied([]string{"pre", "asym"}) // all three, including the muted one

	c.chaos.Heal()
	c.waitConverged()
}

// Latency above the RPC timeout (100ms default) makes a node look dead to its
// peers, yet its delayed messages still arrive, stale. That mix can disturb
// leadership, so the assertions are about safety during the fault (checked by
// the harness monitor) and full recovery after it.
func TestLatencyAboveRPCTimeoutThenRecovery(t *testing.T) {
	c := newRaftCluster(t, 3)
	leader := c.waitLeader()
	slow := c.others(leader)[0]

	var slowLegs atomic.Int64
	c.sw.SetTap(func(e Event) {
		if e.Delay >= 100*time.Millisecond {
			slowLegs.Add(1)
		}
	})
	must(t, c.chaos.SetNodeLatency(slow, 100*time.Millisecond, 500*time.Millisecond))
	t.Logf("injected: %v", c.chaos.Faults())

	time.Sleep(2500 * time.Millisecond)
	if slowLegs.Load() == 0 {
		t.Fatal("no message was delayed: the latency fault never took effect")
	}
	t.Logf("%d delayed legs; leaders during fault: %v", slowLegs.Load(), c.leaders())

	must(t, c.chaos.SetNodeLatency(slow, 0, 0))
	time.Sleep(600 * time.Millisecond) // let packets already in flight (up to 500ms) land

	lead := c.waitConverged()
	c.waitCommit(c.propose(lead, "after"))
	c.waitApplied([]string{"after"})
}

// With 15% of packets lost everywhere, elections and replication still work, and
// every node applies the same commands in the same order.
func TestLossyNetworkMakesProgressAndStaysConsistent(t *testing.T) {
	c := newRaftCluster(t, 3)
	must(t, c.chaos.SetGlobalLatency(1*time.Millisecond, 10*time.Millisecond))
	must(t, c.chaos.SetGlobalDropRate(0.15))
	t.Logf("injected: %v", c.chaos.Faults())

	// Leadership may change mid-write, so a write is retried under a fresh token
	// until every node has applied it. Tokens are unique per attempt, so a retried
	// command can never be mistaken for a duplicate.
	commit := func(name string) string {
		for attempt := 1; attempt <= 15; attempt++ {
			tok := fmt.Sprintf("%s#%d", name, attempt)
			leader := c.waitLeader()
			if _, _, err := c.nodes[leader].Propose([]byte(tok)); err != nil {
				continue
			}
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				all := true
				for _, id := range c.ids {
					if !contains(c.appliedOn(id), tok) {
						all = false
					}
				}
				if all {
					return tok
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		t.Fatalf("%s never committed after 15 attempts", name)
		return ""
	}

	var committed []string
	for i := 1; i <= 5; i++ {
		committed = append(committed, commit(fmt.Sprintf("cmd%d", i)))
	}

	c.chaos.Reset()
	eventually(t, "every node to hold an identical applied log", func() bool {
		ref := c.appliedOn(c.ids[0])
		for _, id := range c.ids[1:] {
			got := c.appliedOn(id)
			if len(got) != len(ref) {
				return false
			}
			for i := range ref {
				if got[i] != ref[i] {
					return false
				}
			}
		}
		return true
	})
	if !inOrder(c.appliedOn(c.ids[0]), committed) {
		t.Errorf("committed %v not in order within applied log %v", committed, c.appliedOn(c.ids[0]))
	}
}

func contains(seq []string, tok string) bool {
	for _, s := range seq {
		if s == tok {
			return true
		}
	}
	return false
}

// inOrder reports whether toks appear in seq as a subsequence.
func inOrder(seq, toks []string) bool {
	i := 0
	for _, s := range seq {
		if i < len(toks) && s == toks[i] {
			i++
		}
	}
	return i == len(toks)
}
