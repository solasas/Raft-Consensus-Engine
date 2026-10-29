package transport

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

type raftCluster struct {
	sw    *Switch
	chaos *Chaos
	nodes map[string]*raft.Node
}

// newRaftCluster runs real raft nodes whose only link to each other is the
// simulated switch.
func newRaftCluster(t *testing.T, ids ...string) *raftCluster {
	t.Helper()
	c := &raftCluster{sw: NewSwitch(Options{Seed: 42}), nodes: map[string]*raft.Node{}}
	c.chaos = NewChaos(c.sw)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, id := range ids {
		var peers []raft.Peer
		for _, o := range ids {
			if o != id {
				peers = append(peers, raft.Peer{ID: o, Addr: "sim://" + o})
			}
		}
		n, err := raft.NewNode(raft.Config{
			ID: id, Peers: peers, Logger: quiet,
			ApplyCh:   make(chan raft.ApplyMsg, 128),
			Transport: AsRaftTransport(c.sw.NewTransport(id, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
		c.sw.Register(id, ServeNode(n))
	}
	for _, n := range c.nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *raftCluster) status(id string) raft.Status {
	s, _ := c.nodes[id].Status()
	return s
}

// leaders returns the IDs, among `among`, that currently believe they lead.
func (c *raftCluster) leaders(among ...string) []string {
	var out []string
	for _, id := range among {
		if c.status(id).State == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRaftClusterSurvivesIsolationOverSimulatedNetwork(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	c := newRaftCluster(t, ids...)
	if err := c.chaos.SetGlobalLatency(1*time.Millisecond, 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// 1. A leader emerges over a jittery network.
	var old string
	eventually(t, "initial leader", func() bool {
		if l := c.leaders(ids...); len(l) == 1 {
			old = l[0]
			return true
		}
		return false
	})
	oldTerm := c.status(old).Term

	// 2. Cut the leader off. The majority side elects a new leader in a higher term.
	c.chaos.Isolate(old)
	var rest []string
	for _, id := range ids {
		if id != old {
			rest = append(rest, id)
		}
	}
	var next string
	eventually(t, "new leader on the majority side", func() bool {
		l := c.leaders(rest...)
		if len(l) == 1 && c.status(l[0]).Term > oldTerm {
			next = l[0]
			return true
		}
		return false
	})

	// 3. Split brain, as seen from outside: two nodes both believe they lead,
	//    but in different terms, and only the newer one can make progress.
	stale := c.status(old)
	if stale.State != raft.Leader || stale.Term != oldTerm {
		t.Fatalf("isolated node = %+v, want still Leader in term %d", stale, oldTerm)
	}
	commitBefore := stale.CommitIndex
	if _, _, err := c.nodes[old].Propose([]byte("doomed")); err != nil {
		t.Fatalf("isolated leader refused a proposal it should accept locally: %v", err)
	}
	idx, _, err := c.nodes[next].Propose([]byte("real"))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "majority side to commit", func() bool {
		for _, id := range rest {
			if c.status(id).CommitIndex < idx {
				return false
			}
		}
		return true
	})
	if got := c.status(old).CommitIndex; got != commitBefore {
		t.Errorf("isolated leader's commitIndex moved %d -> %d without a quorum", commitBefore, got)
	}

	// 4. Heal. The old leader sees the higher term, steps down, drops its
	//    uncommitted entry, and catches up.
	c.chaos.Reconnect(old)
	eventually(t, "cluster to converge on one leader", func() bool {
		if len(c.leaders(ids...)) != 1 {
			return false
		}
		want := c.status(next)
		for _, id := range ids {
			s := c.status(id)
			if s.Term != want.Term || s.CommitIndex < idx || s.LastLogIndex != want.LastLogIndex {
				return false
			}
		}
		return true
	})
	if got := c.leaders(ids...); len(got) != 1 || got[0] != next {
		t.Errorf("leaders after heal = %v, want [%s]", got, next)
	}
	if s := c.status(old); s.State != raft.Follower {
		t.Errorf("old leader state = %v, want Follower", s.State)
	}
}
