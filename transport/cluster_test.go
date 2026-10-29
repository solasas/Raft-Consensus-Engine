package transport

import (
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

// raftCluster is the chaos test harness: real raft nodes wired together only
// through a simulated switch, plus instruments that watch them.
//
//   - applied: what each node's state machine received, in order.
//   - leading: every (term, node) pair ever observed in the Leader state. A
//     background monitor samples all nodes every few milliseconds, and cleanup
//     fails the test if any term had two different leaders (Raft's Election
//     Safety property), no matter which scenario ran.
type raftCluster struct {
	t     *testing.T
	sw    *Switch
	chaos *Chaos
	ids   []string
	nodes map[string]*raft.Node

	mu      sync.Mutex
	applied map[string][]string
	leading map[uint64]map[string]bool
}

func newRaftCluster(t *testing.T, size int) *raftCluster {
	t.Helper()
	c := &raftCluster{
		t:       t,
		sw:      NewSwitch(Options{Seed: 42}),
		nodes:   map[string]*raft.Node{},
		applied: map[string][]string{},
		leading: map[uint64]map[string]bool{},
	}
	c.chaos = NewChaos(c.sw)
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for _, id := range c.ids {
		var peers []raft.Peer
		for _, o := range c.ids {
			if o != id {
				peers = append(peers, raft.Peer{ID: o, Addr: "sim://" + o})
			}
		}
		applyCh := make(chan raft.ApplyMsg, 128)
		n, err := raft.NewNode(raft.Config{
			ID: id, Peers: peers, Logger: quiet, ApplyCh: applyCh,
			Transport: AsRaftTransport(c.sw.NewTransport(id, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
		c.sw.Register(id, ServeNode(n))

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case m := <-applyCh:
					c.mu.Lock()
					c.applied[id] = append(c.applied[id], string(m.Command))
					c.mu.Unlock()
				case <-stop:
					return
				}
			}
		}()
	}
	for _, n := range c.nodes {
		n.Start()
	}

	wg.Add(1)
	go func() { // election-safety monitor
		defer wg.Done()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				for _, id := range c.ids {
					st, err := c.nodes[id].Status()
					if err != nil || st.State != raft.Leader {
						continue
					}
					c.mu.Lock()
					if c.leading[st.Term] == nil {
						c.leading[st.Term] = map[string]bool{}
					}
					c.leading[st.Term][id] = true
					c.mu.Unlock()
				}
			}
		}
	}()

	t.Cleanup(func() {
		close(stop)
		wg.Wait()
		for _, n := range c.nodes {
			n.Stop()
		}
		c.sw.Close()
		c.mu.Lock()
		defer c.mu.Unlock()
		for term, who := range c.leading {
			if len(who) > 1 {
				t.Errorf("ELECTION SAFETY VIOLATED: term %d had %d leaders: %v", term, len(who), who)
			}
		}
	})
	return c
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

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (c *raftCluster) status(id string) raft.Status {
	s, _ := c.nodes[id].Status()
	return s
}

// others returns every node ID except `id`.
func (c *raftCluster) others(id string) []string {
	var out []string
	for _, o := range c.ids {
		if o != id {
			out = append(out, o)
		}
	}
	return out
}

// leaders returns those of `among` (default: all) that currently believe they lead.
func (c *raftCluster) leaders(among ...string) []string {
	if len(among) == 0 {
		among = c.ids
	}
	var out []string
	for _, id := range among {
		if c.status(id).State == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// waitLeader blocks until exactly one of `among` (default: all) claims leadership.
func (c *raftCluster) waitLeader(among ...string) string {
	c.t.Helper()
	var id string
	eventually(c.t, fmt.Sprintf("a single leader among %v", among), func() bool {
		if l := c.leaders(among...); len(l) == 1 {
			id = l[0]
			return true
		}
		return false
	})
	return id
}

// waitConverged blocks until one leader exists and every node agrees on term,
// log length and commit index. It returns the leader.
func (c *raftCluster) waitConverged() string {
	c.t.Helper()
	var lead string
	eventually(c.t, "the cluster to converge on one leader, term and log", func() bool {
		l := c.leaders()
		if len(l) != 1 {
			return false
		}
		want := c.status(l[0])
		for _, id := range c.ids {
			s := c.status(id)
			if s.Term != want.Term || s.LastLogIndex != want.LastLogIndex || s.CommitIndex != want.CommitIndex {
				return false
			}
		}
		lead = l[0]
		return true
	})
	return lead
}

func (c *raftCluster) propose(leader, cmd string) uint64 {
	c.t.Helper()
	idx, _, err := c.nodes[leader].Propose([]byte(cmd))
	if err != nil {
		c.t.Fatalf("propose %q on %s: %v", cmd, leader, err)
	}
	return idx
}

// waitCommit blocks until every one of `among` (default: all) has committed idx.
func (c *raftCluster) waitCommit(idx uint64, among ...string) {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	eventually(c.t, fmt.Sprintf("commitIndex >= %d on %v", idx, among), func() bool {
		for _, id := range among {
			if c.status(id).CommitIndex < idx {
				return false
			}
		}
		return true
	})
}

func (c *raftCluster) appliedOn(id string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.applied[id]...)
}

// waitApplied blocks until every one of `among` (default: all) has applied
// exactly `want`, in that order.
func (c *raftCluster) waitApplied(want []string, among ...string) {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	eventually(c.t, fmt.Sprintf("applied log %v on %v", want, among), func() bool {
		for _, id := range among {
			if got := c.appliedOn(id); !reflect.DeepEqual(got, want) {
				return false
			}
		}
		return true
	})
}
