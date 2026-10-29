package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// memNet is an in-memory network that can partition nodes.
type memNet struct {
	mu    sync.Mutex
	nodes map[string]*Node
	down  map[string]bool
}

type memTransport struct {
	net  *memNet
	self string
}

var errUnreachable = errors.New("unreachable")

func (m *memNet) route(from, to string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down[from] || m.down[to] {
		return nil, errUnreachable
	}
	return m.nodes[to], nil
}

func (t memTransport) RequestVote(_ context.Context, to Peer, a RequestVoteArgs) (RequestVoteReply, error) {
	n, err := t.net.route(t.self, to.ID)
	if err != nil {
		return RequestVoteReply{}, err
	}
	return n.RequestVote(a)
}

func (t memTransport) AppendEntries(_ context.Context, to Peer, a AppendEntriesArgs) (AppendEntriesReply, error) {
	n, err := t.net.route(t.self, to.ID)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return n.AppendEntries(a)
}

type cluster struct {
	net     *memNet
	nodes   map[string]*Node
	mu      sync.Mutex
	applied map[string][]string
}

func newCluster(t *testing.T, ids ...string) *cluster {
	t.Helper()
	c := &cluster{
		net:     &memNet{nodes: map[string]*Node{}, down: map[string]bool{}},
		nodes:   map[string]*Node{},
		applied: map[string][]string{},
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, id := range ids {
		var peers []Peer
		for _, o := range ids {
			if o != id {
				peers = append(peers, Peer{ID: o, Addr: o + ":7000"})
			}
		}
		applyCh := make(chan ApplyMsg, 128)
		n, err := NewNode(Config{
			ID: id, Peers: peers, ApplyCh: applyCh, Logger: quiet,
			Transport: memTransport{net: c.net, self: id},
		})
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = n
		c.net.nodes[id] = n
		go func(id string) {
			for m := range applyCh {
				c.mu.Lock()
				c.applied[id] = append(c.applied[id], string(m.Command))
				c.mu.Unlock()
			}
		}(id)
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

func (c *cluster) setDown(id string, down bool) {
	c.net.mu.Lock()
	c.net.down[id] = down
	c.net.mu.Unlock()
}

// waitLeader waits until exactly one reachable node is Leader and returns it.
func (c *cluster) waitLeader(t *testing.T, skip string) *Node {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for id, n := range c.nodes {
			if id == skip {
				continue
			}
			if s, err := n.Status(); err == nil && s.State == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no single leader elected")
	return nil
}

func (c *cluster) waitApplied(t *testing.T, id string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := append([]string(nil), c.applied[id]...)
		c.mu.Unlock()
		if fmt.Sprint(got) == fmt.Sprint(want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.Fatalf("node %s applied %v, want %v", id, c.applied[id], want)
}

func TestElectsSingleLeader(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	l := c.waitLeader(t, "")
	ls, _ := l.Status()
	for id, n := range c.nodes {
		s, _ := n.Status()
		if s.State != Leader && s.Term > ls.Term {
			t.Errorf("follower %s has term %d > leader term %d", id, s.Term, ls.Term)
		}
	}
}

func TestReplicatesAndApplies(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	l := c.waitLeader(t, "")
	for _, cmd := range []string{"x=1", "y=2", "z=3"} {
		if _, _, err := l.Propose([]byte(cmd)); err != nil {
			t.Fatal(err)
		}
	}
	for id := range c.nodes {
		c.waitApplied(t, id, "x=1", "y=2", "z=3")
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	l := c.waitLeader(t, "")
	for id, n := range c.nodes {
		if n == l {
			continue
		}
		_, _, err := n.Propose([]byte("nope"))
		var nl *NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("node %s: want NotLeaderError, got %v", id, err)
		}
	}
}

func TestLeaderFailoverAndCatchUp(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	old := c.waitLeader(t, "")
	oldID := func() string { s, _ := old.Status(); return s.ID }()
	oldTerm := func() uint64 { s, _ := old.Status(); return s.Term }()

	if _, _, err := old.Propose([]byte("before")); err != nil {
		t.Fatal(err)
	}
	for id := range c.nodes {
		c.waitApplied(t, id, "before")
	}

	c.setDown(oldID, true) // partition the leader away
	next := c.waitLeader(t, oldID)
	if s, _ := next.Status(); s.Term <= oldTerm {
		t.Fatalf("new leader term %d not greater than %d", s.Term, oldTerm)
	}
	if _, _, err := next.Propose([]byte("after")); err != nil {
		t.Fatal(err)
	}

	c.setDown(oldID, false) // heal: old leader must step down and catch up
	for id := range c.nodes {
		c.waitApplied(t, id, "before", "after")
	}
	if s, _ := old.Status(); s.State == Leader {
		t.Fatalf("old leader %s did not step down", oldID)
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, "solo")
	l := c.waitLeader(t, "")
	if _, _, err := l.Propose([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(t, "solo", "hello")
}
