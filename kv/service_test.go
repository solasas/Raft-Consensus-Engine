package kv

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

// memNet routes raft RPCs between in-process services and can cut nodes off.
type memNet struct {
	mu   sync.Mutex
	svcs map[string]*Service
	down map[string]bool
}

type memTransport struct {
	net  *memNet
	self string
}

var errUnreachable = errors.New("unreachable")

func (m *memNet) route(from, to string) (*raft.Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down[from] || m.down[to] {
		return nil, errUnreachable
	}
	return m.svcs[to].Node(), nil
}

func (t memTransport) RequestVote(_ context.Context, to raft.Peer, a raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	n, err := t.net.route(t.self, to.ID)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	return n.RequestVote(a)
}

func (t memTransport) InstallSnapshot(_ context.Context, to raft.Peer, a raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	n, err := t.net.route(t.self, to.ID)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	return n.InstallSnapshot(a)
}

func (t memTransport) AppendEntries(_ context.Context, to raft.Peer, a raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	n, err := t.net.route(t.self, to.ID)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	return n.AppendEntries(a)
}

type cluster struct {
	net  *memNet
	svcs map[string]*Service
}

func newCluster(t *testing.T, ids ...string) *cluster { return newClusterWith(t, nil, ids...) }

// newClusterWith lets a test adjust each node's raft.Config (e.g. the snapshot
// threshold) before the node is built.
func newClusterWith(t *testing.T, tweak func(*raft.Config), ids ...string) *cluster {
	t.Helper()
	c := &cluster{
		net:  &memNet{svcs: map[string]*Service{}, down: map[string]bool{}},
		svcs: map[string]*Service{},
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, id := range ids {
		var peers []raft.Peer
		for _, o := range ids {
			if o != id {
				peers = append(peers, raft.Peer{ID: o, Addr: o + ":7000"})
			}
		}
		cfg := raft.Config{
			ID: id, Peers: peers, Logger: quiet,
			Transport: memTransport{net: c.net, self: id},
		}
		if tweak != nil {
			tweak(&cfg)
		}
		svc, err := NewService(cfg)
		if err != nil {
			t.Fatal(err)
		}
		c.svcs[id] = svc
		c.net.svcs[id] = svc
	}
	for _, s := range c.svcs {
		s.Start()
	}
	t.Cleanup(func() {
		for _, s := range c.svcs {
			s.Stop()
		}
	})
	return c
}

func (c *cluster) setDown(id string, down bool) {
	c.net.mu.Lock()
	c.net.down[id] = down
	c.net.mu.Unlock()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitLeader returns the service that is the single leader among non-skipped nodes.
func (c *cluster) waitLeader(t *testing.T, skip string) (string, *Service) {
	t.Helper()
	var id string
	var svc *Service
	eventually(t, "a single leader", func() bool {
		var found []string
		for sid, s := range c.svcs {
			if sid == skip {
				continue
			}
			if st, err := s.Node().Status(); err == nil && st.State == raft.Leader {
				found = append(found, sid)
			}
		}
		if len(found) == 1 {
			id, svc = found[0], c.svcs[found[0]]
			return true
		}
		return false
	})
	return id, svc
}

func (c *cluster) everywhere(t *testing.T, what string, key, want string, present bool) {
	t.Helper()
	eventually(t, what, func() bool {
		for _, s := range c.svcs {
			v, ok := s.Get(key)
			if ok != present || (present && v != want) {
				return false
			}
		}
		return true
	})
}

func ctx3s(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestPutAndDeleteReplicateToEveryStore(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	_, leader := c.waitLeader(t, "")
	ctx := ctx3s(t)

	if err := leader.Put(ctx, "color", "red"); err != nil {
		t.Fatal(err)
	}
	// Put returns only after the entry is committed AND applied on the leader.
	if v, ok := leader.Get("color"); !ok || v != "red" {
		t.Fatalf("leader read-after-write: %q,%v", v, ok)
	}
	c.everywhere(t, "color=red on all nodes", "color", "red", true)

	if err := leader.Put(ctx, "color", "blue"); err != nil {
		t.Fatal(err)
	}
	c.everywhere(t, "color=blue on all nodes", "color", "blue", true)

	if err := leader.Delete(ctx, "color"); err != nil {
		t.Fatal(err)
	}
	c.everywhere(t, "color deleted on all nodes", "color", "", false)

	// Every replica applied the same log, so the applied indexes agree.
	eventually(t, "equal applied indexes", func() bool {
		want := leader.Store().AppliedIndex()
		for _, s := range c.svcs {
			if s.Store().AppliedIndex() != want {
				return false
			}
		}
		return true
	})
}

func TestPutOnFollowerIsRejected(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	lid, _ := c.waitLeader(t, "")
	for id, s := range c.svcs {
		if id == lid {
			continue
		}
		err := s.Put(ctx3s(t), "k", "v")
		var nl *raft.NotLeaderError
		if !errors.As(err, &nl) {
			t.Errorf("follower %s: err = %v, want NotLeaderError", id, err)
		}
	}
}

func TestDataSurvivesLeaderFailover(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	oldID, old := c.waitLeader(t, "")
	if err := old.Put(ctx3s(t), "k1", "v1"); err != nil {
		t.Fatal(err)
	}
	c.everywhere(t, "k1 replicated", "k1", "v1", true)

	c.setDown(oldID, true)
	_, next := c.waitLeader(t, oldID)
	if v, ok := next.Get("k1"); !ok || v != "v1" {
		t.Fatalf("new leader lost committed data: %q,%v", v, ok)
	}
	if err := next.Put(ctx3s(t), "k2", "v2"); err != nil {
		t.Fatal(err)
	}

	c.setDown(oldID, false) // the old leader must catch up through its applier
	c.everywhere(t, "k1 and k2 on every node", "k2", "v2", true)
	c.everywhere(t, "k1 still present", "k1", "v1", true)
}

// A deposed leader accepts a write it can never commit. When the partition
// heals, the new leader's log overwrites that entry; the client must be told
// the write was NOT applied, and no node may expose it.
func TestOverwrittenProposalReportsNotCommitted(t *testing.T) {
	c := newCluster(t, "a", "b", "c")
	oldID, old := c.waitLeader(t, "")
	// Commit one entry first so the followers have definitely seen this leader.
	if err := old.Put(ctx3s(t), "seed", "1"); err != nil {
		t.Fatal(err)
	}
	c.everywhere(t, "seed replicated", "seed", "1", true)

	c.setDown(oldID, true)
	before, _ := old.Node().Status()

	lost := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		lost <- old.Put(ctx, "lost", "x") // appended locally, can never reach a quorum
	}()
	eventually(t, "isolated leader to append the proposal", func() bool {
		st, _ := old.Node().Status()
		return st.LastLogIndex > before.LastLogIndex
	})

	_, next := c.waitLeader(t, oldID)
	if err := next.Put(ctx3s(t), "won", "1"); err != nil {
		t.Fatal(err)
	}

	c.setDown(oldID, false)
	select {
	case err := <-lost:
		if !errors.Is(err, ErrNotCommitted) {
			t.Fatalf("Put on the deposed leader = %v, want ErrNotCommitted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deposed leader's Put never returned after the partition healed")
	}

	c.everywhere(t, "won=1 on every node", "won", "1", true)
	c.everywhere(t, "lost never applied anywhere", "lost", "", false)
}
