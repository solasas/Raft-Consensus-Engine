package dashboard

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

// newTestCluster runs a real cluster at normal Raft speed (Slow: 1).
func newTestCluster(t *testing.T, nodes int) *Cluster {
	t.Helper()
	c, err := NewCluster(Options{Nodes: nodes, Slow: 1, SnapshotThreshold: 20, Seed: 7})
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	t.Cleanup(c.Stop)
	return c
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func nodeByID(v ClusterView, id string) (NodeView, bool) {
	for _, n := range v.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeView{}, false
}

func linkOf(v ClusterView, from, to string) LinkView {
	for _, l := range v.Links {
		if l.From == from && l.To == to {
			return l
		}
	}
	return LinkView{}
}

func leadersIn(v ClusterView) (ids []string) {
	for _, n := range v.Nodes {
		if n.State == "Leader" {
			ids = append(ids, n.ID)
		}
	}
	return
}

func TestNewClusterValidatesOptions(t *testing.T) {
	for name, o := range map[string]Options{
		"too few nodes":  {Nodes: 2},
		"too many nodes": {Nodes: 10},
		"negative slow":  {Nodes: 3, Slow: -1},
	} {
		if c, err := NewCluster(o); err == nil {
			c.cancel()
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestViewShowsLeaderAndReplicationProgress(t *testing.T) {
	c := newTestCluster(t, 3)

	var v ClusterView
	eventually(t, "a leader whose followers have all replicated its no-op", func() bool {
		v = c.View()
		if v.Leader == "" || len(v.Nodes) != 3 {
			return false
		}
		for _, n := range v.Nodes {
			if n.LastLogIndex < 1 {
				return false
			}
		}
		l, _ := nodeByID(v, v.Leader)
		return len(l.Replication) == 2 && l.Replication[0].Match >= 1 && l.Replication[1].Match >= 1
	})

	if got := len(v.Links); got != 6 {
		t.Errorf("%d directed links, want 3*2 = 6", got)
	}
	for _, l := range v.Links {
		if l.Blocked || l.DropRate != 0 || l.MaxLatencyMs != 0 {
			t.Errorf("fresh network has a fault: %+v", l)
		}
	}
	l, _ := nodeByID(v, v.Leader)
	if l.State != "Leader" || l.Term == 0 {
		t.Errorf("leader view = %+v", l)
	}
	if len(l.Tail) == 0 || !l.Tail[len(l.Tail)-1].Noop {
		t.Errorf("leader's log tail %+v should end with its election no-op", l.Tail)
	}
	for _, n := range v.Nodes {
		if n.ID != v.Leader && len(n.Replication) != 0 {
			t.Errorf("follower %s reports replication progress", n.ID)
		}
	}
}

func TestSeverAndRestoreLinks(t *testing.T) {
	c := newTestCluster(t, 3)

	if err := c.SeverLink("n1", "n2", true); err != nil {
		t.Fatal(err)
	}
	v := c.View()
	if !linkOf(v, "n1", "n2").Blocked || !linkOf(v, "n2", "n1").Blocked {
		t.Error("both directions of n1-n2 should be blocked")
	}
	if linkOf(v, "n1", "n3").Blocked || linkOf(v, "n3", "n1").Blocked {
		t.Error("severing n1-n2 must not touch n1-n3")
	}

	if err := c.SeverLink("n3", "n1", false); err != nil {
		t.Fatal(err)
	}
	v = c.View()
	if !linkOf(v, "n3", "n1").Blocked || linkOf(v, "n1", "n3").Blocked {
		t.Error("a one-way cut must block exactly one direction")
	}

	if err := c.RestoreLink("n1", "n2", true); err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreLink("n3", "n1", false); err != nil {
		t.Fatal(err)
	}
	for _, l := range c.View().Links {
		if l.Blocked {
			t.Errorf("link still blocked after restoring: %+v", l)
		}
	}

	if err := c.SeverLink("n1", "ghost", true); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("unknown node: err = %v, want ErrUnknownNode", err)
	}
	if err := c.SeverLink("n1", "n1", true); !errors.Is(err, ErrBadRequest) {
		t.Errorf("self link: err = %v, want ErrBadRequest", err)
	}
}

func TestActionsRejectInvalidInput(t *testing.T) {
	c := newTestCluster(t, 3)
	bad := map[string]error{
		"drop > 1":            c.SetDropRate(1.5),
		"negative drop":       c.SetDropRate(-0.1),
		"latency min > max":   c.SetLatency(50, 10),
		"latency too large":   c.SetLatency(0, 60_000),
		"one-group partition": c.Partition([][]string{{"n1", "n2", "n3"}}),
		"overlapping groups":  c.Partition([][]string{{"n1", "n2"}, {"n2", "n3"}}),
		"burst of zero":       c.Burst(0),
		"burst too big":       c.Burst(10_000),
		"negative load":       c.SetLoad(-1),
		"load too high":       c.SetLoad(1000),
		"empty key":           c.Put("", "", "v"),
		"huge key":            c.Put("", strings.Repeat("k", 65), "v"),
	}
	for name, err := range bad {
		if !errors.Is(err, ErrBadRequest) {
			t.Errorf("%s: err = %v, want ErrBadRequest", name, err)
		}
	}
	if err := c.Put("ghost", "k", "v"); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("put to unknown node: err = %v", err)
	}
	if err := c.Isolate("ghost"); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("isolate unknown node: err = %v", err)
	}
	if err := c.Partition([][]string{{"n1"}, {"ghost"}}); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("partition with unknown node: err = %v", err)
	}
	if got := c.View(); linkOf(got, "n1", "n2").Blocked || linkOf(got, "n1", "n2").DropRate != 0 {
		t.Error("a rejected action changed the network")
	}
}

func TestLossLatencyAndPartitionAreVisibleInTheView(t *testing.T) {
	c := newTestCluster(t, 3)
	if err := c.SetDropRate(0.2); err != nil {
		t.Fatal(err)
	}
	if err := c.SetLatency(10, 30); err != nil {
		t.Fatal(err)
	}
	l := linkOf(c.View(), "n2", "n3")
	if l.DropRate != 0.2 || l.MinLatencyMs != 10 || l.MaxLatencyMs != 30 {
		t.Errorf("link = %+v, want 20%% loss and 10-30 ms", l)
	}

	if err := c.Partition([][]string{{"n1", "n2"}, {"n3"}}); err != nil {
		t.Fatal(err)
	}
	v := c.View()
	if linkOf(v, "n1", "n2").Blocked || !linkOf(v, "n1", "n3").Blocked || !linkOf(v, "n3", "n2").Blocked {
		t.Error("partition {n1,n2}|{n3} not reflected in the links")
	}

	c.Heal()
	v = c.View()
	if linkOf(v, "n1", "n3").Blocked || linkOf(v, "n1", "n3").DropRate != 0.2 {
		t.Errorf("Heal must clear partitions but keep loss: %+v", linkOf(v, "n1", "n3"))
	}
	c.Reset()
	if l := linkOf(c.View(), "n1", "n3"); l.DropRate != 0 || l.MaxLatencyMs != 0 {
		t.Errorf("Reset must clear everything: %+v", l)
	}
}

// The scenario the dashboard exists to show: cut the leader off and watch a new
// one win while the old one still believes it is in charge.
func TestIsolatingTheLeaderCausesAnElectionAndSplitBrain(t *testing.T) {
	c := newTestCluster(t, 3)
	var old string
	var oldTerm uint64
	eventually(t, "an initial leader", func() bool {
		v := c.View()
		if v.Leader == "" {
			return false
		}
		n, _ := nodeByID(v, v.Leader)
		old, oldTerm = v.Leader, n.Term
		return len(leadersIn(v)) == 1
	})

	if err := c.Isolate(old); err != nil {
		t.Fatal(err)
	}
	var next string
	eventually(t, "the majority to elect a new leader in a higher term", func() bool {
		v := c.View()
		n, ok := nodeByID(v, v.Leader)
		if !ok || v.Leader == old || n.Term <= oldTerm {
			return false
		}
		next = v.Leader
		return true
	})

	v := c.View()
	if got := leadersIn(v); len(got) != 2 {
		t.Errorf("leaders = %v, want the stale and the new leader both", got)
	}
	if stale, _ := nodeByID(v, old); stale.State != "Leader" || stale.Term != oldTerm {
		t.Errorf("isolated leader = %+v, should still believe it leads in term %d", stale, oldTerm)
	}

	// The feed must tell the story, in order, with the right actors.
	var sawChaos, sawCandidate, sawNewLeader bool
	for _, e := range c.History() {
		switch {
		case e.Kind == "chaos" && strings.Contains(e.Detail, "isolated "+old):
			sawChaos = true
		case e.Kind == string(raft.EventCandidate) && e.Term > oldTerm && sawChaos:
			sawCandidate = true
		case e.Kind == string(raft.EventLeader) && e.Node == next && e.Term > oldTerm && sawCandidate:
			sawNewLeader = true
		}
	}
	if !sawChaos || !sawCandidate || !sawNewLeader {
		t.Errorf("history missing the story (chaos=%v candidate=%v leader=%v): %+v", sawChaos, sawCandidate, sawNewLeader, c.History())
	}

	if err := c.Reconnect(old); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a single leader and one term after healing", func() bool {
		v := c.View()
		if len(leadersIn(v)) != 1 {
			return false
		}
		for _, n := range v.Nodes {
			if n.Term != v.Nodes[0].Term {
				return false
			}
		}
		return true
	})
}

func TestWritesReplicateAndShowUpInEveryLogTail(t *testing.T) {
	c := newTestCluster(t, 3)
	eventually(t, "a leader", func() bool { return c.View().Leader != "" })

	if err := c.Put("", "color", "red"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the write on every node, committed", func() bool {
		v := c.View()
		if v.Writes != 1 {
			return false
		}
		for _, n := range v.Nodes {
			found := false
			for _, e := range n.Tail {
				found = found || (e.Cmd == "put color=red" && e.Committed)
			}
			if !found || n.Keys != 1 {
				return false
			}
		}
		return true
	})
	var ok bool
	for _, e := range c.History() {
		ok = ok || (e.Kind == "write-ok" && strings.Contains(e.Detail, "color=red"))
	}
	if !ok {
		t.Error("no write-ok event in the feed")
	}
}

// A write sent to an isolated leader is appended to its log but can never reach
// a quorum: it must show up as uncommitted, then be reported as failed.
func TestWriteToAnIsolatedLeaderStaysUncommitted(t *testing.T) {
	c := newTestCluster(t, 3)
	var leader string
	eventually(t, "a leader", func() bool { leader = c.View().Leader; return leader != "" })

	if err := c.Isolate(leader); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(leader, "doomed", "x"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the entry in the isolated leader's log, uncommitted", func() bool {
		n, _ := nodeByID(c.View(), leader)
		for _, e := range n.Tail {
			if e.Cmd == "put doomed=x" {
				return !e.Committed
			}
		}
		return false
	})

	eventually(t, "the write to be reported as failed", func() bool {
		for _, e := range c.History() {
			if e.Kind == "write-failed" && e.Node == leader && strings.Contains(e.Detail, "doomed") {
				return true
			}
		}
		return false
	})
	if v := c.View(); v.Failed == 0 || v.Writes != 0 {
		t.Errorf("writes %d failed %d, want 0 committed and at least 1 failed", v.Writes, v.Failed)
	}
}

func TestBurstAndBackgroundLoad(t *testing.T) {
	c := newTestCluster(t, 3)
	eventually(t, "a leader", func() bool { return c.View().Leader != "" })

	if err := c.Burst(25); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the burst to commit", func() bool { return c.View().Writes >= 25 })

	before := c.View().Writes
	if err := c.SetLoad(40); err != nil {
		t.Fatal(err)
	}
	if got := c.Load(); got != 40 {
		t.Errorf("Load() = %d, want 40", got)
	}
	eventually(t, "background writes to flow", func() bool { return c.View().Writes >= before+20 })

	if err := c.SetLoad(0); err != nil {
		t.Fatal(err)
	}
	if got := c.View().Load; got != 0 {
		t.Errorf("View().Load = %d after stopping", got)
	}
	time.Sleep(150 * time.Millisecond) // let anything in flight land
	settled := c.View().Writes
	time.Sleep(300 * time.Millisecond)
	if after := c.View().Writes; after != settled {
		t.Errorf("writes kept growing after the load stopped: %d -> %d", settled, after)
	}
}

func TestEventHandlerSeesEveryEventInOrder(t *testing.T) {
	c, err := NewCluster(Options{Nodes: 3, Slow: 1})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got []EventView
	done := make(chan struct{}, 1)
	c.SetEventHandler(func(e EventView) {
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
		if e.Kind == string(raft.EventLeader) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	c.Start()
	defer c.Stop()

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("no leader event")
	}
	c.Heal() // one more event, emitted from this goroutine

	hist := c.History()
	if len(hist) < 2 {
		t.Fatalf("history has %d events", len(hist))
	}
	for i := 1; i < len(hist); i++ {
		if hist[i].Seq != hist[i-1].Seq+1 {
			t.Errorf("sequence gap or reorder at %d: %d then %d", i, hist[i-1].Seq, hist[i].Seq)
		}
	}
	// Every event is recorded in the history and handed to the handler under one
	// lock, so a handler snapshot taken after the history one can only be longer.
	mu.Lock()
	seen := len(got)
	mu.Unlock()
	if seen < len(hist) {
		t.Errorf("handler saw %d events but %d are in the history", seen, len(hist))
	}
}

func TestDescribeCommand(t *testing.T) {
	enc := func(op, k, v string) []byte {
		return []byte(`{"op":"` + op + `","key":"` + k + `","value":"` + v + `"}`)
	}
	tests := []struct {
		name string
		e    raft.LogEntry
		want string
	}{
		{"put", raft.LogEntry{Command: enc("put", "a", "1")}, "put a=1"},
		{"delete", raft.LogEntry{Command: enc("delete", "a", "")}, "del a"},
		{"no-op", raft.LogEntry{Noop: true}, "no-op"},
		{"not json", raft.LogEntry{Command: []byte("hello")}, "hello"},
		{"long junk is shortened", raft.LogEntry{Command: []byte(strings.Repeat("x", 80))}, strings.Repeat("x", 24) + "…"},
		{"unknown op", raft.LogEntry{Command: enc("boom", "a", "1")}, string(enc("boom", "a", "1"))[:24] + "…"},
	}
	for _, tc := range tests {
		if got := describeCommand(tc.e); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}
