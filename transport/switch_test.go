package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

// testNet is a set of echo nodes on one switch. Every node grants any vote and
// counts how many requests its handler actually executed.
type testNet struct {
	sw    *Switch
	tr    map[string]*SimulatedTransport
	hits  map[string]*atomic.Int64
	chaos *Chaos
}

func newTestNet(t *testing.T, opts Options, ids ...string) *testNet {
	t.Helper()
	n := &testNet{
		sw:   NewSwitch(opts),
		tr:   map[string]*SimulatedTransport{},
		hits: map[string]*atomic.Int64{},
	}
	n.chaos = NewChaos(n.sw)
	for _, id := range ids {
		hits := new(atomic.Int64)
		n.hits[id] = hits
		n.sw.Register(id, HandlerFunc(func(m raft.RPC) (raft.RPC, error) {
			hits.Add(1)
			a := m.(raft.RequestVoteArgs)
			return raft.RequestVoteReply{Term: a.Term, VoteGranted: true}, nil
		}))
		n.tr[id] = n.sw.NewTransport(id, nil)
	}
	return n
}

// rpc performs one RequestVote round trip from -> to.
func (n *testNet) rpc(from, to string) (raft.RPC, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return n.tr[from].SendRPC(ctx, to, raft.RequestVoteArgs{Term: 1, CandidateID: from})
}

func (n *testNet) reach(from, to string) bool {
	_, err := n.rpc(from, to)
	return err == nil
}

func TestRoundTripIsDeliveredAndObserved(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b")
	var mu sync.Mutex
	var events []Event
	n.sw.SetTap(func(e Event) { mu.Lock(); events = append(events, e); mu.Unlock() })

	resp, err := n.rpc("a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if r, ok := resp.(raft.RequestVoteReply); !ok || !r.VoteGranted || r.Term != 1 {
		t.Fatalf("reply = %#v", resp)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []Event{
		{From: "a", To: "b", Kind: "RequestVote", Dir: DirRequest, Outcome: OutcomeDelivered},
		{From: "b", To: "a", Kind: "RequestVoteReply", Dir: DirResponse, Outcome: OutcomeDelivered},
	}
	if len(events) != 2 || events[0] != want[0] || events[1] != want[1] {
		t.Errorf("events = %+v, want %+v", events, want)
	}
}

func TestBlockedLinkFailsAndNeverReachesHandler(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b")
	n.sw.SetLink("a", "b", LinkPolicy{Blocked: true})

	_, err := n.rpc("a", "b")
	if !errors.Is(err, ErrBlocked) || !errors.Is(err, ErrUnreachable) {
		t.Fatalf("err = %v, want ErrBlocked (wrapping ErrUnreachable)", err)
	}
	if got := n.hits["b"].Load(); got != 0 {
		t.Errorf("blocked request still executed on b %d times", got)
	}
}

// B -> A is cut but A -> B is open. A's request reaches B and runs there, yet A
// never hears the reply. B's own requests to A are lost outright.
func TestAsymmetricLinkExecutesRequestButLosesReply(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b")
	n.chaos.BlockOneWay("b", "a")

	if _, err := n.rpc("a", "b"); !errors.Is(err, ErrBlocked) {
		t.Fatalf("a->b err = %v, want ErrBlocked on the reply leg", err)
	}
	if got := n.hits["b"].Load(); got != 1 {
		t.Errorf("b executed %d requests, want 1 (the request leg was open)", got)
	}

	if _, err := n.rpc("b", "a"); !errors.Is(err, ErrBlocked) {
		t.Fatalf("b->a err = %v, want ErrBlocked", err)
	}
	if got := n.hits["a"].Load(); got != 0 {
		t.Errorf("a executed %d requests from b, want 0", got)
	}

	n.chaos.UnblockOneWay("b", "a")
	if !n.reach("a", "b") || !n.reach("b", "a") {
		t.Error("links did not recover after UnblockOneWay")
	}
}

func TestDropRate(t *testing.T) {
	t.Run("0 never drops, 1 always drops", func(t *testing.T) {
		n := newTestNet(t, Options{Seed: 1}, "a", "b")
		for i := 0; i < 50; i++ {
			if !n.reach("a", "b") {
				t.Fatal("dropped with rate 0")
			}
		}
		if err := n.chaos.SetDropRate("a", "b", 1); err != nil {
			t.Fatal(err)
		}
		before := n.hits["b"].Load()
		for i := 0; i < 50; i++ {
			if _, err := n.rpc("a", "b"); !errors.Is(err, ErrDropped) {
				t.Fatalf("err = %v, want ErrDropped", err)
			}
		}
		if n.hits["b"].Load() != before {
			t.Error("dropped requests reached the handler")
		}
	})

	t.Run("fractional rate drops about that share", func(t *testing.T) {
		n := newTestNet(t, Options{Seed: 7}, "a", "b")
		if err := n.chaos.SetDropRate("a", "b", 0.5); err != nil { // request leg only
			t.Fatal(err)
		}
		const N = 2000
		failures := 0
		for i := 0; i < N; i++ {
			if !n.reach("a", "b") {
				failures++
			}
		}
		delivered := int(n.hits["b"].Load())
		if delivered+failures != N {
			t.Errorf("delivered %d + failed %d != %d", delivered, failures, N)
		}
		if frac := float64(delivered) / N; frac < 0.4 || frac > 0.6 {
			t.Errorf("delivered fraction %.3f, want ~0.5", frac)
		}
	})
}

func TestLatencyIsInjectedOnBothLegs(t *testing.T) {
	n := newTestNet(t, Options{Seed: 3}, "a", "b")
	if err := n.chaos.SetNodeLatency("b", 40*time.Millisecond, 60*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var delays []time.Duration
	n.sw.SetTap(func(e Event) { mu.Lock(); delays = append(delays, e.Delay); mu.Unlock() })

	start := time.Now()
	if _, err := n.rpc("a", "b"); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	if elapsed < 80*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Errorf("round trip took %v, want >= 80ms (2 legs x >= 40ms)", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(delays) != 2 {
		t.Fatalf("saw %d legs, want 2", len(delays))
	}
	for _, d := range delays {
		if d < 40*time.Millisecond || d > 60*time.Millisecond {
			t.Errorf("leg delay %v outside [40ms, 60ms]", d)
		}
	}
}

func TestLatencyRespectsContext(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b")
	if err := n.chaos.SetLatency("a", "b", 2*time.Second, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := n.tr["a"].SendRPC(ctx, "b", raft.RequestVoteArgs{Term: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("SendRPC held the caller for %v despite a 50ms deadline", time.Since(start))
	}
	if n.hits["b"].Load() != 0 {
		t.Error("request was delivered even though it never left the delay")
	}
}

func TestSilentDropsLookLikeTimeouts(t *testing.T) {
	n := newTestNet(t, Options{SilentDrops: true}, "a", "b")
	n.chaos.Isolate("b")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := n.tr["a"].SendRPC(ctx, "b", raft.RequestVoteArgs{Term: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded (no fast failure in silent mode)", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Errorf("returned after %v, should have waited for the deadline", time.Since(start))
	}
}

func TestUnknownPeer(t *testing.T) {
	n := newTestNet(t, Options{}, "a")
	if _, err := n.rpc("a", "ghost"); !errors.Is(err, ErrUnknownPeer) {
		t.Errorf("err = %v, want ErrUnknownPeer", err)
	}
}

// innerStub stands in for a real network transport.
type innerStub struct{ calls atomic.Int64 }

func (s *innerStub) SendRPC(_ context.Context, _ string, m raft.RPC) (raft.RPC, error) {
	s.calls.Add(1)
	return raft.RequestVoteReply{Term: m.(raft.RequestVoteArgs).Term, VoteGranted: true}, nil
}

func TestSimulatedTransportWrapsAnInnerTransport(t *testing.T) {
	sw := NewSwitch(Options{})
	inner := &innerStub{}
	tr := sw.NewTransport("a", inner)
	send := func() error {
		_, err := tr.SendRPC(context.Background(), "x", raft.RequestVoteArgs{Term: 2})
		return err
	}

	if err := send(); err != nil || inner.calls.Load() != 1 {
		t.Fatalf("err %v, inner calls %d, want success via inner", err, inner.calls.Load())
	}

	sw.SetLink("a", "x", LinkPolicy{Blocked: true}) // request leg
	if err := send(); !errors.Is(err, ErrBlocked) || inner.calls.Load() != 1 {
		t.Fatalf("err %v, inner calls %d, want blocked before the inner transport", err, inner.calls.Load())
	}

	sw.SetLink("a", "x", LinkPolicy{})
	sw.SetLink("x", "a", LinkPolicy{Blocked: true}) // reply leg
	if err := send(); !errors.Is(err, ErrBlocked) || inner.calls.Load() != 2 {
		t.Fatalf("err %v, inner calls %d, want inner called but reply blocked", err, inner.calls.Load())
	}
}

func TestAsRaftTransport(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b")
	typed := AsRaftTransport(n.tr["a"])

	reply, err := typed.RequestVote(context.Background(), raft.Peer{ID: "b", Addr: "ignored:1"}, raft.RequestVoteArgs{Term: 4})
	if err != nil || !reply.VoteGranted || reply.Term != 4 {
		t.Fatalf("reply %+v err %v", reply, err)
	}

	// A peer answering with the wrong message type is an error, not a zero value.
	n.sw.Register("b", HandlerFunc(func(raft.RPC) (raft.RPC, error) { return raft.AppendEntriesReply{}, nil }))
	if _, err := typed.RequestVote(context.Background(), raft.Peer{ID: "b"}, raft.RequestVoteArgs{}); !errors.Is(err, ErrBadResponse) {
		t.Errorf("err = %v, want ErrBadResponse", err)
	}
}

// ---------------------------------------------------------------------------
// Chaos engine
// ---------------------------------------------------------------------------

func TestChaosPartitionAndHeal(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b", "c", "d")
	n.chaos.Partition([]string{"a", "b"}, []string{"c"}) // d is unlisted -> isolated alone

	for _, tc := range []struct {
		from, to string
		want     bool
	}{
		{"a", "b", true}, {"b", "a", true},
		{"a", "c", false}, {"c", "a", false}, {"b", "c", false},
		{"c", "d", false}, {"d", "a", false}, {"a", "d", false},
	} {
		if got := n.reach(tc.from, tc.to); got != tc.want {
			t.Errorf("reach(%s -> %s) = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}

	n.chaos.Heal()
	for _, from := range []string{"a", "b", "c", "d"} {
		for _, to := range []string{"a", "b", "c", "d"} {
			if from != to && !n.reach(from, to) {
				t.Errorf("reach(%s -> %s) still failing after Heal", from, to)
			}
		}
	}
}

func TestChaosIsolateAndReconnect(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b", "c")
	n.chaos.Isolate("c")
	if !n.reach("a", "b") || !n.reach("b", "a") {
		t.Error("a and b should still talk")
	}
	if n.reach("a", "c") || n.reach("c", "a") || n.reach("c", "b") {
		t.Error("c should be unreachable and unable to reach out")
	}
	n.chaos.Reconnect("c")
	if !n.reach("a", "c") || !n.reach("c", "a") {
		t.Error("c did not come back")
	}
}

func TestChaosHealKeepsLatencyAndLoss(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b")
	if err := n.chaos.SetLatency("a", "b", 5*time.Millisecond, 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := n.chaos.SetDropRate("a", "b", 0.25); err != nil {
		t.Fatal(err)
	}
	n.chaos.BlockOneWay("a", "b")
	n.chaos.Heal()

	p := n.sw.Link("a", "b")
	if p.Blocked || p.MinLatency != 5*time.Millisecond || p.DropRate != 0.25 {
		t.Errorf("after Heal: %+v, want unblocked with latency and loss intact", p)
	}
	n.chaos.Reset()
	if p := n.sw.Link("a", "b"); p != (LinkPolicy{}) {
		t.Errorf("after Reset: %+v, want zero policy", p)
	}
}

func TestChaosNodeAndGlobalSettings(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b", "c")
	if err := n.chaos.SetNodeLatency("a", 5*time.Millisecond, 6*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	for _, l := range []Link{{"a", "b"}, {"b", "a"}, {"a", "c"}, {"c", "a"}} {
		if p := n.sw.Link(l.From, l.To); p.MinLatency != 5*time.Millisecond || p.MaxLatency != 6*time.Millisecond {
			t.Errorf("link %v = %+v, want 5-6ms", l, p)
		}
	}
	if p := n.sw.Link("b", "c"); p.MinLatency != 0 {
		t.Errorf("b->c = %+v, must be unaffected", p)
	}

	if err := n.chaos.SetGlobalDropRate(1); err != nil {
		t.Fatal(err)
	}
	if n.sw.Link("a", "b").DropRate != 1 || n.sw.Link("b", "c").DropRate != 1 || n.sw.Link("x", "y").DropRate != 1 {
		t.Error("global drop rate must reach explicit links and the default")
	}
}

func TestChaosRejectsInvalidArguments(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b")
	if n.chaos.SetDropRate("a", "b", 1.5) == nil || n.chaos.SetDropRate("a", "b", -0.1) == nil {
		t.Error("out-of-range drop rate accepted")
	}
	if n.chaos.SetLatency("a", "b", 10*time.Millisecond, 5*time.Millisecond) == nil {
		t.Error("min > max accepted")
	}
	if n.chaos.SetNodeLatency("a", -time.Millisecond, time.Millisecond) == nil {
		t.Error("negative latency accepted")
	}
	if p := n.sw.Link("a", "b"); p != (LinkPolicy{}) {
		t.Errorf("invalid calls modified the matrix: %+v", p)
	}
}
