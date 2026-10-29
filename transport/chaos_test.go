package transport

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestChaosNodeDropRateCutsOneNodesTraffic(t *testing.T) {
	n := newTestNet(t, Options{Seed: 5}, "a", "b", "c")
	must(t, n.chaos.SetNodeDropRate("c", 1))

	if _, err := n.rpc("a", "c"); !errors.Is(err, ErrDropped) {
		t.Errorf("a->c err = %v, want ErrDropped", err)
	}
	if _, err := n.rpc("c", "b"); !errors.Is(err, ErrDropped) {
		t.Errorf("c->b err = %v, want ErrDropped", err)
	}
	if !n.reach("a", "b") || !n.reach("b", "a") {
		t.Error("traffic not involving c must be unaffected")
	}
	if n.chaos.SetNodeDropRate("c", 2) == nil {
		t.Error("rate 2 accepted")
	}
}

func TestChaosFaultsListsOnlyActiveFaultsInStableOrder(t *testing.T) {
	n := newTestNet(t, Options{}, "a", "b", "c")
	if got := n.chaos.Faults(); len(got) != 0 {
		t.Fatalf("healthy network reports faults: %v", got)
	}

	n.chaos.BlockOneWay("a", "c")
	must(t, n.chaos.SetLatency("b", "a", 100*time.Millisecond, 500*time.Millisecond))
	must(t, n.chaos.SetNodeDropRate("c", 0.3))

	want := []string{
		"a->c: blocked drop=30%",
		"b->a: latency=100ms-500ms",
		"b->c: drop=30%",
		"c->a: drop=30%",
		"c->b: drop=30%",
	}
	if got := n.chaos.Faults(); !reflect.DeepEqual(got, want) {
		t.Errorf("Faults() =\n  %q\nwant\n  %q", got, want)
	}

	n.chaos.Reset()
	if got := n.chaos.Faults(); len(got) != 0 {
		t.Errorf("faults remain after Reset: %v", got)
	}
}
