package kv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

// ---------------------------------------------------------------------------
// Store: Snapshot / Restore
// ---------------------------------------------------------------------------

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	a := NewStore()
	a.Apply(put(1, 1, "x", "1"))
	a.Apply(put(2, 1, "y", "2"))
	a.Apply(del(3, 1, "x"))
	a.Apply(put(5, 2, "z", "3")) // index 4 was a no-op

	data, idx, err := a.Snapshot()
	if err != nil || idx != 5 {
		t.Fatalf("Snapshot = (%d bytes, index %d, %v), want index 5", len(data), idx, err)
	}

	b := NewStore()
	if err := b.Restore(data, idx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Dump(), b.Dump()) || b.AppliedIndex() != 5 {
		t.Errorf("restored %v @%d, want %v @5", b.Dump(), b.AppliedIndex(), a.Dump())
	}

	// The restored replica carries on exactly where the original was.
	a.Apply(put(6, 2, "w", "4"))
	b.Apply(put(6, 2, "w", "4"))
	if !reflect.DeepEqual(a.Dump(), b.Dump()) {
		t.Errorf("replicas diverged after restore: %v vs %v", a.Dump(), b.Dump())
	}
}

func TestSnapshotOfEmptyStoreRestoresToEmptyMap(t *testing.T) {
	data, idx, err := NewStore().Snapshot()
	if err != nil || idx != 0 {
		t.Fatalf("Snapshot = (%v, index %d, %v)", string(data), idx, err)
	}
	b := NewStore()
	b.Apply(put(1, 1, "stale", "x"))
	if err := b.Restore(data, 3); err != nil {
		t.Fatal(err)
	}
	if len(b.Dump()) != 0 || b.AppliedIndex() != 3 {
		t.Errorf("state %v @%d, want empty @3: restore must REPLACE, not merge", b.Dump(), b.AppliedIndex())
	}
	b.Apply(put(4, 1, "k", "v")) // map must be writable
	if v, _ := b.Get("k"); v != "v" {
		t.Error("store unusable after restoring an empty snapshot")
	}
}

func TestRestoreIgnoresSnapshotsNotNewerThanState(t *testing.T) {
	src := NewStore()
	src.Apply(put(1, 1, "old", "x"))
	data, idx, _ := src.Snapshot()

	s := NewStore()
	s.Apply(put(1, 1, "a", "1"))
	s.Apply(put(2, 1, "b", "2"))
	if err := s.Restore(data, idx); err != nil { // idx 1 < applied 2
		t.Fatal(err)
	}
	if want := map[string]string{"a": "1", "b": "2"}; !reflect.DeepEqual(s.Dump(), want) || s.AppliedIndex() != 2 {
		t.Errorf("state %v @%d: an older snapshot must not roll the store back", s.Dump(), s.AppliedIndex())
	}
}

func TestRestoreRejectsCorruptSnapshotsWithoutTouchingState(t *testing.T) {
	s := NewStore()
	s.Apply(put(1, 1, "k", "v"))
	for name, data := range map[string]string{
		"not json":      "nope",
		"future format": `{"v":2,"data":{"x":"y"}}`,
		"unversioned":   `{"data":{"x":"y"}}`,
	} {
		if err := s.Restore([]byte(data), 9); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if v, _ := s.Get("k"); v != "v" || s.AppliedIndex() != 1 {
		t.Errorf("a rejected snapshot changed the state: k=%q @%d", v, s.AppliedIndex())
	}
}

func TestApplyAfterRestoreSkipsCoveredIndexes(t *testing.T) {
	src := NewStore()
	src.Apply(put(1, 1, "k", "from-snapshot"))
	data, _, _ := src.Snapshot()

	s := NewStore()
	if err := s.Restore(data, 10); err != nil {
		t.Fatal(err)
	}
	// Commands queued before the snapshot arrived may still trail in.
	s.Apply(put(8, 1, "k", "stale"))
	s.Apply(put(10, 1, "k", "stale-too"))
	s.Apply(put(11, 2, "k2", "fresh"))

	if v, _ := s.Get("k"); v != "from-snapshot" {
		t.Errorf("k = %q: a command covered by the snapshot was re-applied", v)
	}
	if v, _ := s.Get("k2"); v != "fresh" || s.AppliedIndex() != 11 {
		t.Errorf("k2 = %q @%d: entries after the snapshot must apply", v, s.AppliedIndex())
	}
}

// A writer whose entry was folded into a snapshot can't be told whether the
// entry was theirs, so it must hear "unknown", not a false success or failure.
func TestRestoreTellsCoveredWaitersTheOutcomeIsUnknown(t *testing.T) {
	src := NewStore()
	src.Apply(put(1, 1, "k", "v"))
	data, _, _ := src.Snapshot()

	s := NewStore()
	ctx := context.Background()
	covered := awaitAsync(s, ctx, 4, 2)
	beyond := awaitAsync(s, ctx, 12, 2)
	expectBlocked(t, covered)

	if err := s.Restore(data, 10); err != nil {
		t.Fatal(err)
	}
	expect(t, covered, ErrOutcomeUnknown)
	expectBlocked(t, beyond) // index 12 is still in the future

	s.Apply(put(12, 2, "k2", "v2"))
	expect(t, beyond, nil)

	// And a later Await for an index the snapshot swallowed is also "unknown".
	if err := s.Await(ctx, 7, 2); err == nil || err != ErrOutcomeUnknown {
		t.Errorf("Await(7) = %v, want ErrOutcomeUnknown", err)
	}
}

func TestRunRestoresFromSnapshotMessages(t *testing.T) {
	src := NewStore()
	src.Apply(put(1, 1, "a", "1"))
	src.Apply(put(2, 1, "b", "2"))
	data, idx, _ := src.Snapshot()

	s := NewStore()
	ch := make(chan raft.ApplyMsg, 4)
	done := make(chan struct{})
	go func() { s.Run(context.Background(), ch); close(done) }()

	ch <- raft.ApplyMsg{Snapshot: data, SnapshotIndex: idx, SnapshotTerm: 1}
	ch <- put(3, 1, "c", "3")
	close(ch)
	<-done

	if want := map[string]string{"a": "1", "b": "2", "c": "3"}; !reflect.DeepEqual(s.Dump(), want) || s.AppliedIndex() != 3 {
		t.Errorf("state %v @%d, want %v @3", s.Dump(), s.AppliedIndex(), want)
	}
}

// ---------------------------------------------------------------------------
// Whole clusters
// ---------------------------------------------------------------------------

func withThreshold(n int) func(*raft.Config) {
	return func(c *raft.Config) { c.SnapshotThreshold = n }
}

// tryPutRange writes keys k<from>..k<to-1> and reports the first failure. It is
// safe to call from any goroutine.
func tryPutRange(svc *Service, from, to int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := from; i < to; i++ {
		if err := svc.Put(ctx, fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i)); err != nil {
			return fmt.Errorf("put %d: %w", i, err)
		}
	}
	return nil
}

func putRange(t *testing.T, svc *Service, from, to int) {
	t.Helper()
	if err := tryPutRange(svc, from, to); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionKeepsTheLogBoundedWithoutLosingData(t *testing.T) {
	const threshold = 20
	c := newClusterWith(t, withThreshold(threshold), "a", "b", "c")
	_, leader := c.waitLeader(t, "")

	putRange(t, leader, 0, 200)

	eventually(t, "every node to have compacted and be holding a small log", func() bool {
		for _, s := range c.svcs {
			st, _ := s.Node().Status()
			if st.SnapshotIndex == 0 || st.LogEntries > 2*threshold {
				return false
			}
		}
		return true
	})
	// The map holds all 200 keys, not just the tail the log still remembers.
	eventually(t, "all 200 keys on every node", func() bool {
		for _, s := range c.svcs {
			if len(s.Store().Dump()) != 200 {
				return false
			}
		}
		return true
	})
	c.everywhere(t, "first key", "k000", "v0", true)
	c.everywhere(t, "last key", "k199", "v199", true)
}

func TestConcurrentWritesDuringCompaction(t *testing.T) {
	c := newClusterWith(t, withThreshold(15), "a", "b", "c")
	_, leader := c.waitLeader(t, "")

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := tryPutRange(leader, w*50, (w+1)*50); err != nil {
				t.Errorf("writer %d: %v", w, err)
			}
		}()
	}
	wg.Wait()

	eventually(t, "all 200 keys on every node", func() bool {
		for _, s := range c.svcs {
			if len(s.Store().Dump()) != 200 {
				return false
			}
		}
		return true
	})
	for _, s := range c.svcs {
		if st, _ := s.Node().Status(); st.SnapshotIndex == 0 {
			t.Error("a node never compacted")
		}
	}
}

// A follower is cut off while the rest of the cluster writes and compacts past
// the point it stopped at. The entries it needs no longer exist anywhere, so
// the only way back is a snapshot.
func TestLaggingFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	c := newClusterWith(t, withThreshold(20), "a", "b", "c")
	leaderID, leader := c.waitLeader(t, "")
	var lagID string
	for id := range c.svcs {
		if id != leaderID {
			lagID = id
			break
		}
	}
	lag := c.svcs[lagID]

	c.setDown(lagID, true)
	frozen, _ := lag.Node().Status()

	putRange(t, leader, 0, 150)
	eventually(t, "the connected nodes to compact past the lagging node's log", func() bool {
		for id, s := range c.svcs {
			if id == lagID {
				continue
			}
			if st, _ := s.Node().Status(); st.SnapshotIndex <= frozen.LastLogIndex {
				return false
			}
		}
		return true
	})
	if got := len(lag.Store().Dump()); got != 0 {
		t.Fatalf("isolated node already holds %d keys", got)
	}

	c.setDown(lagID, false)
	eventually(t, "the lagging node to receive all 150 keys", func() bool {
		return len(lag.Store().Dump()) == 150
	})

	if v, ok := lag.Get("k149"); !ok || v != "v149" {
		t.Errorf("k149 = %q,%v", v, ok)
	}
	if v, ok := lag.Get("k000"); !ok || v != "v0" {
		t.Errorf("k000 = %q,%v", v, ok)
	}
	// It cannot have replayed the history: that history was compacted away. Its
	// snapshot boundary jumping past where it froze proves a snapshot was installed.
	if st, _ := lag.Node().Status(); st.SnapshotIndex <= frozen.LastLogIndex {
		t.Errorf("lagging node: snapshot index %d never passed its frozen log end %d", st.SnapshotIndex, frozen.LastLogIndex)
	}

	// And it is a full participant again: new writes reach it.
	_, current := c.waitLeader(t, "")
	putRange(t, current, 150, 160)
	c.everywhere(t, "post-recovery write on the formerly lagging node", "k159", "v159", true)
}

// A node that restarts has its state machine rebuilt from the persisted snapshot,
// then the log entries after it are replayed on top.
func TestRestartRebuildsFromSnapshotPlusLog(t *testing.T) {
	st := raft.NewMemoryStorage()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := func() *Service {
		svc, err := NewService(raft.Config{
			ID: "solo", Storage: st, SnapshotThreshold: 10, Logger: quiet,
			Transport: memTransport{net: &memNet{svcs: map[string]*Service{}, down: map[string]bool{}}, self: "solo"},
		})
		if err != nil {
			t.Fatal(err)
		}
		svc.Start()
		return svc
	}

	first := start()
	eventually(t, "the node to elect itself", func() bool {
		s, _ := first.Node().Status()
		return s.State == raft.Leader
	})
	putRange(t, first, 0, 40)
	eventually(t, "a snapshot", func() bool {
		s, _ := first.Node().Status()
		return s.SnapshotIndex > 0
	})
	first.Stop()

	snap, _ := st.LoadSnapshot()
	if snap.Index == 0 || len(snap.Data) == 0 {
		t.Fatalf("nothing durable to restart from: %+v", snap)
	}

	second := start()
	t.Cleanup(second.Stop)
	eventually(t, "the restarted node to hold all 40 keys", func() bool {
		return len(second.Store().Dump()) == 40
	})
	for _, k := range []int{0, 17, 39} {
		key, want := fmt.Sprintf("k%03d", k), fmt.Sprintf("v%d", k)
		if v, ok := second.Get(key); !ok || v != want {
			t.Errorf("%s = %q,%v after restart, want %q", key, v, ok, want)
		}
	}
	if s, _ := second.Node().Status(); s.SnapshotIndex < snap.Index {
		t.Errorf("restarted with snapshot index %d, want at least %d", s.SnapshotIndex, snap.Index)
	}
}
