package kv

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sashanks/raft/raft"
)

func put(i, term uint64, k, v string) raft.ApplyMsg {
	return raft.ApplyMsg{Index: i, Term: term, Command: EncodeCommand(Command{Op: OpPut, Key: k, Value: v})}
}

func del(i, term uint64, k string) raft.ApplyMsg {
	return raft.ApplyMsg{Index: i, Term: term, Command: EncodeCommand(Command{Op: OpDelete, Key: k})}
}

func TestStoreAppliesInOrder(t *testing.T) {
	s := NewStore()
	s.Apply(put(1, 1, "a", "1"))
	s.Apply(put(2, 1, "a", "2"))
	s.Apply(put(3, 1, "b", "3"))
	s.Apply(del(5, 2, "b")) // index 4 was a leader no-op: gaps are normal

	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Errorf("a = %q,%v want 2,true", v, ok)
	}
	if _, ok := s.Get("b"); ok {
		t.Error("b should have been deleted")
	}
	if got := s.AppliedIndex(); got != 5 {
		t.Errorf("AppliedIndex = %d, want 5", got)
	}
}

func TestStoreIgnoresReplaysAndReordering(t *testing.T) {
	s := NewStore()
	s.Apply(put(3, 1, "x", "new"))
	s.Apply(put(2, 1, "x", "old"))   // late, lower index
	s.Apply(put(3, 1, "x", "dup"))   // duplicate of an applied index
	s.Apply(del(1, 1, "x"))          // even a delete cannot go back in time
	if v, _ := s.Get("x"); v != "new" {
		t.Errorf("x = %q, want new", v)
	}
	if got := s.AppliedIndex(); got != 3 {
		t.Errorf("AppliedIndex = %d, want 3", got)
	}
}

func TestStoreSkipsMalformedCommands(t *testing.T) {
	s := NewStore()
	s.Apply(raft.ApplyMsg{Index: 1, Term: 1, Command: []byte("not json")})
	s.Apply(raft.ApplyMsg{Index: 2, Term: 1, Command: EncodeCommand(Command{Op: "explode", Key: "k"})})
	s.Apply(raft.ApplyMsg{Index: 3, Term: 1, Command: nil})
	s.Apply(put(4, 1, "ok", "yes"))

	if got := s.AppliedIndex(); got != 4 {
		t.Errorf("AppliedIndex = %d, want 4 (bad commands still consume their index)", got)
	}
	if want := map[string]string{"ok": "yes"}; !reflect.DeepEqual(s.Dump(), want) {
		t.Errorf("state = %v, want %v", s.Dump(), want)
	}
}

// Replicas fed the same stream must converge to identical state.
func TestStoreIsDeterministic(t *testing.T) {
	stream := []raft.ApplyMsg{
		put(1, 1, "a", "1"), put(2, 1, "b", "2"), del(3, 1, "a"),
		{Index: 4, Term: 1, Command: []byte("junk")}, put(6, 2, "a", "9"),
	}
	a, b := NewStore(), NewStore()
	for _, m := range stream {
		a.Apply(m)
		b.Apply(m)
	}
	if !reflect.DeepEqual(a.Dump(), b.Dump()) || a.AppliedIndex() != b.AppliedIndex() {
		t.Errorf("replicas diverged: %v vs %v", a.Dump(), b.Dump())
	}
}

func TestStoreRunAppliesFromChannelUntilClosed(t *testing.T) {
	s := NewStore()
	ch := make(chan raft.ApplyMsg, 4)
	done := make(chan struct{})
	go func() { s.Run(context.Background(), ch); close(done) }()

	ch <- put(1, 1, "k", "v1")
	ch <- put(2, 1, "k", "v2")
	close(ch)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not exit after the channel closed")
	}
	if v, _ := s.Get("k"); v != "v2" || s.AppliedIndex() != 2 {
		t.Errorf("k=%q applied=%d, want v2 and 2", v, s.AppliedIndex())
	}
}

func TestStoreRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { NewStore().Run(ctx, make(chan raft.ApplyMsg)); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run ignored context cancellation")
	}
}

func awaitAsync(s *Store, ctx context.Context, index, term uint64) <-chan error {
	out := make(chan error, 1)
	go func() { out <- s.Await(ctx, index, term) }()
	return out
}

func expect(t *testing.T, ch <-chan error, want error) {
	t.Helper()
	select {
	case got := <-ch:
		if !errors.Is(got, want) {
			t.Errorf("Await = %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("Await did not return (want %v)", want)
	}
}

func expectBlocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("Await returned early with %v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAwait(t *testing.T) {
	ctx := context.Background()

	t.Run("already applied with matching term", func(t *testing.T) {
		s := NewStore()
		s.Apply(put(1, 4, "k", "v"))
		if err := s.Await(ctx, 1, 4); err != nil {
			t.Errorf("Await = %v, want nil", err)
		}
	})

	t.Run("already applied but a different leader's entry", func(t *testing.T) {
		s := NewStore()
		s.Apply(put(1, 5, "k", "v"))
		if err := s.Await(ctx, 1, 4); !errors.Is(err, ErrNotCommitted) {
			t.Errorf("Await = %v, want ErrNotCommitted", err)
		}
	})

	t.Run("blocks until the index is applied, then succeeds", func(t *testing.T) {
		s := NewStore()
		res := awaitAsync(s, ctx, 2, 5)
		expectBlocked(t, res)
		s.Apply(put(1, 5, "a", "1"))
		expectBlocked(t, res) // index 1 is not our index
		s.Apply(put(2, 5, "b", "2"))
		expect(t, res, nil)
	})

	t.Run("slot overwritten by another term while waiting", func(t *testing.T) {
		s := NewStore()
		res := awaitAsync(s, ctx, 1, 2)
		s.Apply(put(1, 3, "k", "v"))
		expect(t, res, ErrNotCommitted)
	})

	t.Run("index skipped entirely while waiting", func(t *testing.T) {
		s := NewStore()
		res := awaitAsync(s, ctx, 2, 5)
		s.Apply(put(1, 1, "a", "1"))
		s.Apply(put(3, 6, "b", "2")) // 2 was replaced by a no-op; 3 is applied
		expect(t, res, ErrNotCommitted)
	})

	t.Run("context deadline", func(t *testing.T) {
		s := NewStore()
		c, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if err := s.Await(c, 10, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Await = %v, want DeadlineExceeded", err)
		}
		if len(s.waiters) != 0 {
			t.Errorf("%d waiters leaked after cancellation", len(s.waiters))
		}
	})

	t.Run("fell out of the tracking window", func(t *testing.T) {
		s := NewStore()
		s.Apply(put(1, 1, "a", "1"))
		s.Apply(put(1+recentWindow, 1, "b", "2")) // reuses ring slot of index 1
		if err := s.Await(ctx, 1, 1); !errors.Is(err, ErrOutcomeUnknown) {
			t.Errorf("Await = %v, want ErrOutcomeUnknown", err)
		}
	})
}
