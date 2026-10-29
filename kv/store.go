// Package kv is a replicated key-value state machine on top of package raft.
//
// Data flow:
//
//	raft.Node ──ApplyCh──► Store.Run (applier loop) ──► in-memory map
//	    ▲                                                   │
//	    └── Service.Put/Delete ── Propose ── Store.Await ◄──┘
//
// The Store is the state machine. It must be deterministic: every replica
// applies the same commands in the same order and must end in the same state,
// so Apply never consults clocks, randomness, or anything node-local.
package kv

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/sashanks/raft/raft"
)

// recentWindow is how many applied (index, term) pairs Await can look back at.
const recentWindow = 1 << 12

var (
	// ErrNotCommitted: the log slot a proposal was appended to ended up holding a
	// different entry (a new leader overwrote it). The command was NOT applied.
	ErrNotCommitted = errors.New("kv: proposal was overwritten before it committed")
	// ErrOutcomeUnknown: the proposal's index was applied long enough ago that
	// we can no longer tell whether it was ours. Callers must treat the result
	// as indeterminate (and retry only if the operation is idempotent).
	ErrOutcomeUnknown = errors.New("kv: outcome unknown")
)

// Operations understood by the state machine.
const (
	OpPut    = "put"
	OpDelete = "delete"
)

// Command is the payload stored in a raft log entry.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// EncodeCommand serialises a command for Node.Propose.
func EncodeCommand(c Command) []byte {
	b, _ := json.Marshal(c) // a struct of strings cannot fail to marshal
	return b
}

type applied struct{ index, term uint64 }

type waiter struct {
	index uint64
	ch    chan uint64 // receives the term applied at index, or 0 if index was skipped
}

// Store is the key-value state machine. Apply/Run are driven by a single
// applier goroutine; reads and Await come from arbitrary goroutines, so the
// map is guarded by an RWMutex.
type Store struct {
	mu         sync.RWMutex
	data       map[string]string
	appliedIdx uint64
	recent     [recentWindow]applied // ring: slot index%recentWindow
	waiters    []waiter
}

func NewStore() *Store { return &Store{data: make(map[string]string)} }

// Run is the applier loop. It applies committed entries, in order, until ctx
// is cancelled or the channel is closed.
func (s *Store) Run(ctx context.Context, in <-chan raft.ApplyMsg) {
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-in:
			if !ok {
				return
			}
			s.Apply(m)
		}
	}
}

// Apply executes one committed entry. It is idempotent with respect to replays:
// anything at or below the applied index is ignored, so a duplicated or
// reordered delivery can never apply a command twice or move state backwards.
//
// Indexes may skip (the leader's no-op entries are never delivered), so strict
// contiguity is not required, only strict monotonicity.
func (s *Store) Apply(m raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if m.Index <= s.appliedIdx {
		return
	}

	var c Command
	if err := json.Unmarshal(m.Command, &c); err == nil {
		switch c.Op {
		case OpPut:
			s.data[c.Key] = c.Value
		case OpDelete:
			delete(s.data, c.Key)
		}
	}
	// A malformed or unknown command is skipped, not fatal: every replica sees the
	// same bytes and skips them identically, so state stays consistent, and one bad
	// client request cannot crash the cluster.

	s.appliedIdx = m.Index
	s.recent[m.Index%recentWindow] = applied{index: m.Index, term: m.Term}

	kept := s.waiters[:0]
	for _, w := range s.waiters {
		switch {
		case w.index == m.Index:
			w.ch <- m.Term
		case w.index < m.Index:
			w.ch <- 0 // our slot was passed over: it never held a command
		default:
			kept = append(kept, w)
		}
	}
	s.waiters = kept
}

// Get reads the local replica's value. It may be stale on a follower, or on a
// leader that has been deposed but doesn't know it yet. It is not linearizable.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// AppliedIndex is the highest raft index applied to the map.
func (s *Store) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.appliedIdx
}

// Snapshot returns a copy of the whole map.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Await blocks until the entry proposed at (index, term) has been applied and
// returns nil only if the entry applied at that index really carries that term,
// i.e. it is the proposal we made and not one a new leader wrote over it.
func (s *Store) Await(ctx context.Context, index, term uint64) error {
	s.mu.Lock()
	if s.appliedIdx >= index {
		err := s.checkLocked(index, term)
		s.mu.Unlock()
		return err
	}
	w := waiter{index: index, ch: make(chan uint64, 1)}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case got := <-w.ch:
		if got != term {
			return ErrNotCommitted
		}
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for i, x := range s.waiters {
			if x.ch == w.ch {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *Store) checkLocked(index, term uint64) error {
	slot := s.recent[index%recentWindow]
	switch {
	case slot.index != index:
		return ErrOutcomeUnknown // skipped (no-op) or evicted from the ring
	case slot.term != term:
		return ErrNotCommitted
	}
	return nil
}
