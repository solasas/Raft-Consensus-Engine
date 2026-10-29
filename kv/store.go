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
	"fmt"
	"sync"

	"github.com/sashanks/raft/raft"
)

// recentWindow is how many applied (index, term) pairs Await can look back at.
const recentWindow = 1 << 12

// unknownTerm is what a waiter is told when its index was swallowed by a
// snapshot: the entry was applied, but whose it was can no longer be known.
const unknownTerm = ^uint64(0)

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
			if m.IsSnapshot() {
				if err := s.Restore(m.Snapshot, m.SnapshotIndex); err != nil {
					// The replica cannot continue from a state it can't rebuild;
					// guessing would make it diverge silently.
					panic(fmt.Sprintf("kv: cannot restore snapshot at index %d: %v", m.SnapshotIndex, err))
				}
				continue
			}
			s.Apply(m)
		}
	}
}

// snapshotImage is the serialised form of the store.
type snapshotImage struct {
	Version int               `json:"v"`
	Data    map[string]string `json:"data"`
}

// Snapshot implements raft.Snapshotter. It returns the whole map together with
// the index of the last entry applied to it, both taken under one lock so they
// are consistent with each other. Writers wait while the map is serialised.
func (s *Store) Snapshot() ([]byte, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := json.Marshal(snapshotImage{Version: 1, Data: s.data})
	return data, s.appliedIdx, err
}

// Restore replaces the entire state with a snapshot covering log entries up to
// and including index. A snapshot that is not newer than what is already applied
// is ignored. Waiters at or below index are told their outcome is unknown, since
// the individual entries are no longer there to inspect.
func (s *Store) Restore(data []byte, index uint64) error {
	var img snapshotImage
	if err := json.Unmarshal(data, &img); err != nil {
		return err
	}
	if img.Version != 1 {
		return fmt.Errorf("unsupported snapshot version %d", img.Version)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.appliedIdx {
		return nil
	}
	if img.Data == nil {
		img.Data = make(map[string]string)
	}
	s.data = img.Data
	s.appliedIdx = index
	s.recent = [recentWindow]applied{} // history before the snapshot can't be vouched for

	kept := s.waiters[:0]
	for _, w := range s.waiters {
		if w.index <= index {
			w.ch <- unknownTerm
		} else {
			kept = append(kept, w)
		}
	}
	s.waiters = kept
	return nil
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

// Dump returns a plain copy of the whole map, for inspection and tests. (It is
// not the raft snapshot; see Snapshot.)
func (s *Store) Dump() map[string]string {
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
		switch got {
		case term:
			return nil
		case unknownTerm:
			return ErrOutcomeUnknown
		default:
			return ErrNotCommitted
		}
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
