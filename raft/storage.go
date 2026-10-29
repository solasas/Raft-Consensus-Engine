package raft

import "sync"

// MemoryStorage is a non-durable Storage for tests and development.
type MemoryStorage struct {
	mu      sync.Mutex
	hs      HardState
	entries []LogEntry
	snap    Snapshot
}

// SaveSnapshot and LoadSnapshot make MemoryStorage a SnapshotStorage.
func (m *MemoryStorage) SaveSnapshot(s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = s
	return nil
}

func (m *MemoryStorage) LoadSnapshot() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap, nil
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Save(hs HardState, entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	m.entries = append([]LogEntry(nil), entries...)
	return nil
}

func (m *MemoryStorage) Load() (HardState, []LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, append([]LogEntry(nil), m.entries...), nil
}
