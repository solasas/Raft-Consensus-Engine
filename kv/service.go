package kv

import (
	"context"

	"github.com/sashanks/raft/raft"
)

// Service is one replicated KV server: a raft node plus its state machine and
// the applier loop connecting them.
type Service struct {
	node    *raft.Node
	store   *Store
	applyCh chan raft.ApplyMsg

	cancel context.CancelFunc
	ctx    context.Context
	done   chan struct{}
}

// NewService builds a node whose committed entries feed a fresh Store. Any
// cfg.ApplyCh is replaced.
func NewService(cfg raft.Config) (*Service, error) {
	ch := make(chan raft.ApplyMsg, 256)
	cfg.ApplyCh = ch
	node, err := raft.NewNode(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		node: node, store: NewStore(), applyCh: ch,
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}, nil
}

// Start launches the raft node and the applier. Call exactly once.
func (s *Service) Start() {
	s.node.Start()
	go func() {
		defer close(s.done)
		s.store.Run(s.ctx, s.applyCh)
	}()
}

// Stop shuts down the node and the applier. Call only after Start.
func (s *Service) Stop() {
	s.node.Stop()
	s.cancel()
	<-s.done
}

// Node exposes the underlying raft node (to wire up the transport server).
func (s *Service) Node() *raft.Node { return s.node }

// Store exposes the state machine (mainly for inspection and tests).
func (s *Service) Store() *Store { return s.store }

// Put replicates key=value and returns once it is committed and applied on this
// node. On a non-leader it returns *raft.NotLeaderError.
func (s *Service) Put(ctx context.Context, key, value string) error {
	return s.submit(ctx, Command{Op: OpPut, Key: key, Value: value})
}

// Delete replicates the removal of key, with the same guarantees as Put.
func (s *Service) Delete(ctx context.Context, key string) error {
	return s.submit(ctx, Command{Op: OpDelete, Key: key})
}

// Get is a local, possibly stale read; see Store.Get.
func (s *Service) Get(key string) (string, bool) { return s.store.Get(key) }

func (s *Service) submit(ctx context.Context, c Command) error {
	index, term, err := s.node.Propose(EncodeCommand(c))
	if err != nil {
		return err
	}
	return s.store.Await(ctx, index, term)
}
