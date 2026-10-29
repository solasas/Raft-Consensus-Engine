// Package transport is the network layer beneath Raft.
//
// Layering:
//
//	raft.Node ──► raft.Transport (typed: RequestVote / AppendEntries)
//	                  │  AsRaftTransport adapter
//	                  ▼
//	             transport.Transport  SendRPC(ctx, peerID, msg)   <- this package's contract
//	                  │
//	      ┌───────────┴────────────┐
//	      ▼                        ▼
//	 real transport         SimulatedTransport ──► Switch (link matrix, chaos-controlled)
//	 (TCP/gRPC, later)                                  │
//	                                                    ▼
//	                                      destination Handler (ServeNode) or an inner Transport
//
// The Raft core never learns which implementation it is talking to.
package transport

import (
	"context"
	"errors"
	"fmt"

	"github.com/sashanks/raft/raft"
)

// Transport delivers a request to a peer and returns its reply. An
// implementation is bound to one local node (the sender). Implementations must
// be safe for concurrent use and must honour ctx cancellation.
type Transport interface {
	SendRPC(ctx context.Context, peerID string, msg raft.RPC) (raft.RPC, error)
}

// Handler is the receiving side: it executes a request and returns the reply.
type Handler interface {
	HandleRPC(msg raft.RPC) (raft.RPC, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(raft.RPC) (raft.RPC, error)

func (f HandlerFunc) HandleRPC(m raft.RPC) (raft.RPC, error) { return f(m) }

var (
	// ErrUnreachable is the base error for any request or reply the network lost.
	ErrUnreachable = errors.New("transport: peer unreachable")
	// ErrBlocked: a hard partition on the link. Wraps ErrUnreachable.
	ErrBlocked = fmt.Errorf("%w (link blocked)", ErrUnreachable)
	// ErrDropped: random packet loss. Wraps ErrUnreachable.
	ErrDropped = fmt.Errorf("%w (packet dropped)", ErrUnreachable)

	ErrUnknownPeer    = errors.New("transport: unknown peer")
	ErrUnsupportedRPC = errors.New("transport: unsupported RPC type")
	ErrBadResponse    = errors.New("transport: unexpected response type")
)

// ServeNode exposes a raft.Node as a Handler, dispatching on message type.
func ServeNode(n *raft.Node) Handler {
	return HandlerFunc(func(msg raft.RPC) (raft.RPC, error) {
		switch m := msg.(type) {
		case raft.RequestVoteArgs:
			r, err := n.RequestVote(m)
			if err != nil {
				return nil, err
			}
			return r, nil
		case raft.AppendEntriesArgs:
			r, err := n.AppendEntries(m)
			if err != nil {
				return nil, err
			}
			return r, nil
		case raft.InstallSnapshotArgs:
			r, err := n.InstallSnapshot(m)
			if err != nil {
				return nil, err
			}
			return r, nil
		default:
			return nil, fmt.Errorf("%w: %T", ErrUnsupportedRPC, msg)
		}
	})
}

// AsRaftTransport adapts a message-based Transport to the typed interface the
// Raft node consumes. Peer addresses are ignored: the underlying Transport maps
// peer IDs to wherever they live.
func AsRaftTransport(t Transport) raft.Transport { return typed{t} }

type typed struct{ t Transport }

func (a typed) RequestVote(ctx context.Context, to raft.Peer, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	return call[raft.RequestVoteReply](ctx, a.t, to.ID, args)
}

func (a typed) AppendEntries(ctx context.Context, to raft.Peer, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	return call[raft.AppendEntriesReply](ctx, a.t, to.ID, args)
}

// InstallSnapshot makes the adapter a raft.SnapshotTransport, which nodes with
// log compaction enabled require.
func (a typed) InstallSnapshot(ctx context.Context, to raft.Peer, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	return call[raft.InstallSnapshotReply](ctx, a.t, to.ID, args)
}

func call[T raft.RPC](ctx context.Context, t Transport, peerID string, req raft.RPC) (T, error) {
	var zero T
	resp, err := t.SendRPC(ctx, peerID, req)
	if err != nil {
		return zero, err
	}
	out, ok := resp.(T)
	if !ok {
		return zero, fmt.Errorf("%w: got %T, want %T", ErrBadResponse, resp, zero)
	}
	return out, nil
}
