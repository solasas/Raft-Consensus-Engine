package raft

import (
	"context"
	"errors"
	"fmt"
)

// State is the role a node currently plays in the cluster.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// LogEntry is one slot in the replicated log. Indexes start at 1; index 0 is
// an in-memory sentinel (term 0) so that PrevLogIndex == 0 needs no special case.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
	// Noop entries are appended by a new leader to commit entries from earlier
	// terms (Raft §5.4.2). They are replicated but never delivered to the app.
	Noop bool
}

// ApplyMsg is a committed command delivered to the state machine, in log order.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Peer identifies another cluster member and its network address.
type Peer struct {
	ID   string
	Addr string
}

// --- RPC messages (Raft paper, Figure 2) ---

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Fast log backtracking hints, valid when Success is false.
	// ConflictTerm == 0 means the follower's log is simply too short and
	// ConflictIndex is its last index + 1.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// RPC is any message that crosses the network: a request or its reply. The
// transport layer routes on this interface so it can delay, drop, or block
// traffic without knowing the concrete protocol.
type RPC interface{ RPCName() string }

func (RequestVoteArgs) RPCName() string    { return "RequestVote" }
func (RequestVoteReply) RPCName() string   { return "RequestVoteReply" }
func (AppendEntriesArgs) RPCName() string  { return "AppendEntries" }
func (AppendEntriesReply) RPCName() string { return "AppendEntriesReply" }

// Transport sends RPCs to peers. Implementations (gRPC, HTTP, in-memory for
// tests) must be safe for concurrent use. The inbound side calls
// Node.RequestVote and Node.AppendEntries.
type Transport interface {
	RequestVote(ctx context.Context, to Peer, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, to Peer, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// HardState is the state Raft requires to survive restarts (besides the log).
type HardState struct {
	CurrentTerm uint64
	VotedFor    string
}

// Storage persists hard state and the log. Save must be durable (fsynced)
// before it returns: the node replies to RPCs only after Save succeeds.
// entries excludes the index-0 sentinel.
type Storage interface {
	Save(hs HardState, entries []LogEntry) error
	Load() (HardState, []LogEntry, error)
}

var (
	ErrStopped = errors.New("raft: node stopped")
)

// NotLeaderError is returned by Propose on a non-leader. LeaderID is a hint
// and may be empty if no leader is currently known.
type NotLeaderError struct{ LeaderID string }

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "raft: not the leader (leader unknown)"
	}
	return "raft: not the leader (leader is " + e.LeaderID + ")"
}

// Status is a consistent snapshot of a node's volatile state.
type Status struct {
	ID           string
	State        State
	Term         uint64
	VotedFor     string
	LeaderID     string
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
}
