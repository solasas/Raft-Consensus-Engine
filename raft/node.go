// Package raft implements the Raft consensus algorithm core.
//
// Concurrency model: a single "run loop" goroutine owns ALL mutable Raft state
// (term, vote, log, indexes, role, timers). Nothing else reads or writes it, so
// there are no mutexes around the state and state transitions are serialized by
// construction. Everything else talks to the loop over channels:
//
//	inbound RPCs      -> voteRPCs / appendRPCs   (reply via per-request channel)
//	client proposals  -> proposals
//	status queries    -> statusReqs
//	outbound RPC results (from short-lived goroutines) -> voteResults / appendResults
//
// Outbound network calls run in their own goroutines and never touch node
// state; they only send their result back to the loop. Each result carries the
// term it was sent in so stale responses can be discarded.
package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Config configures a Node.
type Config struct {
	ID    string
	Peers []Peer // all OTHER cluster members, with their network addresses

	Transport Transport
	Storage   Storage        // nil => MemoryStorage
	ApplyCh   chan<- ApplyMsg // committed commands are delivered here, in order

	ElectionTimeoutMin time.Duration // default 150ms
	ElectionTimeoutMax time.Duration // default 300ms
	HeartbeatInterval  time.Duration // default 50ms; must be << ElectionTimeoutMin
	RPCTimeout         time.Duration // default 100ms
	MaxEntriesPerRPC   int           // default 256

	// Log compaction. Setting Snapshotter turns it on; the Storage must then
	// implement SnapshotStorage and the Transport SnapshotTransport.
	Snapshotter       Snapshotter
	SnapshotThreshold int           // compact once the in-memory log exceeds this many entries; default 1024
	SnapshotTimeout   time.Duration // per InstallSnapshot RPC; default 5s

	Logger *slog.Logger // nil => slog.Default()
}

func (c *Config) setDefaults() error {
	if c.ID == "" {
		return fmt.Errorf("raft: Config.ID is required")
	}
	if c.Transport == nil {
		return fmt.Errorf("raft: Config.Transport is required")
	}
	if c.ApplyCh == nil {
		return fmt.Errorf("raft: Config.ApplyCh is required")
	}
	for _, p := range c.Peers {
		if p.ID == c.ID {
			return fmt.Errorf("raft: Config.Peers must not contain self (%q)", c.ID)
		}
	}
	if c.Storage == nil {
		c.Storage = NewMemoryStorage()
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return fmt.Errorf("raft: ElectionTimeoutMax must exceed ElectionTimeoutMin")
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 100 * time.Millisecond
	}
	if c.MaxEntriesPerRPC == 0 {
		c.MaxEntriesPerRPC = 256
	}
	if c.Snapshotter != nil {
		if _, ok := c.Storage.(SnapshotStorage); !ok {
			return fmt.Errorf("raft: Snapshotter set but Storage %T does not implement SnapshotStorage", c.Storage)
		}
		if _, ok := c.Transport.(SnapshotTransport); !ok {
			return fmt.Errorf("raft: Snapshotter set but Transport %T does not implement SnapshotTransport", c.Transport)
		}
		if c.SnapshotThreshold == 0 {
			c.SnapshotThreshold = 1024
		}
		if c.SnapshotTimeout == 0 {
			c.SnapshotTimeout = 5 * time.Second
		}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// Node is a single Raft participant.
type Node struct {
	// ---- immutable after NewNode ----
	id        string
	peers     []Peer
	transport Transport
	storage   Storage
	cfg       Config
	logger    *slog.Logger

	snapStore     SnapshotStorage   // nil when snapshots are disabled
	snapTransport SnapshotTransport // nil when snapshots are disabled

	// ---- owned exclusively by the run loop goroutine ----
	state State

	// Persistent state (persisted before replying to any RPC that changed it).
	currentTerm uint64
	votedFor    string

	// The log is a window onto the full history: everything up to snapIndex has
	// been folded into the snapshot and discarded. log[0] is a sentinel standing
	// for the last discarded entry (Index == snapIndex, Term == snapTerm) and
	// log[i] holds the entry with index snapIndex+i. With no snapshot snapIndex is
	// 0 and the sentinel is the blank "entry 0". Always go through entry/termAt/
	// lastIndex rather than indexing the slice with a raft index.
	log       []LogEntry
	snapIndex uint64
	snapTerm  uint64
	snapshot  []byte // latest snapshot image (immutable once set; shared with RPCs)

	// Snapshot/compaction bookkeeping.
	snapshotting bool            // a Snapshotter call is in flight
	snapRetryAt  uint64          // don't re-trigger until lastApplied reaches this
	snapInFlight map[string]bool // leader: peers with an InstallSnapshot outstanding

	// Volatile state, all servers.
	commitIndex uint64
	lastApplied uint64 // highest index handed to the applier goroutine
	leaderID    string

	// Volatile state, candidates.
	votesGranted map[string]bool

	// Volatile state, leaders (reinitialized on election), keyed by peer ID.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionTimer *time.Timer
	heartbeat     *time.Ticker // non-nil only while Leader
	rng           *rand.Rand

	// ---- event channels into the run loop ----
	voteRPCs      chan voteRPC
	appendRPCs    chan appendRPC
	voteResults   chan voteResult
	appendResults chan appendResult
	proposals     chan proposal
	statusReqs    chan chan Status
	snapRPCs      chan snapRPC
	snapResults   chan snapResult
	compactDone   chan compactResult

	applyQ *applyQueue

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stopOnce sync.Once
}

type (
	voteRPC struct {
		args  RequestVoteArgs
		reply chan RequestVoteReply // buffered(1): the loop never blocks on reply
	}
	appendRPC struct {
		args  AppendEntriesArgs
		reply chan AppendEntriesReply
	}
	voteResult struct {
		from  string
		term  uint64 // term the request was sent in
		reply RequestVoteReply
	}
	appendResult struct {
		from  string
		term  uint64 // term the request was sent in
		args  AppendEntriesArgs
		reply AppendEntriesReply
	}
	proposal struct {
		cmd   []byte
		reply chan proposeResult
	}
	proposeResult struct {
		index, term uint64
		err         error
	}
	snapRPC struct {
		args  InstallSnapshotArgs
		reply chan InstallSnapshotReply
	}
	snapResult struct {
		from  string
		term  uint64 // term the request was sent in
		args  InstallSnapshotArgs
		reply InstallSnapshotReply
		err   error
	}
	// compactResult is the state machine's answer to a snapshot request.
	compactResult struct {
		data  []byte
		index uint64
		err   error
	}
)

// NewNode builds a node, restoring persisted state from cfg.Storage. Call Start
// to begin participating in the cluster.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	hs, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}

	// A persisted snapshot is honoured even if snapshotting is now switched off:
	// the log before it is gone, so ignoring it would silently lose state.
	snapStore, _ := cfg.Storage.(SnapshotStorage)
	var snap Snapshot
	if snapStore != nil {
		if snap, err = snapStore.LoadSnapshot(); err != nil {
			return nil, fmt.Errorf("raft: load snapshot: %w", err)
		}
	}
	if snap.Index > 0 {
		// Entries the snapshot already covers can survive a crash between saving the
		// snapshot and saving the trimmed log. Drop them; what remains must start
		// right after the snapshot.
		kept := make([]LogEntry, 0, len(entries))
		for _, e := range entries {
			if e.Index > snap.Index {
				kept = append(kept, e)
			}
		}
		if len(kept) > 0 && kept[0].Index != snap.Index+1 {
			return nil, fmt.Errorf("raft: stored log starts at index %d but snapshot ends at %d", kept[0].Index, snap.Index)
		}
		entries = kept
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:        cfg.ID,
		peers:     append([]Peer(nil), cfg.Peers...),
		transport: cfg.Transport,
		storage:   cfg.Storage,
		cfg:       cfg,
		logger:    cfg.Logger.With("node", cfg.ID),
		snapStore: snapStore,

		state:       Follower,
		currentTerm: hs.CurrentTerm,
		votedFor:    hs.VotedFor,
		log:         append([]LogEntry{{Index: snap.Index, Term: snap.Term}}, entries...),
		snapIndex:   snap.Index,
		snapTerm:    snap.Term,
		snapshot:    snap.Data,
		snapInFlight: make(map[string]bool),

		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
		voteRPCs:      make(chan voteRPC, 64),
		appendRPCs:    make(chan appendRPC, 64),
		voteResults:   make(chan voteResult, 64),
		appendResults: make(chan appendResult, 64),
		proposals:     make(chan proposal, 64),
		statusReqs:    make(chan chan Status, 8),
		snapRPCs:      make(chan snapRPC, 16),
		snapResults:   make(chan snapResult, 16),
		compactDone:   make(chan compactResult, 4),
		applyQ:        newApplyQueue(),
		ctx:           ctx,
		cancel:        cancel,
	}
	if cfg.Snapshotter != nil {
		n.snapTransport = cfg.Transport.(SnapshotTransport) // validated in setDefaults
	}
	if snap.Index > 0 {
		// Everything in the snapshot is committed and already applied by definition.
		// Hand it to the state machine first so it can rebuild before the log replays.
		n.commitIndex, n.lastApplied = snap.Index, snap.Index
		n.applyQ.push([]ApplyMsg{{Snapshot: snap.Data, SnapshotIndex: snap.Index, SnapshotTerm: snap.Term}})
	}
	n.electionTimer = time.NewTimer(time.Hour) // reset by run()
	return n, nil
}

// Start launches the run loop and the applier. It must be called exactly once.
func (n *Node) Start() {
	n.wg.Add(2)
	go func() { defer n.wg.Done(); n.run() }()
	go func() { defer n.wg.Done(); n.applier() }()
}

// Stop shuts the node down and waits for its goroutines to exit.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.cancel()
		n.applyQ.close()
	})
	n.wg.Wait()
}

// ---------------------------------------------------------------------------
// Public, thread-safe API. Each call marshals a request onto the run loop.
// ---------------------------------------------------------------------------

// RequestVote handles an inbound RequestVote RPC.
func (n *Node) RequestVote(args RequestVoteArgs) (RequestVoteReply, error) {
	req := voteRPC{args: args, reply: make(chan RequestVoteReply, 1)}
	select {
	case n.voteRPCs <- req:
	case <-n.ctx.Done():
		return RequestVoteReply{}, ErrStopped
	}
	select {
	case r := <-req.reply:
		return r, nil
	case <-n.ctx.Done():
		return RequestVoteReply{}, ErrStopped
	}
}

// AppendEntries handles an inbound AppendEntries RPC.
func (n *Node) AppendEntries(args AppendEntriesArgs) (AppendEntriesReply, error) {
	req := appendRPC{args: args, reply: make(chan AppendEntriesReply, 1)}
	select {
	case n.appendRPCs <- req:
	case <-n.ctx.Done():
		return AppendEntriesReply{}, ErrStopped
	}
	select {
	case r := <-req.reply:
		return r, nil
	case <-n.ctx.Done():
		return AppendEntriesReply{}, ErrStopped
	}
}

// InstallSnapshot handles an inbound InstallSnapshot RPC. It returns
// ErrSnapshotsDisabled if this node was not configured with a Snapshotter.
func (n *Node) InstallSnapshot(args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	if n.cfg.Snapshotter == nil {
		return InstallSnapshotReply{}, ErrSnapshotsDisabled
	}
	req := snapRPC{args: args, reply: make(chan InstallSnapshotReply, 1)}
	select {
	case n.snapRPCs <- req:
	case <-n.ctx.Done():
		return InstallSnapshotReply{}, ErrStopped
	}
	select {
	case r := <-req.reply:
		return r, nil
	case <-n.ctx.Done():
		return InstallSnapshotReply{}, ErrStopped
	}
}

// Propose submits a command for replication. It returns as soon as the leader
// has appended the entry locally; the command is committed once it appears on
// ApplyCh. On a non-leader it returns *NotLeaderError.
func (n *Node) Propose(cmd []byte) (index, term uint64, err error) {
	req := proposal{cmd: cmd, reply: make(chan proposeResult, 1)}
	select {
	case n.proposals <- req:
	case <-n.ctx.Done():
		return 0, 0, ErrStopped
	}
	select {
	case r := <-req.reply:
		return r.index, r.term, r.err
	case <-n.ctx.Done():
		return 0, 0, ErrStopped
	}
}

// Status returns a consistent snapshot of the node's state.
func (n *Node) Status() (Status, error) {
	reply := make(chan Status, 1)
	select {
	case n.statusReqs <- reply:
	case <-n.ctx.Done():
		return Status{}, ErrStopped
	}
	select {
	case s := <-reply:
		return s, nil
	case <-n.ctx.Done():
		return Status{}, ErrStopped
	}
}

// ---------------------------------------------------------------------------
// Run loop
// ---------------------------------------------------------------------------

func (n *Node) run() {
	n.resetElectionTimer()
	defer func() {
		n.electionTimer.Stop()
		if n.heartbeat != nil {
			n.heartbeat.Stop()
		}
	}()

	for {
		select {
		case <-n.ctx.Done():
			return

		case <-n.electionTimer.C:
			// Leaders stop this timer; guard anyway against a fire already in flight.
			if n.state != Leader {
				n.startElection()
			}

		case <-n.heartbeatC():
			n.broadcastAppend()

		case req := <-n.voteRPCs:
			req.reply <- n.handleRequestVote(req.args)
		case req := <-n.appendRPCs:
			req.reply <- n.handleAppendEntries(req.args)

		case res := <-n.voteResults:
			n.handleVoteResult(res)
		case res := <-n.appendResults:
			n.handleAppendResult(res)

		case req := <-n.snapRPCs:
			req.reply <- n.handleInstallSnapshot(req.args)
		case res := <-n.snapResults:
			n.handleSnapshotResult(res)
		case res := <-n.compactDone:
			n.handleCompactResult(res)

		case p := <-n.proposals:
			p.reply <- n.handlePropose(p.cmd)
		case reply := <-n.statusReqs:
			reply <- n.status()
		}
	}
}

// heartbeatC is nil (blocks forever in select) unless we are the leader.
func (n *Node) heartbeatC() <-chan time.Time {
	if n.heartbeat == nil {
		return nil
	}
	return n.heartbeat.C
}

func (n *Node) status() Status {
	return Status{
		ID:           n.id,
		State:        n.state,
		Term:         n.currentTerm,
		VotedFor:     n.votedFor,
		LeaderID:     n.leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: n.lastIndex(),

		SnapshotIndex: n.snapIndex,
		LogEntries:    len(n.log) - 1,
	}
}

// ---------------------------------------------------------------------------
// State transitions (run loop only)
// ---------------------------------------------------------------------------

// becomeFollower moves to Follower in the given term. If term is newer, the
// vote is cleared. Safe to call with the current term (e.g. a Candidate that
// hears from a legitimate leader).
func (n *Node) becomeFollower(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persist()
	}
	wasLeader := n.state == Leader
	if n.state != Follower {
		n.logger.Info("became follower", "term", n.currentTerm, "from", n.state.String())
	}
	n.state = Follower
	n.votesGranted = nil
	if wasLeader {
		n.heartbeat.Stop()
		n.heartbeat = nil
		n.nextIndex, n.matchIndex = nil, nil
		n.snapInFlight = make(map[string]bool)
		n.leaderID = ""
		n.resetElectionTimer() // it was stopped while we led
	}
}

func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.votesGranted = map[string]bool{n.id: true}
	n.persist() // term+vote must be durable before asking for votes
	n.resetElectionTimer()
	n.logger.Info("starting election", "term", n.currentTerm)

	if n.quorum() <= 1 { // single-node cluster
		n.becomeLeader()
		return
	}
	lastIdx, lastTerm := n.lastLog()
	args := RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
	for _, p := range n.peers {
		go n.sendRequestVote(p, args)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.votesGranted = nil
	n.logger.Info("became leader", "term", n.currentTerm)

	// Leaders don't time out.
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}

	last := n.lastIndex()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	n.snapInFlight = make(map[string]bool)
	for _, p := range n.peers {
		n.nextIndex[p.ID] = last + 1
		n.matchIndex[p.ID] = 0
	}

	// No-op in the new term lets us commit entries from earlier terms (§5.4.2).
	n.log = append(n.log, LogEntry{Index: last + 1, Term: n.currentTerm, Noop: true})
	n.persist()

	n.heartbeat = time.NewTicker(n.cfg.HeartbeatInterval)
	n.advanceCommit() // single-node clusters commit immediately
	n.broadcastAppend()
}

// ---------------------------------------------------------------------------
// RequestVote
// ---------------------------------------------------------------------------

func (n *Node) handleRequestVote(a RequestVoteArgs) RequestVoteReply {
	if a.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm}
	}
	if a.Term > n.currentTerm {
		n.becomeFollower(a.Term)
	}

	// §5.4.1: only vote for candidates whose log is at least as up-to-date.
	lastIdx, lastTerm := n.lastLog()
	upToDate := a.LastLogTerm > lastTerm ||
		(a.LastLogTerm == lastTerm && a.LastLogIndex >= lastIdx)

	if (n.votedFor == "" || n.votedFor == a.CandidateID) && upToDate {
		n.votedFor = a.CandidateID
		n.persist()
		n.resetElectionTimer() // granting a vote counts as hearing from a viable leader
		n.logger.Info("granted vote", "term", n.currentTerm, "to", a.CandidateID)
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}
	return RequestVoteReply{Term: n.currentTerm}
}

func (n *Node) sendRequestVote(p Peer, args RequestVoteArgs) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	reply, err := n.transport.RequestVote(ctx, p, args)
	if err != nil {
		return // peer unreachable; the next election will retry
	}
	select {
	case n.voteResults <- voteResult{from: p.ID, term: args.Term, reply: reply}:
	case <-n.ctx.Done():
	}
}

func (n *Node) handleVoteResult(res voteResult) {
	if res.reply.Term > n.currentTerm {
		n.becomeFollower(res.reply.Term)
		return
	}
	// Drop stale replies: we moved on to a new term or are no longer campaigning.
	if n.state != Candidate || res.term != n.currentTerm || !res.reply.VoteGranted {
		return
	}
	n.votesGranted[res.from] = true
	if len(n.votesGranted) >= n.quorum() {
		n.becomeLeader()
	}
}

// ---------------------------------------------------------------------------
// AppendEntries: follower side
// ---------------------------------------------------------------------------

func (n *Node) handleAppendEntries(a AppendEntriesArgs) AppendEntriesReply {
	if a.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm}
	}
	// a.Term >= currentTerm: a legitimate leader exists. A Candidate in the same
	// term also steps down here.
	n.becomeFollower(a.Term)
	n.leaderID = a.LeaderID
	n.resetElectionTimer()

	prev, prevTerm, entries := a.PrevLogIndex, a.PrevLogTerm, a.Entries
	if prev < n.snapIndex {
		// The leader is probing inside the prefix we already compacted. Everything
		// up to snapIndex is committed here, so by Log Matching it is identical on
		// the leader. Skip the entries it covers and carry on from the snapshot edge.
		skip := n.snapIndex - prev
		if skip >= uint64(len(entries)) {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev, prevTerm = n.snapIndex, n.snapTerm
	}

	// Consistency check (§5.3) with fast-backtrack hints.
	if prev > n.lastIndex() {
		return AppendEntriesReply{Term: n.currentTerm, ConflictIndex: n.lastIndex() + 1}
	}
	if t := n.termAt(prev); t != prevTerm {
		ci := prev
		for ci > n.snapIndex+1 && n.termAt(ci-1) == t {
			ci--
		}
		return AppendEntriesReply{Term: n.currentTerm, ConflictIndex: ci, ConflictTerm: t}
	}

	// Append, truncating only on a real conflict so that stale/duplicate RPCs
	// can never delete entries we already hold.
	changed := false
	for i, e := range entries {
		idx := prev + 1 + uint64(i)
		if idx <= n.lastIndex() {
			if n.termAt(idx) == e.Term {
				continue
			}
			n.log = n.log[:idx-n.snapIndex]
		}
		n.log = append(n.log, entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persist()
	}

	if a.LeaderCommit > n.commitIndex {
		lastNew := a.PrevLogIndex + uint64(len(a.Entries))
		if c := min(a.LeaderCommit, lastNew); c > n.commitIndex {
			n.commitIndex = c
			n.applyCommitted()
		}
	}
	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// ---------------------------------------------------------------------------
// AppendEntries: leader side
// ---------------------------------------------------------------------------

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		n.replicateTo(p)
	}
}

func (n *Node) replicateTo(p Peer) {
	next := n.nextIndex[p.ID]
	if next <= n.snapIndex {
		// The entries this follower needs were compacted into the snapshot, so
		// the only way to catch it up is to ship the snapshot itself.
		n.sendSnapshotTo(p)
		return
	}
	prev := next - 1
	end := min(n.lastIndex()+1, next+uint64(n.cfg.MaxEntriesPerRPC))
	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  n.termAt(prev),
		Entries:      append([]LogEntry(nil), n.log[next-n.snapIndex:end-n.snapIndex]...), // copy: log mutates under us
		LeaderCommit: n.commitIndex,
	}
	go n.sendAppendEntries(p, args)
}

func (n *Node) sendAppendEntries(p Peer, args AppendEntriesArgs) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	reply, err := n.transport.AppendEntries(ctx, p, args)
	if err != nil {
		return // retried on the next heartbeat
	}
	select {
	case n.appendResults <- appendResult{from: p.ID, term: args.Term, args: args, reply: reply}:
	case <-n.ctx.Done():
	}
}

func (n *Node) handleAppendResult(res appendResult) {
	if res.reply.Term > n.currentTerm {
		n.becomeFollower(res.reply.Term)
		return
	}
	if n.state != Leader || res.term != n.currentTerm {
		return // stale response from a previous leadership
	}
	p := Peer{ID: res.from}
	for _, cand := range n.peers {
		if cand.ID == res.from {
			p = cand
			break
		}
	}

	if res.reply.Success {
		matched := res.args.PrevLogIndex + uint64(len(res.args.Entries))
		if matched > n.matchIndex[res.from] {
			n.matchIndex[res.from] = matched
		}
		n.nextIndex[res.from] = n.matchIndex[res.from] + 1
		n.advanceCommit()
		if n.nextIndex[res.from] <= n.lastIndex() {
			n.replicateTo(p) // keep catching up without waiting for the next tick
		}
		return
	}

	// Rejection. Ignore if it answers a probe we've already moved past.
	if res.args.PrevLogIndex+1 != n.nextIndex[res.from] {
		return
	}
	next := res.reply.ConflictIndex
	if t := res.reply.ConflictTerm; t != 0 {
		// If we have entries of the conflicting term, resume just after our last one.
		for i := n.lastIndex(); i > n.snapIndex; i-- {
			if n.termAt(i) == t {
				next = i + 1
				break
			}
			if n.termAt(i) < t {
				break
			}
		}
	}
	next = min(next, res.args.PrevLogIndex)    // a rejection must always move us backwards
	next = max(next, n.matchIndex[res.from]+1) // never regress below what's known replicated
	next = max(next, 1)
	n.nextIndex[res.from] = next
	n.replicateTo(p)
}

// advanceCommit moves commitIndex to the highest index replicated on a quorum
// whose entry is from the current term (§5.4.2).
func (n *Node) advanceCommit() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.currentTerm {
			break // older-term entries commit only indirectly
		}
		count := 1 // self
		for _, p := range n.peers {
			if n.matchIndex[p.ID] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.applyCommitted()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Client proposals
// ---------------------------------------------------------------------------

func (n *Node) handlePropose(cmd []byte) proposeResult {
	if n.state != Leader {
		return proposeResult{err: &NotLeaderError{LeaderID: n.leaderID}}
	}
	e := LogEntry{Index: n.lastIndex() + 1, Term: n.currentTerm, Command: cmd}
	n.log = append(n.log, e)
	n.persist()
	n.advanceCommit() // single-node clusters
	n.broadcastAppend()
	return proposeResult{index: e.Index, term: e.Term}
}

// ---------------------------------------------------------------------------
// Applying committed entries
// ---------------------------------------------------------------------------

// applyCommitted hands newly committed entries to the applier goroutine. The
// loop must never block on the application's consumption of ApplyCh, so
// delivery goes through an unbounded queue.
func (n *Node) applyCommitted() {
	if n.lastApplied >= n.commitIndex {
		return
	}
	batch := make([]ApplyMsg, 0, n.commitIndex-n.lastApplied)
	for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
		e := n.entry(i) // lastApplied >= snapIndex always holds, so i is never compacted
		if e.Noop {
			continue
		}
		batch = append(batch, ApplyMsg{Index: e.Index, Term: e.Term, Command: e.Command})
	}
	n.lastApplied = n.commitIndex
	n.applyQ.push(batch)
	n.maybeSnapshot()
}

func (n *Node) applier() {
	for {
		batch, ok := n.applyQ.popAll()
		if !ok {
			return
		}
		for _, m := range batch {
			select {
			case n.cfg.ApplyCh <- m:
			case <-n.ctx.Done():
				return
			}
		}
	}
}

// applyQueue is an unbounded FIFO between the run loop and the applier.
type applyQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []ApplyMsg
	closed bool
}

func newApplyQueue() *applyQueue {
	q := &applyQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *applyQueue) push(batch []ApplyMsg) {
	if len(batch) == 0 {
		return
	}
	q.mu.Lock()
	q.items = append(q.items, batch...)
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *applyQueue) popAll() ([]ApplyMsg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return nil, false
	}
	out := q.items
	q.items = nil
	return out, true
}

func (q *applyQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// ---------------------------------------------------------------------------
// Snapshots and log compaction
// ---------------------------------------------------------------------------
//
// Flow:
//
//	applyCommitted ─► maybeSnapshot ─(goroutine)─► Snapshotter.Snapshot() ─► compactDone
//	                                                                              │
//	      run loop: handleCompactResult ─► compactTo: persist snapshot, THEN drop the log prefix
//
//	leader replicateTo, nextIndex <= snapIndex ─► sendSnapshotTo ─► InstallSnapshot RPC
//	      follower handleInstallSnapshot: replace/trim log, persist, tell the state machine
//
// The state machine, not the node, owns the state, so the node asks it for an
// image and cuts the log at whatever index that image covers. The image is taken
// off the run loop so a large snapshot never stalls heartbeats.

// maybeSnapshot starts a snapshot when the in-memory log has outgrown the
// threshold. At most one is in flight, and after each attempt it waits for a
// little more progress before trying again, so a state machine that lags behind
// the log cannot cause a hot loop of useless snapshots.
func (n *Node) maybeSnapshot() {
	if n.cfg.Snapshotter == nil || n.snapshotting {
		return
	}
	if len(n.log)-1 <= n.cfg.SnapshotThreshold || n.lastApplied < n.snapRetryAt {
		return
	}
	n.snapshotting = true
	go func() {
		data, index, err := n.cfg.Snapshotter.Snapshot()
		select {
		case n.compactDone <- compactResult{data: data, index: index, err: err}:
		case <-n.ctx.Done():
		}
	}()
}

func (n *Node) handleCompactResult(res compactResult) {
	n.snapshotting = false
	n.snapRetryAt = n.lastApplied + 1 + uint64(n.cfg.SnapshotThreshold/4)
	if res.err != nil {
		n.logger.Warn("snapshot failed", "err", res.err)
		return
	}
	n.compactTo(res.index, res.data)
}

// compactTo folds the log prefix through `index` into the snapshot `data` and
// drops it from memory. It ignores snapshots that don't advance the boundary and
// refuses ones beyond what has been committed and delivered, since a snapshot
// must only ever contain committed history.
func (n *Node) compactTo(index uint64, data []byte) {
	if index <= n.snapIndex {
		return
	}
	if index > n.lastApplied {
		n.logger.Warn("snapshot is ahead of the applied log; ignoring", "snapshot", index, "applied", n.lastApplied)
		return
	}
	term := n.termAt(index)

	// Copy the tail into a fresh slice: re-slicing would keep the whole old backing
	// array alive, defeating the point of compacting.
	tail := n.log[index-n.snapIndex+1:]
	n.log = append([]LogEntry{{Index: index, Term: term}}, tail...)
	n.snapIndex, n.snapTerm, n.snapshot = index, term, data

	n.persistSnapshot()
	n.logger.Info("compacted log", "snapshotIndex", index, "retained", len(n.log)-1)
}

// handleInstallSnapshot is the follower side (Raft paper Figure 13, for a
// snapshot sent as a single chunk).
func (n *Node) handleInstallSnapshot(a InstallSnapshotArgs) InstallSnapshotReply {
	if a.Term < n.currentTerm {
		return InstallSnapshotReply{Term: n.currentTerm}
	}
	n.becomeFollower(a.Term)
	n.leaderID = a.LeaderID
	n.resetElectionTimer()

	// Everything it covers is already committed here, so installing it would only
	// roll the state machine backwards. The leader can treat us as caught up to it.
	if a.LastIncludedIndex <= n.commitIndex {
		return InstallSnapshotReply{Term: n.currentTerm}
	}

	if a.LastIncludedIndex <= n.lastIndex() && n.termAt(a.LastIncludedIndex) == a.LastIncludedTerm {
		// We hold that very entry: keep the suffix, it is still valid.
		tail := n.log[a.LastIncludedIndex-n.snapIndex+1:]
		n.log = append([]LogEntry{{Index: a.LastIncludedIndex, Term: a.LastIncludedTerm}}, tail...)
	} else {
		// Our log disagrees with, or stops short of, the snapshot: discard all of it.
		n.log = []LogEntry{{Index: a.LastIncludedIndex, Term: a.LastIncludedTerm}}
	}
	n.snapIndex, n.snapTerm, n.snapshot = a.LastIncludedIndex, a.LastIncludedTerm, a.Data
	n.commitIndex = a.LastIncludedIndex
	n.lastApplied = a.LastIncludedIndex

	n.persistSnapshot() // durable before we answer
	// Queued behind any entries already handed over, so the state machine sees
	// the snapshot last and ends in exactly the snapshot's state.
	n.applyQ.push([]ApplyMsg{{Snapshot: a.Data, SnapshotIndex: a.LastIncludedIndex, SnapshotTerm: a.LastIncludedTerm}})
	n.logger.Info("installed snapshot", "index", a.LastIncludedIndex, "from", a.LeaderID)
	return InstallSnapshotReply{Term: n.currentTerm}
}

// sendSnapshotTo ships the current snapshot to a follower, at most one transfer
// per follower at a time (heartbeats would otherwise re-send a big snapshot
// every tick while the first is still on the wire).
func (n *Node) sendSnapshotTo(p Peer) {
	if n.snapTransport == nil || n.snapInFlight[p.ID] {
		return
	}
	n.snapInFlight[p.ID] = true
	args := InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: n.snapIndex,
		LastIncludedTerm:  n.snapTerm,
		Data:              n.snapshot, // never mutated after it is set, so safe to share
	}
	go n.sendInstallSnapshot(p, args)
}

func (n *Node) sendInstallSnapshot(p Peer, args InstallSnapshotArgs) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.SnapshotTimeout)
	defer cancel()
	reply, err := n.snapTransport.InstallSnapshot(ctx, p, args)
	// Always report back, even on failure, so the loop clears the in-flight mark.
	select {
	case n.snapResults <- snapResult{from: p.ID, term: args.Term, args: args, reply: reply, err: err}:
	case <-n.ctx.Done():
	}
}

func (n *Node) handleSnapshotResult(res snapResult) {
	delete(n.snapInFlight, res.from)
	if res.err != nil {
		return // retried on the next heartbeat
	}
	if res.reply.Term > n.currentTerm {
		n.becomeFollower(res.reply.Term)
		return
	}
	if n.state != Leader || res.term != n.currentTerm {
		return
	}
	// The follower now holds everything through LastIncludedIndex, either by
	// installing the snapshot or because it already had those entries committed.
	if li := res.args.LastIncludedIndex; li > n.matchIndex[res.from] {
		n.matchIndex[res.from] = li
	}
	n.nextIndex[res.from] = n.matchIndex[res.from] + 1
	n.advanceCommit()
	if n.nextIndex[res.from] <= n.lastIndex() {
		n.replicateTo(n.peerByID(res.from)) // stream the entries after the snapshot
	}
}

func (n *Node) peerByID(id string) Peer {
	for _, p := range n.peers {
		if p.ID == id {
			return p
		}
	}
	return Peer{ID: id}
}

// persistSnapshot makes the snapshot durable and then the trimmed log. In that
// order: a crash between the two leaves a snapshot plus an untrimmed log, which
// NewNode reconciles; the opposite order would leave a hole in the history.
func (n *Node) persistSnapshot() {
	if n.snapStore != nil {
		s := Snapshot{Index: n.snapIndex, Term: n.snapTerm, Data: n.snapshot}
		if err := n.snapStore.SaveSnapshot(s); err != nil {
			panic(fmt.Sprintf("raft[%s]: persist snapshot failed: %v", n.id, err))
		}
	}
	n.persist()
}

// ---------------------------------------------------------------------------
// Helpers (run loop only)
// ---------------------------------------------------------------------------

func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

// lastIndex is the index of the newest log entry (the snapshot boundary if the
// log holds nothing else).
func (n *Node) lastIndex() uint64 { return n.snapIndex + uint64(len(n.log)-1) }

// entry and termAt translate a raft index to a slot in the compacted log. They
// are valid for snapIndex <= idx <= lastIndex; below snapIndex the entry is gone.
func (n *Node) entry(idx uint64) LogEntry { return n.log[idx-n.snapIndex] }
func (n *Node) termAt(idx uint64) uint64  { return n.log[idx-n.snapIndex].Term }

func (n *Node) lastLog() (index, term uint64) {
	return n.lastIndex(), n.log[len(n.log)-1].Term
}

// persist writes hard state + log. Raft's safety depends on this being durable
// before we reply to RPCs, so a failure is fatal rather than something to
// limp past.
func (n *Node) persist() {
	hs := HardState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}
	if err := n.storage.Save(hs, n.log[1:]); err != nil {
		panic(fmt.Sprintf("raft[%s]: persist failed: %v", n.id, err))
	}
}

func (n *Node) resetElectionTimer() {
	span := int64(n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin)
	d := n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(span))
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(d)
}
