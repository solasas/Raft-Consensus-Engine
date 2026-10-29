// Package dashboard runs a live, in-process Raft cluster on the simulated
// network and exposes it to a browser: cluster state and election events over a
// WebSocket, and chaos controls (sever a link, isolate a node, add loss and
// latency, write data) over a small JSON API.
package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sashanks/raft/kv"
	"github.com/sashanks/raft/raft"
	"github.com/sashanks/raft/transport"
)

// Options configure a Cluster.
type Options struct {
	Nodes             int     // 3-9; default 5
	Slow              float64 // multiplies every Raft timeout so humans can follow elections; default 3
	SnapshotThreshold int     // log compaction threshold; default 30
	Seed              int64   // network randomness; 0 = time-based
	Logger            *slog.Logger
}

// Errors reported to API clients as 4xx.
var (
	ErrUnknownNode = errors.New("unknown node")
	ErrBadRequest  = errors.New("bad request")
	ErrNoLeader    = errors.New("no leader right now")
)

const (
	tailLen      = 12  // log entries shown per node
	historyLen   = 300 // events kept for late-joining browsers
	maxInFlight  = 32  // concurrent background writes
	writeTimeout = 3 * time.Second
)

// Cluster is the simulated cluster plus the bookkeeping the dashboard needs.
type Cluster struct {
	ids   []string
	sw    *transport.Switch
	chaos *transport.Chaos
	svcs  map[string]*kv.Service

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	evMu    sync.Mutex
	history []EventView
	seq     uint64
	onEvent func(EventView)

	writes  atomic.Uint64
	failed  atomic.Uint64
	nextKey atomic.Uint64

	loadMu   sync.Mutex
	loadStop context.CancelFunc
	loadRate int
	inflight chan struct{}
}

// NewCluster builds, but does not start, a cluster.
func NewCluster(o Options) (*Cluster, error) {
	if o.Nodes == 0 {
		o.Nodes = 5
	}
	if o.Nodes < 3 || o.Nodes > 9 {
		return nil, fmt.Errorf("dashboard: Nodes must be between 3 and 9, got %d", o.Nodes)
	}
	if o.Slow == 0 {
		o.Slow = 3
	}
	if o.Slow <= 0 {
		return nil, fmt.Errorf("dashboard: Slow must be positive")
	}
	if o.SnapshotThreshold == 0 {
		o.SnapshotThreshold = 30
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{
		svcs:     map[string]*kv.Service{},
		sw:       transport.NewSwitch(transport.Options{Seed: o.Seed}),
		ctx:      ctx,
		cancel:   cancel,
		inflight: make(chan struct{}, maxInFlight),
	}
	c.chaos = transport.NewChaos(c.sw)
	for i := 1; i <= o.Nodes; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}

	scale := func(ms float64) time.Duration { return time.Duration(ms * o.Slow * float64(time.Millisecond)) }
	for _, id := range c.ids {
		var peers []raft.Peer
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, raft.Peer{ID: other, Addr: "sim://" + other})
			}
		}
		svc, err := kv.NewService(raft.Config{
			ID: id, Peers: peers,
			Logger:             o.Logger,
			SnapshotThreshold:  o.SnapshotThreshold,
			ElectionTimeoutMin: scale(150),
			ElectionTimeoutMax: scale(300),
			HeartbeatInterval:  scale(50),
			RPCTimeout:         scale(100),
			Transport:          transport.AsRaftTransport(c.sw.NewTransport(id, nil)),
			OnEvent:            c.recordRaftEvent,
		})
		if err != nil {
			cancel()
			return nil, err
		}
		c.svcs[id] = svc
		c.sw.Register(id, transport.ServeNode(svc.Node()))
	}
	return c, nil
}

// Start boots every node.
func (c *Cluster) Start() {
	for _, id := range c.ids {
		c.svcs[id].Start()
	}
}

// Stop halts background work and every node.
func (c *Cluster) Stop() {
	_ = c.SetLoad(0)
	c.cancel()
	c.wg.Wait()
	for _, id := range c.ids {
		c.svcs[id].Stop()
	}
	c.sw.Close()
}

// IDs lists the node IDs in order.
func (c *Cluster) IDs() []string { return append([]string(nil), c.ids...) }

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

// EventView is one entry in the live event feed: a Raft transition, or a chaos
// or write action taken through the dashboard.
type EventView struct {
	Seq    uint64 `json:"seq"`
	AtMs   int64  `json:"atMs"`
	Node   string `json:"node"` // "" for cluster-wide actions
	Kind   string `json:"kind"`
	Term   uint64 `json:"term,omitempty"`
	Detail string `json:"detail"`
}

// SetEventHandler registers fn to receive every new event, in order. fn is called
// while a lock is held, from Raft's own goroutines: it must not block or call back
// into the Cluster.
func (c *Cluster) SetEventHandler(fn func(EventView)) {
	c.evMu.Lock()
	c.onEvent = fn
	c.evMu.Unlock()
}

// History returns the most recent events, oldest first.
func (c *Cluster) History() []EventView {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	return append([]EventView(nil), c.history...)
}

func (c *Cluster) recordRaftEvent(e raft.Event) {
	c.emit(e.At, e.Node, string(e.Kind), e.Term, e.Detail)
}

func (c *Cluster) emit(at time.Time, node, kind string, term uint64, detail string) {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	c.seq++
	ev := EventView{Seq: c.seq, AtMs: at.UnixMilli(), Node: node, Kind: kind, Term: term, Detail: detail}
	c.history = append(c.history, ev)
	if len(c.history) > historyLen {
		c.history = append([]EventView(nil), c.history[len(c.history)-historyLen:]...)
	}
	if c.onEvent != nil {
		c.onEvent(ev)
	}
}

func (c *Cluster) action(kind, detail string) { c.emit(time.Now(), "", kind, 0, detail) }

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

// ClusterView is everything the page needs to draw one frame.
type ClusterView struct {
	AtMs   int64      `json:"atMs"`
	Leader string     `json:"leader"` // the leader with the highest term, "" if none
	Nodes  []NodeView `json:"nodes"`
	Links  []LinkView `json:"links"`
	Writes uint64     `json:"writes"`
	Failed uint64     `json:"failed"`
	Load   int        `json:"load"` // background writes per second
}

type NodeView struct {
	ID            string         `json:"id"`
	State         string         `json:"state"`
	Term          uint64         `json:"term"`
	VotedFor      string         `json:"votedFor"`
	LeaderID      string         `json:"leaderId"`
	CommitIndex   uint64         `json:"commitIndex"`
	LastApplied   uint64         `json:"lastApplied"`
	LastLogIndex  uint64         `json:"lastLogIndex"`
	SnapshotIndex uint64         `json:"snapshotIndex"`
	LogEntries    int            `json:"logEntries"`
	Keys          int            `json:"keys"`
	Replication   []PeerView     `json:"replication,omitempty"` // leaders only
	Tail          []LogEntryView `json:"tail"`
}

type PeerView struct {
	ID           string `json:"id"`
	Match        uint64 `json:"match"`
	Next         uint64 `json:"next"`
	Snapshotting bool   `json:"snapshotting"`
}

type LogEntryView struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Cmd       string `json:"cmd"`
	Noop      bool   `json:"noop"`
	Committed bool   `json:"committed"`
}

// LinkView is one DIRECTED link of the switch matrix.
type LinkView struct {
	From         string  `json:"from"`
	To           string  `json:"to"`
	Blocked      bool    `json:"blocked"`
	DropRate     float64 `json:"dropRate"`
	MinLatencyMs float64 `json:"minLatencyMs"`
	MaxLatencyMs float64 `json:"maxLatencyMs"`
}

// View samples every node and the network.
func (c *Cluster) View() ClusterView {
	v := ClusterView{
		AtMs:   time.Now().UnixMilli(),
		Writes: c.writes.Load(),
		Failed: c.failed.Load(),
		Load:   c.Load(),
		Nodes:  []NodeView{},
		Links:  []LinkView{},
	}
	var leaderTerm uint64
	for _, id := range c.ids {
		svc := c.svcs[id]
		in, err := svc.Node().Inspect(tailLen)
		if err != nil {
			continue
		}
		s := in.Status
		nv := NodeView{
			ID: id, State: s.State.String(), Term: s.Term, VotedFor: s.VotedFor, LeaderID: s.LeaderID,
			CommitIndex: s.CommitIndex, LastApplied: s.LastApplied, LastLogIndex: s.LastLogIndex,
			SnapshotIndex: s.SnapshotIndex, LogEntries: s.LogEntries, Keys: svc.Store().Len(),
			Tail: []LogEntryView{},
		}
		for _, p := range in.Peers {
			nv.Replication = append(nv.Replication, PeerView{ID: p.ID, Match: p.Match, Next: p.Next, Snapshotting: p.Snapshotting})
		}
		for _, e := range in.Tail {
			nv.Tail = append(nv.Tail, LogEntryView{
				Index: e.Index, Term: e.Term, Cmd: describeCommand(e), Noop: e.Noop, Committed: e.Index <= s.CommitIndex,
			})
		}
		v.Nodes = append(v.Nodes, nv)
		if s.State == raft.Leader && s.Term >= leaderTerm {
			leaderTerm, v.Leader = s.Term, id
		}
	}
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a == b {
				continue
			}
			p := c.sw.Link(a, b)
			v.Links = append(v.Links, LinkView{
				From: a, To: b, Blocked: p.Blocked, DropRate: p.DropRate,
				MinLatencyMs: float64(p.MinLatency) / float64(time.Millisecond),
				MaxLatencyMs: float64(p.MaxLatency) / float64(time.Millisecond),
			})
		}
	}
	return v
}

// describeCommand renders a log entry's payload for humans.
func describeCommand(e raft.LogEntry) string {
	if e.Noop {
		return "no-op"
	}
	var c kv.Command
	if err := json.Unmarshal(e.Command, &c); err == nil {
		switch c.Op {
		case kv.OpPut:
			return fmt.Sprintf("put %s=%s", c.Key, c.Value)
		case kv.OpDelete:
			return "del " + c.Key
		}
	}
	s := string(e.Command)
	if len(s) > 24 {
		s = s[:24] + "…"
	}
	return s
}

// ---------------------------------------------------------------------------
// Chaos actions
// ---------------------------------------------------------------------------

func (c *Cluster) known(ids ...string) error {
	for _, id := range ids {
		if _, ok := c.svcs[id]; !ok {
			return fmt.Errorf("%w: %q", ErrUnknownNode, id)
		}
	}
	return nil
}

func (c *Cluster) pair(a, b string) error {
	if err := c.known(a, b); err != nil {
		return err
	}
	if a == b {
		return fmt.Errorf("%w: a link needs two different nodes", ErrBadRequest)
	}
	return nil
}

// SeverLink blocks traffic from -> to, and also to -> from if bothWays.
func (c *Cluster) SeverLink(from, to string, bothWays bool) error {
	if err := c.pair(from, to); err != nil {
		return err
	}
	c.chaos.BlockOneWay(from, to)
	if bothWays {
		c.chaos.BlockOneWay(to, from)
		c.action("chaos", fmt.Sprintf("severed link %s ↔ %s", from, to))
	} else {
		c.action("chaos", fmt.Sprintf("severed %s → %s (one way)", from, to))
	}
	return nil
}

// RestoreLink reopens from -> to, and also to -> from if bothWays.
func (c *Cluster) RestoreLink(from, to string, bothWays bool) error {
	if err := c.pair(from, to); err != nil {
		return err
	}
	c.chaos.UnblockOneWay(from, to)
	if bothWays {
		c.chaos.UnblockOneWay(to, from)
		c.action("chaos", fmt.Sprintf("restored link %s ↔ %s", from, to))
	} else {
		c.action("chaos", fmt.Sprintf("restored %s → %s", from, to))
	}
	return nil
}

func (c *Cluster) Isolate(id string) error {
	if err := c.known(id); err != nil {
		return err
	}
	c.chaos.Isolate(id)
	c.action("chaos", fmt.Sprintf("isolated %s from every other node", id))
	return nil
}

func (c *Cluster) Reconnect(id string) error {
	if err := c.known(id); err != nil {
		return err
	}
	c.chaos.Reconnect(id)
	c.action("chaos", fmt.Sprintf("reconnected %s", id))
	return nil
}

// Partition splits the cluster into the given groups; nodes left out are isolated.
func (c *Cluster) Partition(groups [][]string) error {
	seen := map[string]bool{}
	for _, g := range groups {
		if err := c.known(g...); err != nil {
			return err
		}
		for _, id := range g {
			if seen[id] {
				return fmt.Errorf("%w: node %q is in two groups", ErrBadRequest, id)
			}
			seen[id] = true
		}
	}
	if len(groups) < 2 {
		return fmt.Errorf("%w: a partition needs at least two groups", ErrBadRequest)
	}
	c.chaos.Partition(groups...)
	parts := make([]string, len(groups))
	for i, g := range groups {
		parts[i] = "{" + strings.Join(g, ",") + "}"
	}
	c.action("chaos", "partitioned into "+strings.Join(parts, " | "))
	return nil
}

// Heal lifts every partition; latency and loss stay.
func (c *Cluster) Heal() {
	c.chaos.Heal()
	c.action("chaos", "healed all partitions")
}

// Reset returns the network to perfect.
func (c *Cluster) Reset() {
	c.chaos.Reset()
	c.action("chaos", "network reset: no partitions, loss or latency")
}

// SetDropRate loses the given fraction (0-1) of messages on every link.
func (c *Cluster) SetDropRate(rate float64) error {
	if err := c.chaos.SetGlobalDropRate(rate); err != nil {
		return fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	c.action("chaos", fmt.Sprintf("packet loss set to %.0f%%", rate*100))
	return nil
}

// SetLatency adds uniform [min,max] ms delay to every message.
func (c *Cluster) SetLatency(minMs, maxMs float64) error {
	if minMs < 0 || maxMs < minMs || maxMs > 10_000 {
		return fmt.Errorf("%w: latency range [%v, %v] ms", ErrBadRequest, minMs, maxMs)
	}
	ms := func(f float64) time.Duration { return time.Duration(f * float64(time.Millisecond)) }
	if err := c.chaos.SetGlobalLatency(ms(minMs), ms(maxMs)); err != nil {
		return fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	c.action("chaos", fmt.Sprintf("latency set to %.0f-%.0f ms", minMs, maxMs))
	return nil
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

// leaderService returns the leader with the highest term, as a client would find it.
func (c *Cluster) leaderService() (string, *kv.Service) {
	var best string
	var bestTerm uint64
	for _, id := range c.ids {
		if s, err := c.svcs[id].Node().Status(); err == nil && s.State == raft.Leader && s.Term >= bestTerm {
			best, bestTerm = id, s.Term
		}
	}
	if best == "" {
		return "", nil
	}
	return best, c.svcs[best]
}

// Put writes key=value asynchronously, to the named node or (target "") to the
// current leader. Writing to a stale, isolated leader is useful: its entry is
// appended but can never commit. The outcome is reported in the event feed.
func (c *Cluster) Put(target, key, value string) error {
	if key == "" || len(key) > 64 || len(value) > 256 {
		return fmt.Errorf("%w: key must be 1-64 bytes and value at most 256", ErrBadRequest)
	}
	id, svc := target, (*kv.Service)(nil)
	if target == "" {
		if id, svc = c.leaderService(); svc == nil {
			return ErrNoLeader
		}
	} else if err := c.known(target); err != nil {
		return err
	} else {
		svc = c.svcs[target]
	}
	c.goWrite(id, svc, key, value, true)
	return nil
}

// goWrite performs one write in the background. announce controls whether its
// outcome appears in the event feed (bulk writers stay quiet).
func (c *Cluster) goWrite(id string, svc *kv.Service, key, value string, announce bool) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
		defer cancel()
		err := svc.Put(ctx, key, value)
		if err != nil {
			c.failed.Add(1)
			if announce && c.ctx.Err() == nil {
				c.emit(time.Now(), id, "write-failed", 0, fmt.Sprintf("put %s=%s: %s", key, value, shortErr(err)))
			}
			return
		}
		c.writes.Add(1)
		if announce {
			c.emit(time.Now(), id, "write-ok", 0, fmt.Sprintf("put %s=%s committed", key, value))
		}
	}()
}

func shortErr(err error) string {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		if nl.LeaderID != "" {
			return "not the leader (try " + nl.LeaderID + ")"
		}
		return "not the leader"
	case errors.Is(err, context.DeadlineExceeded):
		return "timed out: no quorum could commit it"
	case errors.Is(err, kv.ErrNotCommitted):
		return "entry was overwritten by a newer leader"
	default:
		return err.Error()
	}
}

func (c *Cluster) autoWrite() (string, string) {
	n := c.nextKey.Add(1)
	return fmt.Sprintf("k%d", n%20), fmt.Sprintf("%d", n)
}

// Burst writes `count` values to the leader as fast as they commit.
func (c *Cluster) Burst(count int) error {
	if count < 1 || count > 500 {
		return fmt.Errorf("%w: burst size must be 1-500", ErrBadRequest)
	}
	c.action("write", fmt.Sprintf("burst of %d writes started", count))
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ok := 0
		for i := 0; i < count && c.ctx.Err() == nil; i++ {
			_, svc := c.leaderService()
			if svc == nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			key, val := c.autoWrite()
			ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := svc.Put(ctx, key, val)
			cancel()
			if err != nil {
				c.failed.Add(1)
				continue
			}
			c.writes.Add(1)
			ok++
		}
		if c.ctx.Err() == nil {
			c.action("write", fmt.Sprintf("burst finished: %d of %d committed", ok, count))
		}
	}()
	return nil
}

// Load returns the current background write rate (per second).
func (c *Cluster) Load() int {
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	return c.loadRate
}

// SetLoad runs a steady stream of writes at `rate` per second against whichever
// node is leader. 0 stops it. Writes in flight are capped, so a cluster that
// cannot commit (no quorum) shows up as failures instead of unbounded growth.
func (c *Cluster) SetLoad(rate int) error {
	if rate < 0 || rate > 100 {
		return fmt.Errorf("%w: load must be 0-100 writes per second", ErrBadRequest)
	}
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	if c.loadStop != nil {
		c.loadStop()
		c.loadStop = nil
	}
	c.loadRate = rate
	if rate == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(c.ctx)
	c.loadStop = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(time.Second / time.Duration(rate))
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			select {
			case c.inflight <- struct{}{}:
			default:
				c.failed.Add(1) // the cluster is not keeping up
				continue
			}
			_, svc := c.leaderService()
			if svc == nil {
				<-c.inflight
				c.failed.Add(1)
				continue
			}
			key, val := c.autoWrite()
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				defer func() { <-c.inflight }()
				wctx, wcancel := context.WithTimeout(ctx, writeTimeout)
				defer wcancel()
				if err := svc.Put(wctx, key, val); err != nil {
					if ctx.Err() == nil {
						c.failed.Add(1)
					}
					return
				}
				c.writes.Add(1)
			}()
		}
	}()
	c.action("write", fmt.Sprintf("background load set to %d writes/s", rate))
	return nil
}
