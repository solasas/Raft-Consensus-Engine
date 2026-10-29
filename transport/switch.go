package transport

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sashanks/raft/raft"
)

// Link names a DIRECTED edge in the switch matrix: traffic flowing From -> To.
// A request from A to B crosses Link{A,B}; its reply crosses Link{B,A}.
type Link struct{ From, To string }

// LinkPolicy is the fault profile of one directed link.
type LinkPolicy struct {
	Blocked    bool          // hard partition: nothing passes
	DropRate   float64       // probability in [0,1] that a message is lost
	MinLatency time.Duration // delay is uniform in [MinLatency, MaxLatency]
	MaxLatency time.Duration
}

// String summarises the active faults, e.g. "drop=30% latency=100ms-500ms".
func (p LinkPolicy) String() string {
	var parts []string
	if p.Blocked {
		parts = append(parts, "blocked")
	}
	if p.DropRate > 0 {
		parts = append(parts, fmt.Sprintf("drop=%.4g%%", p.DropRate*100))
	}
	if p.MaxLatency > 0 || p.MinLatency > 0 {
		parts = append(parts, fmt.Sprintf("latency=%v-%v", p.MinLatency, p.MaxLatency))
	}
	if len(parts) == 0 {
		return "ok"
	}
	return strings.Join(parts, " ")
}

type Direction uint8

const (
	DirRequest Direction = iota
	DirResponse
)

func (d Direction) String() string {
	if d == DirRequest {
		return "request"
	}
	return "response"
}

type Outcome uint8

const (
	OutcomeDelivered Outcome = iota // passed the switch
	OutcomeBlocked
	OutcomeDropped
	OutcomeCanceled // the switch was closed while the message was in flight
)

func (o Outcome) String() string {
	return [...]string{"delivered", "blocked", "dropped", "canceled"}[o]
}

// Event describes one message crossing one link. Observers see every leg of
// every RPC, including the ones the switch kills.
type Event struct {
	From, To string
	Kind     string // raft.RPC.RPCName()
	Dir      Direction
	Outcome  Outcome
	Delay    time.Duration // latency injected (zero if the message never got that far)
}

// Options configure a Switch.
type Options struct {
	// Seed makes random drops and jitter reproducible per call order. 0 = time-based.
	Seed int64
	// SilentDrops makes lost messages behave like a real network: the caller gets
	// no error and simply waits until its context expires. By default a lost
	// message fails fast, which keeps tests quick.
	SilentDrops bool
}

// errSilent marks a lost message in SilentDrops mode: nobody is told.
var errSilent = errors.New("transport: message lost silently")

// Switch is the central switch matrix. Every message between simulated nodes
// passes through route(), which consults the policy of the directed link it is
// crossing. A chaos engine (see Chaos) mutates the matrix at runtime; the data
// path only ever reads it.
//
// A message in flight is independent of the caller that sent it: if the caller
// gives up (its context expires) while the message is still being delayed, the
// message is still delivered later, as on a real network. That is what lets
// latency faults produce stale, reordered RPCs and not just losses.
type Switch struct {
	opts   Options
	ctx    context.Context // cancelled by Close: ends every in-flight message
	cancel context.CancelFunc

	mu       sync.RWMutex
	def      LinkPolicy // applies to any link without an explicit entry
	links    map[Link]LinkPolicy
	members  map[string]struct{}
	handlers map[string]Handler
	tap      func(Event)

	rngMu sync.Mutex
	rng   *rand.Rand
}

func NewSwitch(opts Options) *Switch {
	seed := opts.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Switch{
		opts:     opts,
		ctx:      ctx,
		cancel:   cancel,
		links:    make(map[Link]LinkPolicy),
		members:  make(map[string]struct{}),
		handlers: make(map[string]Handler),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Close discards every message still in flight. Call it when the test is over.
func (s *Switch) Close() { s.cancel() }

// Register attaches the receiving side of node id to the switch.
func (s *Switch) Register(id string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members[id] = struct{}{}
	s.handlers[id] = h
}

// NewTransport returns the sending side for node `self`. If inner is nil,
// messages are delivered to handlers registered on this switch. Otherwise the
// switch only filters traffic and inner carries what survives, which lets the
// same chaos rules wrap a real network transport.
func (s *Switch) NewTransport(self string, inner Transport) *SimulatedTransport {
	s.mu.Lock()
	s.members[self] = struct{}{}
	s.mu.Unlock()
	return &SimulatedTransport{self: self, sw: s, inner: inner}
}

// Members lists every node known to the switch, sorted.
func (s *Switch) Members() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.members))
	for m := range s.members {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// SetTap installs an observer called synchronously for every message leg. It
// must be fast and safe for concurrent use. nil removes it.
func (s *Switch) SetTap(fn func(Event)) {
	s.mu.Lock()
	s.tap = fn
	s.mu.Unlock()
}

// ---- matrix control plane ----

// Link returns the effective policy for a directed link.
func (s *Switch) Link(from, to string) LinkPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.links[Link{from, to}]; ok {
		return p
	}
	return s.def
}

// Default returns the policy used by links with no explicit entry.
func (s *Switch) Default() LinkPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.def
}

// SetLink replaces the policy of one directed link.
func (s *Switch) SetLink(from, to string, p LinkPolicy) {
	s.mu.Lock()
	s.links[Link{from, to}] = p
	s.mu.Unlock()
}

// UpdateLink atomically modifies one directed link, starting from its current
// effective policy.
func (s *Switch) UpdateLink(from, to string, fn func(*LinkPolicy)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := Link{from, to}
	p, ok := s.links[k]
	if !ok {
		p = s.def
	}
	fn(&p)
	s.links[k] = p
}

// UpdateAll applies fn to the default policy and every explicit link entry.
func (s *Switch) UpdateAll(fn func(*LinkPolicy)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.def)
	for k, p := range s.links {
		fn(&p)
		s.links[k] = p
	}
}

// SetDefault sets the policy used by links with no explicit entry.
func (s *Switch) SetDefault(p LinkPolicy) {
	s.mu.Lock()
	s.def = p
	s.mu.Unlock()
}

// ClearBlocked lifts every hard partition but leaves latency and loss alone.
func (s *Switch) ClearBlocked() {
	s.UpdateAll(func(p *LinkPolicy) { p.Blocked = false })
}

// Reset returns the whole matrix to a perfect network.
func (s *Switch) Reset() {
	s.mu.Lock()
	s.def = LinkPolicy{}
	s.links = make(map[Link]LinkPolicy)
	s.mu.Unlock()
}

// Snapshot copies the explicit link entries, for debugging and assertions.
func (s *Switch) Snapshot() map[Link]LinkPolicy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[Link]LinkPolicy, len(s.links))
	for k, v := range s.links {
		out[k] = v
	}
	return out
}

// ---- data plane ----

func (s *Switch) handler(id string) Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.handlers[id]
}

// route carries one RPC on behalf of a caller. The journey runs in its own
// goroutine, so the caller can stop waiting (context expiry) without stopping
// the message.
func (s *Switch) route(ctx context.Context, from, to string, msg raft.RPC,
	next func(context.Context) (raft.RPC, error)) (raft.RPC, error) {

	type result struct {
		resp raft.RPC
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := s.carry(ctx, from, to, msg, next)
		if errors.Is(err, errSilent) {
			return // lost without a trace: the caller only ever sees its own timeout
		}
		done <- result{resp, err}
	}()

	select {
	case r := <-done:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// carry is the full round trip: request leg, delivery via next, reply leg. The
// legs are judged independently against the matrix, each on its own direction,
// so a one-way fault yields the realistic "request arrived and was executed,
// but the caller never heard back" case.
func (s *Switch) carry(ctx context.Context, from, to string, msg raft.RPC,
	next func(context.Context) (raft.RPC, error)) (raft.RPC, error) {

	if err := s.leg(from, to, msg, DirRequest); err != nil {
		return nil, err
	}
	resp, err := next(ctx)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, ErrBadResponse
	}
	if err := s.leg(to, from, resp, DirResponse); err != nil {
		return nil, err
	}
	return resp, nil
}

// leg subjects one message to the policy of the directed link it crosses.
func (s *Switch) leg(from, to string, msg raft.RPC, dir Direction) error {
	p := s.Link(from, to)
	ev := Event{From: from, To: to, Kind: msg.RPCName(), Dir: dir}

	if p.Blocked {
		ev.Outcome = OutcomeBlocked
		s.emit(ev)
		return s.lose(ErrBlocked)
	}
	if p.DropRate > 0 && s.roll() < p.DropRate {
		ev.Outcome = OutcomeDropped
		s.emit(ev)
		return s.lose(ErrDropped)
	}

	ev.Delay = s.jitter(p)
	if err := s.sleep(ev.Delay); err != nil {
		ev.Outcome = OutcomeCanceled
		s.emit(ev)
		return err
	}
	ev.Outcome = OutcomeDelivered
	s.emit(ev)
	return nil
}

func (s *Switch) lose(err error) error {
	if s.opts.SilentDrops {
		return errSilent
	}
	return err
}

func (s *Switch) emit(ev Event) {
	s.mu.RLock()
	tap := s.tap
	s.mu.RUnlock()
	if tap != nil {
		tap(ev)
	}
}

func (s *Switch) roll() float64 {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return s.rng.Float64()
}

func (s *Switch) jitter(p LinkPolicy) time.Duration {
	if p.MaxLatency <= p.MinLatency {
		return max(p.MinLatency, 0)
	}
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return p.MinLatency + time.Duration(s.rng.Int63n(int64(p.MaxLatency-p.MinLatency)+1))
}

// sleep waits out an injected delay, ending early only if the switch is closed.
func (s *Switch) sleep(d time.Duration) error {
	if d <= 0 {
		return s.ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// SimulatedTransport is the sending side of one node on a Switch. It implements
// Transport, so it can be handed to the Raft node via AsRaftTransport.
type SimulatedTransport struct {
	self  string
	sw    *Switch
	inner Transport
}

// SendRPC sends msg to peerID through the switch. It returns when the reply
// arrives or ctx ends, whichever is first; a message still in flight when ctx
// ends is delivered anyway and its reply discarded.
//
// With an inner transport, the inner call is made with the caller's ctx, so it
// is abandoned if the caller has already given up by the time the (delayed)
// message reaches it.
func (t *SimulatedTransport) SendRPC(ctx context.Context, peerID string, msg raft.RPC) (raft.RPC, error) {
	var next func(context.Context) (raft.RPC, error)
	if t.inner != nil {
		next = func(c context.Context) (raft.RPC, error) { return t.inner.SendRPC(c, peerID, msg) }
	} else {
		h := t.sw.handler(peerID)
		if h == nil {
			return nil, ErrUnknownPeer
		}
		next = func(context.Context) (raft.RPC, error) { return h.HandleRPC(msg) }
	}
	return t.sw.route(ctx, t.self, peerID, msg, next)
}
