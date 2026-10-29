package dashboard

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/sashanks/raft/dashboard/ws"
)

// tickInterval is how often a fresh ClusterView is pushed to connected browsers.
const tickInterval = 100 * time.Millisecond

// message is the envelope for everything sent to the browser.
//
//	hello  - first message on a connection: current state plus recent event history
//	state  - a fresh ClusterView, every tickInterval
//	event  - one new entry for the feed, pushed the instant it happens
type message struct {
	Type    string       `json:"type"`
	State   *ClusterView `json:"state,omitempty"`
	Event   *EventView   `json:"event,omitempty"`
	History []EventView  `json:"history,omitempty"`
}

type client struct {
	conn *ws.Conn
	send chan []byte   // never closed; done signals shutdown instead
	done chan struct{} // closed exactly once, by drop
	once sync.Once
}

// Hub fans cluster state and events out to every connected browser.
//
// A browser that cannot keep up is disconnected rather than allowed to hold up
// the sender: events are published from inside Raft's own goroutines, which must
// never block on a slow network peer. The page reconnects on its own and gets a
// fresh hello.
type Hub struct {
	cluster *Cluster
	log     *slog.Logger

	mu      sync.Mutex
	clients map[*client]struct{}
}

func newHub(c *Cluster, log *slog.Logger) *Hub {
	h := &Hub{cluster: c, log: log, clients: map[*client]struct{}{}}
	c.SetEventHandler(h.onEvent)
	return h
}

// onEvent runs on Raft goroutines (see Cluster.SetEventHandler): marshal, queue, return.
func (h *Hub) onEvent(ev EventView) {
	b, err := json.Marshal(message{Type: "event", Event: &ev})
	if err != nil {
		return
	}
	h.broadcast(b)
}

// broadcast queues b for every client without ever blocking.
func (h *Hub) broadcast(b []byte) {
	var slow []*client
	h.mu.Lock()
	for cl := range h.clients {
		select {
		case cl.send <- b:
		default:
			slow = append(slow, cl)
		}
	}
	h.mu.Unlock()
	for _, cl := range slow {
		go h.drop(cl) // closing a socket can take time; never do it on the caller's goroutine
	}
}

func (h *Hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *Hub) drop(cl *client) {
	cl.once.Do(func() {
		h.mu.Lock()
		delete(h.clients, cl)
		h.mu.Unlock()
		close(cl.done)
		_ = cl.conn.Close()
	})
}

// run pushes a ClusterView to everyone every tickInterval until ctx ends. It
// samples the cluster only while someone is watching.
func (h *Hub) run(ctx context.Context) {
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if h.count() == 0 {
			continue
		}
		v := h.cluster.View()
		b, err := json.Marshal(message{Type: "state", State: &v})
		if err != nil {
			continue
		}
		h.broadcast(b)
	}
}

// serveWS upgrades the request and streams to the browser until it goes away.
func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request) {
	// Browsers do not apply the same-origin policy to WebSockets, so any page the
	// user visits could otherwise open this stream. Refuse foreign origins.
	if !sameOrigin(r) {
		http.Error(w, "cross-origin websocket refused", http.StatusForbidden)
		return
	}
	conn, err := ws.Upgrade(w, r)
	if err != nil {
		return // Upgrade already answered the request
	}
	cl := &client{conn: conn, send: make(chan []byte, 64), done: make(chan struct{})}

	// Register before sampling, so no event can fall between the history snapshot
	// and the first live one. The page de-duplicates by sequence number.
	h.mu.Lock()
	h.clients[cl] = struct{}{}
	h.mu.Unlock()

	view := h.cluster.View()
	hello, err := json.Marshal(message{Type: "hello", State: &view, History: h.cluster.History()})
	if err == nil {
		select {
		case cl.send <- hello:
		default:
		}
	}
	go h.writer(cl)

	// The page never sends anything meaningful; reading keeps pings, pongs and the
	// closing handshake working, and tells us when the browser has gone.
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
	h.drop(cl)
}

func (h *Hub) writer(cl *client) {
	for {
		select {
		case b := <-cl.send:
			if err := cl.conn.WriteText(b); err != nil {
				h.drop(cl)
				return
			}
		case <-cl.done:
			return
		}
	}
}
