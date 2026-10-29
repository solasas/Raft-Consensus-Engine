package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
)

//go:embed web/index.html
var indexHTML []byte

// Server is the HTTP face of a Cluster:
//
//	GET  /              the single-page UI
//	GET  /ws            WebSocket: hello, then state (10 Hz) and event messages
//	GET  /api/state     the same ClusterView as one JSON document
//	POST /api/...       chaos and write controls (see routes)
//
// State-changing requests must carry the header "X-Requested-With: raftviz" and,
// when the browser sends an Origin, it must match the Host. Both checks stop a
// web page on another origin from driving a dashboard bound to localhost.
type Server struct {
	cluster *Cluster
	hub     *Hub
	mux     *http.ServeMux
	log     *slog.Logger
}

func NewServer(c *Cluster, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Server{cluster: c, hub: newHub(c, log), mux: http.NewServeMux(), log: log}
	s.routes()
	return s
}

// Handler returns the routes, ready to serve.
func (s *Server) Handler() http.Handler { return s.mux }

// Run pushes state to WebSocket clients until ctx ends. Call it in a goroutine.
func (s *Server) Run(ctx context.Context) { s.hub.run(ctx) }

func (s *Server) routes() {
	m := s.mux
	m.HandleFunc("GET /{$}", s.index)
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	m.HandleFunc("GET /ws", s.hub.serveWS)
	m.HandleFunc("GET /api/state", s.state)

	m.HandleFunc("POST /api/links/sever", s.api(func(r *http.Request) error {
		var b struct {
			From string `json:"from"`
			To   string `json:"to"`
			Both bool   `json:"both"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.SeverLink(b.From, b.To, b.Both)
	}))
	m.HandleFunc("POST /api/links/restore", s.api(func(r *http.Request) error {
		var b struct {
			From string `json:"from"`
			To   string `json:"to"`
			Both bool   `json:"both"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.RestoreLink(b.From, b.To, b.Both)
	}))
	m.HandleFunc("POST /api/nodes/isolate", s.api(func(r *http.Request) error {
		var b struct {
			ID string `json:"id"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.Isolate(b.ID)
	}))
	m.HandleFunc("POST /api/nodes/reconnect", s.api(func(r *http.Request) error {
		var b struct {
			ID string `json:"id"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.Reconnect(b.ID)
	}))
	m.HandleFunc("POST /api/chaos/partition", s.api(func(r *http.Request) error {
		var b struct {
			Groups [][]string `json:"groups"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.Partition(b.Groups)
	}))
	m.HandleFunc("POST /api/chaos/heal", s.api(func(*http.Request) error { s.cluster.Heal(); return nil }))
	m.HandleFunc("POST /api/chaos/reset", s.api(func(*http.Request) error { s.cluster.Reset(); return nil }))
	m.HandleFunc("POST /api/chaos/drop", s.api(func(r *http.Request) error {
		var b struct {
			Rate float64 `json:"rate"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.SetDropRate(b.Rate)
	}))
	m.HandleFunc("POST /api/chaos/latency", s.api(func(r *http.Request) error {
		var b struct {
			MinMs float64 `json:"minMs"`
			MaxMs float64 `json:"maxMs"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.SetLatency(b.MinMs, b.MaxMs)
	}))
	m.HandleFunc("POST /api/kv/put", s.api(func(r *http.Request) error {
		var b struct {
			Target string `json:"target"`
			Key    string `json:"key"`
			Value  string `json:"value"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.Put(b.Target, b.Key, b.Value)
	}))
	m.HandleFunc("POST /api/kv/burst", s.api(func(r *http.Request) error {
		var b struct {
			Count int `json:"count"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.Burst(b.Count)
	}))
	m.HandleFunc("POST /api/load", s.api(func(r *http.Request) error {
		var b struct {
			Rate int `json:"rate"`
		}
		if err := decode(r, &b); err != nil {
			return err
		}
		return s.cluster.SetLoad(b.Rate)
	}))
}

func (s *Server) index(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	// The page is one self-contained file: inline script and style, no third-party loads.
	h.Set("Content-Security-Policy",
		"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self' ws: wss:; base-uri 'none'; form-action 'none'")
	_, _ = w.Write(indexHTML)
}

func (s *Server) state(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.cluster.View())
}

// api wraps a state-changing handler with the cross-site checks, a body size
// limit and uniform JSON responses.
func (s *Server) api(h func(*http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !sameOrigin(r) || r.Header.Get("X-Requested-With") != "raftviz" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "cross-site request refused"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
		if err := h(r); err != nil {
			status := http.StatusInternalServerError
			switch {
			case errors.Is(err, ErrUnknownNode), errors.Is(err, ErrBadRequest):
				status = http.StatusBadRequest
			case errors.Is(err, ErrNoLeader):
				status = http.StatusConflict
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

// decode reads a JSON body strictly: unknown fields are an error, so a typo in
// a client shows up as a 400 instead of a silently ignored setting.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// sameOrigin allows requests that carry no Origin (curl, server-to-server) and
// browser requests whose Origin names this very host.
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}
