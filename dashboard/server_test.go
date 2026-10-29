package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sashanks/raft/dashboard/ws/wstest"
)

type env struct {
	c   *Cluster
	srv *Server
	ts  *httptest.Server
}

func newEnv(t *testing.T) *env {
	t.Helper()
	c := newTestCluster(t, 3)
	srv := NewServer(c, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Run(ctx)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); cancel() })
	return &env{c: c, srv: srv, ts: ts}
}

// call sends a request carrying the anti-CSRF header unless headers removes it
// (set a header to "-" to omit it).
func (e *env) call(t *testing.T, method, path, body string, headers map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "raftviz")
	for k, v := range headers {
		if v == "-" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (e *env) state(t *testing.T) ClusterView {
	t.Helper()
	code, body := e.call(t, "GET", "/api/state", "", nil)
	if code != 200 {
		t.Fatalf("GET /api/state = %d: %s", code, body)
	}
	var v ClusterView
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("bad state JSON: %v\n%s", err, body)
	}
	return v
}

func TestServesThePageWithSecurityHeaders(t *testing.T) {
	e := newEnv(t)
	resp, err := http.Get(e.ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("status %d type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if !bytes.Contains(body, []byte("Raft Control Room")) || !bytes.Contains(body, []byte("Sever Network Link")) {
		t.Error("embedded page is missing expected content")
	}
	if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") {
		t.Errorf("CSP = %q", csp)
	}
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("missing nosniff")
	}

	if code, _ := e.call(t, "GET", "/nope", "", nil); code != 404 {
		t.Errorf("unknown path = %d, want 404", code)
	}
	if code, body := e.call(t, "GET", "/healthz", "", nil); code != 200 || body != "ok\n" {
		t.Errorf("healthz = %d %q", code, body)
	}
}

func TestStateEndpointReturnsTheClusterView(t *testing.T) {
	e := newEnv(t)
	eventually(t, "a leader", func() bool { return e.state(t).Leader != "" })
	v := e.state(t)
	if len(v.Nodes) != 3 || len(v.Links) != 6 {
		t.Errorf("%d nodes, %d links; want 3 and 6", len(v.Nodes), len(v.Links))
	}
}

func TestStateChangingRequestsRefuseCrossSiteCallers(t *testing.T) {
	e := newEnv(t)
	const sever = `{"from":"n1","to":"n2","both":true}`

	tests := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"missing anti-CSRF header", map[string]string{"X-Requested-With": "-"}, http.StatusForbidden},
		{"wrong anti-CSRF header", map[string]string{"X-Requested-With": "XMLHttpRequest"}, http.StatusForbidden},
		{"foreign Origin", map[string]string{"Origin": "http://evil.example"}, http.StatusForbidden},
		{"foreign Origin with the right header", map[string]string{"Origin": "https://attacker.test:8443"}, http.StatusForbidden},
	}
	for _, tc := range tests {
		code, _ := e.call(t, "POST", "/api/links/sever", sever, tc.headers)
		if code != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, code, tc.want)
		}
	}
	if linkOf(e.state(t), "n1", "n2").Blocked {
		t.Fatal("a refused request still severed the link")
	}

	// A same-origin browser request is accepted.
	origin := strings.TrimPrefix(e.ts.URL, "http://")
	if code, body := e.call(t, "POST", "/api/links/sever", sever, map[string]string{"Origin": "http://" + origin}); code != 200 {
		t.Fatalf("same-origin request refused: %d %s", code, body)
	}
	if !linkOf(e.state(t), "n1", "n2").Blocked {
		t.Error("the accepted request did not sever the link")
	}
}

func TestSeverAndRestoreOverHTTP(t *testing.T) {
	e := newEnv(t)
	code, body := e.call(t, "POST", "/api/links/sever", `{"from":"n1","to":"n2","both":true}`, nil)
	if code != 200 || strings.TrimSpace(body) != `{"ok":true}` {
		t.Fatalf("sever = %d %s", code, body)
	}
	v := e.state(t)
	if !linkOf(v, "n1", "n2").Blocked || !linkOf(v, "n2", "n1").Blocked || linkOf(v, "n1", "n3").Blocked {
		t.Error("sever did not produce exactly the n1-n2 cut")
	}

	if code, _ := e.call(t, "POST", "/api/links/restore", `{"from":"n1","to":"n2","both":true}`, nil); code != 200 {
		t.Fatalf("restore = %d", code)
	}
	if linkOf(e.state(t), "n1", "n2").Blocked {
		t.Error("restore did not reopen the link")
	}
}

func TestEveryControlEndpointWorks(t *testing.T) {
	e := newEnv(t)
	eventually(t, "a leader", func() bool { return e.state(t).Leader != "" })
	// Writes go first: they need a leader, which the chaos actions below may
	// briefly take away.
	for _, tc := range []struct{ path, body string }{
		{"/api/kv/put", `{"target":"","key":"a","value":"1"}`},
		{"/api/kv/burst", `{"count":5}`},
		{"/api/load", `{"rate":10}`},
		{"/api/load", `{"rate":0}`},
		{"/api/nodes/isolate", `{"id":"n3"}`},
		{"/api/nodes/reconnect", `{"id":"n3"}`},
		{"/api/chaos/partition", `{"groups":[["n1","n2"],["n3"]]}`},
		{"/api/chaos/heal", ``},
		{"/api/chaos/drop", `{"rate":0.1}`},
		{"/api/chaos/latency", `{"minMs":5,"maxMs":15}`},
		{"/api/chaos/reset", ``},
	} {
		if code, body := e.call(t, "POST", tc.path, tc.body, nil); code != 200 {
			t.Errorf("%s %s = %d %s", tc.path, tc.body, code, body)
		}
	}
}

func TestBadRequestsGetClearErrors(t *testing.T) {
	e := newEnv(t)
	tests := []struct {
		name, method, path, body string
		want                     int
		errContains              string
	}{
		{"unknown node", "POST", "/api/links/sever", `{"from":"n1","to":"zz","both":true}`, 400, "unknown node"},
		{"link to self", "POST", "/api/links/sever", `{"from":"n1","to":"n1"}`, 400, "two different nodes"},
		{"malformed JSON", "POST", "/api/links/sever", `{"from":`, 400, "bad request"},
		{"unknown field", "POST", "/api/links/sever", `{"from":"n1","to":"n2","bogus":1}`, 400, "unknown field"},
		{"drop rate out of range", "POST", "/api/chaos/drop", `{"rate":3}`, 400, "outside [0, 1]"},
		{"empty body where one is needed", "POST", "/api/nodes/isolate", ``, 400, "bad request"},
		{"wrong method", "GET", "/api/links/sever", ``, 405, ""},
		{"POST to a read-only route", "POST", "/api/state", `{}`, 405, ""},
	}
	for _, tc := range tests {
		code, body := e.call(t, tc.method, tc.path, tc.body, nil)
		if code != tc.want {
			t.Errorf("%s: status %d, want %d (%s)", tc.name, code, tc.want, body)
		}
		if tc.errContains != "" && !strings.Contains(body, tc.errContains) {
			t.Errorf("%s: body %q does not mention %q", tc.name, body, tc.errContains)
		}
	}
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

type wsMessage struct {
	Type    string       `json:"type"`
	State   *ClusterView `json:"state"`
	Event   *EventView   `json:"event"`
	History []EventView  `json:"history"`
}

func readMsg(t *testing.T, c *wstest.Client) wsMessage {
	t.Helper()
	s, err := c.ReadText(5 * time.Second)
	if err != nil {
		t.Fatalf("websocket read: %v", err)
	}
	var m wsMessage
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("bad websocket JSON: %v\n%s", err, s)
	}
	return m
}

// readUntil reads messages until pred is satisfied or 8 seconds pass.
func readUntil(t *testing.T, c *wstest.Client, what string, pred func(wsMessage) bool) wsMessage {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if m := readMsg(t, c); pred(m) {
			return m
		}
	}
	t.Fatalf("websocket: timed out waiting for %s", what)
	return wsMessage{}
}

func TestWebSocketStreamsHelloStateAndEvents(t *testing.T) {
	e := newEnv(t)
	eventually(t, "a leader before connecting", func() bool { return e.c.View().Leader != "" })

	c, err := wstest.Dial(e.ts.URL+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// 1. hello: full state plus the history of what already happened.
	hello := readUntil(t, c, "hello", func(m wsMessage) bool { return m.Type == "hello" })
	if hello.State == nil || len(hello.State.Nodes) != 3 || hello.State.Leader == "" {
		t.Fatalf("hello state = %+v", hello.State)
	}
	var sawLeader bool
	for _, ev := range hello.History {
		sawLeader = sawLeader || ev.Kind == "leader"
	}
	if !sawLeader {
		t.Errorf("hello history lacks the election that already happened: %+v", hello.History)
	}

	// 2. a stream of states, ten per second.
	readUntil(t, c, "a state message", func(m wsMessage) bool { return m.Type == "state" && m.State != nil && len(m.State.Nodes) == 3 })

	// 3. a chaos action over HTTP arrives as a live event, and shows up in the next state.
	if code, body := e.call(t, "POST", "/api/nodes/isolate", `{"id":"n1"}`, nil); code != 200 {
		t.Fatalf("isolate = %d %s", code, body)
	}
	ev := readUntil(t, c, "the chaos event", func(m wsMessage) bool {
		return m.Type == "event" && m.Event != nil && m.Event.Kind == "chaos" && strings.Contains(m.Event.Detail, "isolated n1")
	})
	if ev.Event.Seq == 0 || ev.Event.AtMs == 0 {
		t.Errorf("event lacks a sequence number or timestamp: %+v", ev.Event)
	}
	readUntil(t, c, "a state with n1 cut off", func(m wsMessage) bool {
		return m.Type == "state" && m.State != nil && linkOf(*m.State, "n1", "n2").Blocked && linkOf(*m.State, "n3", "n1").Blocked
	})
}

func TestWebSocketRefusesForeignOrigins(t *testing.T) {
	e := newEnv(t)
	_, err := wstest.Dial(e.ts.URL+"/ws", http.Header{"Origin": {"http://evil.example"}})
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("err = %v, want a 403 refusal", err)
	}
}

func TestSeveralBrowsersAllReceiveEvents(t *testing.T) {
	e := newEnv(t)
	var clients []*wstest.Client
	for i := 0; i < 3; i++ {
		c, err := wstest.Dial(e.ts.URL+"/ws", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		readUntil(t, c, "hello", func(m wsMessage) bool { return m.Type == "hello" })
		clients = append(clients, c)
	}

	e.c.Heal()
	for _, c := range clients {
		readUntil(t, c, "the heal event", func(m wsMessage) bool {
			return m.Type == "event" && m.Event != nil && strings.Contains(m.Event.Detail, "healed")
		})
	}
}

func TestDisconnectedBrowsersAreForgotten(t *testing.T) {
	e := newEnv(t)
	c, err := wstest.Dial(e.ts.URL+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	readUntil(t, c, "hello", func(m wsMessage) bool { return m.Type == "hello" })

	if e.srv.hub.count() != 1 {
		t.Fatalf("hub has %d clients while one is connected", e.srv.hub.count())
	}
	c.Close()
	// The server's read loop sees the disconnect and drops the client.
	eventually(t, "the hub to forget the closed browser", func() bool { return e.srv.hub.count() == 0 })
}
