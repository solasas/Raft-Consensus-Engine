package ws

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sashanks/raft/dashboard/ws/wstest"
)

// echoServer upgrades every request and echoes text/binary messages back until
// the connection closes. Read errors are delivered on the returned channel.
func echoServer(t *testing.T) (*httptest.Server, <-chan error) {
	t.Helper()
	errs := make(chan error, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			op, data, err := c.ReadMessage()
			if err != nil {
				errs <- err
				return
			}
			if op == OpBinary {
				_ = c.WriteBinary(data)
			} else {
				_ = c.WriteText(data)
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, errs
}

func dial(t *testing.T, srv *httptest.Server) *wstest.Client {
	t.Helper()
	c, err := wstest.Dial(srv.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// The worked example in RFC 6455 §1.3.
func TestAcceptKeyMatchesTheRFCExample(t *testing.T) {
	if got, want := AcceptKey("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="; got != want {
		t.Errorf("AcceptKey = %q, want %q", got, want)
	}
}

func TestEchoAcrossAllLengthEncodings(t *testing.T) {
	srv, _ := echoServer(t)
	c := dial(t, srv)

	// 7-bit length, 16-bit length, 64-bit length, and the empty message.
	for _, size := range []int{0, 5, 125, 126, 300, 65535, 65536, 200000} {
		msg := strings.Repeat("x", size)
		if err := c.WriteText(msg); err != nil {
			t.Fatalf("size %d: write: %v", size, err)
		}
		got, err := c.ReadText(3 * time.Second)
		if err != nil || got != msg {
			t.Fatalf("size %d: echoed %d bytes, err %v", size, len(got), err)
		}
	}
}

func TestBinaryMessagesKeepTheirOpcode(t *testing.T) {
	srv, _ := echoServer(t)
	c := dial(t, srv)
	payload := []byte{0, 1, 2, 0xFF, 0xFE}
	if err := c.WriteFrame(true, wstest.OpBinary, payload, true); err != nil {
		t.Fatal(err)
	}
	_, op, got, err := c.ReadFrame(3 * time.Second)
	if err != nil || op != wstest.OpBinary || !bytes.Equal(got, payload) {
		t.Errorf("op %#x payload %v err %v", op, got, err)
	}
}

func TestFragmentedMessagesAreReassembled(t *testing.T) {
	srv, _ := echoServer(t)
	c := dial(t, srv)

	// "Hel" + "lo, " + "world", with a ping injected between the fragments.
	steps := []struct {
		fin bool
		op  byte
		p   string
	}{
		{false, wstest.OpText, "Hel"},
		{true, wstest.OpPing, "mid"},
		{false, wstest.OpContinuation, "lo, "},
		{true, wstest.OpContinuation, "world"},
	}
	for _, s := range steps {
		if err := c.WriteFrame(s.fin, s.op, []byte(s.p), true); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.ReadText(3 * time.Second) // the pong is skipped by ReadText
	if err != nil || got != "Hello, world" {
		t.Errorf("got %q, err %v", got, err)
	}
}

func TestFragmentedMessageWithEmptyFirstFragment(t *testing.T) {
	srv, _ := echoServer(t)
	c := dial(t, srv)
	if err := c.WriteFrame(false, wstest.OpText, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteFrame(true, wstest.OpContinuation, []byte("tail"), true); err != nil {
		t.Fatal(err)
	}
	if got, err := c.ReadText(3 * time.Second); err != nil || got != "tail" {
		t.Errorf("got %q, err %v", got, err)
	}
}

func TestPingIsAnsweredWithAMatchingPong(t *testing.T) {
	srv, _ := echoServer(t)
	c := dial(t, srv)
	if err := c.WriteFrame(true, wstest.OpPing, []byte("are you there"), true); err != nil {
		t.Fatal(err)
	}
	_, op, payload, err := c.ReadFrame(3 * time.Second)
	if err != nil || op != wstest.OpPong || string(payload) != "are you there" {
		t.Errorf("op %#x payload %q err %v", op, payload, err)
	}
}

func TestClosingHandshake(t *testing.T) {
	srv, errs := echoServer(t)
	c := dial(t, srv)
	if err := c.WriteFrame(true, wstest.OpClose, []byte{0x03, 0xE8}, true); err != nil {
		t.Fatal(err)
	}
	if _, op, _, err := c.ReadFrame(3 * time.Second); err != nil || op != wstest.OpClose {
		t.Errorf("expected the server to echo a close frame, got op %#x err %v", op, err)
	}
	select {
	case err := <-errs:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("server read error = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("server never noticed the close")
	}
}

func TestAbruptDisconnectIsReportedAsClosed(t *testing.T) {
	srv, errs := echoServer(t)
	c := dial(t, srv)
	c.Close() // no closing handshake
	select {
	case err := <-errs:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("err = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("server never noticed the disconnect")
	}
}

func TestProtocolViolationsAreRejected(t *testing.T) {
	tests := []struct {
		name  string
		write func(c *wstest.Client) error
	}{
		{"unmasked client frame", func(c *wstest.Client) error { return c.WriteFrame(true, wstest.OpText, []byte("hi"), false) }},
		{"continuation with nothing to continue", func(c *wstest.Client) error {
			return c.WriteFrame(true, wstest.OpContinuation, []byte("x"), true)
		}},
		{"new message inside a fragmented one", func(c *wstest.Client) error {
			if err := c.WriteFrame(false, wstest.OpText, []byte("a"), true); err != nil {
				return err
			}
			return c.WriteFrame(true, wstest.OpText, []byte("b"), true)
		}},
		{"fragmented control frame", func(c *wstest.Client) error { return c.WriteFrame(false, wstest.OpPing, []byte("x"), true) }},
		{"oversized control frame", func(c *wstest.Client) error {
			return c.WriteFrame(true, wstest.OpPing, bytes.Repeat([]byte("x"), 126), true)
		}},
		{"unknown opcode", func(c *wstest.Client) error { return c.WriteFrame(true, 0x3, []byte("x"), true) }},
		{"message over the size limit", func(c *wstest.Client) error {
			return c.WriteFrame(true, wstest.OpBinary, make([]byte, MaxMessageSize+1), true)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, errs := echoServer(t)
			c := dial(t, srv)
			_ = tc.write(c)
			select {
			case err := <-errs:
				if !errors.Is(err, ErrProtocol) {
					t.Errorf("server error = %v, want ErrProtocol", err)
				}
			case <-time.After(3 * time.Second):
				t.Error("server accepted a protocol violation")
			}
		})
	}
}

func TestHandshakeRejections(t *testing.T) {
	var mu sync.Mutex
	var upgradeErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r)
		if err != nil {
			mu.Lock()
			upgradeErr = err
			mu.Unlock()
			return
		}
		c.Close()
	}))
	defer srv.Close()

	// Always build headers with Set: a map literal with "Sec-WebSocket-Key" is NOT
	// the canonical key ("Sec-Websocket-Key"), so Del/Set/Get would see different entries.
	good := func() http.Header {
		h := http.Header{}
		h.Set("Connection", "Upgrade")
		h.Set("Upgrade", "websocket")
		h.Set("Sec-WebSocket-Version", "13")
		h.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		return h
	}
	tests := []struct {
		name   string
		method string
		mutate func(http.Header)
		want   int
	}{
		{"plain GET", "GET", func(h http.Header) { h.Del("Upgrade"); h.Del("Connection") }, http.StatusBadRequest},
		{"wrong version", "GET", func(h http.Header) { h.Set("Sec-WebSocket-Version", "8") }, http.StatusUpgradeRequired},
		{"missing key", "GET", func(h http.Header) { h.Del("Sec-WebSocket-Key") }, http.StatusBadRequest},
		{"key of the wrong length", "GET", func(h http.Header) { h.Set("Sec-WebSocket-Key", "c2hvcnQ=") }, http.StatusBadRequest},
		{"key that is not base64", "GET", func(h http.Header) { h.Set("Sec-WebSocket-Key", "!!!not base64!!!") }, http.StatusBadRequest},
		{"POST", "POST", func(http.Header) {}, http.StatusMethodNotAllowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := good()
			tc.mutate(h)
			req, _ := http.NewRequest(tc.method, srv.URL, nil)
			req.Header = h
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				mu.Lock()
				defer mu.Unlock()
				t.Errorf("status = %d, want %d (upgrade error: %v)", resp.StatusCode, tc.want, upgradeErr)
			}
		})
	}
}

func TestUpgradeAcceptsTokenListsInHeaders(t *testing.T) {
	srv, _ := echoServer(t)
	// Dial already sends "Connection: Upgrade"; this adds a second Connection
	// header line, as proxies and some browsers do ("keep-alive", then "Upgrade").
	c, err := wstest.Dial(srv.URL+"/", http.Header{
		"Connection": {"keep-alive"},
		"Origin":     {"http://example.test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}

func TestUpgradeFailsCleanlyWithoutHijackSupport(t *testing.T) {
	rec := httptest.NewRecorder() // does not implement http.Hijacker
	req := httptest.NewRequest("GET", "/", nil)
	req.Header = http.Header{}
	req.Header.Set("Connection", "keep-alive, Upgrade")
	req.Header.Set("Upgrade", "WebSocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	if _, err := Upgrade(rec, req); err == nil {
		t.Fatal("Upgrade succeeded on a ResponseWriter that cannot be hijacked")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestConcurrentWritersNeverInterleaveFrames(t *testing.T) {
	ready := make(chan *Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := Upgrade(w, r)
		if err != nil {
			return
		}
		ready <- c
		for { // keep reading so control frames are processed
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	client := dial(t, srv)
	server := <-ready
	defer server.Close()

	const writers, perWriter = 8, 40
	done := make(chan struct{})
	for w := 0; w < writers; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			msg := []byte(strings.Repeat(string(rune('a'+w)), 3000)) // > 125: extended length header
			for i := 0; i < perWriter; i++ {
				_ = server.WriteText(msg)
			}
		}()
	}
	for i := 0; i < writers*perWriter; i++ {
		s, err := client.ReadText(3 * time.Second)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if len(s) != 3000 || strings.Count(s, s[:1]) != 3000 {
			t.Fatalf("message %d is corrupted: len %d", i, len(s))
		}
	}
	for w := 0; w < writers; w++ {
		<-done
	}
}
