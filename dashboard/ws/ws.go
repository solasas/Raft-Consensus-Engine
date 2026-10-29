// Package ws is a small, dependency-free WebSocket (RFC 6455) server endpoint.
//
// It supports what a live dashboard needs: the opening handshake, text and
// binary messages in both directions (including fragmented ones), ping/pong, and
// the closing handshake. It does not implement extensions (e.g. compression) or
// subprotocols. For a public-facing service prefer a battle-tested library; for
// an in-process control room it keeps the module free of third-party code.
package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// handshakeGUID is fixed by RFC 6455 §1.3.
const handshakeGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Frame opcodes.
const (
	OpContinuation byte = 0x0
	OpText         byte = 0x1
	OpBinary       byte = 0x2
	OpClose        byte = 0x8
	OpPing         byte = 0x9
	OpPong         byte = 0xA
)

const (
	// MaxMessageSize bounds one (reassembled) message from a client. A dashboard
	// client only ever sends tiny control frames.
	MaxMessageSize = 1 << 20
	writeTimeout   = 5 * time.Second
)

var (
	// ErrClosed is returned by ReadMessage once the peer has closed the connection.
	ErrClosed = errors.New("ws: connection closed")
	// ErrProtocol is returned (wrapped) for malformed or forbidden frames.
	ErrProtocol = errors.New("ws: protocol error")
)

// Conn is one upgraded connection. WriteText/WriteBinary/Close may be called
// from any goroutine; ReadMessage must be called from a single goroutine.
type Conn struct {
	c  net.Conn
	br *bufio.Reader

	wmu    sync.Mutex // serialises frames on the wire
	closed bool

	// partial holds a fragmented message while it is being reassembled.
	partial   []byte
	partialOp byte
}

// AcceptKey computes the Sec-WebSocket-Accept value for a client key.
func AcceptKey(clientKey string) string {
	sum := sha1.Sum([]byte(clientKey + handshakeGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// Upgrade validates a WebSocket handshake request and, if valid, takes over the
// connection. On failure it has already written an HTTP error to w. Callers
// should check Origin themselves before calling Upgrade.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	fail := func(code int, msg string) (*Conn, error) {
		http.Error(w, msg, code)
		return nil, fmt.Errorf("ws: bad handshake: %s", msg)
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		return fail(http.StatusMethodNotAllowed, "method must be GET")
	}
	if !headerHasToken(r.Header, "Connection", "upgrade") || !headerHasToken(r.Header, "Upgrade", "websocket") {
		return fail(http.StatusBadRequest, "not a websocket upgrade request")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		return fail(http.StatusUpgradeRequired, "unsupported websocket version")
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if raw, err := base64.StdEncoding.DecodeString(key); err != nil || len(raw) != 16 {
		return fail(http.StatusBadRequest, "missing or malformed Sec-WebSocket-Key")
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		return fail(http.StatusInternalServerError, "server does not support connection hijacking")
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, fmt.Errorf("ws: hijack: %w", err)
	}

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + AcceptKey(key) + "\r\n\r\n"
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := rw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Time{})

	// rw.Reader may already hold bytes the client sent right after its request.
	return &Conn{c: conn, br: rw.Reader}, nil
}

// headerHasToken reports whether a comma-separated header contains token,
// case-insensitively (e.g. "Connection: keep-alive, Upgrade").
func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// WriteText sends one text message. p must be valid UTF-8.
func (c *Conn) WriteText(p []byte) error { return c.writeFrame(OpText, p) }

// WriteBinary sends one binary message.
func (c *Conn) WriteBinary(p []byte) error { return c.writeFrame(OpBinary, p) }

// Ping sends a ping control frame (payload at most 125 bytes).
func (c *Conn) Ping(p []byte) error { return c.writeFrame(OpPing, p) }

func (c *Conn) writeFrame(op byte, p []byte) error {
	if op >= OpClose && len(p) > 125 {
		return fmt.Errorf("%w: control frame payload too large", ErrProtocol)
	}
	// Server-to-client frames are never masked (RFC 6455 §5.1).
	buf := make([]byte, 0, 10+len(p))
	buf = append(buf, 0x80|op) // FIN set: no outgoing fragmentation
	switch n := len(p); {
	case n < 126:
		buf = append(buf, byte(n))
	case n <= 0xFFFF:
		buf = append(buf, 126, 0, 0)
		binary.BigEndian.PutUint16(buf[len(buf)-2:], uint16(n))
	default:
		buf = append(buf, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(n))
	}
	buf = append(buf, p...)

	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closed {
		return ErrClosed
	}
	_ = c.c.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.c.Write(buf)
	return err
}

// Close sends a close frame (best effort) and tears the connection down. It is
// safe to call more than once and from any goroutine.
func (c *Conn) Close() error {
	c.wmu.Lock()
	if c.closed {
		c.wmu.Unlock()
		return nil
	}
	c.closed = true
	// 1000 = normal closure.
	_ = c.c.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = c.c.Write([]byte{0x80 | OpClose, 2, 0x03, 0xE8})
	c.wmu.Unlock()
	return c.c.Close()
}

// ReadMessage returns the next complete text or binary message. Control frames
// are handled internally: pings are answered, pongs ignored, and a close frame
// is acknowledged and reported as ErrClosed.
func (c *Conn) ReadMessage() (op byte, data []byte, err error) {
	for {
		fin, opcode, payload, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch opcode {
		case OpPing:
			if err := c.writeFrame(OpPong, payload); err != nil {
				return 0, nil, err
			}
		case OpPong:
			// unsolicited pongs are legal and ignored
		case OpClose:
			_ = c.Close()
			return 0, nil, ErrClosed
		case OpText, OpBinary:
			if c.partial != nil {
				return 0, nil, fmt.Errorf("%w: new message started inside a fragmented one", ErrProtocol)
			}
			if fin {
				return opcode, payload, nil
			}
			// Non-nil even when the first fragment is empty: nil means "no message open".
			c.partial, c.partialOp = append(make([]byte, 0, len(payload)), payload...), opcode
		case OpContinuation:
			if c.partial == nil {
				return 0, nil, fmt.Errorf("%w: continuation without a started message", ErrProtocol)
			}
			if len(c.partial)+len(payload) > MaxMessageSize {
				return 0, nil, fmt.Errorf("%w: message too large", ErrProtocol)
			}
			c.partial = append(c.partial, payload...)
			if fin {
				op, data := c.partialOp, c.partial
				c.partial = nil
				return op, data, nil
			}
		default:
			return 0, nil, fmt.Errorf("%w: unknown opcode %#x", ErrProtocol, opcode)
		}
	}
}

func (c *Conn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(c.br, hdr[:]); err != nil {
		return false, 0, nil, c.readErr(err)
	}
	fin = hdr[0]&0x80 != 0
	if hdr[0]&0x70 != 0 {
		return false, 0, nil, fmt.Errorf("%w: reserved bits set (no extensions negotiated)", ErrProtocol)
	}
	opcode = hdr[0] & 0x0F
	if hdr[1]&0x80 == 0 {
		return false, 0, nil, fmt.Errorf("%w: client frames must be masked", ErrProtocol)
	}

	length := uint64(hdr[1] & 0x7F)
	switch length {
	case 126:
		var b [2]byte
		if _, err = io.ReadFull(c.br, b[:]); err != nil {
			return false, 0, nil, c.readErr(err)
		}
		length = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err = io.ReadFull(c.br, b[:]); err != nil {
			return false, 0, nil, c.readErr(err)
		}
		length = binary.BigEndian.Uint64(b[:])
		if length>>63 != 0 {
			return false, 0, nil, fmt.Errorf("%w: frame length has the high bit set", ErrProtocol)
		}
	}
	if opcode >= OpClose && (length > 125 || !fin) {
		return false, 0, nil, fmt.Errorf("%w: control frames must be unfragmented and at most 125 bytes", ErrProtocol)
	}
	if length > MaxMessageSize {
		return false, 0, nil, fmt.Errorf("%w: frame of %d bytes exceeds the %d byte limit", ErrProtocol, length, MaxMessageSize)
	}

	var mask [4]byte
	if _, err = io.ReadFull(c.br, mask[:]); err != nil {
		return false, 0, nil, c.readErr(err)
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return false, 0, nil, c.readErr(err)
	}
	for i := range payload {
		payload[i] ^= mask[i&3]
	}
	return fin, opcode, payload, nil
}

// readErr turns "the peer went away" errors into ErrClosed.
func (c *Conn) readErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return ErrClosed
	}
	return err
}
