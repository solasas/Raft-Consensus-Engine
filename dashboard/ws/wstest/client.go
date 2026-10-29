// Package wstest is a tiny WebSocket client for tests. It is deliberately
// independent of package ws so tests exercise the server against an
// implementation of the wire format written separately from it.
package wstest

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

const guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Opcodes.
const (
	OpContinuation byte = 0x0
	OpText         byte = 0x1
	OpBinary       byte = 0x2
	OpClose        byte = 0x8
	OpPing         byte = 0x9
	OpPong         byte = 0xA
)

// Client is one client-side connection.
type Client struct {
	c  net.Conn
	br *bufio.Reader
}

// Dial connects to a server URL (http:// or ws://, e.g. httptest.Server.URL+"/ws")
// and performs the opening handshake. extra headers are added to the request.
func Dial(rawURL string, extra http.Header) (*Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", u.Host, 3*time.Second)
	if err != nil {
		return nil, err
	}

	keyRaw := make([]byte, 16)
	_, _ = rand.Read(keyRaw)
	key := base64.StdEncoding.EncodeToString(keyRaw)

	path := u.RequestURI()
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n", path, u.Host, key)
	for name, vals := range extra {
		for _, v := range vals {
			req += name + ": " + v + "\r\n"
		}
	}
	req += "\r\n"
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("handshake refused: %s", resp.Status)
	}
	sum := sha1.Sum([]byte(key + guid))
	if got, want := resp.Header.Get("Sec-WebSocket-Accept"), base64.StdEncoding.EncodeToString(sum[:]); got != want {
		conn.Close()
		return nil, fmt.Errorf("bad Sec-WebSocket-Accept %q, want %q", got, want)
	}
	_ = conn.SetDeadline(time.Time{})
	return &Client{c: conn, br: br}, nil
}

// Close closes the TCP connection without a closing handshake.
func (c *Client) Close() error { return c.c.Close() }

// WriteFrame sends one frame. Clients must mask; pass masked=false only to test
// that a server rejects unmasked frames.
func (c *Client) WriteFrame(fin bool, op byte, payload []byte, masked bool) error {
	b0 := op
	if fin {
		b0 |= 0x80
	}
	hdr := []byte{b0}
	var maskBit byte
	if masked {
		maskBit = 0x80
	}
	switch n := len(payload); {
	case n < 126:
		hdr = append(hdr, maskBit|byte(n))
	case n <= 0xFFFF:
		hdr = append(hdr, maskBit|126, 0, 0)
		binary.BigEndian.PutUint16(hdr[len(hdr)-2:], uint16(n))
	default:
		hdr = append(hdr, maskBit|127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(hdr[len(hdr)-8:], uint64(n))
	}
	body := payload
	if masked {
		mask := make([]byte, 4)
		_, _ = rand.Read(mask)
		hdr = append(hdr, mask...)
		body = make([]byte, len(payload))
		for i := range payload {
			body[i] = payload[i] ^ mask[i&3]
		}
	}
	_ = c.c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	_, err := c.c.Write(append(hdr, body...))
	return err
}

// WriteText sends a masked, unfragmented text message.
func (c *Client) WriteText(s string) error { return c.WriteFrame(true, OpText, []byte(s), true) }

// ReadFrame reads one frame from the server (which never masks).
func (c *Client) ReadFrame(timeout time.Duration) (fin bool, op byte, payload []byte, err error) {
	_ = c.c.SetReadDeadline(time.Now().Add(timeout))
	var hdr [2]byte
	if _, err = io.ReadFull(c.br, hdr[:]); err != nil {
		return
	}
	fin, op = hdr[0]&0x80 != 0, hdr[0]&0x0F
	if hdr[1]&0x80 != 0 {
		err = fmt.Errorf("server sent a masked frame")
		return
	}
	n := uint64(hdr[1] & 0x7F)
	switch n {
	case 126:
		var b [2]byte
		if _, err = io.ReadFull(c.br, b[:]); err != nil {
			return
		}
		n = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err = io.ReadFull(c.br, b[:]); err != nil {
			return
		}
		n = binary.BigEndian.Uint64(b[:])
	}
	payload = make([]byte, n)
	_, err = io.ReadFull(c.br, payload)
	return
}

// ReadText returns the next text message, answering pings and skipping pongs.
// It returns io.EOF if the server closes.
func (c *Client) ReadText(timeout time.Duration) (string, error) {
	for {
		_, op, payload, err := c.ReadFrame(timeout)
		if err != nil {
			return "", err
		}
		switch op {
		case OpText:
			return string(payload), nil
		case OpPing:
			if err := c.WriteFrame(true, OpPong, payload, true); err != nil {
				return "", err
			}
		case OpPong:
		case OpClose:
			return "", io.EOF
		default:
			return "", fmt.Errorf("unexpected opcode %#x", op)
		}
	}
}
