// Package gorilla adapts github.com/gorilla/websocket to the managed
// transport core.
package gorilla

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"github.com/adshao/go-binance/v2/common/websocket/managed"
)

const defaultHandshakeTimeout = 45 * time.Second

// Dialer creates managed-compatible Gorilla WebSocket connections.
type Dialer struct {
	Endpoint string
	Header   http.Header

	// Proxy defaults to http.ProxyFromEnvironment when nil.
	Proxy func(*http.Request) (*url.URL, error)

	HandshakeTimeout  time.Duration
	EnableCompression bool
	ReadLimit         int64
}

// Dial implements managed.Dialer.
func (d Dialer) Dial(ctx context.Context) (managed.Socket, error) {
	if ctx == nil {
		return nil, fmt.Errorf("managed websocket gorilla dial: nil context")
	}
	if d.Endpoint == "" {
		return nil, fmt.Errorf("managed websocket gorilla dial: endpoint is required")
	}

	proxy := d.Proxy
	if proxy == nil {
		proxy = http.ProxyFromEnvironment
	}
	handshakeTimeout := d.HandshakeTimeout
	if handshakeTimeout == 0 {
		handshakeTimeout = defaultHandshakeTimeout
	}
	if handshakeTimeout < 0 {
		return nil, fmt.Errorf("managed websocket gorilla dial: handshake timeout must not be negative")
	}

	wsDialer := gorillaws.Dialer{
		Proxy:             proxy,
		HandshakeTimeout:  handshakeTimeout,
		EnableCompression: d.EnableCompression,
	}
	conn, response, err := wsDialer.DialContext(ctx, d.Endpoint, d.Header.Clone())
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, fmt.Errorf("managed websocket gorilla dial %s: %w", d.Endpoint, err)
	}
	if d.ReadLimit > 0 {
		conn.SetReadLimit(d.ReadLimit)
	}
	return conn, nil
}

var _ managed.Dialer = Dialer{}
