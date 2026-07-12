package futures

import (
	"context"
	"net/http"
	"net/url"
	"time"

	managedws "github.com/adshao/go-binance/v2/common/websocket/managed"
	managedgorilla "github.com/adshao/go-binance/v2/common/websocket/managed/gorilla"
	"github.com/gorilla/websocket"
)

// WsHandler handle raw websocket message
type WsHandler func(message []byte)

// ErrHandler handles errors
type ErrHandler func(err error)

// WsConfig webservice configuration
type WsConfig struct {
	Endpoint string
	Proxy    *string
}

func newWsConfig(endpoint string) *WsConfig {
	return &WsConfig{Endpoint: endpoint, Proxy: getWsProxyUrl()}
}

// wsServe remains a variable for compatibility with the legacy test/mocking
// surface. Its production implementation is backed by ManagedConnection.
var wsServe = managedWsServe

func managedWsServe(cfg *WsConfig, handler WsHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	if spec, ok := parseLegacyDynamicStream(cfg.Endpoint); ok {
		return managedDynamicStreamServe(cfg, spec, handler, errHandler)
	}
	return managedRawWsServe(cfg, handler, errHandler)
}

func managedRawWsServe(cfg *WsConfig, handler WsHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	proxy := http.ProxyFromEnvironment
	if cfg.Proxy != nil {
		u, parseErr := url.Parse(*cfg.Proxy)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		proxy = http.ProxyURL(u)
	}

	options := managedws.Options{
		Dialer: managedgorilla.Dialer{
			Endpoint:          cfg.Endpoint,
			Proxy:             proxy,
			HandshakeTimeout:  45 * time.Second,
			EnableCompression: true,
			ReadLimit:         655350,
		},
		Reconnect: managedws.ReconnectPolicy{Enabled: true},
		// Binance closes Futures WebSocket connections after 24 hours. Recycle
		// the physical connection before that deadline and let the managed
		// transport restore the same legacy URL subscription.
		MaxConnectionAge: 23*time.Hour + 50*time.Minute,
	}
	if WebsocketKeepalive {
		pingInterval := WebsocketTimeout
		if pingInterval <= 0 {
			pingInterval = 5 * time.Second
		}
		pongTimeout := WebsocketPongTimeout
		if pongTimeout <= 0 {
			pongTimeout = 3 * time.Second
		}
		options.Heartbeat = managedws.HeartbeatOptions{
			Enabled:      true,
			PingInterval: pingInterval,
			PongTimeout:  pongTimeout,
			WriteTimeout: pongTimeout,
		}
	}

	conn, err := managedws.NewConnection(options)
	if err != nil {
		return nil, nil, err
	}
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	if err := conn.Start(lifecycleCtx); err != nil {
		cancel()
		return nil, nil, err
	}
	readyCtx, readyCancel := context.WithTimeout(lifecycleCtx, 45*time.Second)
	err = conn.WaitReady(readyCtx)
	readyCancel()
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, nil, err
	}

	doneC = make(chan struct{})
	stopC = make(chan struct{}, 1)
	go func() {
		defer close(doneC)
		defer cancel()
		defer conn.Close()

		frames := conn.Frames()
		errorsC := conn.Errors()
		for frames != nil || errorsC != nil {
			select {
			case <-stopC:
				return
			case <-conn.Done():
				return
			case frame, ok := <-frames:
				if !ok {
					frames = nil
					continue
				}
				if frame.Type == managedws.TextMessage || frame.Type == managedws.BinaryMessage {
					handler(frame.Payload)
				}
			case event, ok := <-errorsC:
				if !ok {
					errorsC = nil
					continue
				}
				// Proactive age rotation is expected lifecycle, not an error
				// that legacy callers need to handle.
				if event.Kind != managedws.ErrorMaxAgeReached && errHandler != nil {
					errHandler(event.Err)
				}
			}
		}
	}()
	return doneC, stopC, nil
}

// WsGetReadWriteConnection is retained for source compatibility. New Futures
// WSAPI services use futures/wsapi managed sessions instead.
var WsGetReadWriteConnection = func(cfg *WsConfig) (*websocket.Conn, error) {
	proxy := http.ProxyFromEnvironment
	if cfg.Proxy != nil {
		u, err := url.Parse(*cfg.Proxy)
		if err != nil {
			return nil, err
		}
		proxy = http.ProxyURL(u)
	}

	dialer := websocket.Dialer{
		Proxy:             proxy,
		HandshakeTimeout:  45 * time.Second,
		EnableCompression: false,
	}
	c, _, err := dialer.Dial(cfg.Endpoint, nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}
