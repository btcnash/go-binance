package futures

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	managedgorilla "github.com/btcnash/go-binance/v2/common/websocket/managed/gorilla"
	managedstream "github.com/btcnash/go-binance/v2/futures/stream"
)

type legacyDynamicStreamSpec struct {
	class         managedstream.StreamClass
	endpoint      string
	subscriptions []managedstream.Subscription
	combined      bool
}

func parseLegacyDynamicStream(endpoint string) (legacyDynamicStreamSpec, bool) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return legacyDynamicStreamSpec{}, false
	}
	path := parsed.Path
	var class managedstream.StreamClass
	var prefix string
	switch {
	case strings.HasPrefix(path, "/public/ws/"):
		class, prefix = managedstream.StreamClassPublic, "/public/ws/"
	case strings.HasPrefix(path, "/market/ws/"):
		class, prefix = managedstream.StreamClassMarket, "/market/ws/"
	case path == "/public/stream":
		class = managedstream.StreamClassPublic
	case path == "/market/stream":
		class = managedstream.StreamClassMarket
	default:
		return legacyDynamicStreamSpec{}, false
	}

	names := []string{}
	combined := prefix == ""
	if prefix != "" {
		name := strings.TrimPrefix(path, prefix)
		if name != "" {
			names = []string{name}
		}
	} else {
		names = strings.Split(parsed.Query().Get("streams"), "/")
	}
	subscriptions := make([]managedstream.Subscription, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		subscriptions = append(subscriptions, managedstream.RawSubscription(class, name))
	}
	if len(subscriptions) == 0 {
		return legacyDynamicStreamSpec{}, false
	}
	dynamicPath := "/public/stream"
	if class == managedstream.StreamClassMarket {
		dynamicPath = "/market/stream"
	}
	parsed.Path = dynamicPath
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return legacyDynamicStreamSpec{class: class, endpoint: parsed.String(), subscriptions: subscriptions, combined: combined}, true
}

func managedDynamicStreamServe(cfg *WsConfig, spec legacyDynamicStreamSpec, handler WsHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	proxy := http.ProxyFromEnvironment
	if cfg.Proxy != nil {
		u, parseErr := url.Parse(*cfg.Proxy)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		proxy = http.ProxyURL(u)
	}
	connectionOptions := managedws.Options{
		Dialer:           managedgorilla.Dialer{Endpoint: spec.endpoint, Proxy: proxy, HandshakeTimeout: 45 * time.Second, EnableCompression: true, ReadLimit: 655350},
		Reconnect:        managedws.ReconnectPolicy{Enabled: true},
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
		connectionOptions.Heartbeat = managedws.HeartbeatOptions{Enabled: true, PingInterval: pingInterval, PongTimeout: pongTimeout, WriteTimeout: pongTimeout}
	}
	session, err := managedstream.NewStreamSession(managedstream.StreamSessionOptions{
		Class:                spec.class,
		Endpoint:             spec.endpoint,
		InitialSubscriptions: spec.subscriptions,
		ConnectionOptions:    connectionOptions,
	})
	if err != nil {
		return nil, nil, err
	}
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	if err := session.Start(lifecycleCtx); err != nil {
		cancel()
		return nil, nil, err
	}
	readyCtx, readyCancel := context.WithTimeout(lifecycleCtx, 45*time.Second)
	err = session.WaitReady(readyCtx)
	readyCancel()
	if err != nil {
		cancel()
		_ = session.Close()
		return nil, nil, err
	}

	doneC, stopC = make(chan struct{}), make(chan struct{}, 1)
	go func() {
		defer close(doneC)
		defer cancel()
		defer session.Close()
		events, errorsC, gaps := session.Events(), session.Errors(), session.Gaps()
		for events != nil || errorsC != nil || gaps != nil {
			select {
			case <-stopC:
				return
			case <-session.Done():
				return
			case event, ok := <-events:
				if !ok {
					events = nil
					continue
				}
				payload := event.Data
				if spec.combined || len(payload) == 0 {
					payload = event.Raw
				}
				handler(payload)
			case event, ok := <-errorsC:
				if !ok {
					errorsC = nil
					continue
				}
				if errHandler != nil {
					errHandler(event.Err)
				}
			case gap, ok := <-gaps:
				if !ok {
					gaps = nil
					continue
				}
				if errHandler != nil {
					errHandler(fmt.Errorf("legacy stream gap (%s): %w", gap.Reason, gap.Err))
				}
			}
		}
	}()
	return doneC, stopC, nil
}
