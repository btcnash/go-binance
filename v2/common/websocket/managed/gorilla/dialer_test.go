package gorilla

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"github.com/btcnash/go-binance/v2/common/websocket/managed"
)

func TestDialerConnectsAndCarriesFrames(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}
		defer conn.Close()
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.WriteMessage(messageType, payload)
	}))
	defer server.Close()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	socket, err := (Dialer{Endpoint: endpoint, HandshakeTimeout: time.Second}).Dial(context.Background())
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer socket.Close()

	if err := socket.WriteMessage(gorillaws.TextMessage, []byte("hello")); err != nil {
		t.Fatalf("WriteMessage() error = %v", err)
	}
	messageType, payload, err := socket.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if messageType != gorillaws.TextMessage || string(payload) != "hello" {
		t.Fatalf("received type=%d payload=%q", messageType, payload)
	}
}

func TestDialerRejectsMissingEndpoint(t *testing.T) {
	_, err := (Dialer{}).Dial(context.Background())
	if err == nil {
		t.Fatal("Dial() error = nil, want error")
	}
}

func TestManagedConnectionHeartbeatOverRealWebSocket(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, err := managed.NewConnection(managed.Options{
		Dialer: Dialer{Endpoint: endpoint},
		Heartbeat: managed.HeartbeatOptions{
			Enabled:      true,
			PingInterval: 20 * time.Millisecond,
			PongTimeout:  20 * time.Millisecond,
			WriteTimeout: 10 * time.Millisecond,
		},
		Reconnect: managed.ReconnectPolicy{Enabled: false},
	})
	if err != nil {
		t.Fatalf("NewConnection() error = %v", err)
	}
	defer conn.Close()
	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case heartbeat := <-conn.Heartbeats():
		for heartbeat.Kind != managed.HeartbeatPongReceived {
			select {
			case heartbeat = <-conn.Heartbeats():
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for real websocket pong")
			}
		}
		if heartbeat.RTT <= 0 {
			t.Fatalf("RTT = %s, want positive", heartbeat.RTT)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for heartbeat")
	}
}

func TestManagedConnectionPongTimeoutReconnectsOverRealWebSocket(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		// Intentionally do not read. Gorilla therefore does not process the
		// client's Ping control frame and cannot return a Pong.
		<-r.Context().Done()
	}))
	defer server.Close()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, err := managed.NewConnection(managed.Options{
		Dialer: Dialer{Endpoint: endpoint},
		Heartbeat: managed.HeartbeatOptions{
			Enabled:      true,
			PingInterval: 15 * time.Millisecond,
			PongTimeout:  15 * time.Millisecond,
			WriteTimeout: 10 * time.Millisecond,
		},
		Reconnect: managed.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Multiplier:   1,
		},
	})
	if err != nil {
		t.Fatalf("NewConnection() error = %v", err)
	}
	defer conn.Close()
	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case state := <-conn.States():
			if state.State == managed.StateReady && state.Generation >= 2 {
				return
			}
		case <-timer.C:
			t.Fatal("timeout waiting for reconnect after real websocket pong timeout")
		}
	}
}

func TestDialerPreservesHandshakeStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid listen key", http.StatusUnauthorized)
	}))
	defer server.Close()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	_, err := (Dialer{Endpoint: endpoint}).Dial(context.Background())
	var handshakeErr *HandshakeError
	if !errors.As(err, &handshakeErr) {
		t.Fatalf("Dial() error = %T %v, want *HandshakeError", err, err)
	}
	if handshakeErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", handshakeErr.StatusCode, http.StatusUnauthorized)
	}
}
