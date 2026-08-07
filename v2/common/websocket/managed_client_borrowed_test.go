package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiws "github.com/btcnash/go-binance/v2/common/websocket/api"
	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	managedgorilla "github.com/btcnash/go-binance/v2/common/websocket/managed/gorilla"
	"github.com/gorilla/websocket"
)

type borrowedWireRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
}

type borrowedTestServer struct {
	t           *testing.T
	server      *httptest.Server
	endpoint    string
	upgrader    websocket.Upgrader
	connections atomic.Int64
	mu          sync.Mutex
	conns       map[int64]*websocket.Conn
}

func newBorrowedTestServer(t *testing.T) *borrowedTestServer {
	t.Helper()
	s := &borrowedTestServer{
		t:        t,
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
		conns:    make(map[int64]*websocket.Conn),
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		id := s.connections.Add(1)
		s.mu.Lock()
		s.conns[id] = conn
		s.mu.Unlock()
		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.conns, id)
				s.mu.Unlock()
				_ = conn.Close()
			}()
			for {
				_, payload, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var req borrowedWireRequest
				if err := json.Unmarshal(payload, &req); err != nil {
					t.Errorf("decode request: %v", err)
					return
				}
				if err := conn.WriteJSON(map[string]any{
					"id":     req.ID,
					"status": 200,
					"result": map[string]any{"method": req.Method},
				}); err != nil {
					return
				}
				if req.Method == "trigger.unsolicited" {
					if err := conn.WriteJSON(map[string]any{"event": "owner-only"}); err != nil {
						return
					}
				}
			}
		}()
	}))
	s.endpoint = "ws" + strings.TrimPrefix(s.server.URL, "http")
	t.Cleanup(s.close)
	return s
}

func (s *borrowedTestServer) closeConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
}

func (s *borrowedTestServer) close() {
	s.closeConnections()
	s.server.Close()
}

func newBorrowedTestSession(t *testing.T, endpoint string) *apiws.Session {
	t.Helper()
	session, err := apiws.NewSession(apiws.Options{
		ConnectionOptions: managedws.Options{
			Dialer: managedgorilla.Dialer{Endpoint: endpoint},
			Reconnect: managedws.ReconnectPolicy{
				Enabled:      true,
				InitialDelay: time.Millisecond,
				MaxDelay:     time.Millisecond,
				Multiplier:   1,
			},
		},
		RequestTimeout:    500 * time.Millisecond,
		StateBuffer:       64,
		ErrorBuffer:       64,
		UnsolicitedBuffer: 64,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		select {
		case <-session.Done():
		case <-time.After(time.Second):
			t.Error("session did not close")
		}
	})
	return session
}

func startBorrowedTestSession(t *testing.T, session *apiws.Session) {
	t.Helper()
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func legacyPayload(t *testing.T, id, method string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"id": id, "method": method, "params": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func drainStateEvents(ch <-chan apiws.StateEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func TestBorrowedManagedClientDoesNotOwnIdleSessionLifecycle(t *testing.T) {
	session := newBorrowedTestSession(t, "ws://127.0.0.1:1")
	if got := session.State(); got != apiws.StateIdle {
		t.Fatalf("initial state = %s, want idle", got)
	}

	client, err := NewBorrowedManagedClient(session)
	if err != nil {
		t.Fatalf("NewBorrowedManagedClient: %v", err)
	}
	if got := session.State(); got != apiws.StateIdle {
		t.Fatalf("state after borrowed construction = %s, want idle", got)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("borrowed Close: %v", err)
	}
	if got := session.State(); got != apiws.StateIdle {
		t.Fatalf("state after borrowed Close = %s, want idle", got)
	}
}

func TestBorrowedManagedClientCloseDoesNotBreakSibling(t *testing.T) {
	server := newBorrowedTestServer(t)
	session := newBorrowedTestSession(t, server.endpoint)
	startBorrowedTestSession(t, session)

	a, err := NewBorrowedManagedClient(session)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBorrowedManagedClient(session)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}
	if got := session.State(); got != apiws.StateReady {
		t.Fatalf("session state after Close A = %s, want ready", got)
	}

	response, err := b.WriteSync("b-1", legacyPayload(t, "b-1", "order.status"), time.Second)
	if err != nil {
		t.Fatalf("B request after Close A: %v", err)
	}
	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.ID != "b-1" {
		t.Fatalf("response id = %q, want b-1", envelope.ID)
	}
}

func TestBorrowedManagedClientsDoNotConsumeOwnerLifecycleEvents(t *testing.T) {
	server := newBorrowedTestServer(t)
	session := newBorrowedTestSession(t, server.endpoint)
	startBorrowedTestSession(t, session)
	drainStateEvents(session.States())

	borrowed, err := NewBorrowedManagedClient(session)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := NewBorrowedManagedClient(session); err != nil {
			t.Fatal(err)
		}
	}
	server.closeConnections()

	deadline := time.After(2 * time.Second)
	sawDisconnected := false
	sawReconnecting := false
	sawReady := false
	for !(sawDisconnected && sawReconnecting && sawReady) {
		select {
		case event := <-session.States():
			switch event.State {
			case apiws.StateDisconnected:
				sawDisconnected = true
			case apiws.StateReconnecting:
				sawReconnecting = true
			case apiws.StateReady:
				if sawDisconnected {
					sawReady = true
				}
			}
		case <-deadline:
			t.Fatalf("owner lifecycle incomplete: disconnected=%v reconnecting=%v ready=%v", sawDisconnected, sawReconnecting, sawReady)
		}
	}
	if got := borrowed.GetReconnectCount(); got < 1 {
		t.Fatalf("borrowed reconnect count = %d, want at least 1 from session stats", got)
	}
}

func TestBorrowedManagedClientsDoNotConsumeOwnerUnsolicitedFrames(t *testing.T) {
	server := newBorrowedTestServer(t)
	session := newBorrowedTestSession(t, server.endpoint)
	startBorrowedTestSession(t, session)

	a, err := NewBorrowedManagedClient(session)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBorrowedManagedClient(session)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := b.WriteSync("u-1", legacyPayload(t, "u-1", "trigger.unsolicited"), time.Second); err != nil {
		t.Fatalf("trigger request: %v", err)
	}
	select {
	case frame := <-session.Unsolicited():
		if !strings.Contains(string(frame.Payload), "owner-only") {
			t.Fatalf("unexpected unsolicited payload: %s", frame.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("session owner did not receive unsolicited frame")
	}

	select {
	case payload := <-a.GetReadChannel():
		t.Fatalf("borrowed A consumed unsolicited frame: %s", payload)
	case payload := <-b.GetReadChannel():
		t.Fatalf("borrowed B consumed unsolicited frame: %s", payload)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestManagedClientStillOwnsSessionLifecycle(t *testing.T) {
	server := newBorrowedTestServer(t)
	session := newBorrowedTestSession(t, server.endpoint)
	client, err := NewManagedClient(session)
	if err != nil {
		t.Fatalf("NewManagedClient: %v", err)
	}
	if got := session.State(); got != apiws.StateReady {
		t.Fatalf("state after owned construction = %s, want ready", got)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("owned Close: %v", err)
	}
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("owned Close did not close session")
	}
	if got := session.State(); got != apiws.StateClosed {
		t.Fatalf("state after owned Close = %s, want closed", got)
	}
}
