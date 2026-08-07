package websocket

import (
	"context"
	"encoding/json"
	"errors"
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

type contextManagedTestServer struct {
	server   *httptest.Server
	endpoint string
	requests atomic.Int64
	writeMu  sync.Mutex
}

func newContextManagedTestServer(t *testing.T, onRequest func(*websocket.Conn, borrowedWireRequest)) *contextManagedTestServer {
	t.Helper()
	s := &contextManagedTestServer{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		go func() {
			defer conn.Close()
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
				s.requests.Add(1)
				if onRequest != nil {
					onRequest(conn, req)
				}
			}
		}()
	}))
	s.endpoint = "ws" + strings.TrimPrefix(s.server.URL, "http")
	t.Cleanup(s.server.Close)
	return s
}

func (s *contextManagedTestServer) writeResponse(t *testing.T, conn *websocket.Conn, id string) {
	t.Helper()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := conn.WriteJSON(map[string]any{"id": id, "status": 200, "result": map[string]any{"ok": true}}); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func newContextManagedSession(t *testing.T, endpoint string) *apiws.Session {
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
		RequestTimeout:    2 * time.Second,
		StateBuffer:       64,
		ErrorBuffer:       64,
		UnsolicitedBuffer: 64,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	startBorrowedTestSession(t, session)
	return session
}

func borrowedManagedClient(t *testing.T, session *apiws.Session) *ManagedClient {
	t.Helper()
	client, err := NewBorrowedManagedClient(session)
	if err != nil {
		t.Fatalf("NewBorrowedManagedClient: %v", err)
	}
	managedClient, ok := client.(*ManagedClient)
	if !ok {
		t.Fatalf("client type = %T, want *ManagedClient", client)
	}
	return managedClient
}

func TestWriteSyncContextCallerCanceledBeforeSend(t *testing.T) {
	var server *contextManagedTestServer
	server = newContextManagedTestServer(t, func(conn *websocket.Conn, req borrowedWireRequest) {
		server.writeResponse(t, conn, req.ID)
	})
	session := newContextManagedSession(t, server.endpoint)
	client := borrowedManagedClient(t, session)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.WriteSyncContext(ctx, "cancel-before-send", legacyPayload(t, "cancel-before-send", "order.place"), time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %T %v, want context.Canceled", err, err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := server.requests.Load(); got != 0 {
		t.Fatalf("wire requests = %d, want 0", got)
	}
	if got := session.State(); got != apiws.StateReady {
		t.Fatalf("session state = %s, want ready", got)
	}
}

func TestWriteSyncContextCallerDeadlineAfterSendPreservesUnknownOutcome(t *testing.T) {
	received := make(chan struct{}, 1)
	server := newContextManagedTestServer(t, func(_ *websocket.Conn, _ borrowedWireRequest) {
		select {
		case received <- struct{}{}:
		default:
		}
	})
	session := newContextManagedSession(t, server.endpoint)
	client := borrowedManagedClient(t, session)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := client.WriteSyncContext(ctx, "unknown-deadline", legacyPayload(t, "unknown-deadline", "order.place"), time.Second)
	select {
	case <-received:
	default:
		t.Fatal("server did not receive request before caller deadline")
	}
	var unknown *apiws.UnknownOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("error = %T %v, want UnknownOutcomeError", err, err)
	}
	if unknown.RequestID != "unknown-deadline" || unknown.Method != "order.place" {
		t.Fatalf("unknown outcome = %#v", unknown)
	}
	stats := session.Stats()
	if stats.RequestsStarted != 1 || stats.RequestsCompleted != 1 || stats.RequestFailures != 1 {
		t.Fatalf("stats = %#v, want one completed failed request", stats)
	}
}

func TestWriteSyncContextSafeRequestDeadlineStaysSafe(t *testing.T) {
	server := newContextManagedTestServer(t, func(_ *websocket.Conn, _ borrowedWireRequest) {})
	session := newContextManagedSession(t, server.endpoint)
	client := borrowedManagedClient(t, session)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := client.WriteSyncContext(ctx, "safe-deadline", legacyPayload(t, "safe-deadline", "order.status"), time.Second)
	var unknown *apiws.UnknownOutcomeError
	if errors.As(err, &unknown) {
		t.Fatalf("safe request returned UnknownOutcomeError: %v", err)
	}
	var requestErr *apiws.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error = %T %v, want RequestError", err, err)
	}
	if requestErr.RequestID != "safe-deadline" || requestErr.Method != "order.status" {
		t.Fatalf("request error = %#v", requestErr)
	}
}

func TestWriteSyncContextBorrowerCloseDoesNotAffectSibling(t *testing.T) {
	var server *contextManagedTestServer
	server = newContextManagedTestServer(t, func(conn *websocket.Conn, req borrowedWireRequest) {
		server.writeResponse(t, conn, req.ID)
	})
	session := newContextManagedSession(t, server.endpoint)
	clientA := borrowedManagedClient(t, session)
	clientB := borrowedManagedClient(t, session)

	if err := clientA.Close(); err != nil {
		t.Fatal(err)
	}
	if got := session.State(); got != apiws.StateReady {
		t.Fatalf("session state after borrower close = %s, want ready", got)
	}
	response, err := clientB.WriteSyncContext(context.Background(), "sibling", legacyPayload(t, "sibling", "order.status"), time.Second)
	if err != nil {
		t.Fatalf("sibling request: %v", err)
	}
	var envelope struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.ID != "sibling" {
		t.Fatalf("response id = %q, want sibling", envelope.ID)
	}
}
