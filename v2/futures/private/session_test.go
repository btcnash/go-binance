package private

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"github.com/btcnash/go-binance/v2/futures"
)

type fakeProvider struct {
	mu              sync.Mutex
	keys            []string
	acquireCalls    int
	keepAliveCalls  int
	releaseCalls    int
	keepAliveErr    error
	keepAliveErrors []error
	acquireErrors   []error
	invalid         bool
}

func (p *fakeProvider) Acquire(context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acquireCalls++
	if idx := p.acquireCalls - 1; idx < len(p.acquireErrors) && p.acquireErrors[idx] != nil {
		return "", p.acquireErrors[idx]
	}
	if len(p.keys) == 0 {
		return "", errors.New("no key")
	}
	idx := p.acquireCalls - 1
	if idx >= len(p.keys) {
		idx = len(p.keys) - 1
	}
	return p.keys[idx], nil
}
func (p *fakeProvider) KeepAlive(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keepAliveCalls++
	if idx := p.keepAliveCalls - 1; idx < len(p.keepAliveErrors) {
		return p.keepAliveErrors[idx]
	}
	return p.keepAliveErr
}
func (p *fakeProvider) Release(context.Context, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseCalls++
	return nil
}
func (p *fakeProvider) IsInvalidListenKey(error) bool { return p.invalid }

func websocketRoot(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func testConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		HeartbeatPingInterval: 20 * time.Millisecond,
		HeartbeatPongTimeout:  20 * time.Millisecond,
		HeartbeatWriteTimeout: 10 * time.Millisecond,
		ReconnectInitialDelay: 5 * time.Millisecond,
		ReconnectMaxDelay:     5 * time.Millisecond,
	}
}

func TestPrivateSessionSingleSourceLifecycle(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	pathC := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case pathC <- r.URL.RequestURI():
		default:
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{
			"e": "ACCOUNT_UPDATE", "E": 1, "T": 1,
			"a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}},
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	provider := &fakeProvider{keys: []string{"listen-key-1"}}
	session, err := NewSession(SessionOptions{
		Mode:        ModeIsolated,
		Environment: EnvironmentMainnet,
		Endpoint:    websocketRoot(server),
		Sources:     []Source{{ID: "account-1", Provider: provider, Events: []futures.UserDataEventType{futures.UserDataEventTypeAccountUpdate}}},
		KeepAlive:   KeepAliveOptions{Interval: 25 * time.Millisecond, Timeout: 20 * time.Millisecond, MaxAttempts: 1},
		Connection:  testConnectionOptions(),
	})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := session.WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}

	select {
	case uri := <-pathC:
		if uri != "/private/ws?listenKey=listen-key-1&events=ACCOUNT_UPDATE" {
			t.Fatalf("request URI = %q", uri)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for endpoint")
	}

	select {
	case event := <-session.Events():
		if event.SourceID != "account-1" || event.Type != futures.UserDataEventTypeAccountUpdate || event.Decoded == nil {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		provider.mu.Lock()
		calls := provider.keepAliveCalls
		provider.mu.Unlock()
		if calls > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	provider.mu.Lock()
	keepAliveCalls := provider.keepAliveCalls
	provider.mu.Unlock()
	if keepAliveCalls == 0 {
		t.Fatal("KeepAlive() was not called")
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	<-session.Done()
	provider.mu.Lock()
	releaseCalls := provider.releaseCalls
	provider.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("Release() calls = %d, want 1", releaseCalls)
	}
}

func TestPrivateSessionListenKeyExpiredReacquiresAndReconnects(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	uriC := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uriC <- r.URL.RequestURI()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		n := connections.Add(1)
		if n == 1 {
			_ = conn.WriteJSON(map[string]any{"e": "listenKeyExpired", "E": 1})
		} else {
			_ = conn.WriteJSON(map[string]any{
				"e": "ORDER_TRADE_UPDATE", "E": 2, "T": 2,
				"o": map[string]any{"s": "BTCUSDT"},
			})
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	provider := &fakeProvider{keys: []string{"key-old", "key-new"}}
	session, err := NewSession(SessionOptions{
		Mode:       ModeIsolated,
		Endpoint:   websocketRoot(server),
		Sources:    []Source{{ID: "account-1", Provider: provider}},
		KeepAlive:  KeepAliveOptions{Interval: time.Hour},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	sawNewEvent := false
	for !sawNewEvent {
		select {
		case event := <-session.Events():
			if event.Type == futures.UserDataEventTypeOrderTradeUpdate && event.Generation >= 2 {
				sawNewEvent = true
			}
		case <-deadline:
			t.Fatal("timeout waiting for event after listen key refresh")
		}
	}

	first := <-uriC
	second := <-uriC
	if !strings.Contains(first, "listenKey=key-old") || !strings.Contains(second, "listenKey=key-new") {
		t.Fatalf("URIs = %q, %q", first, second)
	}
	provider.mu.Lock()
	acquires := provider.acquireCalls
	provider.mu.Unlock()
	if acquires < 2 {
		t.Fatalf("Acquire calls = %d, want >= 2", acquires)
	}

	select {
	case gap := <-session.Gaps():
		if gap.Reason != GapReasonListenKeyExpired {
			t.Fatalf("gap = %+v", gap)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listen key gap")
	}
}

func TestSharedPrivateSessionInfersUniqueSourceByEventFilter(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	uriC := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uriC <- r.URL.RequestURI()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{
			"e": "ACCOUNT_UPDATE", "E": 1, "T": 1,
			"a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}},
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	session, err := NewSession(SessionOptions{
		Mode:     ModeShared,
		Endpoint: websocketRoot(server),
		Sources: []Source{
			{ID: "orders", Provider: &fakeProvider{keys: []string{"key-a"}}, Events: []futures.UserDataEventType{futures.UserDataEventTypeOrderTradeUpdate}},
			{ID: "account", Provider: &fakeProvider{keys: []string{"key-b"}}, Events: []futures.UserDataEventType{futures.UserDataEventTypeAccountUpdate}},
		},
		KeepAlive:  KeepAliveOptions{Interval: time.Hour},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case uri := <-uriC:
		want := "/private/stream?listenKey=key-a&events=ORDER_TRADE_UPDATE&listenKey=key-b&events=ACCOUNT_UPDATE"
		if uri != want {
			t.Fatalf("request URI = %q, want %q", uri, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for endpoint")
	}
	select {
	case event := <-session.Events():
		if event.SourceID != "account" || len(event.CandidateSourceIDs) != 0 || event.SourceResolution != SourceResolutionEventFilter {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for shared event")
	}
}

func TestKeepAliveTransientFailureRetriesWithoutReconnect(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
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

	provider := &fakeProvider{
		keys:            []string{"key-1"},
		keepAliveErrors: []error{errors.New("temporary network error"), nil},
	}
	session, err := NewSession(SessionOptions{
		Mode:       ModeIsolated,
		Endpoint:   websocketRoot(server),
		Sources:    []Source{{ID: "account-1", Provider: provider}},
		KeepAlive:  KeepAliveOptions{Interval: 20 * time.Millisecond, Timeout: 20 * time.Millisecond, RetryInitial: 5 * time.Millisecond, RetryMax: 5 * time.Millisecond, Multiplier: 1, MaxAttempts: 2},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		provider.mu.Lock()
		calls := provider.keepAliveCalls
		provider.mu.Unlock()
		if calls >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	provider.mu.Lock()
	calls := provider.keepAliveCalls
	provider.mu.Unlock()
	if calls < 2 {
		t.Fatalf("keepalive calls = %d, want >= 2", calls)
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections = %d, want 1", got)
	}
	select {
	case gap := <-session.Gaps():
		t.Fatalf("unexpected gap: %+v", gap)
	default:
	}
}

func TestKeepAliveInvalidKeyReacquiresAndReconnects(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	uriC := make(chan string, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case uriC <- r.URL.RequestURI():
		default:
		}
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

	provider := &fakeProvider{keys: []string{"key-old", "key-new"}, keepAliveErr: errors.New("invalid listen key"), invalid: true}
	session, err := NewSession(SessionOptions{
		Mode:       ModeIsolated,
		Endpoint:   websocketRoot(server),
		Sources:    []Source{{ID: "account-1", Provider: provider}},
		KeepAlive:  KeepAliveOptions{Interval: 20 * time.Millisecond, Timeout: 20 * time.Millisecond, RetryInitial: 5 * time.Millisecond, RetryMax: 5 * time.Millisecond, Multiplier: 1, MaxAttempts: 3},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	var first, second string
	deadline := time.After(2 * time.Second)
	for second == "" {
		select {
		case uri := <-uriC:
			if first == "" {
				first = uri
			} else if uri != first {
				second = uri
			}
		case <-deadline:
			t.Fatalf("timeout waiting for key rotation; first=%q", first)
		}
	}
	if !strings.Contains(first, "listenKey=key-old") || !strings.Contains(second, "listenKey=key-new") {
		t.Fatalf("URIs = %q, %q", first, second)
	}
	select {
	case gap := <-session.Gaps():
		if gap.Reason != GapReasonKeepAliveFailed {
			t.Fatalf("gap = %+v", gap)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for keepalive gap")
	}
}

func TestSharedPrivateSessionPreservesAmbiguousCandidates(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{
			"e": "ACCOUNT_UPDATE", "E": 1, "T": 1,
			"a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}},
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	session, err := NewSession(SessionOptions{
		Mode:     ModeShared,
		Endpoint: websocketRoot(server),
		Sources: []Source{
			{ID: "account-a", Provider: &fakeProvider{keys: []string{"key-a"}}},
			{ID: "account-b", Provider: &fakeProvider{keys: []string{"key-b"}}},
		},
		KeepAlive:  KeepAliveOptions{Interval: time.Hour},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-session.Events():
		if event.SourceID != "" {
			t.Fatalf("source ID = %q, want empty", event.SourceID)
		}
		if got := strings.Join(event.CandidateSourceIDs, ","); got != "account-a,account-b" {
			t.Fatalf("candidates = %v", event.CandidateSourceIDs)
		}
		if event.SourceResolution != SourceResolutionAmbiguous {
			t.Fatalf("resolution = %s", event.SourceResolution)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ambiguous event")
	}
}

func TestSharedPrivateSessionUsesExplicitListenKeyEnvelope(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{
			"listenKey": "key-b",
			"data":      map[string]any{"e": "ACCOUNT_UPDATE", "E": 1, "T": 1, "a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}}},
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	session, err := NewSession(SessionOptions{
		Mode:     ModeShared,
		Endpoint: websocketRoot(server),
		Sources: []Source{
			{ID: "account-a", Provider: &fakeProvider{keys: []string{"key-a"}}},
			{ID: "account-b", Provider: &fakeProvider{keys: []string{"key-b"}}},
		},
		KeepAlive:  KeepAliveOptions{Interval: time.Hour},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-session.Events():
		if event.SourceID != "account-b" || len(event.CandidateSourceIDs) != 0 || event.SourceResolution != SourceResolutionExplicit {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for explicit-source event")
	}
}

func TestTransportDisconnectPublishesGapAndReconnects(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		n := connections.Add(1)
		if n == 1 {
			_ = conn.WriteJSON(map[string]any{"e": "ACCOUNT_UPDATE", "E": 1, "T": 1, "a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}}})
			_ = conn.Close()
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

	session, err := NewSession(SessionOptions{
		Mode: ModeIsolated, Endpoint: websocketRoot(server), Sources: []Source{{ID: "account-1", Provider: &fakeProvider{keys: []string{"key-1"}}}},
		KeepAlive: KeepAliveOptions{Interval: time.Hour}, Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case gap := <-session.Gaps():
		if gap.Reason != GapReasonDisconnected || strings.Join(gap.SourceIDs, ",") != "account-1" {
			t.Fatalf("gap = %+v", gap)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for disconnect gap")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if session.Generation() >= 2 && session.State() == StateReady {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("session did not reconnect: state=%s generation=%d", session.State(), session.Generation())
}

func TestCloseBeforeStartReleasesNothingAndDoesNotDeadlock(t *testing.T) {
	provider := &fakeProvider{keys: []string{"unused"}}
	session, err := NewSession(SessionOptions{Mode: ModeIsolated, Sources: []Source{{ID: "account-1", Provider: provider}}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- session.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() deadlocked before Start")
	}
	provider.mu.Lock()
	releases := provider.releaseCalls
	provider.mu.Unlock()
	if releases != 0 {
		t.Fatalf("release calls = %d, want 0", releases)
	}
}

func TestNewSessionRejectsInvalidModesAndSources(t *testing.T) {
	provider := &fakeProvider{keys: []string{"key"}}
	cases := []SessionOptions{
		{Mode: "bad", Sources: []Source{{ID: "a", Provider: provider}}},
		{Mode: ModeIsolated, Sources: []Source{{ID: "a", Provider: provider}, {ID: "b", Provider: provider}}},
		{Mode: ModeShared},
		{Mode: ModeShared, Sources: []Source{{ID: "", Provider: provider}}},
		{Mode: ModeShared, Sources: []Source{{ID: "a", Provider: nil}}},
		{Mode: ModeShared, Sources: []Source{{ID: "a", Provider: provider}, {ID: "a", Provider: provider}}},
	}
	for i, options := range cases {
		if _, err := NewSession(options); err == nil {
			t.Fatalf("case %d: error = nil", i)
		}
	}
}

func TestHandshakeAuthenticationFailureReacquiresBeforeRetry(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	uriC := make(chan string, 4)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case uriC <- r.URL.RequestURI():
		default:
		}
		if attempts.Add(1) == 1 {
			http.Error(w, "invalid listen key", http.StatusUnauthorized)
			return
		}
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

	provider := &fakeProvider{keys: []string{"key-old", "key-new"}}
	session, err := NewSession(SessionOptions{
		Mode: ModeIsolated, Endpoint: websocketRoot(server), Sources: []Source{{ID: "account-1", Provider: provider}},
		KeepAlive: KeepAliveOptions{Interval: time.Hour}, Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	first, second := <-uriC, <-uriC
	if !strings.Contains(first, "listenKey=key-old") || !strings.Contains(second, "listenKey=key-new") {
		t.Fatalf("URIs = %q, %q", first, second)
	}
}

func TestPrivateRejectionFrameReacquiresListenKey(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	uriC := make(chan string, 4)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case uriC <- r.URL.RequestURI():
		default:
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if attempts.Add(1) == 1 {
			_ = conn.WriteJSON(map[string]any{"code": -1125, "msg": "This listenKey does not exist."})
		} else {
			_ = conn.WriteJSON(map[string]any{"e": "ACCOUNT_UPDATE", "E": 2, "T": 2, "a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}}})
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	provider := &fakeProvider{keys: []string{"key-old", "key-new"}}
	session, err := NewSession(SessionOptions{
		Mode: ModeIsolated, Endpoint: websocketRoot(server), Sources: []Source{{ID: "account-1", Provider: provider}},
		KeepAlive: KeepAliveOptions{Interval: time.Hour}, Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-session.Events():
		if event.Generation < 2 || event.SourceID != "account-1" {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event after rejection recovery")
	}
	first, second := <-uriC, <-uriC
	if !strings.Contains(first, "listenKey=key-old") || !strings.Contains(second, "listenKey=key-new") {
		t.Fatalf("URIs = %q, %q", first, second)
	}
	select {
	case gap := <-session.Gaps():
		if gap.Reason != GapReasonListenKeyExpired {
			t.Fatalf("gap = %+v", gap)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for rejection gap")
	}
}

func TestRetainListenKeysSkipsRelease(t *testing.T) {
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
	provider := &fakeProvider{keys: []string{"key-1"}}
	session, err := NewSession(SessionOptions{Mode: ModeIsolated, Endpoint: websocketRoot(server), Sources: []Source{{ID: "account-1", Provider: provider}}, RetainListenKeys: true, KeepAlive: KeepAliveOptions{Interval: time.Hour}, Connection: testConnectionOptions()})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	<-session.Done()
	provider.mu.Lock()
	calls := provider.releaseCalls
	provider.mu.Unlock()
	if calls != 0 {
		t.Fatalf("release calls = %d, want 0", calls)
	}
}

func TestEventBufferOverflowPublishesGapAndReconnects(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connections.Add(1)
		for i := 0; i < 3; i++ {
			if err := conn.WriteJSON(map[string]any{
				"e": "ACCOUNT_UPDATE", "E": i + 1, "T": i + 1,
				"a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}},
			}); err != nil {
				return
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	session, err := NewSession(SessionOptions{
		Mode: ModeIsolated, Endpoint: websocketRoot(server),
		Sources:     []Source{{ID: "account-1", Provider: &fakeProvider{keys: []string{"key-1"}}}},
		EventBuffer: 1,
		KeepAlive:   KeepAliveOptions{Interval: time.Hour},
		Connection:  testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case gap := <-session.Gaps():
		if gap.Reason != GapReasonEventOverflow || strings.Join(gap.SourceIDs, ",") != "account-1" {
			t.Fatalf("gap = %+v", gap)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for overflow gap")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if connections.Load() >= 2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("connections = %d, want reconnect after overflow", connections.Load())
}

func TestAcquireTransientFailureRecoversOnReconnect(t *testing.T) {
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

	provider := &fakeProvider{
		keys:          []string{"key-1", "key-1"},
		acquireErrors: []error{errors.New("temporary REST outage")},
	}
	session, err := NewSession(SessionOptions{
		Mode: ModeIsolated, Endpoint: websocketRoot(server),
		Sources:    []Source{{ID: "account-1", Provider: provider}},
		KeepAlive:  KeepAliveOptions{Interval: time.Hour},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	calls := provider.acquireCalls
	provider.mu.Unlock()
	if calls < 2 {
		t.Fatalf("Acquire calls = %d, want >= 2", calls)
	}
	kinds := make(map[ListenKeyEventKind]bool)
	deadline := time.After(time.Second)
	for !(kinds[ListenKeyAcquireFailed] && kinds[ListenKeyAcquired]) {
		select {
		case event := <-session.ListenKeyEvents():
			kinds[event.Kind] = true
		case <-deadline:
			t.Fatalf("listen key lifecycle kinds = %v", kinds)
		}
	}
}

func TestFastFirstEventThenDisconnectStillPublishesGap(t *testing.T) {
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if connections.Add(1) == 1 {
			_ = conn.WriteJSON(map[string]any{
				"e": "ACCOUNT_UPDATE", "E": 1, "T": 1,
				"a": map[string]any{"m": "ORDER", "B": []any{}, "P": []any{}},
			})
			_ = conn.Close()
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

	session, err := NewSession(SessionOptions{
		Mode: ModeIsolated, Endpoint: websocketRoot(server),
		Sources:    []Source{{ID: "account-1", Provider: &fakeProvider{keys: []string{"key-1"}}}},
		KeepAlive:  KeepAliveOptions{Interval: time.Hour},
		Connection: testConnectionOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-session.Events():
		if event.SourceResolution != SourceResolutionIsolated {
			t.Fatalf("resolution = %s", event.SourceResolution)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first event")
	}
	select {
	case gap := <-session.Gaps():
		if gap.Reason != GapReasonDisconnected {
			t.Fatalf("gap = %+v", gap)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for disconnect gap")
	}
}
