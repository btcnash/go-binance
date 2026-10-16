package private

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	"github.com/btcnash/go-binance/v2/futures"
	gorillaws "github.com/gorilla/websocket"
)

type renewalTestProvider struct {
	*fakeProvider
	renew    func(context.Context, string) (string, error)
	classify func(error) bool
}

func (p *renewalTestProvider) KeepAliveWithResponse(ctx context.Context, key string) (string, error) {
	_ = p.fakeProvider.KeepAlive(ctx, key)
	return p.renew(ctx, key)
}
func (p *renewalTestProvider) IsInvalidListenKey(err error) bool {
	if p.classify != nil {
		return p.classify(err)
	}
	return p.fakeProvider.IsInvalidListenKey(err)
}

func startRenewalSession(t *testing.T, p *renewalTestProvider, interval time.Duration) (*Session, <-chan string, <-chan *gorillaws.Conn) {
	t.Helper()
	paths, connections := make(chan string, 8), make(chan *gorillaws.Conn, 8)
	upgrader := gorillaws.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		paths <- r.URL.Path
		if strings.HasSuffix(r.URL.Path, "key-new") {
			_ = conn.WriteMessage(gorillaws.TextMessage, []byte(`{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"m":"ORDER","B":[],"P":[]}}`))
			_ = conn.WriteMessage(gorillaws.TextMessage, []byte(`{"e":"ORDER_TRADE_UPDATE","E":2,"T":2,"o":{"s":"BTCUSDT","i":1,"x":"TRADE","X":"FILLED","S":"BUY","ps":"BOTH","o":"MARKET","q":"1","z":"1","ap":"100"}}`))
		}
		connections <- conn
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	s, err := NewSession(SessionOptions{Mode: ModeIsolated, Endpoint: websocketRoot(server), Sources: []Source{{ID: "account-1", Provider: p}}, KeepAlive: KeepAliveOptions{Interval: interval, Timeout: time.Second, MaxAttempts: 1}, Connection: ConnectionOptions{HeartbeatPingInterval: time.Second, HeartbeatPongTimeout: time.Second, HeartbeatWriteTimeout: time.Second, ReconnectInitialDelay: time.Millisecond, ReconnectMaxDelay: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.Close()
		select {
		case <-s.Done():
		case <-time.After(2 * time.Second):
			t.Error("session did not finish")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	return s, paths, connections
}
func waitRenewalPath(t *testing.T, paths <-chan string, want string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case path := <-paths:
			if strings.HasSuffix(path, want) {
				return
			}
		case <-deadline:
			t.Fatalf("connection did not adopt %s", want)
		}
	}
}

// Risk: a successful PUT can return a new key without an Acquire call.
func TestKeepAliveResponseAdoptsChangedKeyAndDeliversEvents(t *testing.T) {
	p := &renewalTestProvider{fakeProvider: &fakeProvider{keys: []string{"key-old"}}, renew: func(context.Context, string) (string, error) { return "key-new", nil }}
	s, paths, _ := startRenewalSession(t, p, time.Hour)
	source := s.sourceByID["account-1"]
	key, version := source.current()
	if !s.keepAliveWithRetry(context.Background(), source, key, version) {
		t.Fatal("successful renewal failed")
	}
	waitRenewalPath(t, paths, "key-new")
	seen := map[futures.UserDataEventType]bool{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case event := <-s.Events():
			if event.Generation >= 2 {
				seen[event.Type] = true
			}
		case failure := <-s.Errors():
			t.Fatalf("normal key rotation emitted failure: %v", failure.Err)
		case <-deadline:
			t.Fatalf("new binding did not deliver account and order events: %v", seen)
		}
	}
	if !seen[futures.UserDataEventTypeAccountUpdate] || !seen[futures.UserDataEventTypeOrderTradeUpdate] {
		t.Fatalf("wrong event types: %v", seen)
	}
	current, nextVersion := source.current()
	if current != "key-new" || nextVersion != version+1 {
		t.Fatal("new key not owned by original source")
	}
	if !s.keepAliveWithRetry(context.Background(), source, current, nextVersion) {
		t.Fatal("same-key renewal failed")
	}
	_, unchanged := source.current()
	if unchanged != nextVersion {
		t.Fatal("same key advanced version")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.acquireCalls != 1 || p.releaseCalls != 0 {
		t.Fatalf("unexpected Acquire/Delete during rotation: %d/%d", p.acquireCalls, p.releaseCalls)
	}
}

// Risk: an old failure must not clear a key acquired while classification ran.
func TestKeepAliveStaleResponseCannotInvalidateNewKey(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	p := &renewalTestProvider{fakeProvider: &fakeProvider{keys: []string{"key-old", "key-new"}, keepAliveErr: errors.New("old failure")}, renew: func(context.Context, string) (string, error) { return "", errors.New("old failure") }, classify: func(error) bool { close(entered); <-release; return true }}
	s, paths, connections := startRenewalSession(t, p, time.Hour)
	source := s.sourceByID["account-1"]
	key, version := source.current()
	done := make(chan bool, 1)
	go func() { done <- s.keepAliveWithRetry(context.Background(), source, key, version) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("classification not reached")
	}
	conn := <-connections
	if err := conn.WriteMessage(gorillaws.TextMessage, []byte(`{"e":"listenKeyExpired","E":1,"listenKey":"key-old"}`)); err != nil {
		t.Fatal(err)
	}
	waitRenewalPath(t, paths, "key-new")
	// The new dial has ensured the source before its path reaches the server.
	expectedKey, expectedVersion := source.current()
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("old failure did not finish")
	}
	gotKey, gotVersion := source.current()
	if gotKey != expectedKey || gotVersion != expectedVersion || gotKey != "key-new" {
		t.Fatalf("old response changed current source: %s/%d -> %s/%d", expectedKey, expectedVersion, gotKey, gotVersion)
	}
}

// Risk: a provider may return success after Stop canceled its request.
func TestKeepAliveResponseAfterStopDoesNotReviveSession(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	p := &renewalTestProvider{fakeProvider: &fakeProvider{keys: []string{"key-old"}}, renew: func(ctx context.Context, _ string) (string, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return "key-new", nil
	}}
	s, _, _ := startRenewalSession(t, p, time.Millisecond)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal not reached")
	}
	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not cancel renewal")
	}
	once.Do(func() { close(release) })
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked")
	}
	select {
	case <-s.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session did not finish")
	}
	if key, _ := s.sourceByID["account-1"].current(); key != "" {
		t.Fatal("late success revived source")
	}
	for event := range s.ListenKeyEvents() {
		if event.Kind == "renewed" {
			t.Fatal("canceled response was adopted")
		}
	}
}

func TestKeepAliveOldSuccessAndControlFrameCannotReplaceRenewedSource(t *testing.T) {
	calls := 0
	p := &renewalTestProvider{fakeProvider: &fakeProvider{keys: []string{"key-old"}}, renew: func(context.Context, string) (string, error) { calls++; return "key-new", nil }}
	s, _, _ := startRenewalSession(t, p, time.Hour)
	source := s.sourceByID["account-1"]
	key, version := source.current()
	generation := s.Generation()
	// Exercise the original source owner before connection refresh, retaining
	// the exact old binding. Both control messages are stale on that binding.
	_, changed, err := source.keepAlive(context.Background(), key, version)
	if err != nil || !changed {
		t.Fatalf("renewal: %v/%v", changed, err)
	}
	for _, payload := range []string{`{"e":"listenKeyExpired","E":1}`, `{"code":-1125,"msg":"expired"}`} {
		s.handleFrame(managedws.Frame{Type: managedws.TextMessage, Generation: generation, Payload: []byte(payload)})
		got, current := source.current()
		if got != "key-new" || current != version+1 {
			t.Fatal("old connection invalidated renewed source")
		}
	}
	if !s.keepAliveWithRetry(context.Background(), source, key, version) {
		t.Fatal("stale success did not settle")
	}
	if calls != 1 {
		t.Fatal("stale request sent another PUT")
	}
}

func TestMalformedPrivateHeaderKeepsJSONTypeError(t *testing.T) {
	session, _ := startMalformedPayloadSession(t, []byte(`{"e":123,"E":1}`))
	gap := waitGap(t, session)
	var typed *json.UnmarshalTypeError
	if !errors.As(gap.Err, &typed) || strings.Contains(gap.Err.Error(), "missing event type") {
		t.Fatalf("lost header decode cause: %v", gap.Err)
	}
	if gap.Reason != GapReasonMalformedEvent {
		t.Fatalf("changed malformed handling: %s", gap.Reason)
	}
}

func TestKeepAliveExhaustionMarksFinalFailure(t *testing.T) {
	s := startKeepAliveFailureSession(t, errors.New("temporary connection failure"))
	event := waitKeepAliveFailure(t, s)
	var cause *PrivateError
	if !errors.As(event.Err, &cause) || cause.Operation != "keepalive_exhausted" {
		t.Fatalf("missing exhaustion classification: %v", event.Err)
	}
}

type renewalEndpointDialer struct {
	dial func(context.Context, string) (managedws.Socket, error)
}

func (d renewalEndpointDialer) Dial(ctx context.Context, endpoint string) (managedws.Socket, error) {
	return d.dial(ctx, endpoint)
}

func TestRenewalStaleHandshakeCannotInvalidateNewKey(t *testing.T) {
	p := &renewalTestProvider{fakeProvider: &fakeProvider{keys: []string{"key-old"}}, renew: func(context.Context, string) (string, error) { return "key-new", nil }}
	var events []ListenKeyEvent
	source := newSourceRuntime(Source{ID: "account-1", Provider: p}, func(e ListenKeyEvent) { events = append(events, e) })
	dialer := &privateDialer{mode: ModeIsolated, root: "wss://unused.invalid", sources: []*sourceRuntime{source}, invalidDialError: func(error) bool { return true }}
	dialer.endpointDialer = renewalEndpointDialer{dial: func(ctx context.Context, _ string) (managedws.Socket, error) {
		key, version := source.current()
		if _, changed, err := source.keepAlive(ctx, key, version); err != nil || !changed {
			t.Fatalf("renewal: %v/%v", changed, err)
		}
		return nil, errors.New("old handshake rejected")
	}}
	if _, err := dialer.Dial(context.Background()); err == nil {
		t.Fatal("missing old handshake failure")
	}
	if key, version := source.current(); key != "key-new" || version != 2 {
		t.Fatal("stale handshake invalidated current key")
	}
	if events[len(events)-1].Kind != ListenKeyResponseIgnored {
		t.Fatal("stale handshake not observable")
	}
}

func TestRenewalStaleVersionCannotInvalidateReacquiredIdenticalKey(t *testing.T) {
	p := &fakeProvider{keys: []string{"same-key"}}
	source := newSourceRuntime(Source{ID: "account-1", Provider: p}, nil)
	key, version, err := source.ensure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source.invalidateIfCurrent(key, version, ErrListenKeyExpired)
	_, next, err := source.ensure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, changed := source.invalidateIfCurrent(key, version, ErrListenKeyExpired); changed {
		t.Fatal("old version invalidated reacquired identical key")
	}
	if _, _, err := source.keepAlive(context.Background(), key, version); !errors.Is(err, errStaleListenKeyResponse) {
		t.Fatalf("old request not fenced: %v", err)
	}
	if current, v := source.current(); current != key || v != next {
		t.Fatal("new version changed")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.keepAliveCalls != 0 {
		t.Fatal("stale version sent a renewal")
	}
}
