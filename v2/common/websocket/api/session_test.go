package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	managedgorilla "github.com/btcnash/go-binance/v2/common/websocket/managed/gorilla"
	"github.com/gorilla/websocket"
)

type apiWireRequest struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type apiTestServer struct {
	t           *testing.T
	server      *httptest.Server
	endpoint    string
	upgrader    websocket.Upgrader
	requests    chan apiWireRequest
	connections atomic.Int64
	mu          sync.Mutex
	conns       map[int64]*websocket.Conn
	authCount   atomic.Int64
}

func newAPITestServer(t *testing.T, handler func(*apiTestServer, int64, *websocket.Conn, apiWireRequest)) *apiTestServer {
	t.Helper()
	s := &apiTestServer{
		t:        t,
		requests: make(chan apiWireRequest, 128),
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
				var req apiWireRequest
				if err := json.Unmarshal(payload, &req); err != nil {
					t.Errorf("decode request: %v", err)
					return
				}
				s.requests <- req
				handler(s, id, conn, req)
			}
		}()
	}))
	s.endpoint = "ws" + strings.TrimPrefix(s.server.URL, "http")
	t.Cleanup(s.close)
	return s
}

func (s *apiTestServer) close() {
	s.mu.Lock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.server.Close()
}

func writeAPIResponse(t *testing.T, conn *websocket.Conn, id string, result any) {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{"id": id, "status": 200, "result": result}); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func requestPayload(t *testing.T, id, method string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"id": id, "method": method, "params": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newTestSession(t *testing.T, endpoint string, mutate func(*Options)) *Session {
	t.Helper()
	opts := Options{
		ConnectionOptions: managedws.Options{
			Dialer:    managedgorilla.Dialer{Endpoint: endpoint},
			Reconnect: managedws.ReconnectPolicy{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1},
		},
		RequestTimeout:     200 * time.Millisecond,
		MaxPendingRequests: 32,
		StateBuffer:        32,
		ErrorBuffer:        32,
	}
	if mutate != nil {
		mutate(&opts)
	}
	session, err := NewSession(opts)
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

func startReady(t *testing.T, s *Session) {
	t.Helper()
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func TestConcurrentRequestsAreMatchedByIDOutOfOrder(t *testing.T) {
	var mu sync.Mutex
	received := make([]apiWireRequest, 0, 2)
	server := newAPITestServer(t, func(s *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		mu.Lock()
		received = append(received, req)
		if len(received) == 2 {
			copyReqs := append([]apiWireRequest(nil), received...)
			mu.Unlock()
			writeAPIResponse(t, conn, copyReqs[1].ID, copyReqs[1].Method)
			writeAPIResponse(t, conn, copyReqs[0].ID, copyReqs[0].Method)
			return
		}
		mu.Unlock()
	})
	session := newTestSession(t, server.endpoint, nil)
	startReady(t, session)

	type result struct {
		id       string
		response Response
		err      error
	}
	results := make(chan result, 2)
	for _, tc := range []struct{ id, method string }{{"one", "order.status"}, {"two", "account.status"}} {
		tc := tc
		go func() {
			resp, err := session.Do(context.Background(), Request{ID: tc.id, Method: tc.method, Payload: requestPayload(t, tc.id, tc.method), Outcome: OutcomeSafe})
			results <- result{tc.id, resp, err}
		}()
	}
	got := map[string]string{}
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("Do(%s): %v", r.id, r.err)
		}
		var envelope struct {
			Result string `json:"result"`
		}
		if err := json.Unmarshal(r.response.Payload, &envelope); err != nil {
			t.Fatal(err)
		}
		got[r.id] = envelope.Result
	}
	if got["one"] != "order.status" || got["two"] != "account.status" {
		t.Fatalf("responses = %v", got)
	}
}

func TestRequestTimeoutOnlyFailsCurrentRequest(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		if req.ID == "fast" {
			writeAPIResponse(t, conn, req.ID, "ok")
		}
	})
	session := newTestSession(t, server.endpoint, func(o *Options) { o.RequestTimeout = 40 * time.Millisecond })
	startReady(t, session)

	slowErr := make(chan error, 1)
	go func() {
		_, err := session.Do(context.Background(), Request{ID: "slow", Method: "order.place", Payload: requestPayload(t, "slow", "order.place"), Outcome: OutcomeUnknown})
		slowErr <- err
	}()
	resp, err := session.Do(context.Background(), Request{ID: "fast", Method: "time", Payload: requestPayload(t, "fast", "time"), Outcome: OutcomeSafe})
	if err != nil {
		t.Fatalf("fast request: %v", err)
	}
	if resp.ID != "fast" {
		t.Fatalf("response ID = %q", resp.ID)
	}
	var unknown *UnknownOutcomeError
	if err := <-slowErr; !errors.As(err, &unknown) || !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("slow error = %T %v", err, err)
	}
}

func TestDisconnectReturnsUnknownOutcomeWithoutReplayAndReconnects(t *testing.T) {
	var tradeCount atomic.Int64
	server := newAPITestServer(t, func(_ *apiTestServer, connID int64, conn *websocket.Conn, req apiWireRequest) {
		if req.Method == "order.place" {
			tradeCount.Add(1)
			_ = conn.Close()
			return
		}
		writeAPIResponse(t, conn, req.ID, fmt.Sprintf("conn-%d", connID))
	})
	session := newTestSession(t, server.endpoint, nil)
	startReady(t, session)

	_, err := session.Do(context.Background(), Request{ID: "trade-1", Method: "order.place", Payload: requestPayload(t, "trade-1", "order.place"), Outcome: OutcomeUnknown})
	var unknown *UnknownOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("error = %T %v, want UnknownOutcomeError", err, err)
	}
	if tradeCount.Load() != 1 {
		t.Fatalf("trade sent %d times", tradeCount.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady after reconnect: %v", err)
	}
	resp, err := session.Do(context.Background(), Request{ID: "after", Method: "time", Payload: requestPayload(t, "after", "time"), Outcome: OutcomeSafe})
	if err != nil {
		t.Fatalf("request after reconnect: %v", err)
	}
	if resp.Generation < 2 {
		t.Fatalf("generation = %d, want >= 2", resp.Generation)
	}
	if tradeCount.Load() != 1 {
		t.Fatalf("trade replayed, count=%d", tradeCount.Load())
	}
}

func TestDuplicateRequestIDRejected(t *testing.T) {
	release := make(chan struct{})
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		if req.ID == "same" {
			<-release
			writeAPIResponse(t, conn, req.ID, "ok")
		}
	})
	session := newTestSession(t, server.endpoint, nil)
	startReady(t, session)
	first := make(chan error, 1)
	go func() {
		_, err := session.Do(context.Background(), Request{ID: "same", Method: "time", Payload: requestPayload(t, "same", "time"), Outcome: OutcomeSafe})
		first <- err
	}()
	select {
	case <-server.requests:
	case <-time.After(time.Second):
		t.Fatal("first request not received")
	}
	_, err := session.Do(context.Background(), Request{ID: "same", Method: "time", Payload: requestPayload(t, "same", "time"), Outcome: OutcomeSafe})
	if !errors.Is(err, ErrDuplicateRequestID) {
		t.Fatalf("duplicate error = %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first request: %v", err)
	}
}

type testAuthenticator struct{ calls atomic.Int64 }

func (a *testAuthenticator) BuildRequest(generation uint64) (Request, error) {
	a.calls.Add(1)
	id := fmt.Sprintf("auth-%d", generation)
	return Request{ID: id, Method: "session.logon", Payload: requestPayloadNoTest(id, "session.logon"), Outcome: OutcomeSafe}, nil
}
func (a *testAuthenticator) ValidateResponse(response Response) error { return nil }
func requestPayloadNoTest(id, method string) []byte {
	raw, _ := json.Marshal(map[string]any{"id": id, "method": method, "params": map[string]any{}})
	return raw
}

func TestAuthenticatorRunsOnEveryGenerationBeforeReady(t *testing.T) {
	auth := &testAuthenticator{}
	var normalConnectionsMu sync.Mutex
	normalConnections := []int64{}
	server := newAPITestServer(t, func(s *apiTestServer, connID int64, conn *websocket.Conn, req apiWireRequest) {
		if req.Method == "session.logon" {
			s.authCount.Add(1)
			writeAPIResponse(t, conn, req.ID, "authenticated")
			return
		}
		if req.Method == "force.disconnect" {
			_ = conn.Close()
			return
		}
		normalConnectionsMu.Lock()
		normalConnections = append(normalConnections, connID)
		normalConnectionsMu.Unlock()
		writeAPIResponse(t, conn, req.ID, "ok")
	})
	session := newTestSession(t, server.endpoint, func(o *Options) { o.Authenticator = auth })
	startReady(t, session)
	if auth.calls.Load() != 1 || server.authCount.Load() != 1 {
		t.Fatalf("initial auth calls build=%d server=%d", auth.calls.Load(), server.authCount.Load())
	}

	_, err := session.Do(context.Background(), Request{ID: "disconnect", Method: "force.disconnect", Payload: requestPayload(t, "disconnect", "force.disconnect"), Outcome: OutcomeSafe})
	if err == nil {
		t.Fatal("disconnect request unexpectedly succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady after auth reconnect: %v", err)
	}
	if auth.calls.Load() < 2 || server.authCount.Load() < 2 {
		t.Fatalf("auth not repeated build=%d server=%d", auth.calls.Load(), server.authCount.Load())
	}
	if _, err := session.Do(context.Background(), Request{ID: "normal", Method: "time", Payload: requestPayload(t, "normal", "time"), Outcome: OutcomeSafe}); err != nil {
		t.Fatalf("normal request: %v", err)
	}
}

func TestGracefulRotationDrainsOldPendingRequests(t *testing.T) {
	slowRelease := make(chan struct{})
	var requestConnsMu sync.Mutex
	requestConns := map[string]int64{}
	server := newAPITestServer(t, func(_ *apiTestServer, connID int64, conn *websocket.Conn, req apiWireRequest) {
		requestConnsMu.Lock()
		requestConns[req.ID] = connID
		requestConnsMu.Unlock()
		if req.ID == "slow" {
			<-slowRelease
			writeAPIResponse(t, conn, req.ID, "slow-ok")
			return
		}
		writeAPIResponse(t, conn, req.ID, "ok")
	})
	session := newTestSession(t, server.endpoint, func(o *Options) {
		o.RequestTimeout = time.Second
		o.Rotation = RotationOptions{Enabled: true, MaxAge: 40 * time.Millisecond, DrainTimeout: 500 * time.Millisecond}
	})
	startReady(t, session)
	slowResult := make(chan error, 1)
	go func() {
		_, err := session.Do(context.Background(), Request{ID: "slow", Method: "order.status", Payload: requestPayload(t, "slow", "order.status"), Outcome: OutcomeSafe})
		slowResult <- err
	}()
	select {
	case <-server.requests:
	case <-time.After(time.Second):
		t.Fatal("slow request not received")
	}
	deadline := time.Now().Add(time.Second)
	for (server.connections.Load() < 2 || session.Generation() < 2) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.connections.Load() < 2 || session.Generation() < 2 {
		t.Fatalf("rotation did not switch to the second connection: connections=%d generation=%d", server.connections.Load(), session.Generation())
	}
	if _, err := session.Do(context.Background(), Request{ID: "new", Method: "time", Payload: requestPayload(t, "new", "time"), Outcome: OutcomeSafe}); err != nil {
		t.Fatalf("new request: %v", err)
	}
	close(slowRelease)
	if err := <-slowResult; err != nil {
		t.Fatalf("slow request did not drain: %v", err)
	}
	requestConnsMu.Lock()
	oldConn, newConn := requestConns["slow"], requestConns["new"]
	requestConnsMu.Unlock()
	if oldConn == 0 || newConn == 0 || oldConn == newConn {
		t.Fatalf("request connections slow=%d new=%d", oldConn, newConn)
	}
}

func TestUnsolicitedFramesArePublished(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		writeAPIResponse(t, conn, req.ID, "ok")
		_ = conn.WriteJSON(map[string]any{"event": "notice"})
	})
	session := newTestSession(t, server.endpoint, nil)
	startReady(t, session)
	if _, err := session.Do(context.Background(), Request{ID: "x", Method: "time", Payload: requestPayload(t, "x", "time"), Outcome: OutcomeSafe}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-session.Unsolicited():
		if !strings.Contains(string(event.Payload), "notice") {
			t.Fatalf("payload=%s", event.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("no unsolicited frame")
	}
}

func TestNormalizeIDAcceptsStringAndNumber(t *testing.T) {
	cases := map[string]string{`"abc"`: "abc", `42`: "42"}
	keys := make([]string, 0, len(cases))
	for k := range cases {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, raw := range keys {
		got, err := normalizeWireID(json.RawMessage(raw))
		if err != nil || got != cases[raw] {
			t.Fatalf("normalize %s = %q,%v", raw, got, err)
		}
	}
}

func TestDefaultOutcomePolicyIsUnknown(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		if req.ID == "default-unknown" {
			_ = conn.Close()
		}
	})
	session := newTestSession(t, server.endpoint, nil)
	startReady(t, session)
	_, err := session.Do(context.Background(), Request{ID: "default-unknown", Method: "order.place", Payload: requestPayload(t, "default-unknown", "order.place")})
	var unknown *UnknownOutcomeError
	if !errors.As(err, &unknown) || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("error = %T %v, want default UnknownOutcomeError", err, err)
	}
}

func TestSafeRequestDisconnectIsNotUnknown(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, _ apiWireRequest) {
		_ = conn.Close()
	})
	session := newTestSession(t, server.endpoint, nil)
	startReady(t, session)
	_, err := session.Do(context.Background(), Request{ID: "safe", Method: "time", Payload: requestPayload(t, "safe", "time"), Outcome: OutcomeSafe})
	var requestErr *RequestError
	var unknown *UnknownOutcomeError
	if !errors.As(err, &requestErr) || !errors.Is(err, ErrDisconnected) {
		t.Fatalf("error = %T %v, want disconnected RequestError", err, err)
	}
	if errors.As(err, &unknown) {
		t.Fatalf("safe request returned UnknownOutcomeError: %v", err)
	}
}

func TestPendingRequestLimit(t *testing.T) {
	release := make(chan struct{})
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		if req.ID == "held" {
			<-release
			writeAPIResponse(t, conn, req.ID, "ok")
		}
	})
	session := newTestSession(t, server.endpoint, func(o *Options) { o.MaxPendingRequests = 1 })
	startReady(t, session)
	first := make(chan error, 1)
	go func() {
		_, err := session.Do(context.Background(), Request{ID: "held", Method: "time", Payload: requestPayload(t, "held", "time"), Outcome: OutcomeSafe})
		first <- err
	}()
	select {
	case <-server.requests:
	case <-time.After(time.Second):
		t.Fatal("held request not received")
	}
	_, err := session.Do(context.Background(), Request{ID: "second", Method: "time", Payload: requestPayload(t, "second", "time"), Outcome: OutcomeSafe})
	if !errors.Is(err, ErrTooManyPendingRequests) {
		t.Fatalf("second error = %v, want ErrTooManyPendingRequests", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("held request: %v", err)
	}
}

func TestRequestIDCannotBeReusedWithinGenerationButCanAfterReconnect(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		switch req.Method {
		case "no.response":
			return
		case "disconnect":
			_ = conn.Close()
		default:
			writeAPIResponse(t, conn, req.ID, "ok")
		}
	})
	session := newTestSession(t, server.endpoint, func(o *Options) { o.RequestTimeout = 30 * time.Millisecond })
	startReady(t, session)
	_, err := session.Do(context.Background(), Request{ID: "reuse", Method: "no.response", Payload: requestPayload(t, "reuse", "no.response"), Outcome: OutcomeSafe})
	if !errors.Is(err, ErrRequestTimeout) {
		t.Fatalf("first error = %v, want timeout", err)
	}
	_, err = session.Do(context.Background(), Request{ID: "reuse", Method: "time", Payload: requestPayload(t, "reuse", "time"), Outcome: OutcomeSafe})
	if !errors.Is(err, ErrDuplicateRequestID) {
		t.Fatalf("same-generation reuse error = %v, want duplicate", err)
	}
	_, _ = session.Do(context.Background(), Request{ID: "disconnect-id", Method: "disconnect", Payload: requestPayload(t, "disconnect-id", "disconnect"), Outcome: OutcomeSafe})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady after reconnect: %v", err)
	}
	if session.Generation() < 2 || server.connections.Load() < 2 {
		t.Fatalf("generation=%d connections=%d", session.Generation(), server.connections.Load())
	}
	if _, err := session.Do(context.Background(), Request{ID: "reuse", Method: "time", Payload: requestPayload(t, "reuse", "time"), Outcome: OutcomeSafe}); err != nil {
		t.Fatalf("reuse after reconnect: %v", err)
	}
}

type rejectingAuthenticator struct{}

func (rejectingAuthenticator) BuildRequest(generation uint64) (Request, error) {
	id := fmt.Sprintf("reject-auth-%d", generation)
	return Request{ID: id, Method: "session.logon", Payload: requestPayloadNoTest(id, "session.logon"), Outcome: OutcomeSafe}, nil
}
func (rejectingAuthenticator) ValidateResponse(Response) error {
	return errors.New("invalid credentials")
}

func TestAuthenticationValidationFailureIsTerminal(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		writeAPIResponse(t, conn, req.ID, "rejected")
	})
	session := newTestSession(t, server.endpoint, func(o *Options) { o.Authenticator = rejectingAuthenticator{} })
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := session.WaitReady(ctx)
	if !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("WaitReady error = %v, want authentication failure", err)
	}
	if session.State() != StateFailed {
		t.Fatalf("state = %s, want failed", session.State())
	}
}

func TestRotationDrainTimeoutFailsUnknownRequestWithoutReplay(t *testing.T) {
	var slowCount atomic.Int64
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		if req.ID == "slow-trade" {
			slowCount.Add(1)
			return
		}
		writeAPIResponse(t, conn, req.ID, "ok")
	})
	session := newTestSession(t, server.endpoint, func(o *Options) {
		o.RequestTimeout = time.Second
		o.Rotation = RotationOptions{Enabled: true, MaxAge: 30 * time.Millisecond, DrainTimeout: 20 * time.Millisecond}
	})
	startReady(t, session)
	result := make(chan error, 1)
	go func() {
		_, err := session.Do(context.Background(), Request{ID: "slow-trade", Method: "order.place", Payload: requestPayload(t, "slow-trade", "order.place"), Outcome: OutcomeUnknown})
		result <- err
	}()
	select {
	case <-server.requests:
	case <-time.After(time.Second):
		t.Fatal("slow trade not received")
	}
	select {
	case err := <-result:
		var unknown *UnknownOutcomeError
		if !errors.As(err, &unknown) || !errors.Is(err, ErrDisconnected) {
			t.Fatalf("drain timeout error = %T %v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("rotation drain timeout did not fail pending request")
	}
	if slowCount.Load() != 1 {
		t.Fatalf("slow trade sent %d times", slowCount.Load())
	}
}

func TestCloseFailsPendingUnknownRequest(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, _ *websocket.Conn, _ apiWireRequest) {})
	session := newTestSession(t, server.endpoint, func(o *Options) { o.RequestTimeout = time.Second })
	startReady(t, session)
	result := make(chan error, 1)
	go func() {
		_, err := session.Do(context.Background(), Request{ID: "pending-close", Method: "order.place", Payload: requestPayload(t, "pending-close", "order.place"), Outcome: OutcomeUnknown})
		result <- err
	}()
	select {
	case <-server.requests:
	case <-time.After(time.Second):
		t.Fatal("pending request not received")
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		var unknown *UnknownOutcomeError
		if !errors.As(err, &unknown) || !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("close error = %T %v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending request did not finish on close")
	}
}

func TestOneHundredConcurrentRequestsWithReverseResponses(t *testing.T) {
	const total = 100
	var mu sync.Mutex
	received := make([]apiWireRequest, 0, total)
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		mu.Lock()
		received = append(received, req)
		if len(received) != total {
			mu.Unlock()
			return
		}
		batch := append([]apiWireRequest(nil), received...)
		mu.Unlock()
		for i := len(batch) - 1; i >= 0; i-- {
			writeAPIResponse(t, conn, batch[i].ID, batch[i].Method)
		}
	})
	session := newTestSession(t, server.endpoint, func(o *Options) {
		o.RequestTimeout = 2 * time.Second
		o.MaxPendingRequests = total + 8
	})
	startReady(t, session)

	type result struct {
		id  string
		err error
	}
	results := make(chan result, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("request-%03d", i)
		method := fmt.Sprintf("method-%03d", i)
		go func() {
			response, err := session.Do(context.Background(), Request{ID: id, Method: method, Payload: requestPayloadNoTest(id, method), Outcome: OutcomeSafe})
			if err == nil && response.ID != id {
				err = fmt.Errorf("response id = %q, want %q", response.ID, id)
			}
			results <- result{id: id, err: err}
		}()
	}
	seen := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("request %s: %v", r.id, r.err)
		}
		seen[r.id] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("completed %d unique requests, want %d", len(seen), total)
	}
}
