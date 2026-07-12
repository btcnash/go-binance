package stream

import (
	"context"
	"encoding/json"
	"errors"
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

type streamWireRequest struct {
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
	ID     uint64            `json:"id"`
}

func (r streamWireRequest) stringParams(t *testing.T) []string {
	t.Helper()
	out := make([]string, 0, len(r.Params))
	for _, raw := range r.Params {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatalf("decode string param: %v", err)
		}
		out = append(out, value)
	}
	return out
}

type localStreamServer struct {
	t        *testing.T
	server   *httptest.Server
	endpoint string
	upgrader websocket.Upgrader

	connections int64
	requests    chan streamWireRequest
	connsMu     sync.Mutex
	conns       []*websocket.Conn
}

func newLocalStreamServer(t *testing.T) *localStreamServer {
	t.Helper()
	s := &localStreamServer{
		t:        t,
		requests: make(chan streamWireRequest, 64),
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		atomic.AddInt64(&s.connections, 1)
		s.connsMu.Lock()
		s.conns = append(s.conns, conn)
		s.connsMu.Unlock()
		go s.readLoop(conn)
	}))
	s.endpoint = "ws" + strings.TrimPrefix(s.server.URL, "http")
	t.Cleanup(s.close)
	return s
}

func (s *localStreamServer) readLoop(conn *websocket.Conn) {
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req streamWireRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			s.t.Errorf("decode request %s: %v", payload, err)
			return
		}
		s.requests <- req
	}
}

func (s *localStreamServer) close() {
	s.connsMu.Lock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.connsMu.Unlock()
	s.server.Close()
}

func (s *localStreamServer) latestConn(t *testing.T) *websocket.Conn {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.connsMu.Lock()
		if len(s.conns) > 0 {
			conn := s.conns[len(s.conns)-1]
			s.connsMu.Unlock()
			return conn
		}
		s.connsMu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no websocket connection")
	return nil
}

func (s *localStreamServer) waitRequest(t *testing.T) streamWireRequest {
	t.Helper()
	select {
	case req := <-s.requests:
		return req
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for websocket request")
		return streamWireRequest{}
	}
}

func writeJSON(t *testing.T, conn *websocket.Conn, value interface{}) {
	t.Helper()
	if err := conn.WriteJSON(value); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}
}

func newTestStreamSession(t *testing.T, server *localStreamServer, initial []Subscription, mutate func(*StreamSessionOptions)) *StreamSession {
	t.Helper()
	options := StreamSessionOptions{
		Class:                StreamClassMarket,
		InitialSubscriptions: initial,
		AckTimeout:           100 * time.Millisecond,
		RequestInterval:      time.Nanosecond,
		MaxBatchSize:         2,
		MaxStreams:           16,
		EventBuffer:          8,
		GapBuffer:            8,
		DisableHeartbeat:     true,
		ConnectionOptions: managedws.Options{
			Dialer: managedgorilla.Dialer{Endpoint: server.endpoint},
			Reconnect: managedws.ReconnectPolicy{
				Enabled:      true,
				InitialDelay: time.Millisecond,
				MaxDelay:     time.Millisecond,
				Multiplier:   1,
			},
		},
	}
	if mutate != nil {
		mutate(&options)
	}
	session, err := NewStreamSession(options)
	if err != nil {
		t.Fatalf("NewStreamSession() error = %v", err)
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

func TestStreamSessionSubscribeUnsubscribeListAndOutOfOrderResponses(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, nil)
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- session.Subscribe(context.Background(), AggTrade("BTCUSDT"), Ticker("ETHUSDT"))
	}()
	subscribeReq := server.waitRequest(t)
	if subscribeReq.Method != methodSubscribe {
		t.Fatalf("method = %s, want SUBSCRIBE", subscribeReq.Method)
	}
	gotParams := subscribeReq.stringParams(t)
	sort.Strings(gotParams)
	wantParams := []string{"btcusdt@aggTrade", "ethusdt@ticker"}
	if strings.Join(gotParams, ",") != strings.Join(wantParams, ",") {
		t.Fatalf("subscribe params = %v, want %v", gotParams, wantParams)
	}
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": subscribeReq.ID})
	if err := <-subscribeDone; err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	listResult := make(chan []Subscription, 1)
	listErr := make(chan error, 1)
	combinedResult := make(chan bool, 1)
	combinedErr := make(chan error, 1)
	go func() {
		result, err := session.ListSubscriptions(context.Background())
		listResult <- result
		listErr <- err
	}()
	go func() {
		result, err := session.GetCombined(context.Background())
		combinedResult <- result
		combinedErr <- err
	}()

	first := server.waitRequest(t)
	second := server.waitRequest(t)
	conn := server.latestConn(t)
	for _, req := range []streamWireRequest{second, first} {
		switch req.Method {
		case methodListSubscriptions:
			writeJSON(t, conn, map[string]interface{}{"result": []string{"btcusdt@aggTrade", "ethusdt@ticker"}, "id": req.ID})
		case methodGetProperty:
			writeJSON(t, conn, map[string]interface{}{"result": true, "id": req.ID})
		default:
			t.Fatalf("unexpected method %s", req.Method)
		}
	}
	if err := <-listErr; err != nil {
		t.Fatalf("ListSubscriptions() error = %v", err)
	}
	listed := <-listResult
	if len(listed) != 2 {
		t.Fatalf("listed count = %d, want 2", len(listed))
	}
	if err := <-combinedErr; err != nil {
		t.Fatalf("GetCombined() error = %v", err)
	}
	if !<-combinedResult {
		t.Fatal("GetCombined() = false, want true")
	}

	unsubscribeDone := make(chan error, 1)
	go func() { unsubscribeDone <- session.Unsubscribe(context.Background(), Ticker("ETHUSDT")) }()
	unsubscribeReq := server.waitRequest(t)
	if unsubscribeReq.Method != methodUnsubscribe {
		t.Fatalf("method = %s, want UNSUBSCRIBE", unsubscribeReq.Method)
	}
	writeJSON(t, conn, map[string]interface{}{"result": nil, "id": unsubscribeReq.ID})
	if err := <-unsubscribeDone; err != nil {
		t.Fatalf("Unsubscribe() error = %v", err)
	}

	active := session.ActiveSubscriptions()
	if len(active) != 1 || active[0].String() != "btcusdt@aggTrade" {
		t.Fatalf("active subscriptions = %v", active)
	}
}

func TestStreamSessionReconnectRestoresLatestDesiredAndWaitsForACK(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, []Subscription{AggTrade("BTCUSDT")}, nil)
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	firstSubscribe := server.waitRequest(t)
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": firstSubscribe.ID})
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	_ = server.latestConn(t).Close()
	waitStreamState(t, session.States(), StreamStateDisconnected, time.Second)

	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- session.ReplaceSubscriptions(context.Background(), []Subscription{
			Kline("ETHUSDT", "1m"),
			Ticker("BNBUSDT"),
		})
	}()

	restored := server.waitRequest(t)
	if restored.Method != methodSubscribe {
		t.Fatalf("restore method = %s, want SUBSCRIBE", restored.Method)
	}
	params := restored.stringParams(t)
	sort.Strings(params)
	want := []string{"bnbusdt@ticker", "ethusdt@kline_1m"}
	if strings.Join(params, ",") != strings.Join(want, ",") {
		t.Fatalf("restored params = %v, want %v", params, want)
	}

	assertNoStreamState(t, session.States(), StreamStateReady, 30*time.Millisecond)
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": restored.ID})
	if err := <-replaceDone; err != nil {
		t.Fatalf("ReplaceSubscriptions() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	active := session.ActiveSubscriptions()
	if len(active) != 2 {
		t.Fatalf("active count = %d, want 2", len(active))
	}
}

func TestStreamSessionRejectsWrongClassWithoutSending(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, nil)
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	err := session.Subscribe(context.Background(), BookTicker("BTCUSDT"))
	if !errors.Is(err, ErrWrongStreamClass) {
		t.Fatalf("Subscribe() error = %v, want ErrWrongStreamClass", err)
	}
	select {
	case req := <-server.requests:
		t.Fatalf("unexpected wire request: %+v", req)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestStreamSessionACKTimeoutIsTypedAndForcesReconnect(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, func(options *StreamSessionOptions) {
		options.AckTimeout = 20 * time.Millisecond
	})
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := session.Subscribe(ctx, AggTrade("BTCUSDT"))
	if !errors.Is(err, ErrSubscriptionACKTimeout) {
		t.Fatalf("Subscribe() error = %v, want ErrSubscriptionACKTimeout", err)
	}
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt64(&server.connections) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt64(&server.connections); got < 2 {
		t.Fatalf("connection count = %d, want reconnect", got)
	}
}

func TestStreamSessionSlowConsumerPublishesGapAndReconnects(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, func(options *StreamSessionOptions) {
		options.EventBuffer = 1
	})
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	conn := server.latestConn(t)
	writeJSON(t, conn, map[string]interface{}{"stream": "btcusdt@aggTrade", "data": map[string]interface{}{"e": "aggTrade", "n": 1}})
	writeJSON(t, conn, map[string]interface{}{"stream": "btcusdt@aggTrade", "data": map[string]interface{}{"e": "aggTrade", "n": 2}})

	select {
	case gap := <-session.Gaps():
		if gap.Reason != GapReasonEventOverflow {
			t.Fatalf("gap reason = %s, want %s", gap.Reason, GapReasonEventOverflow)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for gap event")
	}

	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt64(&server.connections) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt64(&server.connections); got < 2 {
		t.Fatalf("connection count = %d, want reconnect after overflow", got)
	}
}

func TestReplaceSubscriptionsUsesConfiguredBatches(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, func(options *StreamSessionOptions) {
		options.MaxBatchSize = 2
	})
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	done := make(chan error, 1)
	go func() {
		done <- session.ReplaceSubscriptions(context.Background(), []Subscription{
			AggTrade("BTCUSDT"), Ticker("ETHUSDT"), Kline("BNBUSDT", "1m"),
		})
	}()
	first := server.waitRequest(t)
	if len(first.Params) != 2 {
		t.Fatalf("first batch size = %d, want 2", len(first.Params))
	}
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": first.ID})
	second := server.waitRequest(t)
	if len(second.Params) != 1 {
		t.Fatalf("second batch size = %d, want 1", len(second.Params))
	}
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": second.ID})
	if err := <-done; err != nil {
		t.Fatalf("ReplaceSubscriptions() error = %v", err)
	}
}

func waitStreamState(t *testing.T, states <-chan StreamStateEvent, want StreamSessionState, timeout time.Duration) StreamStateEvent {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-states:
			if !ok {
				t.Fatalf("state channel closed before %s", want)
			}
			if event.State == want {
				return event
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for state %s", want)
		}
	}
}

func assertNoStreamState(t *testing.T, states <-chan StreamStateEvent, forbidden StreamSessionState, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-states:
			if !ok {
				return
			}
			if event.State == forbidden {
				t.Fatalf("unexpected state %s before subscription ACK", forbidden)
			}
		case <-timer.C:
			return
		}
	}
}

func TestSetCombinedAndRequestRejection(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, nil)
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	setDone := make(chan error, 1)
	go func() { setDone <- session.SetCombined(context.Background(), false) }()
	setReq := server.waitRequest(t)
	if setReq.Method != methodSetProperty || len(setReq.Params) != 2 {
		t.Fatalf("SET_PROPERTY request = %+v", setReq)
	}
	var property string
	var enabled bool
	if err := json.Unmarshal(setReq.Params[0], &property); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(setReq.Params[1], &enabled); err != nil {
		t.Fatal(err)
	}
	if property != "combined" || enabled {
		t.Fatalf("SET_PROPERTY params = %q, %v", property, enabled)
	}
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": setReq.ID})
	if err := <-setDone; err != nil {
		t.Fatalf("SetCombined() error = %v", err)
	}

	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- session.Subscribe(context.Background(), AggTrade("BTCUSDT")) }()
	req := server.waitRequest(t)
	writeJSON(t, server.latestConn(t), map[string]interface{}{"code": 2, "msg": "invalid stream", "id": req.ID})
	if err := <-subscribeDone; !errors.Is(err, ErrRequestRejected) {
		t.Fatalf("Subscribe() error = %v, want ErrRequestRejected", err)
	}
}

func TestStreamSessionDeliversCombinedAndRawEvents(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, nil)
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)
	conn := server.latestConn(t)

	writeJSON(t, conn, map[string]interface{}{
		"stream": "btcusdt@aggTrade",
		"data":   map[string]interface{}{"e": "aggTrade", "s": "BTCUSDT"},
	})
	combined := <-session.Events()
	if combined.Stream != "btcusdt@aggTrade" || !strings.Contains(string(combined.Data), "aggTrade") {
		t.Fatalf("combined event = %+v", combined)
	}

	writeJSON(t, conn, map[string]interface{}{"e": "markPriceUpdate", "s": "BTCUSDT"})
	raw := <-session.Events()
	if raw.Stream != "" || !strings.Contains(string(raw.Data), "markPriceUpdate") {
		t.Fatalf("raw event = %+v", raw)
	}
}

func TestStreamSessionCallsBeforeStartAndCanceledContextFailImmediately(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, nil)
	if _, err := session.ListSubscriptions(context.Background()); !errors.Is(err, ErrSessionNotStarted) {
		t.Fatalf("ListSubscriptions() error = %v, want ErrSessionNotStarted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := session.Subscribe(ctx, AggTrade("BTCUSDT")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe() error = %v, want context.Canceled", err)
	}
	if err := session.SetCombined(nil, true); !errors.Is(err, ErrInvalidStreamOptions) {
		t.Fatalf("SetCombined(nil) error = %v, want ErrInvalidStreamOptions", err)
	}
}

func TestSubscribeLimitFailureDoesNotCorruptDesiredState(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, func(options *StreamSessionOptions) {
		options.MaxStreams = 1
	})
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	done := make(chan error, 1)
	go func() { done <- session.Subscribe(context.Background(), AggTrade("BTCUSDT")) }()
	req := server.waitRequest(t)
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": req.ID})
	if err := <-done; err != nil {
		t.Fatalf("initial Subscribe() error = %v", err)
	}

	err := session.Subscribe(context.Background(), AggTrade("BTCUSDT"), Ticker("ETHUSDT"))
	if !errors.Is(err, ErrTooManySubscriptions) {
		t.Fatalf("Subscribe() error = %v, want ErrTooManySubscriptions", err)
	}
	desired := session.DesiredSubscriptions()
	if len(desired) != 1 || desired[0].String() != "btcusdt@aggTrade" {
		t.Fatalf("desired after rejected mutation = %v", desired)
	}
}

func TestConflictingDesiredMutationSupersedesEarlierWaiterAndConverges(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, nil)
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	firstDone := make(chan error, 1)
	go func() { firstDone <- session.Subscribe(context.Background(), AggTrade("BTCUSDT")) }()
	subReq := server.waitRequest(t)

	secondDone := make(chan error, 1)
	go func() { secondDone <- session.Unsubscribe(context.Background(), AggTrade("BTCUSDT")) }()
	if err := <-firstDone; !errors.Is(err, ErrOperationSuperseded) {
		t.Fatalf("first operation error = %v, want ErrOperationSuperseded", err)
	}
	select {
	case err := <-secondDone:
		t.Fatalf("unsubscribe completed before in-flight subscribe resolved: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	conn := server.latestConn(t)
	writeJSON(t, conn, map[string]interface{}{"result": nil, "id": subReq.ID})
	unsubReq := server.waitRequest(t)
	if unsubReq.Method != methodUnsubscribe {
		t.Fatalf("method = %s, want UNSUBSCRIBE", unsubReq.Method)
	}
	writeJSON(t, conn, map[string]interface{}{"result": nil, "id": unsubReq.ID})
	if err := <-secondDone; err != nil {
		t.Fatalf("Unsubscribe() error = %v", err)
	}
	if len(session.ActiveSubscriptions()) != 0 || len(session.DesiredSubscriptions()) != 0 {
		t.Fatalf("session did not converge to empty state")
	}
}

func TestSlowObserverDoesNotBlockTransportOrSubscriptionACK(t *testing.T) {
	server := newLocalStreamServer(t)
	block := make(chan struct{})
	session := newTestStreamSession(t, server, nil, func(options *StreamSessionOptions) {
		options.ObserverBuffer = 1
		options.Observer = StreamObserverFuncs{State: func(StreamStateEvent) { <-block }}
	})
	defer close(block)
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	done := make(chan error, 1)
	go func() { done <- session.Subscribe(context.Background(), AggTrade("BTCUSDT")) }()
	req := server.waitRequest(t)
	writeJSON(t, server.latestConn(t), map[string]interface{}{"result": nil, "id": req.ID})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow observer blocked subscription processing")
	}
}

func TestContextCancellationTerminatesSessionAndRejectsFurtherCalls(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	if err := session.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitStreamState(t, session.States(), StreamStateReady, time.Second)

	cancel()
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not terminate after parent cancellation")
	}

	if err := session.Subscribe(context.Background(), AggTrade("BTCUSDT")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Subscribe() after termination error = %v, want ErrSessionClosed", err)
	}
	if _, err := session.ListSubscriptions(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("ListSubscriptions() after termination error = %v, want ErrSessionClosed", err)
	}
	if err := session.SetCombined(context.Background(), true); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("SetCombined() after termination error = %v, want ErrSessionClosed", err)
	}

	assertStreamChannelClosed(t, "events", session.Events())
	assertStateChannelClosed(t, "states", session.States())
	assertErrorChannelClosed(t, "errors", session.Errors())
	assertGapChannelClosed(t, "gaps", session.Gaps())
}

func TestConcurrentMutationAndShutdownDoesNotSendOnClosedChannels(t *testing.T) {
	for iteration := 0; iteration < 25; iteration++ {
		server := newLocalStreamServer(t)
		session := newTestStreamSession(t, server, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		if err := session.Start(ctx); err != nil {
			t.Fatalf("iteration %d: Start() error = %v", iteration, err)
		}
		waitStreamState(t, session.States(), StreamStateReady, time.Second)

		started := make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			close(started)
			callCtx, callCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer callCancel()
			finished <- session.Subscribe(callCtx, AggTrade("BTCUSDT"))
		}()
		<-started
		cancel()
		select {
		case <-session.Done():
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: session did not terminate", iteration)
		}
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: mutation did not return", iteration)
		}
	}
}

func assertStreamChannelClosed(t *testing.T, name string, ch <-chan StreamEvent) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("%s channel did not close", name)
		}
	}
}

func assertStateChannelClosed(t *testing.T, name string, ch <-chan StreamStateEvent) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("%s channel did not close", name)
		}
	}
}

func assertErrorChannelClosed(t *testing.T, name string, ch <-chan StreamErrorEvent) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("%s channel did not close", name)
		}
	}
}

func assertGapChannelClosed(t *testing.T, name string, ch <-chan GapEvent) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("%s channel did not close", name)
		}
	}
}
