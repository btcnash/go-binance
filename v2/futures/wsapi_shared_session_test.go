package futures

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiws "github.com/btcnash/go-binance/v2/common/websocket/api"
	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	managedfutures "github.com/btcnash/go-binance/v2/futures/wsapi"
	"github.com/gorilla/websocket"
)

type sharedSessionWireRequest struct {
	ID     string         `json:"id"`
	Method string         `json:"method"`
	Params map[string]any `json:"params"`
}

type sharedSessionTestServer struct {
	t           *testing.T
	server      *httptest.Server
	endpoint    string
	upgrader    websocket.Upgrader
	connections atomic.Int64
	requests    atomic.Int64
	onRequest   func(*websocket.Conn, sharedSessionWireRequest)
	writeMu     sync.Mutex
}

func newSharedSessionTestServer(t *testing.T, onRequest func(*websocket.Conn, sharedSessionWireRequest)) *sharedSessionTestServer {
	t.Helper()
	s := &sharedSessionTestServer{
		t:         t,
		upgrader:  websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
		onRequest: onRequest,
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		s.connections.Add(1)
		go func() {
			defer conn.Close()
			for {
				_, payload, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var req sharedSessionWireRequest
				if err := json.Unmarshal(payload, &req); err != nil {
					t.Errorf("decode request: %v", err)
					return
				}
				s.requests.Add(1)
				if s.onRequest != nil {
					s.onRequest(conn, req)
				}
			}
		}()
	}))
	s.endpoint = "ws" + strings.TrimPrefix(s.server.URL, "http")
	t.Cleanup(s.server.Close)
	return s
}

func (s *sharedSessionTestServer) writeResponse(conn *websocket.Conn, id string, result any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteJSON(map[string]any{"id": id, "status": 200, "result": result})
}

func newStartedSharedFuturesSession(t *testing.T, endpoint string) *managedfutures.Session {
	t.Helper()
	session, err := managedfutures.NewSession(managedfutures.Options{
		Endpoint: endpoint,
		API: apiws.Options{
			ConnectionOptions: managedws.Options{
				Reconnect: managedws.ReconnectPolicy{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1},
			},
			RequestTimeout: 2 * time.Second,
			StateBuffer:    64,
			ErrorBuffer:    64,
		},
		DisableHeartbeat: true,
		DisableRotation:  true,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		select {
		case <-session.Done():
		case <-time.After(time.Second):
			t.Error("shared session did not close")
		}
	})
	return session
}

func waitForSharedSessionConnections(t *testing.T, server *sharedSessionTestServer, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for server.connections.Load() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := server.connections.Load(); got < want {
		t.Fatalf("connections = %d, want >= %d", got, want)
	}
}

func TestWithSessionConstructorsReuseOnePhysicalConnection(t *testing.T) {
	server := newSharedSessionTestServer(t, nil)
	session := newStartedSharedFuturesSession(t, server.endpoint)
	waitForSharedSessionConnections(t, server, 1)
	if got := server.connections.Load(); got != 1 {
		t.Fatalf("connections after session start = %d, want 1", got)
	}

	account, err := NewWsAccountServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	orderPlace, err := NewOrderPlaceWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	orderModify, err := NewOrderModifyWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	algoPlace, err := NewAlgoOrderPlaceWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	orderCancel, err := NewOrderCancelWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	algoCancel, err := NewAlgoOrderCancelWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	orderStatus, err := NewOrderStatusWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}

	clients := []interface{ Close() error }{
		account.c, orderPlace.c, orderModify.c, algoPlace.c, orderCancel.c, algoCancel.c, orderStatus.c,
	}
	for _, client := range clients {
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := server.connections.Load(); got != 1 {
		t.Fatalf("connections after seven constructors = %d, want 1", got)
	}
	if got := session.State(); got != apiws.StateReady {
		t.Fatalf("session state after closing borrowed clients = %s, want ready", got)
	}
}

func TestSharedSessionServicesMatchConcurrentOutOfOrderResponses(t *testing.T) {
	var mu sync.Mutex
	var queued []sharedSessionWireRequest
	var server *sharedSessionTestServer
	server = newSharedSessionTestServer(t, func(conn *websocket.Conn, req sharedSessionWireRequest) {
		mu.Lock()
		queued = append(queued, req)
		if len(queued) < 4 {
			mu.Unlock()
			return
		}
		batch := append([]sharedSessionWireRequest(nil), queued...)
		mu.Unlock()
		for i := len(batch) - 1; i >= 0; i-- {
			request := batch[i]
			var result any
			switch request.Method {
			case "order.place":
				result = map[string]any{"symbol": "BTCUSDT", "orderId": 101, "clientOrderId": "ordinary", "type": "LIMIT", "side": "BUY"}
			case "algoOrder.place":
				result = map[string]any{"algoId": 202, "clientAlgoId": "algo", "algoType": "CONDITIONAL", "orderType": "STOP_MARKET", "symbol": "BTCUSDT", "side": "BUY"}
			case "order.cancel":
				result = map[string]any{"symbol": "BTCUSDT", "orderId": 303}
			case "order.status":
				result = map[string]any{"symbol": "BTCUSDT", "orderId": 404}
			default:
				t.Errorf("unexpected method %q", request.Method)
				return
			}
			if err := server.writeResponse(conn, request.ID, result); err != nil {
				t.Errorf("write response: %v", err)
				return
			}
		}
	})
	session := newStartedSharedFuturesSession(t, server.endpoint)
	generation := session.Generation()

	orderPlace, _ := NewOrderPlaceWsServiceWithSession(session, "key", "secret")
	algoPlace, _ := NewAlgoOrderPlaceWsServiceWithSession(session, "key", "secret")
	orderCancel, _ := NewOrderCancelWsServiceWithSession(session, "key", "secret")
	orderStatus, _ := NewOrderStatusWsServiceWithSession(session, "key", "secret")

	errs := make(chan error, 4)
	go func() {
		resp, err := orderPlace.SyncDo("place-1", NewOrderPlaceWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Type(OrderTypeLimit).TimeInForce(TimeInForceTypeGTC).Quantity("0.001").Price("50000").NewClientOrderID("ordinary"))
		if err == nil && (resp.Id != "place-1" || resp.Result.OrderID != 101) {
			err = fmt.Errorf("order.place response mismatch: %#v", resp)
		}
		errs <- err
	}()
	go func() {
		resp, err := algoPlace.SyncDo("algo-1", NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Type(AlgoOrderTypeStopMarket).Quantity("0.001").TriggerPrice("59000").ClientAlgoID("algo"))
		if err == nil && (resp.Id != "algo-1" || resp.Result.AlgoId != 202) {
			err = fmt.Errorf("algoOrder.place response mismatch: %#v", resp)
		}
		errs <- err
	}()
	go func() {
		resp, err := orderCancel.SyncDo("cancel-1", NewOrderCancelRequest().Symbol("BTCUSDT").OrderID(303))
		if err == nil && (resp.Id != "cancel-1" || resp.Result.OrderID != 303) {
			err = fmt.Errorf("order.cancel response mismatch: %#v", resp)
		}
		errs <- err
	}()
	go func() {
		resp, err := orderStatus.SyncDo("status-1", NewOrderStatusWsRequest().Symbol("BTCUSDT").OrderID(404))
		if err == nil && (resp.Id != "status-1" || resp.Result.OrderID != 404) {
			err = fmt.Errorf("order.status response mismatch: %#v", resp)
		}
		errs <- err
	}()

	for i := 0; i < 4; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := session.Generation(); got != generation {
		t.Fatalf("generation changed during shared requests: got %d want %d", got, generation)
	}
	if got := server.connections.Load(); got != 1 {
		t.Fatalf("physical connections = %d, want 1", got)
	}
}

func TestAlgoWithSessionKeepsDefaultAlgoTypeAndValidation(t *testing.T) {
	captured := make(chan sharedSessionWireRequest, 2)
	var server *sharedSessionTestServer
	server = newSharedSessionTestServer(t, func(conn *websocket.Conn, req sharedSessionWireRequest) {
		captured <- req
		if err := server.writeResponse(conn, req.ID, map[string]any{"algoId": 7, "algoType": "CONDITIONAL", "orderType": "STOP_MARKET", "symbol": "BTCUSDT", "side": "BUY"}); err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	session := newStartedSharedFuturesSession(t, server.endpoint)
	service, err := NewAlgoOrderPlaceWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}

	valid := NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Type(AlgoOrderTypeStopMarket).Quantity("0.001").TriggerPrice("59000")
	if _, err := service.SyncDo("algo-default", valid); err != nil {
		t.Fatalf("valid shared algo request: %v", err)
	}
	request := <-captured
	if got := request.Params["algoType"]; got != string(OrderAlgoTypeConditional) && got != OrderAlgoTypeConditional {
		t.Fatalf("algoType = %#v, want CONDITIONAL", got)
	}

	invalid := NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Type(AlgoOrderTypeStopMarket).Quantity("0.001")
	if _, err := service.SyncDo("algo-invalid", invalid); err != ErrAlgoOrderTriggerPriceRequired {
		t.Fatalf("invalid request error = %v, want %v", err, ErrAlgoOrderTriggerPriceRequired)
	}
	if got := server.requests.Load(); got != 1 {
		t.Fatalf("requests sent after local validation failure = %d, want 1", got)
	}
}

var _ func(string, string) (*OrderPlaceWsService, error) = NewOrderPlaceWsService
var _ func(string, string) (*AlgoOrderPlaceWsService, error) = NewAlgoOrderPlaceWsService
var _ func(string, string) (*OrderCancelWsService, error) = NewOrderCancelWsService
var _ func(string, string) (*AlgoOrderCancelWsService, error) = NewAlgoOrderCancelWsService
var _ func(string, string) (*OrderStatusWsService, error) = NewOrderStatusWsService
var _ func(string, string, ...int64) (*WsAccountService, error) = NewWsAccountService

var _ func(*managedfutures.Session, string, string) (*OrderPlaceWsService, error) = NewOrderPlaceWsServiceWithSession
var _ func(*managedfutures.Session, string, string) (*AlgoOrderPlaceWsService, error) = NewAlgoOrderPlaceWsServiceWithSession
var _ func(*managedfutures.Session, string, string) (*OrderCancelWsService, error) = NewOrderCancelWsServiceWithSession
var _ func(*managedfutures.Session, string, string) (*AlgoOrderCancelWsService, error) = NewAlgoOrderCancelWsServiceWithSession
var _ func(*managedfutures.Session, string, string) (*OrderStatusWsService, error) = NewOrderStatusWsServiceWithSession
var _ func(*managedfutures.Session, string, string, ...int64) (*WsAccountService, error) = NewWsAccountServiceWithSession
