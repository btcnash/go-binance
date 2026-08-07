package futures

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type noContextSyncClient struct{}

func (noContextSyncClient) Write(string, []byte) error { return nil }
func (noContextSyncClient) WriteSync(string, []byte, time.Duration) ([]byte, error) {
	return nil, nil
}
func (noContextSyncClient) GetReadChannel() <-chan []byte     { return make(chan []byte) }
func (noContextSyncClient) GetReadErrorChannel() <-chan error { return make(chan error) }
func (noContextSyncClient) GetReconnectCount() int64          { return 0 }
func (noContextSyncClient) Wait(time.Duration)                {}
func (noContextSyncClient) Close() error                      { return nil }

func TestContextSyncCapabilityDoesNotSilentlyFallback(t *testing.T) {
	_, err := writeLegacyWSAPISyncContext(context.Background(), noContextSyncClient{}, "unsupported", []byte(`{}`), time.Second)
	if !errors.Is(err, errWSAPIContextSyncUnsupported) {
		t.Fatalf("error = %v, want %v", err, errWSAPIContextSyncUnsupported)
	}
}

func TestSyncDoContextServicesShareOneSessionAndMatchResponses(t *testing.T) {
	var mu sync.Mutex
	var queued []sharedSessionWireRequest
	var server *sharedSessionTestServer
	server = newSharedSessionTestServer(t, func(conn *websocket.Conn, req sharedSessionWireRequest) {
		mu.Lock()
		queued = append(queued, req)
		if len(queued) < 2 {
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
				result = map[string]any{"symbol": "BTCUSDT", "orderId": 101, "clientOrderId": "context-order", "type": "LIMIT", "side": "BUY"}
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
	orderPlace, err := NewOrderPlaceWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	orderStatus, err := NewOrderStatusWsServiceWithSession(session, "key", "secret")
	if err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 2)
	go func() {
		resp, err := orderPlace.SyncDoContext(context.Background(), "context-place", NewOrderPlaceWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Type(OrderTypeLimit).TimeInForce(TimeInForceTypeGTC).Quantity("0.001").Price("50000").NewClientOrderID("context-order"))
		if err == nil && (resp.Id != "context-place" || resp.Result.OrderID != 101) {
			err = fmt.Errorf("order.place response mismatch: %#v", resp)
		}
		errs <- err
	}()
	go func() {
		resp, err := orderStatus.SyncDoContext(context.Background(), "context-status", NewOrderStatusWsRequest().Symbol("BTCUSDT").OrderID(404))
		if err == nil && (resp.Id != "context-status" || resp.Result.OrderID != 404) {
			err = fmt.Errorf("order.status response mismatch: %#v", resp)
		}
		errs <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := server.connections.Load(); got != 1 {
		t.Fatalf("physical connections = %d, want 1", got)
	}
}

func TestAlgoSyncDoContextKeepsValidationAndDefaultAlgoType(t *testing.T) {
	captured := make(chan sharedSessionWireRequest, 1)
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

	invalid := NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Type(AlgoOrderTypeStopMarket).Quantity("0.001")
	if _, err := service.SyncDoContext(context.Background(), "context-algo-invalid", invalid); err != ErrAlgoOrderTriggerPriceRequired {
		t.Fatalf("invalid request error = %v, want %v", err, ErrAlgoOrderTriggerPriceRequired)
	}
	if got := server.requests.Load(); got != 0 {
		t.Fatalf("requests sent after local validation failure = %d, want 0", got)
	}

	valid := NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Type(AlgoOrderTypeStopMarket).Quantity("0.001").TriggerPrice("59000")
	if _, err := service.SyncDoContext(context.Background(), "context-algo-default", valid); err != nil {
		t.Fatalf("valid context algo request: %v", err)
	}
	select {
	case request := <-captured:
		if got := request.Params["algoType"]; got != string(OrderAlgoTypeConditional) && got != OrderAlgoTypeConditional {
			t.Fatalf("algoType = %#v, want CONDITIONAL", got)
		}
	case <-time.After(time.Second):
		t.Fatal("did not capture algo request")
	}
}

func TestContextServiceMethodsPropagateCanceledCallerWithoutWireSend(t *testing.T) {
	server := newSharedSessionTestServer(t, nil)
	session := newStartedSharedFuturesSession(t, server.endpoint)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	orderCancel, _ := NewOrderCancelWsServiceWithSession(session, "key", "secret")
	if _, err := orderCancel.SyncDoContext(ctx, "context-cancel-order", NewOrderCancelRequest().Symbol("BTCUSDT").OrderID(1)); err == nil {
		t.Fatal("order cancel context call unexpectedly succeeded")
	}
	algoCancel, _ := NewAlgoOrderCancelWsServiceWithSession(session, "key", "secret")
	if _, err := algoCancel.SyncDoContext(ctx, "context-cancel-algo", NewAlgoOrderCancelWsRequest().AlgoID(2)); err == nil {
		t.Fatal("algo cancel context call unexpectedly succeeded")
	}
	account, _ := NewWsAccountServiceWithSession(session, "key", "secret")
	if _, err := account.SyncGetAccountInfoContext(ctx, "context-account-info"); err == nil {
		t.Fatal("account info context call unexpectedly succeeded")
	}
	if _, err := account.SyncGetAccountBalanceContext(ctx, "context-account-balance"); err == nil {
		t.Fatal("account balance context call unexpectedly succeeded")
	}
	if got := server.requests.Load(); got != 0 {
		t.Fatalf("wire requests with pre-canceled contexts = %d, want 0", got)
	}
}

var _ func(*OrderPlaceWsService, context.Context, string, *OrderPlaceWsRequest) (*CreateOrderWsResponse, error) = (*OrderPlaceWsService).SyncDoContext
var _ func(*AlgoOrderPlaceWsService, context.Context, string, *AlgoOrderPlaceWsRequest) (*CreateAlgoOrderWsResponse, error) = (*AlgoOrderPlaceWsService).SyncDoContext
var _ func(*OrderCancelWsService, context.Context, string, *OrderCancelRequest) (*OrderCancelWsResponse, error) = (*OrderCancelWsService).SyncDoContext
var _ func(*AlgoOrderCancelWsService, context.Context, string, *AlgoOrderCancelWsRequest) (*CancelAlgoOrderWsResponse, error) = (*AlgoOrderCancelWsService).SyncDoContext
var _ func(*OrderStatusWsService, context.Context, string, *OrderStatusWsRequest) (*QueryOrderWsResponse, error) = (*OrderStatusWsService).SyncDoContext
var _ func(*WsAccountService, context.Context, string) (*WsAccountV2InfoResponse, error) = (*WsAccountService).SyncGetAccountInfoContext
var _ func(*WsAccountService, context.Context, string) (*WsAccountV2BalanceResponse, error) = (*WsAccountService).SyncGetAccountBalanceContext
