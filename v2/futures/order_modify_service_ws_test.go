package futures

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type orderModifyWSFakeClient struct {
	response         []byte
	syncErr          error
	contextSyncErr   error
	writeSyncCalls   int
	contextSyncCalls int
	lastData         []byte
}

func (c *orderModifyWSFakeClient) Write(string, []byte) error { return nil }
func (c *orderModifyWSFakeClient) WriteSync(_ string, data []byte, _ time.Duration) ([]byte, error) {
	c.writeSyncCalls++
	c.lastData = append([]byte(nil), data...)
	if c.syncErr != nil {
		return nil, c.syncErr
	}
	return append([]byte(nil), c.response...), nil
}
func (c *orderModifyWSFakeClient) WriteSyncContext(_ context.Context, _ string, data []byte, _ time.Duration) ([]byte, error) {
	c.contextSyncCalls++
	c.lastData = append([]byte(nil), data...)
	if c.contextSyncErr != nil {
		return nil, c.contextSyncErr
	}
	return append([]byte(nil), c.response...), nil
}
func (c *orderModifyWSFakeClient) GetReadChannel() <-chan []byte     { return make(chan []byte) }
func (c *orderModifyWSFakeClient) GetReadErrorChannel() <-chan error { return make(chan error) }
func (c *orderModifyWSFakeClient) GetReconnectCount() int64          { return 0 }
func (c *orderModifyWSFakeClient) Wait(time.Duration)                {}
func (c *orderModifyWSFakeClient) Close() error                      { return nil }

func validOrderModifyWSRequest() *OrderModifyWsRequest {
	return NewOrderModifyWsRequest().Symbol("BTCUSDT").Side(SideTypeBuy).Quantity("1.0").Price("30005").OrderID(20072994037).OrigClientOrderID("LJ9R4QZDihCaS8UAOOLpgW").ModifyID(123).RecvWindow(5000)
}

func TestOrderModifyWsServiceRequestAndResponseContract(t *testing.T) {
	fake := &orderModifyWSFakeClient{response: []byte(`{"id":"modify-1","status":200,"result":{"orderId":20072994037,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"LJ9R4QZDihCaS8UAOOLpgW","modifyId":123,"price":"30005","origQty":"1.0","executedQty":"0","cumQty":"0","timeInForce":"GTC","type":"LIMIT","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"LONG","stopPrice":"0","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"LIMIT","priceMatch":"NONE","selfTradePreventionMode":"NONE","goodTillDate":0,"updateTime":1629182711600},"rateLimits":[{"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":300,"count":1}]}`)}
	service := &OrderModifyWsService{c: fake, ApiKey: "key", SecretKey: "secret", KeyType: "HMAC"}
	response, err := service.SyncDo("modify-1", validOrderModifyWSRequest())
	if err != nil {
		t.Fatalf("SyncDo: %v", err)
	}
	if response.Result.ModifyID != 123 || response.Result.OrderID != 20072994037 || response.Result.Status != OrderStatusTypeNew {
		t.Fatalf("response: %#v", response)
	}
	if len(response.RateLimits) != 1 || response.RateLimits[0].Count != 1 {
		t.Fatalf("rate limits: %#v", response.RateLimits)
	}
	var wire struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal(fake.lastData, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Method != "order.modify" {
		t.Fatalf("method=%q", wire.Method)
	}
	if wire.Params["symbol"] != "BTCUSDT" || wire.Params["side"] != "BUY" || wire.Params["quantity"] != "1.0" || wire.Params["price"] != "30005" {
		t.Fatalf("params=%#v", wire.Params)
	}
	if wire.Params["orderId"] != float64(20072994037) || wire.Params["modifyId"] != float64(123) || wire.Params["recvWindow"] != float64(5000) {
		t.Fatalf("params=%#v", wire.Params)
	}
}

func TestOrderModifyWsServiceTransportUnknownIsNotReplayed(t *testing.T) {
	transportErr := errors.New("transport outcome unknown")
	fake := &orderModifyWSFakeClient{contextSyncErr: transportErr}
	service := &OrderModifyWsService{c: fake, ApiKey: "key", SecretKey: "secret", KeyType: "HMAC"}
	_, err := service.SyncDoContext(context.Background(), "modify-unknown", validOrderModifyWSRequest())
	if !errors.Is(err, transportErr) {
		t.Fatalf("error=%v", err)
	}
	if fake.contextSyncCalls != 1 || fake.writeSyncCalls != 0 {
		t.Fatalf("context=%d sync=%d", fake.contextSyncCalls, fake.writeSyncCalls)
	}
}

func TestOrderModifyWsResponsePreservesBinanceAPIError(t *testing.T) {
	fake := &orderModifyWSFakeClient{response: []byte(`{"id":"modify-error","status":400,"error":{"code":-2011,"msg":"Unknown order sent."}}`)}
	service := &OrderModifyWsService{c: fake, ApiKey: "key", SecretKey: "secret", KeyType: "HMAC"}
	response, err := service.SyncDo("modify-error", validOrderModifyWSRequest())
	if err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != -2011 || response.Error.Message != "Unknown order sent." {
		t.Fatalf("error=%#v", response.Error)
	}
}
