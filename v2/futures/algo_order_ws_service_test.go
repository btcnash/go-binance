package futures

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type algoWSFakeClient struct {
	writeErr error
	syncErr  error
	response []byte
	lastID   string
	lastData []byte
}

func (c *algoWSFakeClient) Write(id string, data []byte) error {
	c.lastID = id
	c.lastData = append([]byte(nil), data...)
	return c.writeErr
}

func (c *algoWSFakeClient) WriteSync(id string, data []byte, _ time.Duration) ([]byte, error) {
	c.lastID = id
	c.lastData = append([]byte(nil), data...)
	if c.syncErr != nil {
		return nil, c.syncErr
	}
	return append([]byte(nil), c.response...), nil
}

func (c *algoWSFakeClient) GetReadChannel() <-chan []byte     { return make(chan []byte) }
func (c *algoWSFakeClient) GetReadErrorChannel() <-chan error { return make(chan error) }
func (c *algoWSFakeClient) GetReconnectCount() int64          { return 0 }
func (c *algoWSFakeClient) Wait(time.Duration)                {}
func (c *algoWSFakeClient) Close() error                      { return nil }

func validAlgoPlaceRequest() *AlgoOrderPlaceWsRequest {
	return NewAlgoOrderPlaceWsRequest().
		Symbol("BTCUSDT").
		Side(SideTypeBuy).
		Type(AlgoOrderTypeStopMarket).
		Quantity("0.001").
		TriggerPrice("59000")
}

func TestAlgoOrderPlaceWsServiceUsesTypedMethodAndPreservesTransportError(t *testing.T) {
	transportErr := errors.New("transport outcome unknown")
	fake := &algoWSFakeClient{syncErr: transportErr}
	service := &AlgoOrderPlaceWsService{
		c: fake, ApiKey: "key", SecretKey: "secret", KeyType: "HMAC",
	}

	_, err := service.SyncDo("request-1", validAlgoPlaceRequest())
	if !errors.Is(err, transportErr) {
		t.Fatalf("SyncDo() error = %v, want transport error", err)
	}

	var request struct {
		ID     string         `json:"id"`
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.Unmarshal(fake.lastData, &request); err != nil {
		t.Fatalf("request JSON error = %v", err)
	}
	if request.ID != "request-1" || request.Method != "algoOrder.place" {
		t.Fatalf("request envelope = %#v", request)
	}
	if request.Params["symbol"] != "BTCUSDT" || request.Params["triggerPrice"] != "59000" {
		t.Fatalf("request params = %#v", request.Params)
	}
}

func TestAlgoOrderCancelWsServiceDecodesErrorAndRateLimitResponse(t *testing.T) {
	fake := &algoWSFakeClient{response: []byte(`{
		"id":"cancel-1",
		"status":400,
		"error":{"code":-2011,"msg":"Unknown order sent."},
		"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400,"count":2}]
	}`)}
	service := &AlgoOrderCancelWsService{
		c: fake, ApiKey: "key", SecretKey: "secret", KeyType: "HMAC",
	}

	response, err := service.SyncDo("cancel-1", NewAlgoOrderCancelWsRequest().AlgoID(123))
	if err != nil {
		t.Fatalf("SyncDo() error = %v", err)
	}
	if response.Error == nil || response.Error.Code != -2011 {
		t.Fatalf("API error not decoded: %#v", response)
	}
	if len(response.RateLimits) != 1 || response.RateLimits[0].Count != 2 {
		t.Fatalf("rate limits not decoded: %#v", response.RateLimits)
	}
}
