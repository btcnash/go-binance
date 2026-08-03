package binance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/common/websocket"
	"github.com/btcnash/go-binance/v2/futures"
)

func TestAlgoOrderPlaceWsRequestBuildsOfficialContract(t *testing.T) {
	goodTillDate := time.Now().Add(20 * time.Minute).UnixMilli()
	req := NewAlgoOrderPlaceWsRequest().
		Symbol("BTCUSDT").
		Side(futures.SideTypeBuy).
		Type(futures.AlgoOrderTypeStop).
		PositionSide(futures.PositionSideTypeBoth).
		TimeInForce(futures.TimeInForceTypeGTD).
		Quantity("0.001").
		Price("60000").
		TriggerPrice("59000").
		WorkingType(futures.WorkingTypeMarkPrice).
		ReduceOnly(true).
		ClientAlgoID("algo-client-1").
		NewOrderResponseType(futures.NewOrderRespTypeACK).
		SelfTradePreventionMode(futures.SelfTradePreventionModeExpireMaker).
		GoodTillDate(goodTillDate).
		RecvWindow(5000)

	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	params := req.GetParams()

	want := map[string]any{
		"algoType":                futures.OrderAlgoTypeConditional,
		"symbol":                  "BTCUSDT",
		"side":                    futures.SideTypeBuy,
		"type":                    futures.AlgoOrderTypeStop,
		"positionSide":            futures.PositionSideTypeBoth,
		"timeInForce":             futures.TimeInForceTypeGTD,
		"quantity":                "0.001",
		"price":                   "60000",
		"triggerPrice":            "59000",
		"workingType":             futures.WorkingTypeMarkPrice,
		"reduceOnly":              true,
		"clientAlgoId":            "algo-client-1",
		"newOrderRespType":        futures.NewOrderRespTypeACK,
		"selfTradePreventionMode": futures.SelfTradePreventionModeExpireMaker,
		"goodTillDate":            goodTillDate,
		"recvWindow":              int64(5000),
	}

	if len(params) != len(want) {
		t.Fatalf("params length = %d, want %d: %#v", len(params), len(want), params)
	}
	for key, wantValue := range want {
		if got := params[key]; got != wantValue {
			t.Errorf("params[%q] = %#v, want %#v", key, got, wantValue)
		}
	}
}

func TestAlgoOrderPlaceWsRequestDefaultsToAckAndOmitsUnsetTriggerPrice(t *testing.T) {
	req := NewAlgoOrderPlaceWsRequest().
		Symbol("BTCUSDT").
		Side(futures.SideTypeSell).
		Type(futures.AlgoOrderTypeTrailingStopMarket).
		Quantity("0.001").
		ActivatePrice("58000").
		CallbackRate("1")

	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	params := req.GetParams()
	if got := params["newOrderRespType"]; got != futures.NewOrderRespTypeACK {
		t.Fatalf("newOrderRespType = %#v, want ACK", got)
	}
	if params["activatePrice"] != "58000" || params["callbackRate"] != "1" {
		t.Fatalf("trailing parameters missing: %#v", params)
	}
	if _, exists := params["triggerPrice"]; exists {
		t.Fatalf("unset triggerPrice must be omitted: %#v", params)
	}
}

func TestAlgoOrderPlaceWsRequestSupportsMarketProtectionParameters(t *testing.T) {
	req := NewAlgoOrderPlaceWsRequest().
		Symbol("BTCUSDT").
		Side(futures.SideTypeSell).
		Type(futures.AlgoOrderTypeStopMarket).
		TriggerPrice("59000").
		ClosePosition(true).
		PriceProtect(true)

	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	params := req.GetParams()
	if params["closePosition"] != true || params["priceProtect"] != true {
		t.Fatalf("market protection parameters missing: %#v", params)
	}
	if _, exists := params["quantity"]; exists {
		t.Fatalf("close-all request must omit quantity: %#v", params)
	}
}

func TestAlgoOrderPlaceWsRequestSupportsPriceMatchWithoutPrice(t *testing.T) {
	req := NewAlgoOrderPlaceWsRequest().
		Symbol("BTCUSDT").
		Side(futures.SideTypeBuy).
		Type(futures.AlgoOrderTypeStop).
		Quantity("0.001").
		TriggerPrice("59000").
		PriceMatch(futures.PriceMatchTypeOpponent)

	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	params := req.GetParams()
	if params["priceMatch"] != futures.PriceMatchTypeOpponent {
		t.Fatalf("priceMatch missing: %#v", params)
	}
	if _, exists := params["price"]; exists {
		t.Fatalf("price must be omitted when priceMatch is used: %#v", params)
	}
}

func TestAlgoOrderPlaceWsRequestRejectsInvalidCombinations(t *testing.T) {
	tests := []struct {
		name string
		req  *AlgoOrderPlaceWsRequest
	}{
		{
			name: "price and priceMatch",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeBuy).
				Type(futures.AlgoOrderTypeStop).Quantity("0.001").TriggerPrice("59000").
				Price("60000").PriceMatch(futures.PriceMatchTypeOpponent),
		},
		{
			name: "closePosition and quantity",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeSell).
				Type(futures.AlgoOrderTypeStopMarket).TriggerPrice("59000").ClosePosition(true).Quantity("0.001"),
		},
		{
			name: "trailing callback out of range",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeSell).
				Type(futures.AlgoOrderTypeTrailingStopMarket).Quantity("0.001").CallbackRate("10.1"),
		},
		{
			name: "invalid client id",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeSell).
				Type(futures.AlgoOrderTypeTrailingStopMarket).Quantity("0.001").CallbackRate("1").ClientAlgoID("invalid id"),
		},
		{
			name: "priceMatch on market order",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeSell).
				Type(futures.AlgoOrderTypeStopMarket).Quantity("0.001").TriggerPrice("59000").PriceMatch(futures.PriceMatchTypeOpponent),
		},
		{
			name: "GTD without goodTillDate",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeBuy).
				Type(futures.AlgoOrderTypeStop).Quantity("0.001").TriggerPrice("59000").Price("60000").TimeInForce(futures.TimeInForceTypeGTD),
		},
		{
			name: "goodTillDate without GTD",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeBuy).
				Type(futures.AlgoOrderTypeStop).Quantity("0.001").TriggerPrice("59000").Price("60000").GoodTillDate(time.Now().Add(20 * time.Minute).UnixMilli()),
		},
		{
			name: "closePosition on limit conditional",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeSell).
				Type(futures.AlgoOrderTypeStop).TriggerPrice("59000").Price("58000").ClosePosition(true),
		},
		{
			name: "priceProtect on limit conditional",
			req: NewAlgoOrderPlaceWsRequest().Symbol("BTCUSDT").Side(futures.SideTypeSell).
				Type(futures.AlgoOrderTypeStop).Quantity("0.001").TriggerPrice("59000").Price("58000").PriceProtect(true),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.req.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestAlgoOrderCancelWsRequestUsesOfficialParameterNames(t *testing.T) {
	req := NewAlgoOrderCancelWsRequest().AlgoID(123).ClientAlgoID("client-123").RecvWindow(5000)
	if err := req.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	params := req.GetParams()

	if got := params["algoId"]; got != int64(123) {
		t.Fatalf("algoId = %#v, want 123", got)
	}
	if got := params["clientAlgoId"]; got != "client-123" {
		t.Fatalf("clientAlgoId = %#v, want client-123", got)
	}
	if _, exists := params["algoid"]; exists {
		t.Fatal("legacy invalid key algoid must not be emitted")
	}
	if _, exists := params["clientalgoid"]; exists {
		t.Fatal("legacy invalid key clientalgoid must not be emitted")
	}
}

func TestAlgoOrderCancelWsRequestRequiresIdentity(t *testing.T) {
	if err := NewAlgoOrderCancelWsRequest().Validate(); err == nil {
		t.Fatal("Validate() error = nil, want missing identity error")
	}
}

func TestAlgoOrderWsResponsePreservesResultAndRateLimits(t *testing.T) {
	payload := []byte(`{
		"id":"request-1",
		"status":200,
		"result":{
			"algoId":214748389,
			"clientAlgoId":"algo-client-1",
			"algoType":"CONDITIONAL",
			"orderType":"STOP_MARKET",
			"symbol":"BTCUSDT",
			"side":"BUY",
			"positionSide":"LONG",
			"timeInForce":"GTC",
			"quantity":"0.001",
			"algoStatus":"NEW",
			"triggerPrice":"59000",
			"price":"0",
			"icebergQuantity":"0.000",
			"selfTradePreventionMode":"EXPIRE_MAKER",
			"workingType":"MARK_PRICE",
			"priceMatch":"NONE",
			"closePosition":false,
			"priceProtect":true,
			"reduceOnly":true,
			"createTime":1750000000000,
			"updateTime":1750000000000,
			"triggerTime":0,
			"goodTillDate":0
		},
		"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400,"count":1}]
	}`)

	var response CreateAlgoOrderWsResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if response.Result.Symbol != "BTCUSDT" || response.Result.AlgoStatus != futures.AlgoOrderStatusTypeNew {
		t.Fatalf("result not fully decoded: %#v", response.Result)
	}
	if response.Result.IcebergQuantity == nil || *response.Result.IcebergQuantity != "0.000" {
		t.Fatalf("icebergQuantity not decoded as decimal string: %#v", response.Result.IcebergQuantity)
	}
	if len(response.RateLimits) != 1 || response.RateLimits[0].Count != 1 {
		t.Fatalf("rate limits not decoded: %#v", response.RateLimits)
	}
}

func TestAlgoOrderLifecycleStatusConstants(t *testing.T) {
	want := map[futures.AlgoOrderStatusType]string{
		futures.AlgoOrderStatusTypeNew:        "NEW",
		futures.AlgoOrderStatusTypeTriggering: "TRIGGERING",
		futures.AlgoOrderStatusTypeTriggered:  "TRIGGERED",
		futures.AlgoOrderStatusTypeFinished:   "FINISHED",
		futures.AlgoOrderStatusTypeCanceled:   "CANCELED",
		futures.AlgoOrderStatusTypeRejected:   "REJECTED",
		futures.AlgoOrderStatusTypeExpired:    "EXPIRED",
	}
	for status, raw := range want {
		if string(status) != raw {
			t.Fatalf("status %q = %q", raw, status)
		}
	}
}

func TestAlgoOrderCancelWsResponseUsesInt64AlgoID(t *testing.T) {
	const largeAlgoID int64 = 3000000000003505
	var response CancelAlgoOrderWsResponse
	if err := json.Unmarshal([]byte(`{"result":{"algoId":3000000000003505,"clientAlgoId":"client-1","code":"200","msg":"success"}}`), &response); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if response.Result.AlgoId != largeAlgoID {
		t.Fatalf("AlgoId = %d, want %d", response.Result.AlgoId, largeAlgoID)
	}
}

func TestAlgoOrderWsMethodConstants(t *testing.T) {
	if websocket.AlgoOrderPlaceFuturesWsApiMethod != "algoOrder.place" {
		t.Fatalf("place method = %q", websocket.AlgoOrderPlaceFuturesWsApiMethod)
	}
	if websocket.AlgoOrderCancelFuturesWsApiMethod != "algoOrder.cancel" {
		t.Fatalf("cancel method = %q", websocket.AlgoOrderCancelFuturesWsApiMethod)
	}
}

func TestFuturesAPIErrorPreservesHTTPResponseMetadata(t *testing.T) {
	responseHeader := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "123")
		w.Header().Set("X-MBX-ORDER-COUNT-10S", "7")
		responseHeader <- w.Header()
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":-1007,"msg":"execution status unknown"}`))
	}))
	defer server.Close()

	client := futures.NewClient("", "").SetApiEndpoint(server.URL)
	_, err := client.NewServerTimeService().Do(context.Background())
	apiErr, ok := err.(*common.APIError)
	if !ok {
		t.Fatalf("error = %T %v, want *common.APIError", err, err)
	}
	if apiErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode = %d", apiErr.StatusCode)
	}
	if apiErr.Header.Get("X-MBX-USED-WEIGHT-1M") != "123" || apiErr.Header.Get("X-MBX-ORDER-COUNT-10S") != "7" {
		t.Fatalf("Header = %#v", apiErr.Header)
	}

	original := <-responseHeader
	original.Set("X-MBX-USED-WEIGHT-1M", "999")
	if apiErr.Header.Get("X-MBX-USED-WEIGHT-1M") != "123" {
		t.Fatal("APIError header was not cloned")
	}
}
