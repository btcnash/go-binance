package futures

import (
	"encoding/json"
	"testing"
)

func TestWsAggTradeEventAdditionalFields(t *testing.T) {
	payload := []byte(`{
		"e":"aggTrade",
		"E":123456789,
		"s":"BNBUSDT",
		"a":5933014,
		"p":"0.001",
		"q":"100",
		"nq":"99.5",
		"f":100,
		"l":105,
		"T":123456785,
		"m":true,
		"st":1
	}`)

	var event WsAggTradeEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("unmarshal aggTrade event: %v", err)
	}
	if event.NormalQuantity != "99.5" {
		t.Fatalf("NormalQuantity = %q, want %q", event.NormalQuantity, "99.5")
	}
	if event.SymbolType != 1 {
		t.Fatalf("SymbolType = %d, want %d", event.SymbolType, 1)
	}
}
