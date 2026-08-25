package futures

import (
	"encoding/json"
	"testing"
)

const aggTradePayloadWithPresence = `{
	"e":"aggTrade",
	"E":123456789,
	"s":"BNBUSDT",
	"a":5933014,
	"p":"0.001",
	"q":"100",
	"nq":"0.014",
	"f":100,
	"l":105,
	"T":123456785,
	"m":true,
	"st":1
}`

func TestWsAggTradeEventAdditionalFields(t *testing.T) {
	var event WsAggTradeEvent
	if err := json.Unmarshal([]byte(aggTradePayloadWithPresence), &event); err != nil {
		t.Fatalf("unmarshal aggTrade event: %v", err)
	}

	if event.Event != "aggTrade" || event.Time != 123456789 || event.Symbol != "BNBUSDT" ||
		event.AggregateTradeID != 5933014 || event.Price != "0.001" || event.Quantity != "100" ||
		event.FirstTradeID != 100 || event.LastTradeID != 105 || event.TradeTime != 123456785 || !event.Maker {
		t.Fatalf("base aggTrade fields decoded incorrectly: %+v", event)
	}
	if event.NormalQuantity != "0.014" || !event.NormalQuantityPresent {
		t.Fatalf("NormalQuantity = %q, present = %v, want %q, true", event.NormalQuantity, event.NormalQuantityPresent, "0.014")
	}
	if event.SymbolType != 1 || !event.SymbolTypePresent {
		t.Fatalf("SymbolType = %d, present = %v, want %d, true", event.SymbolType, event.SymbolTypePresent, 1)
	}
}

func TestWsAggTradeEventPresence(t *testing.T) {
	tests := []struct {
		name                  string
		payload               string
		normalQuantity        string
		normalQuantityPresent bool
		symbolType            int
		symbolTypePresent     bool
	}{
		{
			name:                  "fields missing",
			payload:               `{"e":"aggTrade","s":"BTCUSDT","p":"64226.5","q":"0.015"}`,
			normalQuantity:        "",
			normalQuantityPresent: false,
			symbolType:            0,
			symbolTypePresent:     false,
		},
		{
			name:                  "explicit zero values",
			payload:               `{"e":"aggTrade","nq":"0","st":0}`,
			normalQuantity:        "0",
			normalQuantityPresent: true,
			symbolType:            0,
			symbolTypePresent:     true,
		},
		{
			name:                  "explicit empty normal quantity",
			payload:               `{"e":"aggTrade","nq":""}`,
			normalQuantity:        "",
			normalQuantityPresent: true,
			symbolType:            0,
			symbolTypePresent:     false,
		},
		{
			name:                  "normal quantity only",
			payload:               `{"e":"aggTrade","nq":"0.014"}`,
			normalQuantity:        "0.014",
			normalQuantityPresent: true,
			symbolType:            0,
			symbolTypePresent:     false,
		},
		{
			name:                  "symbol type only",
			payload:               `{"e":"aggTrade","st":2}`,
			normalQuantity:        "",
			normalQuantityPresent: false,
			symbolType:            2,
			symbolTypePresent:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event WsAggTradeEvent
			if err := json.Unmarshal([]byte(tt.payload), &event); err != nil {
				t.Fatalf("unmarshal aggTrade event: %v", err)
			}
			if event.NormalQuantity != tt.normalQuantity || event.NormalQuantityPresent != tt.normalQuantityPresent {
				t.Fatalf("NormalQuantity = %q, present = %v, want %q, %v", event.NormalQuantity, event.NormalQuantityPresent, tt.normalQuantity, tt.normalQuantityPresent)
			}
			if event.SymbolType != tt.symbolType || event.SymbolTypePresent != tt.symbolTypePresent {
				t.Fatalf("SymbolType = %d, present = %v, want %d, %v", event.SymbolType, event.SymbolTypePresent, tt.symbolType, tt.symbolTypePresent)
			}
		})
	}
}

func TestWsAggTradeEventPresenceTypeErrors(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "normal quantity number", payload: `{"e":"aggTrade","nq":123}`},
		{name: "symbol type string", payload: `{"e":"aggTrade","st":"1"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event WsAggTradeEvent
			if err := json.Unmarshal([]byte(tt.payload), &event); err == nil {
				t.Fatal("expected JSON type error, got nil")
			}
		})
	}
}

func TestWsAggTradeServePresence(t *testing.T) {
	original := wsServe
	defer func() { wsServe = original }()

	payload := []byte(aggTradePayloadWithPresence)
	wsServe = func(_ *WsConfig, handler WsHandler, _ ErrHandler) (chan struct{}, chan struct{}, error) {
		handler(payload)
		return make(chan struct{}), make(chan struct{}), nil
	}

	called := false
	_, _, err := WsAggTradeServe("BNBUSDT", func(event *WsAggTradeEvent) {
		called = true
		if event.NormalQuantity != "0.014" || !event.NormalQuantityPresent || event.SymbolType != 1 || !event.SymbolTypePresent {
			t.Fatalf("raw handler presence mismatch: %+v", event)
		}
	}, func(err error) {
		t.Fatalf("unexpected raw handler error: %v", err)
	})
	if err != nil {
		t.Fatalf("WsAggTradeServe: %v", err)
	}
	if !called {
		t.Fatal("raw aggTrade handler was not called")
	}
}

func TestWsCombinedAggTradeServePresence(t *testing.T) {
	original := wsServe
	defer func() { wsServe = original }()

	payload := []byte(`{"stream":"bnbusdt@aggTrade","data":{"e":"aggTrade","E":123456789,"s":"BNBUSDT","a":5933014,"p":"0.001","q":"100","nq":"","f":100,"l":105,"T":123456785,"m":true,"st":0}}`)
	wsServe = func(_ *WsConfig, handler WsHandler, _ ErrHandler) (chan struct{}, chan struct{}, error) {
		handler(payload)
		return make(chan struct{}), make(chan struct{}), nil
	}

	called := false
	_, _, err := WsCombinedAggTradeServe([]string{"BNBUSDT"}, func(event *WsAggTradeEvent) {
		called = true
		if event.NormalQuantity != "" || !event.NormalQuantityPresent || event.SymbolType != 0 || !event.SymbolTypePresent {
			t.Fatalf("combined handler presence mismatch: %+v", event)
		}
	}, func(err error) {
		t.Fatalf("unexpected combined handler error: %v", err)
	})
	if err != nil {
		t.Fatalf("WsCombinedAggTradeServe: %v", err)
	}
	if !called {
		t.Fatal("combined aggTrade handler was not called")
	}
}

func BenchmarkWsAggTradeEventUnmarshal(b *testing.B) {
	payloads := map[string][]byte{
		"with_presence":    []byte(aggTradePayloadWithPresence),
		"without_presence": []byte(`{"e":"aggTrade","E":123456789,"s":"BNBUSDT","a":5933014,"p":"0.001","q":"100","f":100,"l":105,"T":123456785,"m":true}`),
	}
	for name, payload := range payloads {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var event WsAggTradeEvent
				if err := json.Unmarshal(payload, &event); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
