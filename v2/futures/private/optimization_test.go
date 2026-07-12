package private

import (
	"encoding/json"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	"github.com/btcnash/go-binance/v2/futures"
)

func TestHandleFrameTransfersManagedRawBuffer(t *testing.T) {
	binding := sourceBinding{SourceID: "account-1", ListenKey: "key-1", Version: 1}
	session := &Session{
		generation:      1,
		currentBindings: []sourceBinding{binding},
		events:          make(chan Event, 1),
		states:          make(chan StateEvent, 1),
		errors:          make(chan ErrorEvent, 1),
		gaps:            make(chan GapEvent, 1),
		listenKeys:      make(chan ListenKeyEvent, 1),
		observations:    make(chan observation, 1),
	}
	session.bindingIndex.Store(newBindingSnapshot([]sourceBinding{binding}))
	payload := []byte(`{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"m":"ORDER","B":[],"P":[]}}`)
	session.handleFrame(managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    payload,
		ReceivedAt: time.Now(),
	})
	event := <-session.events
	if event.DecodeError != nil {
		t.Fatalf("decode: %v", event.DecodeError)
	}
	if &event.Raw[0] != &payload[0] {
		t.Fatal("private session copied the managed frame payload")
	}
}

func TestBindingSnapshotIndexesSourceResolution(t *testing.T) {
	bindings := []sourceBinding{
		{SourceID: "orders", ListenKey: "key-orders", Events: []futures.UserDataEventType{futures.UserDataEventTypeOrderTradeUpdate}, eventSet: map[futures.UserDataEventType]struct{}{futures.UserDataEventTypeOrderTradeUpdate: {}}},
		{SourceID: "account", ListenKey: "key-account", Events: []futures.UserDataEventType{futures.UserDataEventTypeAccountUpdate}, eventSet: map[futures.UserDataEventType]struct{}{futures.UserDataEventTypeAccountUpdate: {}}},
	}
	snapshot := newBindingSnapshot(bindings)
	bindings[0].SourceID = "mutated"

	sourceID, candidates, resolution := snapshot.resolve(futures.UserDataEventTypeOrderTradeUpdate, "", "")
	if sourceID != "orders" || len(candidates) != 0 || resolution != SourceResolutionEventFilter {
		t.Fatalf("event-filter resolution = source=%q candidates=%v resolution=%q", sourceID, candidates, resolution)
	}
	sourceID, candidates, resolution = snapshot.resolve(futures.UserDataEventTypeAccountUpdate, "key-account", "")
	if sourceID != "account" || len(candidates) != 0 || resolution != SourceResolutionExplicit {
		t.Fatalf("explicit resolution = source=%q candidates=%v resolution=%q", sourceID, candidates, resolution)
	}
}

func TestBindingSnapshotPreservesFirstDuplicateListenKey(t *testing.T) {
	bindings := []sourceBinding{
		{SourceID: "first", ListenKey: "shared"},
		{SourceID: "second", ListenKey: "shared"},
	}
	snapshot := newBindingSnapshot(bindings)

	sourceID, candidates, resolution := snapshot.resolve(futures.UserDataEventTypeAccountUpdate, "shared", "")
	if sourceID != "first" || len(candidates) != 0 || resolution != SourceResolutionExplicit {
		t.Fatalf("duplicate listen-key resolution = source=%q candidates=%v resolution=%q", sourceID, candidates, resolution)
	}
}

func TestBindingSnapshotBuildsEventIndexFromEvents(t *testing.T) {
	bindings := []sourceBinding{
		{SourceID: "orders", ListenKey: "key-orders", Events: []futures.UserDataEventType{futures.UserDataEventTypeOrderTradeUpdate}},
		{SourceID: "account", ListenKey: "key-account", Events: []futures.UserDataEventType{futures.UserDataEventTypeAccountUpdate}},
	}
	snapshot := newBindingSnapshot(bindings)

	sourceID, candidates, resolution := snapshot.resolve(futures.UserDataEventTypeOrderTradeUpdate, "", "")
	if sourceID != "orders" || len(candidates) != 0 || resolution != SourceResolutionEventFilter {
		t.Fatalf("events fallback resolution = source=%q candidates=%v resolution=%q", sourceID, candidates, resolution)
	}
}

func TestBindingSnapshotPreservesStreamMatchOrder(t *testing.T) {
	bindings := []sourceBinding{
		{SourceID: "first", ListenKey: "z-key"},
		{SourceID: "second", ListenKey: "a-key"},
	}
	snapshot := newBindingSnapshot(bindings)

	stream := "listenKey=z-key&listenKey=a-key"
	sourceID, candidates, resolution := snapshot.resolve(futures.UserDataEventTypeAccountUpdate, "", stream)
	if sourceID != "first" || len(candidates) != 0 || resolution != SourceResolutionExplicit {
		t.Fatalf("stream match order = source=%q candidates=%v resolution=%q", sourceID, candidates, resolution)
	}
}

func TestWsUserDataEventOptimizedUnmarshalSupportedEvents(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		check   func(*testing.T, *futures.WsUserDataEvent)
	}{
		{
			name:    "listen key expired",
			payload: `{"e":"listenKeyExpired","E":10}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.Event != futures.UserDataEventTypeListenKeyExpired || event.Time != 10 {
					t.Fatalf("event = %#v", event)
				}
			},
		},
		{
			name:    "margin call",
			payload: `{"e":"MARGIN_CALL","E":11,"cw":"3.1","p":[{"s":"ETHUSDT","ps":"LONG","pa":"1"}]}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.CrossWalletBalance != "3.1" || len(event.MarginCallPositions) != 1 || event.MarginCallPositions[0].Symbol != "ETHUSDT" {
					t.Fatalf("margin call = %#v", event.WsUserDataMarginCall)
				}
			},
		},
		{
			name:    "account update",
			payload: `{"e":"ACCOUNT_UPDATE","E":12,"T":13,"a":{"m":"ORDER","B":[],"P":[]}}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.TransactionTime != 13 || event.AccountUpdate.Reason != "ORDER" {
					t.Fatalf("account update = %#v", event.WsUserDataAccountUpdate)
				}
			},
		},
		{
			name:    "order trade update",
			payload: `{"e":"ORDER_TRADE_UPDATE","E":14,"T":15,"o":{"s":"BTCUSDT","c":"client","S":"BUY","o":"LIMIT","X":"NEW","i":99}}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.OrderTradeUpdate.Symbol != "BTCUSDT" || event.OrderTradeUpdate.ID != 99 {
					t.Fatalf("order update = %#v", event.WsUserDataOrderTradeUpdate)
				}
			},
		},
		{
			name:    "account config update",
			payload: `{"e":"ACCOUNT_CONFIG_UPDATE","E":16,"T":17,"ac":{"s":"BTCUSDT","l":25}}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.AccountConfigUpdate.Symbol != "BTCUSDT" || event.AccountConfigUpdate.Leverage != 25 {
					t.Fatalf("account config = %#v", event.WsUserDataAccountConfigUpdate)
				}
			},
		},
		{
			name:    "conditional order reject",
			payload: `{"e":"CONDITIONAL_ORDER_TRIGGER_REJECT","E":17,"T":18,"or":{"s":"BTCUSDT","i":88,"r":"reject"}}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.ConditionalOrderTriggerReject.Symbol != "BTCUSDT" || event.ConditionalOrderTriggerReject.OrderId != 88 {
					t.Fatalf("conditional reject = %#v", event.WsUserDataConditionalOrderTriggerReject)
				}
			},
		},
		{
			name:    "trade lite",
			payload: `{"e":"TRADE_LITE","E":18,"T":19,"s":"BTCUSDT","q":"0.1","p":"10","S":"BUY","i":100}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.WsUserDataTradeLite.Symbol != "BTCUSDT" || event.WsUserDataTradeLite.OrderID != 100 {
					t.Fatalf("trade lite = %#v", event.WsUserDataTradeLite)
				}
			},
		},
		{
			name:    "algo update",
			payload: `{"e":"ALGO_UPDATE","E":20,"T":21,"o":{"caid":"client-algo","aid":101,"at":"CONDITIONAL","o":"STOP","s":"BTCUSDT","S":"SELL","X":"NEW"}}`,
			check: func(t *testing.T, event *futures.WsUserDataEvent) {
				if event.AlgoUpdate.ClientAlgoID != "client-algo" || event.AlgoUpdate.AlgoID != 101 {
					t.Fatalf("algo update = %#v", event.WsUserDataAlgoUpdate)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var event futures.WsUserDataEvent
			if err := json.Unmarshal([]byte(test.payload), &event); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			test.check(t, &event)
		})
	}
}
