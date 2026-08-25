package stream

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
)

func newTypedTestSession(mode TypedDeliveryMode, policy DeliveryPolicy, buffer int) *StreamSession {
	if buffer <= 0 {
		buffer = 4
	}
	return &StreamSession{
		opts: StreamSessionOptions{
			TypedDelivery:  mode,
			DeliveryPolicy: policy,
		},
		events:       make(chan StreamEvent, buffer),
		typedEvents:  make(chan TypedStreamEvent, buffer),
		errors:       make(chan StreamErrorEvent, buffer),
		gaps:         make(chan GapEvent, buffer),
		coalesced:    make(map[string]coalescedEvent),
		coalesceWake: make(chan struct{}, 1),
	}
}

func TestTypedDeliveryValidation(t *testing.T) {
	if _, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassPublic,
		TypedDelivery:        TypedDeliveryBookTicker,
		InitialSubscriptions: []Subscription{BookTicker("BTCUSDT")},
	}); err != nil {
		t.Fatalf("bookTicker typed options: %v", err)
	}

	if _, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassMarket,
		TypedDelivery:        TypedDeliveryBookTicker,
		InitialSubscriptions: []Subscription{BookTicker("BTCUSDT")},
	}); !errors.Is(err, ErrInvalidStreamOptions) {
		t.Fatalf("wrong class error = %v, want ErrInvalidStreamOptions", err)
	}

	if _, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassPublic,
		TypedDelivery:        TypedDeliveryBookTicker,
		InitialSubscriptions: []Subscription{RawSubscription(StreamClassPublic, "btcusdt@depth")},
	}); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("wrong typed stream error = %v, want ErrInvalidSubscription", err)
	}
}

func TestHandleTypedBookTickerCombinedFrame(t *testing.T) {
	session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 1)
	payload := []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BTCUSDT","b":"100.1","B":"2.3","a":"100.2","A":"4.5"}}`)
	receivedAt := time.Unix(10, 20)

	session.handleFrame(managedws.Frame{Generation: 7, Type: managedws.TextMessage, Payload: payload, ReceivedAt: receivedAt})

	select {
	case event := <-session.typedEvents:
		if event.Kind != TypedDeliveryBookTicker || event.Generation != 7 || event.Stream != "btcusdt@bookTicker" || !event.ReceivedAt.Equal(receivedAt) {
			t.Fatalf("typed metadata = %+v", event)
		}
		if event.DecodeErr != nil {
			t.Fatalf("DecodeErr = %v", event.DecodeErr)
		}
		if event.BookTicker.Symbol != "BTCUSDT" || event.BookTicker.UpdateID != 400900217 || event.BookTicker.BestBidQty != "2.3" || event.BookTicker.BestAskPrice != "100.2" {
			t.Fatalf("bookTicker = %+v", event.BookTicker)
		}
		if len(event.Raw) == 0 || &event.Raw[0] != &payload[0] {
			t.Fatal("typed event did not retain the immutable managed raw payload")
		}
	default:
		t.Fatal("typed event not delivered")
	}

	if len(session.events) != 0 {
		t.Fatal("generic event was materialized in typed mode")
	}
	stats := session.Stats()
	if stats.EventsReceived != 1 || stats.EventsDelivered != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestHandleTypedAggTradePresence(t *testing.T) {
	tests := []struct {
		name                  string
		nq                    string
		st                    string
		normalQuantity        string
		normalQuantityPresent bool
		symbolType            int
		symbolTypePresent     bool
	}{
		{name: "absent"},
		{name: "explicit zero and empty", nq: `,"nq":""`, st: `,"st":0`, normalQuantityPresent: true, symbolTypePresent: true},
		{name: "values", nq: `,"nq":"0.014"`, st: `,"st":1`, normalQuantity: "0.014", normalQuantityPresent: true, symbolType: 1, symbolTypePresent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := newTypedTestSession(TypedDeliveryAggTrade, DeliveryPolicyStrict, 1)
			payload := []byte(`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":123456789,"s":"BTCUSDT","a":5933014,"p":"0.001","q":"100","f":100,"l":105,"T":123456785,"m":true` + tt.nq + tt.st + `}}`)
			session.handleFrame(managedws.Frame{Generation: 3, Type: managedws.TextMessage, Payload: payload, ReceivedAt: time.Unix(1, 0)})
			event := <-session.typedEvents
			if event.DecodeErr != nil {
				t.Fatalf("DecodeErr = %v", event.DecodeErr)
			}
			got := event.AggTrade
			if got.NormalQuantity != tt.normalQuantity || got.NormalQuantityPresent != tt.normalQuantityPresent {
				t.Fatalf("nq = %q present=%v, want %q present=%v", got.NormalQuantity, got.NormalQuantityPresent, tt.normalQuantity, tt.normalQuantityPresent)
			}
			if got.SymbolType != tt.symbolType || got.SymbolTypePresent != tt.symbolTypePresent {
				t.Fatalf("st = %d present=%v, want %d present=%v", got.SymbolType, got.SymbolTypePresent, tt.symbolType, tt.symbolTypePresent)
			}
		})
	}
}

func TestWsAggTradeEventUnmarshalUsesTypedWireContract(t *testing.T) {
	var event WsAggTradeEvent
	if err := json.Unmarshal([]byte(`{"e":"aggTrade","s":"BTCUSDT","nq":"","st":0}`), &event); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if !event.NormalQuantityPresent || event.NormalQuantity != "" || !event.SymbolTypePresent || event.SymbolType != 0 {
		t.Fatalf("presence = %+v", event)
	}
}

func TestHandleTypedDecodeFailureIsApplicationEvent(t *testing.T) {
	session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 1)
	payload := []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":"bad","s":"BTCUSDT"}}`)

	session.handleFrame(managedws.Frame{Generation: 9, Type: managedws.TextMessage, Payload: payload, ReceivedAt: time.Unix(2, 0)})

	event := <-session.typedEvents
	if event.Stream != "btcusdt@bookTicker" || event.Generation != 9 || !errors.Is(event.DecodeErr, ErrApplicationDecode) {
		t.Fatalf("decode failure event = %+v", event)
	}
	if len(session.errors) != 0 {
		t.Fatal("application decode failure was incorrectly promoted to session protocol error")
	}
}

func TestTypedModePreservesProtocolACKAndRejectedHandling(t *testing.T) {
	session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 2)
	session.pending = map[uint64]*pendingRequest{
		11: {generation: 4, method: methodSubscribe, result: make(chan protocolResponse, 1)},
	}
	pending := session.pending[11]

	session.handleFrame(managedws.Frame{Generation: 4, Payload: []byte(`{"result":null,"id":11}`)})
	select {
	case response := <-pending.result:
		if string(response.result) != "null" {
			t.Fatalf("ACK result = %s", response.result)
		}
	default:
		t.Fatal("ACK was not delivered")
	}
	if len(session.typedEvents) != 0 {
		t.Fatal("ACK was incorrectly published as typed application event")
	}

	session.pending = make(map[uint64]*pendingRequest)
	session.handleFrame(managedws.Frame{Generation: 4, Payload: []byte(`{"code":-1121,"msg":"bad symbol"}`)})
	select {
	case event := <-session.errors:
		if event.Kind != StreamErrorRejected {
			t.Fatalf("error kind = %s, want %s", event.Kind, StreamErrorRejected)
		}
	default:
		t.Fatal("rejected response did not preserve error handling")
	}
}

func TestTypedModeRejectsSetCombinedFalse(t *testing.T) {
	session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 1)
	if err := session.SetCombined(context.Background(), false); !errors.Is(err, ErrInvalidStreamOptions) {
		t.Fatalf("SetCombined(false) error = %v, want ErrInvalidStreamOptions", err)
	}
}

func TestTypedBookTickerCoalescingReplacesWholeEvent(t *testing.T) {
	session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyLatestByStream, 1)
	session.typedEvents <- TypedStreamEvent{Kind: TypedDeliveryBookTicker, Stream: "blocker"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.workers.Add(1)
	go session.coalescingLoop(ctx)

	first := TypedStreamEvent{Kind: TypedDeliveryBookTicker, Stream: "btcusdt@bookTicker", BookTicker: WsBookTickerEvent{UpdateID: 1, BestBidPrice: "1"}}
	latest := TypedStreamEvent{Kind: TypedDeliveryBookTicker, Stream: "btcusdt@bookTicker", BookTicker: WsBookTickerEvent{UpdateID: 2, BestBidPrice: "2"}}
	if !session.publishTypedEvent(first) || !session.publishTypedEvent(latest) {
		t.Fatal("typed latest-value events were rejected")
	}

	<-session.typedEvents
	select {
	case got := <-session.typedEvents:
		if got.BookTicker.UpdateID != 2 || got.BookTicker.BestBidPrice != "2" {
			t.Fatalf("coalesced typed event = %+v", got.BookTicker)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced typed event was not delivered")
	}
	cancel()
	session.workers.Wait()
}

func TestTypedAggTradeSessionLifecycleAndReplaceSubscriptions(t *testing.T) {
	server := newLocalStreamServer(t)
	session := newTestStreamSession(t, server, []Subscription{AggTrade("BTCUSDT")}, func(options *StreamSessionOptions) {
		options.TypedDelivery = TypedDeliveryAggTrade
	})
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	conn := server.latestConn(t)

	req := server.waitRequest(t)
	if req.Method != methodSubscribe {
		t.Fatalf("initial method = %s, want %s", req.Method, methodSubscribe)
	}
	writeJSON(t, conn, map[string]interface{}{"result": nil, "id": req.ID})
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := session.WaitReady(readyCtx); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}

	writeJSON(t, conn, map[string]interface{}{
		"stream": "btcusdt@aggTrade",
		"data": map[string]interface{}{
			"e": "aggTrade", "E": 1, "s": "BTCUSDT", "a": 10, "p": "1", "q": "2", "f": 3, "l": 4, "T": 5, "m": false,
		},
	})
	select {
	case event := <-session.TypedEvents():
		if event.Stream != "btcusdt@aggTrade" || event.AggTrade.Symbol != "BTCUSDT" || event.Generation == 0 {
			t.Fatalf("initial typed event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for initial typed event")
	}

	replaceDone := make(chan error, 1)
	go func() {
		replaceDone <- session.ReplaceSubscriptions(context.Background(), []Subscription{AggTrade("ETHUSDT")})
	}()
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		req = server.waitRequest(t)
		seen[req.Method] = true
		writeJSON(t, conn, map[string]interface{}{"result": nil, "id": req.ID})
	}
	if !seen[methodSubscribe] || !seen[methodUnsubscribe] {
		t.Fatalf("replace methods = %+v", seen)
	}
	select {
	case err := <-replaceDone:
		if err != nil {
			t.Fatalf("ReplaceSubscriptions() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ReplaceSubscriptions() did not complete")
	}

	writeJSON(t, conn, map[string]interface{}{
		"stream": "ethusdt@aggTrade",
		"data": map[string]interface{}{
			"e": "aggTrade", "E": 2, "s": "ETHUSDT", "a": 11, "p": "3", "q": "4", "f": 6, "l": 7, "T": 8, "m": true, "nq": "", "st": 0,
		},
	})
	select {
	case event := <-session.TypedEvents():
		if event.Stream != "ethusdt@aggTrade" || event.AggTrade.Symbol != "ETHUSDT" || !event.AggTrade.NormalQuantityPresent || !event.AggTrade.SymbolTypePresent {
			t.Fatalf("replacement typed event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for replacement typed event")
	}
}

func TestTypedModeRejectsUnwrappedAndMismatchedApplicationFrames(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{
			name:    "unwrapped",
			payload: `{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}`,
		},
		{
			name:    "mismatched stream",
			payload: `{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"T":1,"m":false}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 1)
			session.handleFrame(managedws.Frame{Generation: 1, Payload: []byte(tt.payload), ReceivedAt: time.Unix(1, 0)})
			event := <-session.typedEvents
			if !errors.Is(event.DecodeErr, ErrApplicationDecode) {
				t.Fatalf("DecodeErr = %v, want ErrApplicationDecode", event.DecodeErr)
			}
			if len(session.errors) != 0 {
				t.Fatal("typed application mismatch was promoted to protocol error")
			}
		})
	}
}
