package stream

import (
	jsonv2 "encoding/json/v2"
	"errors"
	"reflect"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
)

func TestTypedBookTickerJSONV2Contract(t *testing.T) {
	t.Run("exact names and unknown fields", func(t *testing.T) {
		session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 2)
		payload := []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"E":2,"T":3,"s":"BTCUSDT","S":"WRONG","b":"100.1","B":"2.3","a":"100.2","A":"4.5","future":{"x":1}}}`)
		session.handleFrame(managedws.Frame{Generation: 1, Payload: payload, ReceivedAt: time.Unix(1, 0)})
		event := <-session.typedEvents
		if event.DecodeErr != nil {
			t.Fatalf("DecodeErr = %v", event.DecodeErr)
		}
		got := event.BookTicker
		if got.Symbol != "BTCUSDT" || got.BestBidPrice != "100.1" || got.BestBidQty != "2.3" || got.BestAskPrice != "100.2" || got.BestAskQty != "4.5" {
			t.Fatalf("bookTicker = %+v", got)
		}
	})

	t.Run("malformed typed field is application decode failure", func(t *testing.T) {
		session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 2)
		payload := []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":"bad","s":"BTCUSDT"}}`)
		session.handleFrame(managedws.Frame{Generation: 1, Payload: payload})
		event := <-session.typedEvents
		if !errors.Is(event.DecodeErr, ErrApplicationDecode) || event.BookTicker.UpdateID != 0 {
			t.Fatalf("decode failure event = %+v", event)
		}
	})
}

func TestTypedJSONV2RejectsAmbiguousOrInvalidFrames(t *testing.T) {
	tests := []struct {
		name string
		mode TypedDeliveryMode
		data []byte
	}{
		{
			name: "bookTicker duplicate known field",
			mode: TypedDeliveryBookTicker,
			data: []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","s":"BTCUSDT","b":"1","b":"2"}}`),
		},
		{
			name: "aggTrade duplicate known field",
			mode: TypedDeliveryAggTrade,
			data: []byte(`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","a":1,"a":2}}`),
		},
		{
			name: "kline duplicate known field",
			mode: TypedDeliveryKline,
			data: []byte(`{"stream":"btcusdt@kline_1m","data":{"e":"kline","s":"BTCUSDT","k":{"t":1,"t":2}}}`),
		},
		{
			name: "duplicate envelope field",
			mode: TypedDeliveryBookTicker,
			data: []byte(`{"stream":"btcusdt@bookTicker","stream":"ethusdt@bookTicker","data":{"e":"bookTicker","s":"BTCUSDT"}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := newTypedTestSession(tt.mode, DeliveryPolicyStrict, 2)
			session.handleFrame(managedws.Frame{Generation: 1, Payload: tt.data})
			assertNoSuccessfulTypedEvent(t, session)
		})
	}

	for _, tt := range []struct {
		name   string
		mode   TypedDeliveryMode
		prefix string
		suffix string
	}{
		{"bookTicker invalid UTF-8", TypedDeliveryBookTicker, `{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","s":"BTC`, `USDT"}}`},
		{"aggTrade invalid UTF-8", TypedDeliveryAggTrade, `{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTC`, `USDT"}}`},
		{"kline invalid UTF-8", TypedDeliveryKline, `{"stream":"btcusdt@kline_1m","data":{"e":"kline","s":"BTC`, `USDT","k":{}}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := append([]byte(tt.prefix), 0xff)
			payload = append(payload, []byte(tt.suffix)...)
			session := newTypedTestSession(tt.mode, DeliveryPolicyStrict, 2)
			session.handleFrame(managedws.Frame{Generation: 1, Payload: payload})
			assertNoSuccessfulTypedEvent(t, session)
		})
	}
}

func TestTypedJSONV2StreamMismatchIsDecodeFailure(t *testing.T) {
	tests := []struct {
		name    string
		mode    TypedDeliveryMode
		payload []byte
		stream  string
	}{
		{"bookTicker", TypedDeliveryBookTicker, []byte(`{"stream":"btcusdt@aggTrade","data":{"e":"bookTicker","s":"BTCUSDT"}}`), "btcusdt@aggTrade"},
		{"aggTrade", TypedDeliveryAggTrade, []byte(`{"stream":"btcusdt@kline_1m","data":{"e":"aggTrade","s":"BTCUSDT","a":1}}`), "btcusdt@kline_1m"},
		{"kline", TypedDeliveryKline, []byte(`{"stream":"btcusdt@aggTrade","data":{"e":"kline","s":"BTCUSDT","k":{}}}`), "btcusdt@aggTrade"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := newTypedTestSession(tt.mode, DeliveryPolicyStrict, 2)
			session.handleFrame(managedws.Frame{Generation: 3, Payload: tt.payload})
			event := <-session.typedEvents
			if !errors.Is(event.DecodeErr, ErrApplicationDecode) || event.Stream != tt.stream {
				t.Fatalf("stream mismatch event = %+v", event)
			}
		})
	}
}

func TestDirectJSONV2DecodeMatchesManagedTypedDelivery(t *testing.T) {
	t.Run("bookTicker", func(t *testing.T) {
		managed := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 1)
		managedPayload := []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BTCUSDT","b":"100.1","B":"2.3","a":"100.2","A":"4.5","future":1}}`)
		managed.handleFrame(managedws.Frame{Payload: managedPayload})
		gotManaged := (<-managed.typedEvents).BookTicker

		var direct WsBookTickerEvent
		if err := jsonv2.Unmarshal([]byte(`{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BTCUSDT","b":"100.1","B":"2.3","a":"100.2","A":"4.5","future":1}`), &direct); err != nil {
			t.Fatalf("direct v2 decode: %v", err)
		}
		if !reflect.DeepEqual(gotManaged, direct) {
			t.Fatalf("managed=%+v direct=%+v", gotManaged, direct)
		}
	})

	t.Run("aggTrade", func(t *testing.T) {
		managed := newTypedTestSession(TypedDeliveryAggTrade, DeliveryPolicyStrict, 1)
		managedPayload := []byte(`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":123456789,"s":"BTCUSDT","a":5933014,"p":"0.001","q":"100","nq":"","f":100,"l":105,"T":123456785,"m":true,"st":0,"future":1}}`)
		managed.handleFrame(managedws.Frame{Payload: managedPayload})
		gotManaged := (<-managed.typedEvents).AggTrade

		var direct WsAggTradeEvent
		if err := jsonv2.Unmarshal([]byte(`{"e":"aggTrade","E":123456789,"s":"BTCUSDT","a":5933014,"p":"0.001","q":"100","nq":"","f":100,"l":105,"T":123456785,"m":true,"st":0,"future":1}`), &direct); err != nil {
			t.Fatalf("direct v2 decode: %v", err)
		}
		if !reflect.DeepEqual(gotManaged, direct) {
			t.Fatalf("managed=%+v direct=%+v", gotManaged, direct)
		}
	})

	t.Run("kline", func(t *testing.T) {
		managed := newTypedTestSession(TypedDeliveryKline, DeliveryPolicyStrict, 1)
		managedPayload := []byte(`{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":1,"T":2,"s":"BTCUSDT","i":"1m","x":false,"future":1}}}`)
		managed.handleFrame(managedws.Frame{Payload: managedPayload})
		gotManaged := (<-managed.typedEvents).Kline

		var direct WsKlineEvent
		if err := jsonv2.Unmarshal([]byte(`{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":1,"T":2,"s":"BTCUSDT","i":"1m","x":false,"future":1}}`), &direct); err != nil {
			t.Fatalf("direct v2 decode: %v", err)
		}
		if !reflect.DeepEqual(gotManaged, direct) {
			t.Fatalf("managed=%+v direct=%+v", gotManaged, direct)
		}
	})
}

func TestDirectTypedJSONV2RejectsDuplicateMembers(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		out     any
	}{
		{"bookTicker", []byte(`{"e":"bookTicker","b":"1","b":"2"}`), &WsBookTickerEvent{}},
		{"aggTrade", []byte(`{"e":"aggTrade","a":1,"a":2}`), &WsAggTradeEvent{}},
		{"kline", []byte(`{"e":"kline","k":{"t":1,"t":2}}`), &WsKlineEvent{}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if err := jsonv2.Unmarshal(tt.payload, tt.out); err == nil {
				t.Fatal("duplicate member accepted")
			}
		})
	}
}

func assertNoSuccessfulTypedEvent(t *testing.T, session *StreamSession) {
	t.Helper()
	select {
	case event := <-session.typedEvents:
		if event.DecodeErr == nil {
			t.Fatalf("invalid frame published successful typed event: %+v", event)
		}
	default:
	}
	if len(session.typedEvents) == 0 && len(session.errors) == 0 {
		t.Fatal("invalid frame produced no observable error")
	}
}
