package stream

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
)

func TestTypedKlineDeliveryValidation(t *testing.T) {
	if _, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassMarket,
		TypedDelivery:        TypedDeliveryKline,
		InitialSubscriptions: []Subscription{Kline("BTCUSDT", "1m"), Kline("ETHUSDT", "5m")},
	}); err != nil {
		t.Fatalf("kline typed options: %v", err)
	}

	if _, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassPublic,
		TypedDelivery:        TypedDeliveryKline,
		InitialSubscriptions: []Subscription{BookTicker("BTCUSDT")},
	}); !errors.Is(err, ErrInvalidStreamOptions) {
		t.Fatalf("wrong class error = %v, want ErrInvalidStreamOptions", err)
	}

	if _, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassMarket,
		TypedDelivery:        TypedDeliveryKline,
		InitialSubscriptions: []Subscription{AggTrade("BTCUSDT")},
	}); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("wrong typed stream error = %v, want ErrInvalidSubscription", err)
	}

	if _, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassMarket,
		TypedDelivery:        TypedDeliveryKline,
		DeliveryPolicy:       DeliveryPolicyLatestByStream,
		InitialSubscriptions: []Subscription{Kline("BTCUSDT", "1m")},
	}); !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("latest-value kline error = %v, want ErrInvalidDeliveryPolicy", err)
	}
}

func TestHandleTypedKlinePresence(t *testing.T) {
	tests := []struct {
		name           string
		x              string
		isFinal        bool
		isFinalPresent bool
	}{
		{name: "absent"},
		{name: "explicit false", x: `,"x":false`, isFinalPresent: true},
		{name: "explicit true", x: `,"x":true`, isFinal: true, isFinalPresent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := newTypedTestSession(TypedDeliveryKline, DeliveryPolicyStrict, 1)
			payload := []byte(`{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1499404907056,"s":"BTCUSDT","k":{"t":1499404860000,"T":1499404919999,"s":"BTCUSDT","i":"1m","f":77462,"L":77465,"o":"0.10278577","c":"0.10278645","h":"0.10278712","l":"0.10278518","v":"17.47929838","n":4,"q":"1.79662878","V":"2.34879839","Q":"0.24142166"` + tt.x + `}}}`)
			receivedAt := time.Unix(10, 20)

			session.handleFrame(managedws.Frame{Generation: 7, Type: managedws.TextMessage, Payload: payload, ReceivedAt: receivedAt})

			select {
			case event := <-session.typedEvents:
				if event.Kind != TypedDeliveryKline || event.Generation != 7 || event.Stream != "btcusdt@kline_1m" || !event.ReceivedAt.Equal(receivedAt) {
					t.Fatalf("typed metadata = %+v", event)
				}
				if event.DecodeErr != nil {
					t.Fatalf("DecodeErr = %v", event.DecodeErr)
				}
				got := event.Kline
				if got.Symbol != "BTCUSDT" || got.Kline.Interval != "1m" || got.Kline.Close != "0.10278645" {
					t.Fatalf("kline = %+v", got)
				}
				if got.Kline.IsFinal != tt.isFinal || got.Kline.IsFinalPresent != tt.isFinalPresent {
					t.Fatalf("x = %v present=%v, want %v present=%v", got.Kline.IsFinal, got.Kline.IsFinalPresent, tt.isFinal, tt.isFinalPresent)
				}
				if len(event.Raw) == 0 || &event.Raw[0] != &payload[0] {
					t.Fatal("typed event did not retain the immutable managed raw payload")
				}
			default:
				t.Fatal("typed kline event not delivered")
			}

			if len(session.events) != 0 {
				t.Fatal("generic event was materialized in typed mode")
			}
		})
	}
}

func TestWsKlinePresenceUnmarshal(t *testing.T) {
	tests := []struct {
		name           string
		x              string
		isFinal        bool
		isFinalPresent bool
	}{
		{name: "absent"},
		{name: "explicit false", x: `,"x":false`, isFinalPresent: true},
		{name: "explicit true", x: `,"x":true`, isFinal: true, isFinalPresent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event WsKlineEvent
			payload := []byte(`{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":1,"T":2,"s":"BTCUSDT","i":"1m"` + tt.x + `}}`)
			if err := json.Unmarshal(payload, &event); err != nil {
				t.Fatalf("WsKlineEvent unmarshal: %v", err)
			}
			if event.Kline.IsFinal != tt.isFinal || event.Kline.IsFinalPresent != tt.isFinalPresent {
				t.Fatalf("event x = %v present=%v, want %v present=%v", event.Kline.IsFinal, event.Kline.IsFinalPresent, tt.isFinal, tt.isFinalPresent)
			}

			var kline WsKline
			if err := json.Unmarshal([]byte(`{"t":1,"T":2,"s":"BTCUSDT","i":"1m"`+tt.x+`}`), &kline); err != nil {
				t.Fatalf("WsKline unmarshal: %v", err)
			}
			if kline.IsFinal != tt.isFinal || kline.IsFinalPresent != tt.isFinalPresent {
				t.Fatalf("body x = %v present=%v, want %v present=%v", kline.IsFinal, kline.IsFinalPresent, tt.isFinal, tt.isFinalPresent)
			}
		})
	}
}

func TestTypedKlineDecodeFailureIsApplicationEvent(t *testing.T) {
	session := newTypedTestSession(TypedDeliveryKline, DeliveryPolicyStrict, 1)
	payload := []byte(`{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":1,"T":2,"s":"BTCUSDT","i":"1m","x":"bad"}}}`)

	session.handleFrame(managedws.Frame{Generation: 9, Payload: payload, ReceivedAt: time.Unix(2, 0)})

	event := <-session.typedEvents
	if event.Stream != "btcusdt@kline_1m" || event.Generation != 9 || !errors.Is(event.DecodeErr, ErrApplicationDecode) {
		t.Fatalf("decode failure event = %+v", event)
	}
	if len(event.Raw) == 0 || &event.Raw[0] != &payload[0] {
		t.Fatal("decode failure did not retain immutable raw payload")
	}
	if len(session.errors) != 0 {
		t.Fatal("application decode failure was incorrectly promoted to session protocol error")
	}
}

func TestTypedKlineSupportsDynamicIntervals(t *testing.T) {
	session := newTypedTestSession(TypedDeliveryKline, DeliveryPolicyStrict, 1)
	session.opts.Class = StreamClassMarket
	if err := session.validateSubscriptions([]Subscription{Kline("BTCUSDT", "1m"), Kline("BTCUSDT", "5m")}); err != nil {
		t.Fatalf("multi-interval kline subscriptions: %v", err)
	}
	if err := session.validateSubscriptions([]Subscription{AggTrade("BTCUSDT")}); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("mixed typed subscription error = %v, want ErrInvalidSubscription", err)
	}
}
