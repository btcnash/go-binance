package stream

import (
	"errors"
	"testing"

	managedgorilla "github.com/adshao/go-binance/v2/common/websocket/managed/gorilla"
	"time"
)

func TestSubscriptionBuildersAndClassValidation(t *testing.T) {
	tests := []struct {
		name  string
		sub   Subscription
		class StreamClass
		wire  string
	}{
		{"book ticker", BookTicker("BTCUSDT"), StreamClassPublic, "btcusdt@bookTicker"},
		{"diff depth", DiffDepth("ETHUSDT", DepthSpeed100ms), StreamClassPublic, "ethusdt@depth@100ms"},
		{"partial depth", PartialDepth("BNBUSDT", 20, DepthSpeed500ms), StreamClassPublic, "bnbusdt@depth20@500ms"},
		{"aggregate trade", AggTrade("BTCUSDT"), StreamClassMarket, "btcusdt@aggTrade"},
		{"mark price", MarkPrice("BTCUSDT", time.Second), StreamClassMarket, "btcusdt@markPrice@1s"},
		{"kline", Kline("BTCUSDT", "1m"), StreamClassMarket, "btcusdt@kline_1m"},
		{"ticker", Ticker("BTCUSDT"), StreamClassMarket, "btcusdt@ticker"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sub.Class(); got != tt.class {
				t.Fatalf("Class() = %s, want %s", got, tt.class)
			}
			if got := tt.sub.String(); got != tt.wire {
				t.Fatalf("String() = %q, want %q", got, tt.wire)
			}
			if err := tt.sub.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}

	if err := RawSubscription(StreamClassPublic, "btcusdt@aggTrade").ValidateFor(StreamClassMarket); !errors.Is(err, ErrWrongStreamClass) {
		t.Fatalf("ValidateFor() error = %v, want ErrWrongStreamClass", err)
	}
	if err := BookTicker("").Validate(); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("empty symbol error = %v, want ErrInvalidSubscription", err)
	}
	if err := PartialDepth("BTCUSDT", 7, DepthSpeed100ms).Validate(); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("invalid levels error = %v, want ErrInvalidSubscription", err)
	}
}

func TestDefaultDynamicEndpointAndTransportDefaults(t *testing.T) {
	public, err := normalizeStreamOptions(StreamSessionOptions{Class: StreamClassPublic, Environment: EnvironmentMainnet})
	if err != nil {
		t.Fatalf("normalize public options: %v", err)
	}
	publicDialer, ok := public.ConnectionOptions.Dialer.(managedgorilla.Dialer)
	if !ok {
		t.Fatalf("public dialer type = %T", public.ConnectionOptions.Dialer)
	}
	if publicDialer.Endpoint != "wss://fstream.binance.com/public/stream" {
		t.Fatalf("public endpoint = %q", publicDialer.Endpoint)
	}
	if !public.ConnectionOptions.Heartbeat.Enabled || !public.ConnectionOptions.Reconnect.Enabled {
		t.Fatal("managed heartbeat/reconnect defaults are not enabled")
	}

	market, err := normalizeStreamOptions(StreamSessionOptions{Class: StreamClassMarket, Environment: EnvironmentDemo})
	if err != nil {
		t.Fatalf("normalize market options: %v", err)
	}
	marketDialer := market.ConnectionOptions.Dialer.(managedgorilla.Dialer)
	if marketDialer.Endpoint != "wss://fstream.binancefuture.com/market/stream" {
		t.Fatalf("market endpoint = %q", marketDialer.Endpoint)
	}
}
