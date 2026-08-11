package stream

import (
	"errors"
	"testing"
	"time"

	managedgorilla "github.com/btcnash/go-binance/v2/common/websocket/managed/gorilla"
	"github.com/btcnash/go-binance/v2/internal/networkenv"
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
	previous := networkenv.Current()
	t.Cleanup(func() { _ = networkenv.Set(previous) })

	tests := []struct {
		name     string
		env      networkenv.Environment
		class    StreamClass
		expected string
	}{
		{name: "mainnet public", env: networkenv.Mainnet, class: StreamClassPublic, expected: "wss://fstream.binance.com/public/stream"},
		{name: "mainnet market", env: networkenv.Mainnet, class: StreamClassMarket, expected: "wss://fstream.binance.com/market/stream"},
		{name: "testnet public", env: networkenv.Testnet, class: StreamClassPublic, expected: "wss://demo-fstream.binance.com/public/stream"},
		{name: "testnet market", env: networkenv.Testnet, class: StreamClassMarket, expected: "wss://demo-fstream.binance.com/market/stream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := networkenv.Set(tt.env); err != nil {
				t.Fatal(err)
			}
			opts, err := normalizeStreamOptions(StreamSessionOptions{Class: tt.class})
			if err != nil {
				t.Fatalf("normalize options: %v", err)
			}
			dialer, ok := opts.ConnectionOptions.Dialer.(managedgorilla.Dialer)
			if !ok {
				t.Fatalf("dialer type = %T", opts.ConnectionOptions.Dialer)
			}
			if dialer.Endpoint != tt.expected {
				t.Fatalf("endpoint = %q, want %q", dialer.Endpoint, tt.expected)
			}
			if !opts.ConnectionOptions.Heartbeat.Enabled || !opts.ConnectionOptions.Reconnect.Enabled {
				t.Fatal("managed heartbeat/reconnect defaults are not enabled")
			}
		})
	}

	if err := networkenv.Set(networkenv.Testnet); err != nil {
		t.Fatal(err)
	}
	explicit, err := normalizeStreamOptions(StreamSessionOptions{Class: StreamClassMarket, Endpoint: "ws://custom"})
	if err != nil {
		t.Fatalf("normalize explicit options: %v", err)
	}
	explicitDialer := explicit.ConnectionOptions.Dialer.(managedgorilla.Dialer)
	if explicitDialer.Endpoint != "ws://custom" {
		t.Fatalf("explicit endpoint = %q", explicitDialer.Endpoint)
	}
}
