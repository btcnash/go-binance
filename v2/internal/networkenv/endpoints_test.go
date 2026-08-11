package networkenv

import (
	"reflect"
	"testing"
)

func TestSpotEndpoints(t *testing.T) {
	cases := []struct {
		env  Environment
		want SpotEndpoints
	}{
		{Mainnet, SpotEndpoints{"https://api.binance.com", "wss://stream.binance.com:9443/ws", "wss://stream.binance.com:9443/stream", "wss://ws-api.binance.com/ws-api/v3"}},
		{Testnet, SpotEndpoints{"https://testnet.binance.vision", "wss://stream.testnet.binance.vision/ws", "wss://stream.testnet.binance.vision/stream", "wss://ws-api.testnet.binance.vision/ws-api/v3"}},
	}
	for _, tc := range cases {
		if got := Spot(tc.env); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("Spot(%v) = %#v, want %#v", tc.env, got, tc.want)
		}
	}
}

func TestUSDMEndpoints(t *testing.T) {
	cases := []struct {
		env  Environment
		want USDMEndpoints
	}{
		{Mainnet, USDMEndpoints{"https://fapi.binance.com", "wss://fstream.binance.com/public/ws", "wss://fstream.binance.com/public/stream", "wss://fstream.binance.com/market/ws", "wss://fstream.binance.com/market/stream", "wss://fstream.binance.com/private/ws", "wss://fstream.binance.com/private/stream", "wss://ws-fapi.binance.com/ws-fapi/v1"}},
		{Testnet, USDMEndpoints{"https://demo-fapi.binance.com", "wss://demo-fstream.binance.com/public/ws", "wss://demo-fstream.binance.com/public/stream", "wss://demo-fstream.binance.com/market/ws", "wss://demo-fstream.binance.com/market/stream", "wss://demo-fstream.binance.com/private/ws", "wss://demo-fstream.binance.com/private/stream", "wss://testnet.binancefuture.com/ws-fapi/v1"}},
	}
	for _, tc := range cases {
		if got := USDM(tc.env); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("USDM(%v) = %#v, want %#v", tc.env, got, tc.want)
		}
	}
}

func TestCOINMEndpoints(t *testing.T) {
	cases := []struct {
		env  Environment
		want COINMEndpoints
	}{
		{Mainnet, COINMEndpoints{"https://dapi.binance.com", "wss://dstream.binance.com/ws", "wss://dstream.binance.com/stream"}},
		{Testnet, COINMEndpoints{"https://demo-dapi.binance.com", "wss://demo-dstream.binance.com/ws", "wss://demo-dstream.binance.com/stream"}},
	}
	for _, tc := range cases {
		if got := COINM(tc.env); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("COINM(%v) = %#v, want %#v", tc.env, got, tc.want)
		}
	}
}
