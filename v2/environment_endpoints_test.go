package binance

import "testing"

func TestUnifiedEnvironmentResolvesSpotEndpoints(t *testing.T) {
	if err := SetEnvironment(EnvironmentMainnet); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = SetEnvironment(EnvironmentMainnet) })
	if got := getAPIEndpoint(); got != "https://api.binance.com" {
		t.Fatalf("mainnet REST = %q", got)
	}
	if got := getWsEndpoint(); got != "wss://stream.binance.com:9443/ws" {
		t.Fatalf("mainnet WS = %q", got)
	}
	if got := getCombinedEndpoint(); got != "wss://stream.binance.com:9443/stream?streams=" {
		t.Fatalf("mainnet combined WS = %q", got)
	}
	if got := getWsApiEndpoint(); got != "wss://ws-api.binance.com/ws-api/v3" {
		t.Fatalf("mainnet WSAPI = %q", got)
	}

	if err := SetEnvironment(EnvironmentTestnet); err != nil {
		t.Fatal(err)
	}
	if got := getAPIEndpoint(); got != "https://testnet.binance.vision" {
		t.Fatalf("testnet REST = %q", got)
	}
	if got := getWsEndpoint(); got != "wss://stream.testnet.binance.vision/ws" {
		t.Fatalf("testnet WS = %q", got)
	}
	if got := getCombinedEndpoint(); got != "wss://stream.testnet.binance.vision/stream?streams=" {
		t.Fatalf("testnet combined WS = %q", got)
	}
	if got := getWsApiEndpoint(); got != "wss://ws-api.testnet.binance.vision/ws-api/v3" {
		t.Fatalf("testnet WSAPI = %q", got)
	}
}
