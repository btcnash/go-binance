package futures

import (
	"testing"

	"github.com/btcnash/go-binance/v2/internal/networkenv"
)

func TestUnifiedEnvironmentResolvesUSDMEndpoints(t *testing.T) {
	previous := networkenv.Current()
	t.Cleanup(func() { _ = networkenv.Set(previous) })

	for _, tc := range []struct {
		env                          networkenv.Environment
		rest, public, publicCombined string
		market, marketCombined       string
		private, wsapi               string
	}{
		{
			networkenv.Mainnet,
			"https://fapi.binance.com",
			"wss://fstream.binance.com/public/ws",
			"wss://fstream.binance.com/public/stream?streams=",
			"wss://fstream.binance.com/market/ws",
			"wss://fstream.binance.com/market/stream?streams=",
			"wss://fstream.binance.com/private/ws",
			"wss://ws-fapi.binance.com/ws-fapi/v1",
		},
		{
			networkenv.Testnet,
			"https://demo-fapi.binance.com",
			"wss://demo-fstream.binance.com/public/ws",
			"wss://demo-fstream.binance.com/public/stream?streams=",
			"wss://demo-fstream.binance.com/market/ws",
			"wss://demo-fstream.binance.com/market/stream?streams=",
			"wss://demo-fstream.binance.com/private/ws",
			"wss://testnet.binancefuture.com/ws-fapi/v1",
		},
	} {
		if err := networkenv.Set(tc.env); err != nil {
			t.Fatal(err)
		}
		if got := getApiEndpoint(); got != tc.rest {
			t.Fatalf("%v REST = %q", tc.env, got)
		}
		if got := getWsPublicEndpoint(); got != tc.public {
			t.Fatalf("%v public WS = %q", tc.env, got)
		}
		if got := getCombinedPublicEndpoint(); got != tc.publicCombined {
			t.Fatalf("%v public combined WS = %q", tc.env, got)
		}
		if got := getWsMarketEndpoint(); got != tc.market {
			t.Fatalf("%v market WS = %q", tc.env, got)
		}
		if got := getCombinedMarketEndpoint(); got != tc.marketCombined {
			t.Fatalf("%v market combined WS = %q", tc.env, got)
		}
		if got := getWsPrivateEndpoint(); got != tc.private {
			t.Fatalf("%v private WS = %q", tc.env, got)
		}
		if got := getWsApiEndpoint(); got != tc.wsapi {
			t.Fatalf("%v WSAPI = %q", tc.env, got)
		}
	}
}
