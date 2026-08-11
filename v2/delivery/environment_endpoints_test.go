package delivery

import (
	"testing"

	"github.com/btcnash/go-binance/v2/internal/networkenv"
)

func TestUnifiedEnvironmentResolvesCOINMEndpoints(t *testing.T) {
	previous := networkenv.Current()
	t.Cleanup(func() { _ = networkenv.Set(previous) })

	for _, tc := range []struct {
		env      networkenv.Environment
		rest, ws string
	}{
		{networkenv.Mainnet, "https://dapi.binance.com", "wss://dstream.binance.com/ws"},
		{networkenv.Testnet, "https://demo-dapi.binance.com", "wss://demo-dstream.binance.com/ws"},
	} {
		if err := networkenv.Set(tc.env); err != nil {
			t.Fatal(err)
		}
		if got := getApiEndpoint(); got != tc.rest {
			t.Fatalf("%v REST = %q", tc.env, got)
		}
		if got := getWsEndpoint(); got != tc.ws {
			t.Fatalf("%v WS = %q", tc.env, got)
		}
	}
}
