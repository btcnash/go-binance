package binance

import "github.com/btcnash/go-binance/v2/internal/networkenv"

// Environment identifies the Binance deployment used by Spot, USDⓈ-M Futures,
// and COIN-M Futures default endpoints.
type Environment uint8

const (
	EnvironmentMainnet Environment = iota
	EnvironmentTestnet
)

// SetEnvironment sets the process-wide Binance environment used when a client
// or session does not provide an explicit endpoint override.
func SetEnvironment(environment Environment) error {
	return networkenv.Set(networkenv.Environment(environment))
}
