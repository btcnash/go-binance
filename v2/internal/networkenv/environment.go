package networkenv

import (
	"fmt"
	"sync/atomic"
)

// Environment identifies the Binance deployment used by all supported markets.
type Environment uint8

const (
	Mainnet Environment = iota
	Testnet
)

var current atomic.Uint32

// Set changes the process-wide Binance environment.
func Set(environment Environment) error {
	if environment != Mainnet && environment != Testnet {
		return fmt.Errorf("binance: unsupported environment %d", environment)
	}
	current.Store(uint32(environment))
	return nil
}

// Current returns the process-wide Binance environment.
func Current() Environment {
	return Environment(current.Load())
}
