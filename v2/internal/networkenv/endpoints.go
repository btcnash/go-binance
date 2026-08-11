package networkenv

import "sync"

// SpotEndpoints contains the default Spot endpoints for one environment.
type SpotEndpoints struct {
	REST       string
	WSRaw      string
	WSCombined string
	WSAPI      string
}

// USDMEndpoints contains the default USDⓈ-M Futures endpoints for one environment.
type USDMEndpoints struct {
	REST            string
	PublicRaw       string
	PublicCombined  string
	MarketRaw       string
	MarketCombined  string
	PrivateRaw      string
	PrivateCombined string
	WSAPI           string
}

// COINMEndpoints contains the default COIN-M Futures endpoints for one environment.
type COINMEndpoints struct {
	REST       string
	WSRaw      string
	WSCombined string
}

var spotProfiles = [...]SpotEndpoints{
	Mainnet: {
		REST:       "https://api.binance.com",
		WSRaw:      "wss://stream.binance.com:9443/ws",
		WSCombined: "wss://stream.binance.com:9443/stream",
		WSAPI:      "wss://ws-api.binance.com/ws-api/v3",
	},
	Testnet: {
		REST:       "https://testnet.binance.vision",
		WSRaw:      "wss://stream.testnet.binance.vision/ws",
		WSCombined: "wss://stream.testnet.binance.vision/stream",
		WSAPI:      "wss://ws-api.testnet.binance.vision/ws-api/v3",
	},
}

var usdmProfiles = [...]USDMEndpoints{
	Mainnet: {
		REST:            "https://fapi.binance.com",
		PublicRaw:       "wss://fstream.binance.com/public/ws",
		PublicCombined:  "wss://fstream.binance.com/public/stream",
		MarketRaw:       "wss://fstream.binance.com/market/ws",
		MarketCombined:  "wss://fstream.binance.com/market/stream",
		PrivateRaw:      "wss://fstream.binance.com/private/ws",
		PrivateCombined: "wss://fstream.binance.com/private/stream",
		WSAPI:           "wss://ws-fapi.binance.com/ws-fapi/v1",
	},
	Testnet: {
		REST:            "https://demo-fapi.binance.com",
		PublicRaw:       "wss://demo-fstream.binance.com/public/ws",
		PublicCombined:  "wss://demo-fstream.binance.com/public/stream",
		MarketRaw:       "wss://demo-fstream.binance.com/market/ws",
		MarketCombined:  "wss://demo-fstream.binance.com/market/stream",
		PrivateRaw:      "wss://demo-fstream.binance.com/private/ws",
		PrivateCombined: "wss://demo-fstream.binance.com/private/stream",
		WSAPI:           "wss://testnet.binancefuture.com/ws-fapi/v1",
	},
}

var coinmProfiles = [...]COINMEndpoints{
	Mainnet: {
		REST:       "https://dapi.binance.com",
		WSRaw:      "wss://dstream.binance.com/ws",
		WSCombined: "wss://dstream.binance.com/stream",
	},
	Testnet: {
		REST:       "https://demo-dapi.binance.com",
		WSRaw:      "wss://demo-dstream.binance.com/ws",
		WSCombined: "wss://demo-dstream.binance.com/stream",
	},
}

var (
	testOverrideMu sync.RWMutex
	usdmOverrides  = map[Environment]USDMEndpoints{}
)

func Spot(environment Environment) SpotEndpoints {
	return spotProfiles[normalize(environment)]
}

func USDM(environment Environment) USDMEndpoints {
	testOverrideMu.RLock()
	override, ok := usdmOverrides[environment]
	testOverrideMu.RUnlock()
	if ok {
		return override
	}
	return usdmProfiles[normalize(environment)]
}

func COINM(environment Environment) COINMEndpoints {
	return coinmProfiles[normalize(environment)]
}

func normalize(environment Environment) Environment {
	if environment == Testnet {
		return Testnet
	}
	return Mainnet
}

// OverrideUSDMForTesting replaces one USDⓈ-M profile for deterministic in-module
// protocol tests. It is internal to this module and is not part of the public SDK API.
func OverrideUSDMForTesting(environment Environment, endpoints USDMEndpoints) func() {
	testOverrideMu.Lock()
	previous, hadPrevious := usdmOverrides[environment]
	usdmOverrides[environment] = endpoints
	testOverrideMu.Unlock()
	return func() {
		testOverrideMu.Lock()
		defer testOverrideMu.Unlock()
		if hadPrevious {
			usdmOverrides[environment] = previous
			return
		}
		delete(usdmOverrides, environment)
	}
}
