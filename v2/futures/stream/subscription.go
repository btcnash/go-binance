package stream

import (
	"fmt"
	"strings"
	"time"
)

// Subscription identifies one Binance stream and its required endpoint class.
type Subscription struct {
	class   StreamClass
	name    string
	invalid string
}

// Class returns the required Public or Market endpoint class.
func (s Subscription) Class() StreamClass { return s.class }

// String returns the exact Binance stream name sent in SUBSCRIBE requests.
func (s Subscription) String() string { return s.name }

func (s Subscription) supportsLatestValueDelivery() bool {
	name := strings.ToLower(s.name)
	return strings.HasSuffix(name, "@bookticker") ||
		strings.HasSuffix(name, "@ticker") ||
		strings.HasSuffix(name, "@markprice") ||
		strings.HasSuffix(name, "@markprice@1s")
}

// Validate checks the subscription itself.
func (s Subscription) Validate() error {
	if s.invalid != "" {
		return newStreamError(StreamErrorInvalidSubscription, "", 0, 0, fmt.Errorf("%w: %s", ErrInvalidSubscription, s.invalid))
	}
	if s.class != StreamClassPublic && s.class != StreamClassMarket {
		return newStreamError(StreamErrorInvalidSubscription, "", 0, 0, fmt.Errorf("%w: unsupported class %q", ErrInvalidSubscription, s.class))
	}
	if strings.TrimSpace(s.name) == "" || strings.ContainsAny(s.name, " \t\r\n") {
		return newStreamError(StreamErrorInvalidSubscription, "", 0, 0, fmt.Errorf("%w: invalid stream name %q", ErrInvalidSubscription, s.name))
	}
	return nil
}

// ValidateFor checks that the subscription belongs on the requested endpoint.
func (s Subscription) ValidateFor(class StreamClass) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if s.class != class {
		return newStreamError(StreamErrorWrongClass, "", 0, 0, fmt.Errorf("%w: stream %q requires %s, session is %s", ErrWrongStreamClass, s.name, s.class, class))
	}
	return nil
}

// RawSubscription provides an escape hatch for newly introduced Binance
// streams while preserving endpoint-class validation.
func RawSubscription(class StreamClass, name string) Subscription {
	return Subscription{class: class, name: strings.TrimSpace(name)}
}

func normalizedSymbol(symbol string) (string, string) {
	normalized := strings.ToLower(strings.TrimSpace(symbol))
	if normalized == "" {
		return "", "symbol is required"
	}
	return normalized, ""
}

func symbolSubscription(class StreamClass, symbol, suffix string) Subscription {
	normalized, invalid := normalizedSymbol(symbol)
	return Subscription{class: class, name: normalized + suffix, invalid: invalid}
}

// BookTicker subscribes to an individual symbol book ticker on /public.
func BookTicker(symbol string) Subscription {
	return symbolSubscription(StreamClassPublic, symbol, "@bookTicker")
}

// DepthSpeed selects supported Futures depth update speeds.
type DepthSpeed string

const (
	DepthSpeedDefault DepthSpeed = ""
	DepthSpeed100ms   DepthSpeed = "100ms"
	DepthSpeed250ms   DepthSpeed = "250ms"
	DepthSpeed500ms   DepthSpeed = "500ms"
)

// DiffDepth subscribes to diff depth updates on /public.
func DiffDepth(symbol string, speed DepthSpeed) Subscription {
	normalized, invalid := normalizedSymbol(symbol)
	name := normalized + "@depth"
	if speed != DepthSpeedDefault && speed != DepthSpeed250ms {
		name += "@" + string(speed)
	}
	sub := RawSubscription(StreamClassPublic, name)
	if invalid != "" {
		sub.invalid = invalid
	}
	if speed != DepthSpeedDefault && speed != DepthSpeed100ms && speed != DepthSpeed250ms && speed != DepthSpeed500ms {
		sub.invalid = "unsupported depth speed"
	}
	return sub
}

// PartialDepth subscribes to partial depth updates on /public.
func PartialDepth(symbol string, levels int, speed DepthSpeed) Subscription {
	normalized, invalid := normalizedSymbol(symbol)
	name := fmt.Sprintf("%s@depth%d", normalized, levels)
	if speed != DepthSpeedDefault && speed != DepthSpeed250ms {
		name += "@" + string(speed)
	}
	sub := RawSubscription(StreamClassPublic, name)
	if invalid != "" {
		sub.invalid = invalid
	}
	if levels != 5 && levels != 10 && levels != 20 {
		sub.invalid = "partial depth levels must be 5, 10, or 20"
	}
	if speed != DepthSpeedDefault && speed != DepthSpeed100ms && speed != DepthSpeed250ms && speed != DepthSpeed500ms {
		sub.invalid = "unsupported depth speed"
	}
	return sub
}

// AggTrade subscribes to aggregate trade events on /market.
func AggTrade(symbol string) Subscription {
	return symbolSubscription(StreamClassMarket, symbol, "@aggTrade")
}

// MarkPrice subscribes to mark price events on /market. A one-second rate adds
// @1s; zero or three seconds uses Binance's default cadence.
func MarkPrice(symbol string, rate time.Duration) Subscription {
	normalized, invalid := normalizedSymbol(symbol)
	name := normalized + "@markPrice"
	if rate == time.Second {
		name += "@1s"
	}
	sub := RawSubscription(StreamClassMarket, name)
	if invalid != "" {
		sub.invalid = invalid
	}
	if rate != 0 && rate != time.Second && rate != 3*time.Second {
		sub.invalid = "mark price rate must be 1s or 3s"
	}
	return sub
}

// Kline subscribes to candlestick events on /market.
func Kline(symbol, interval string) Subscription {
	normalized, invalid := normalizedSymbol(symbol)
	interval = strings.TrimSpace(interval)
	sub := RawSubscription(StreamClassMarket, normalized+"@kline_"+interval)
	if invalid != "" {
		sub.invalid = invalid
	}
	if interval == "" {
		sub.invalid = "kline interval is required"
	}
	return sub
}

// Ticker subscribes to 24-hour ticker events on /market.
func Ticker(symbol string) Subscription {
	return symbolSubscription(StreamClassMarket, symbol, "@ticker")
}

// MiniTicker subscribes to individual symbol mini-ticker events on /market.
func MiniTicker(symbol string) Subscription {
	return symbolSubscription(StreamClassMarket, symbol, "@miniTicker")
}

// Liquidation subscribes to individual symbol liquidation events on /market.
func Liquidation(symbol string) Subscription {
	return symbolSubscription(StreamClassMarket, symbol, "@forceOrder")
}

// ContractInfo subscribes to all contract information updates on /market.
func ContractInfo() Subscription {
	return RawSubscription(StreamClassMarket, "!contractInfo")
}
