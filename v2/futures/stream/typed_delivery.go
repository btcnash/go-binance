package stream

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// TypedDeliveryMode selects an optional homogeneous managed typed application
// decoder. The zero value preserves the generic StreamEvent behavior.
type TypedDeliveryMode string

const (
	TypedDeliveryDisabled   TypedDeliveryMode = ""
	TypedDeliveryBookTicker TypedDeliveryMode = "book_ticker"
	TypedDeliveryAggTrade   TypedDeliveryMode = "agg_trade"
)

// WsBookTickerEvent is the Binance Futures best-book ticker payload.
type WsBookTickerEvent struct {
	Event           string `json:"e"`
	UpdateID        int64  `json:"u"`
	Time            int64  `json:"E"`
	TransactionTime int64  `json:"T"`
	Symbol          string `json:"s"`
	BestBidPrice    string `json:"b"`
	BestBidQty      string `json:"B"`
	BestAskPrice    string `json:"a"`
	BestAskQty      string `json:"A"`
}

// WsAggTradeEvent is the Binance Futures aggregate-trade payload. Presence
// fields preserve the distinction between an absent field and an explicit
// zero/empty value.
type WsAggTradeEvent struct {
	Event                 string `json:"e"`
	Time                  int64  `json:"E"`
	Symbol                string `json:"s"`
	AggregateTradeID      int64  `json:"a"`
	Price                 string `json:"p"`
	Quantity              string `json:"q"`
	NormalQuantity        string `json:"nq"`
	NormalQuantityPresent bool   `json:"-"`
	FirstTradeID          int64  `json:"f"`
	LastTradeID           int64  `json:"l"`
	TradeTime             int64  `json:"T"`
	Maker                 bool   `json:"m"`
	SymbolType            int    `json:"st"`
	SymbolTypePresent     bool   `json:"-"`
}

type aggTradeWire struct {
	Event            string  `json:"e"`
	Time             int64   `json:"E"`
	Symbol           string  `json:"s"`
	AggregateTradeID int64   `json:"a"`
	Price            string  `json:"p"`
	Quantity         string  `json:"q"`
	NormalQuantity   *string `json:"nq"`
	FirstTradeID     int64   `json:"f"`
	LastTradeID      int64   `json:"l"`
	TradeTime        int64   `json:"T"`
	Maker            bool    `json:"m"`
	SymbolType       *int    `json:"st"`
}

func aggTradeEventFromWire(wire aggTradeWire) WsAggTradeEvent {
	event := WsAggTradeEvent{
		Event:            wire.Event,
		Time:             wire.Time,
		Symbol:           wire.Symbol,
		AggregateTradeID: wire.AggregateTradeID,
		Price:            wire.Price,
		Quantity:         wire.Quantity,
		FirstTradeID:     wire.FirstTradeID,
		LastTradeID:      wire.LastTradeID,
		TradeTime:        wire.TradeTime,
		Maker:            wire.Maker,
	}
	if wire.NormalQuantity != nil {
		event.NormalQuantity = *wire.NormalQuantity
		event.NormalQuantityPresent = true
	}
	if wire.SymbolType != nil {
		event.SymbolType = *wire.SymbolType
		event.SymbolTypePresent = true
	}
	return event
}

// UnmarshalJSON decodes an aggTrade event using the same wire DTO and
// conversion path as managed typed delivery.
func (e *WsAggTradeEvent) UnmarshalJSON(payload []byte) error {
	var wire aggTradeWire
	if err := json.Unmarshal(payload, &wire); err != nil {
		return err
	}
	*e = aggTradeEventFromWire(wire)
	return nil
}

// TypedStreamEvent is emitted by a session configured with TypedDelivery.
// Exactly one typed payload field is meaningful according to Kind. Raw shares
// the immutable managed transport payload backing bytes. DecodeErr is set only
// on the application-decode failure path; successful typed delivery leaves it
// nil and does not materialize generic Data.
type TypedStreamEvent struct {
	Generation uint64
	Stream     string
	Raw        json.RawMessage
	ReceivedAt time.Time
	Kind       TypedDeliveryMode
	DecodeErr  error

	BookTicker WsBookTickerEvent
	AggTrade   WsAggTradeEvent
}

type typedEnvelopeControl struct {
	ID     *uint64         `json:"id"`
	Result json.RawMessage `json:"result"`
	Code   *int            `json:"code"`
	Msg    string          `json:"msg"`
	Stream string          `json:"stream"`
}

type typedBookTickerEnvelope struct {
	typedEnvelopeControl
	Data WsBookTickerEvent `json:"data"`
}

type typedAggTradeEnvelope struct {
	typedEnvelopeControl
	Data aggTradeWire `json:"data"`
}

func (mode TypedDeliveryMode) validateForClass(class StreamClass) error {
	switch mode {
	case TypedDeliveryDisabled:
		return nil
	case TypedDeliveryBookTicker:
		if class != StreamClassPublic {
			return fmt.Errorf("%w: bookTicker typed delivery requires public stream class", ErrInvalidStreamOptions)
		}
	case TypedDeliveryAggTrade:
		if class != StreamClassMarket {
			return fmt.Errorf("%w: aggTrade typed delivery requires market stream class", ErrInvalidStreamOptions)
		}
	default:
		return fmt.Errorf("%w: unsupported typed delivery mode %q", ErrInvalidStreamOptions, mode)
	}
	return nil
}

func (mode TypedDeliveryMode) supportsSubscription(sub Subscription) bool {
	return mode.supportsStreamName(sub.String())
}

func (mode TypedDeliveryMode) supportsStreamName(streamName string) bool {
	name := strings.ToLower(strings.TrimSpace(streamName))
	switch mode {
	case TypedDeliveryDisabled:
		return true
	case TypedDeliveryBookTicker:
		return strings.HasSuffix(name, "@bookticker")
	case TypedDeliveryAggTrade:
		return strings.HasSuffix(name, "@aggtrade")
	default:
		return false
	}
}
