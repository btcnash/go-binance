package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// PriceLevel is a common structure for bids and asks in the
// order book.
type PriceLevel struct {
	Price    string
	Quantity string
}

// UnmarshalJSON decodes Binance price levels, which are represented on the
// wire as arrays containing price and quantity. Object decoding remains
// supported for compatibility with callers that used the struct form.
func (p *PriceLevel) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	if len(data) == 0 {
		return fmt.Errorf("common.PriceLevel: empty JSON")
	}

	if data[0] == '{' {
		type object PriceLevel
		var value object
		if err := json.Unmarshal(data, &value); err != nil {
			return fmt.Errorf("common.PriceLevel: decode object: %w", err)
		}
		*p = PriceLevel(value)
		return nil
	}

	var values []json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil {
		return fmt.Errorf("common.PriceLevel: decode array: %w", err)
	}
	if len(values) < 2 {
		return fmt.Errorf("common.PriceLevel: expected at least 2 array elements, got %d", len(values))
	}

	var price, quantity string
	if err := json.Unmarshal(values[0], &price); err != nil {
		return fmt.Errorf("common.PriceLevel: decode price: %w", err)
	}
	if err := json.Unmarshal(values[1], &quantity); err != nil {
		return fmt.Errorf("common.PriceLevel: decode quantity: %w", err)
	}
	p.Price = price
	p.Quantity = quantity
	return nil
}

// Parse parses this PriceLevel's Price and Quantity and
// returns them both.  It also returns an error if either
// fails to parse.
func (p *PriceLevel) Parse() (float64, float64, error) {
	price, err := strconv.ParseFloat(p.Price, 64)
	if err != nil {
		return 0, 0, err
	}
	quantity, err := strconv.ParseFloat(p.Quantity, 64)
	if err != nil {
		return price, 0, err
	}
	return price, quantity, nil
}
