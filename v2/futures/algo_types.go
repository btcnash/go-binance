package futures

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/btcnash/go-binance/v2/common"
)

// Decimal is the exact base-10 type used by Algo financial fields.
type Decimal = common.Decimal

// OptionalDecimal distinguishes an absent/null/empty decimal from a present zero.
type OptionalDecimal = common.OptionalDecimal

func ParseDecimal(text string) (Decimal, error) { return common.ParseDecimal(text) }
func MustParseDecimal(text string) Decimal      { return common.MustParseDecimal(text) }
func NewOptionalDecimal(value Decimal) OptionalDecimal {
	return common.NewOptionalDecimal(value)
}

// OrderID is a Binance orderId with int64 width.
type OrderID int64

// AlgoOrderID is a Binance algoId with int64 width.
type AlgoOrderID int64

var (
	ErrOrderIDInvalid     = errors.New("orderId must be greater than zero")
	ErrAlgoOrderIDInvalid = errors.New("algoId must be greater than zero")
)

func (id OrderID) Valid() bool     { return id > 0 }
func (id AlgoOrderID) Valid() bool { return id > 0 }

func (id OrderID) MarshalJSON() ([]byte, error) {
	if !id.Valid() {
		return nil, ErrOrderIDInvalid
	}
	return []byte(strconv.FormatInt(int64(id), 10)), nil
}

func (id *OrderID) UnmarshalJSON(data []byte) error {
	value, err := parsePositiveJSONID(data, "orderId", ErrOrderIDInvalid)
	if err != nil {
		return err
	}
	*id = OrderID(value)
	return nil
}

func (id AlgoOrderID) MarshalJSON() ([]byte, error) {
	if !id.Valid() {
		return nil, ErrAlgoOrderIDInvalid
	}
	return []byte(strconv.FormatInt(int64(id), 10)), nil
}

func (id *AlgoOrderID) UnmarshalJSON(data []byte) error {
	value, err := parsePositiveJSONID(data, "algoId", ErrAlgoOrderIDInvalid)
	if err != nil {
		return err
	}
	*id = AlgoOrderID(value)
	return nil
}

func parsePositiveJSONID(data []byte, name string, invalidErr error) (int64, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] == '"' {
		return 0, fmt.Errorf("%s must be a positive JSON integer", name)
	}
	value, err := strconv.ParseInt(string(trimmed), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if value <= 0 {
		return 0, invalidErr
	}
	return value, nil
}

// OptionalOrderID maps actualOrderId: null and "" are absent; a positive
// decimal string is present. Binance encodes actualOrderId as a JSON string.
type OptionalOrderID struct {
	Value OrderID
	Valid bool
}

func NewOptionalOrderID(value OrderID) (OptionalOrderID, error) {
	if !value.Valid() {
		return OptionalOrderID{}, ErrOrderIDInvalid
	}
	return OptionalOrderID{Value: value, Valid: true}, nil
}

func (id OptionalOrderID) MarshalJSON() ([]byte, error) {
	if !id.Valid {
		return []byte("null"), nil
	}
	if !id.Value.Valid() {
		return nil, ErrOrderIDInvalid
	}
	return json.Marshal(strconv.FormatInt(int64(id.Value), 10))
}

func (id *OptionalOrderID) UnmarshalJSON(data []byte) error {
	if id == nil {
		return errors.New("cannot unmarshal OptionalOrderID into nil receiver")
	}
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte(`""`)) {
		*id = OptionalOrderID{}
		return nil
	}
	var text string
	if err := json.Unmarshal(trimmed, &text); err != nil {
		return fmt.Errorf("actualOrderId must be a decimal string, null, or empty string: %s", string(data))
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("parse actualOrderId %q: %w", text, err)
	}
	optional, err := NewOptionalOrderID(OrderID(value))
	if err != nil {
		return err
	}
	*id = optional
	return nil
}
