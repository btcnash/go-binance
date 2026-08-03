package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	shopspring "github.com/shopspring/decimal"
)

var plainDecimalPattern = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]+)?$`)

var (
	ErrDecimalEmpty      = errors.New("decimal text is empty")
	ErrDecimalSyntax     = errors.New("decimal text must use plain base-10 notation")
	ErrDecimalJSONString = errors.New("decimal JSON value must be a string")
	ErrDecimalJSONNull   = errors.New("decimal JSON value cannot be null")
)

// Decimal is an exact base-10 value used by Binance financial fields.
// It never converts through float32 or float64.
type Decimal struct {
	value shopspring.Decimal
}

// ParseDecimal parses plain base-10 text without exponent notation.
func ParseDecimal(text string) (Decimal, error) {
	if text == "" {
		return Decimal{}, ErrDecimalEmpty
	}
	if !plainDecimalPattern.MatchString(text) {
		return Decimal{}, fmt.Errorf("%w: %q", ErrDecimalSyntax, text)
	}
	value, err := shopspring.NewFromString(text)
	if err != nil {
		return Decimal{}, fmt.Errorf("parse decimal %q: %w", text, err)
	}
	return Decimal{value: value}, nil
}

// MustParseDecimal parses text and panics if it is invalid.
func MustParseDecimal(text string) Decimal {
	value, err := ParseDecimal(text)
	if err != nil {
		panic(err)
	}
	return value
}

// CanonicalString returns fixed-point base-10 text with redundant trailing
// fractional zeros removed. It never returns scientific notation.
func (d Decimal) CanonicalString() string {
	return d.value.String()
}

// String implements fmt.Stringer using Binance-safe fixed-point text.
func (d Decimal) String() string {
	return d.CanonicalString()
}

// Cmp compares d and other and returns -1, 0, or 1.
func (d Decimal) Cmp(other Decimal) int {
	return d.value.Cmp(other.value)
}

// IsZero reports whether d is exactly zero.
func (d Decimal) IsZero() bool {
	return d.value.IsZero()
}

// MarshalJSON emits a Binance decimal string rather than a JSON number.
func (d Decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.CanonicalString())
}

// UnmarshalJSON accepts only a non-empty Binance decimal string.
func (d *Decimal) UnmarshalJSON(data []byte) error {
	if d == nil {
		return errors.New("cannot unmarshal Decimal into nil receiver")
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return ErrDecimalJSONNull
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("%w: %s", ErrDecimalJSONString, string(data))
	}
	value, err := ParseDecimal(text)
	if err != nil {
		return err
	}
	*d = value
	return nil
}

// MarshalText emits canonical fixed-point text for form/query encoding.
func (d Decimal) MarshalText() ([]byte, error) {
	return []byte(d.CanonicalString()), nil
}

// UnmarshalText parses plain base-10 text.
func (d *Decimal) UnmarshalText(text []byte) error {
	value, err := ParseDecimal(string(text))
	if err != nil {
		return err
	}
	*d = value
	return nil
}

// OptionalDecimal distinguishes an absent/null/empty value from a present zero.
type OptionalDecimal struct {
	Value Decimal
	Valid bool
}

// NewOptionalDecimal creates a present optional decimal.
func NewOptionalDecimal(value Decimal) OptionalDecimal {
	return OptionalDecimal{Value: value, Valid: true}
}

// MarshalJSON emits null when absent, otherwise a Binance decimal string.
func (d OptionalDecimal) MarshalJSON() ([]byte, error) {
	if !d.Valid {
		return []byte("null"), nil
	}
	return d.Value.MarshalJSON()
}

// UnmarshalJSON treats null and the empty string as absent.
func (d *OptionalDecimal) UnmarshalJSON(data []byte) error {
	if d == nil {
		return errors.New("cannot unmarshal OptionalDecimal into nil receiver")
	}
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte(`""`)) {
		*d = OptionalDecimal{}
		return nil
	}
	var value Decimal
	if err := value.UnmarshalJSON(trimmed); err != nil {
		return err
	}
	*d = NewOptionalDecimal(value)
	return nil
}
