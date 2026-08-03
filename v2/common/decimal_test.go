package common

import (
	"encoding/json"
	"testing"
)

func TestDecimalParseCanonicalAndJSON(t *testing.T) {
	value, err := ParseDecimal("12345678901234567890.00123000")
	if err != nil {
		t.Fatalf("ParseDecimal() error = %v", err)
	}
	if got := value.CanonicalString(); got != "12345678901234567890.00123" {
		t.Fatalf("CanonicalString() = %q", got)
	}

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if got := string(raw); got != `"12345678901234567890.00123"` {
		t.Fatalf("Marshal() = %s", got)
	}

	var decoded Decimal
	if err := json.Unmarshal([]byte(`"0.00000000000000000001"`), &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got := decoded.CanonicalString(); got != "0.00000000000000000001" {
		t.Fatalf("decoded CanonicalString() = %q", got)
	}
	if decoded.IsZero() {
		t.Fatal("decoded value unexpectedly zero")
	}
	if decoded.Cmp(MustParseDecimal("0")) <= 0 {
		t.Fatal("decoded value must compare greater than zero")
	}
}

func TestDecimalRejectsNonPlainDecimalText(t *testing.T) {
	for _, input := range []string{"", " ", "NaN", "Inf", "-Inf", "1e3", ".1", "1.", "+1", "1 2"} {
		t.Run(input, func(t *testing.T) {
			if _, err := ParseDecimal(input); err == nil {
				t.Fatalf("ParseDecimal(%q) error = nil", input)
			}
		})
	}

	var value Decimal
	if err := json.Unmarshal([]byte(`1.25`), &value); err == nil {
		t.Fatal("numeric JSON must be rejected; Binance decimals are strings")
	}
	if err := json.Unmarshal([]byte(`null`), &value); err == nil {
		t.Fatal("null must be rejected for required Decimal")
	}
}

func TestOptionalDecimalDistinguishesMissingFromZero(t *testing.T) {
	for _, raw := range []string{`null`, `""`} {
		var value OptionalDecimal
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", raw, err)
		}
		if value.Valid {
			t.Fatalf("Unmarshal(%s) Valid = true", raw)
		}
	}

	var zero OptionalDecimal
	if err := json.Unmarshal([]byte(`"0.000"`), &zero); err != nil {
		t.Fatalf("Unmarshal(zero) error = %v", err)
	}
	if !zero.Valid || !zero.Value.IsZero() {
		t.Fatalf("zero = %#v", zero)
	}

	raw, err := json.Marshal(zero)
	if err != nil {
		t.Fatalf("Marshal(zero) error = %v", err)
	}
	if got := string(raw); got != `"0"` {
		t.Fatalf("Marshal(zero) = %s", got)
	}

	raw, err = json.Marshal(OptionalDecimal{})
	if err != nil {
		t.Fatalf("Marshal(invalid) error = %v", err)
	}
	if got := string(raw); got != `null` {
		t.Fatalf("Marshal(invalid) = %s", got)
	}
}
