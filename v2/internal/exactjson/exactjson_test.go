package exactjson

import (
	"encoding/json"
	"testing"
)

func TestNumberStringPreservesExactToken(t *testing.T) {
	for _, raw := range []string{
		"9.324", "123456789.123456789", "0.1234567890123456789",
		"-0.000000000000000001", "5000", "0", "-0", "1e-18", "1E+18", "9007199254740993",
	} {
		t.Run(raw, func(t *testing.T) {
			got, err := NumberString(json.RawMessage(raw))
			if err != nil {
				t.Fatalf("NumberString(%q) error = %v", raw, err)
			}
			if got != raw {
				t.Fatalf("NumberString(%q) = %q, want exact token", raw, got)
			}
		})
	}
}

func TestStrictModes(t *testing.T) {
	got, err := String(json.RawMessage(`"9.324"`))
	if err != nil || got != "9.324" {
		t.Fatalf("String() = %q, %v", got, err)
	}
	if _, err := String(json.RawMessage(`9.324`)); err == nil {
		t.Fatal("String accepted bare number")
	}
	if _, err := NumberString(json.RawMessage(`"9.324"`)); err == nil {
		t.Fatal("NumberString accepted quoted string")
	}
	if _, err := String(json.RawMessage(`"9.324" true`)); err == nil {
		t.Fatal("String accepted trailing JSON token")
	}
	if _, err := NumberOrString(json.RawMessage(`"9.324" true`)); err == nil {
		t.Fatal("NumberOrString accepted trailing JSON token")
	}
	for _, raw := range []string{"null", "true", "[]", "{}", "9.3 4", "9.3 true", "9.3 garbage"} {
		if _, err := NumberString(json.RawMessage(raw)); err == nil {
			t.Fatalf("NumberString(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestNumberOrStringAndOptional(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{{`9.324`, "9.324"}, {`"9.324"`, "9.324"}, {`9007199254740993`, "9007199254740993"}} {
		got, err := NumberOrString(json.RawMessage(tc.raw))
		if err != nil || got != tc.want {
			t.Fatalf("NumberOrString(%q) = %q, %v; want %q", tc.raw, got, err, tc.want)
		}
	}
	got, err := OptionalNumberOrString(json.RawMessage(`null`))
	if err != nil || got != nil {
		t.Fatalf("OptionalNumberOrString(null) = %v, %v; want nil", got, err)
	}
}
