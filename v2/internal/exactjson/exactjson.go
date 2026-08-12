package exactjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Mode defines the exact JSON wire representation accepted for a public string field.
type Mode uint8

const (
	StringMode Mode = iota
	NumberMode
	NumberOrStringMode
)

// String accepts exactly one JSON string token and returns its decoded contents.
func String(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '"' {
		return "", fmt.Errorf("exactjson: expected JSON string")
	}
	var value string
	if err := decodeOne(raw, &value, false); err != nil {
		return "", err
	}
	return value, nil
}

// NumberString accepts exactly one JSON number token and returns its original lexical value.
func NumberString(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] == '"' {
		return "", fmt.Errorf("exactjson: expected JSON number")
	}
	var value json.Number
	if err := decodeOne(raw, &value, true); err != nil {
		return "", err
	}
	if value.String() == "" {
		return "", fmt.Errorf("exactjson: expected JSON number")
	}
	return value.String(), nil
}

// NumberOrString accepts exactly one JSON number or string token and preserves numeric text exactly.
func NumberOrString(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return "", fmt.Errorf("exactjson: empty JSON token")
	}
	if raw[0] == '"' {
		return String(raw)
	}
	return NumberString(raw)
}

// OptionalNumberOrString accepts null or one JSON number/string token.
func OptionalNumberOrString(raw json.RawMessage) (*string, error) {
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	value, err := NumberOrString(raw)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// UnmarshalStringFields decodes an object while routing selected fields through exact string/number policies.
// Selected fields are removed before normal json.Unmarshal so numeric wire values never pass through float.
func UnmarshalStringFields(data []byte, dst any, fields map[string]Mode) (map[string]string, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	values := make(map[string]string, len(fields))
	for name, mode := range fields {
		raw, ok := object[name]
		if !ok {
			continue
		}
		var (
			value string
			err   error
		)
		switch mode {
		case StringMode:
			value, err = String(raw)
		case NumberMode:
			value, err = NumberString(raw)
		case NumberOrStringMode:
			value, err = NumberOrString(raw)
		default:
			err = fmt.Errorf("exactjson: unsupported mode %d", mode)
		}
		if err != nil {
			return nil, fmt.Errorf("exactjson: field %q: %w", name, err)
		}
		values[name] = value
		delete(object, name)
	}
	rest, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rest, dst); err != nil {
		return nil, err
	}
	return values, nil
}

func decodeOne(raw []byte, dst any, useNumber bool) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if useNumber {
		dec.UseNumber()
	}
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("exactjson: decode: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("exactjson: trailing JSON token")
		}
		return fmt.Errorf("exactjson: trailing data: %w", err)
	}
	return nil
}
