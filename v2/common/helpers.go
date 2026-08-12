package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// AmountToLotSize convert amount to lot size
func AmountToLotSize(amount, minQty, stepSize string, precision int) string {
	amountDec := decimal.RequireFromString(amount)
	minQtyDec := decimal.RequireFromString(minQty)
	baseAmountDec := amountDec.Sub(minQtyDec)
	if baseAmountDec.LessThan(decimal.Zero) {
		return "0"
	}
	stepSizeDec := decimal.RequireFromString(stepSize)
	baseAmountDec = baseAmountDec.Div(stepSizeDec).Truncate(0).Mul(stepSizeDec)
	return baseAmountDec.Add(minQtyDec).Truncate(int32(precision)).String()
}

// ToJSONList convert v to json list if v is a map
func ToJSONList(v []byte) []byte {
	if len(v) > 0 && v[0] == '{' {
		var b bytes.Buffer
		b.Write([]byte("["))
		b.Write(v)
		b.Write([]byte("]"))
		return b.Bytes()
	}
	return v
}

func ToInt(digit any) (int, error) {
	switch v := digit.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case json.Number:
		n, err := strconv.ParseInt(v.String(), 10, 0)
		return int(n), err
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("unexpected digit: %v", digit)
	}
}

func ToInt64(digit any) (int64, error) {
	switch v := digit.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case json.Number:
		return strconv.ParseInt(v.String(), 10, 64)
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected digit: %v", digit)
	}
}

const (
	SPOT_ORDER_PREFIX     = "x-TKT5PX2F"
	CONTRACT_ORDER_PREFIX = "x-cvBPrNm9"
)

func BaseUID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func Uuid22() string {
	return BaseUID()[:22]
}

func GenerateSpotId() string {
	return SPOT_ORDER_PREFIX + Uuid22()
}

func GenerateSwapId() string {
	return CONTRACT_ORDER_PREFIX + Uuid22()
}
