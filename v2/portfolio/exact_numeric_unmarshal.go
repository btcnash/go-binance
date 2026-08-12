package portfolio

import "github.com/btcnash/go-binance/v2/internal/exactjson"

func (v *CMBracket) UnmarshalJSON(data []byte) error {
	type alias CMBracket
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"qtyCap": exactjson.NumberMode, "qtyFloor": exactjson.NumberMode,
		"maintMarginRatio": exactjson.NumberMode, "cum": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.QtyCap, v.QtyFloor = values["qtyCap"], values["qtyFloor"]
	v.MaintMarginRatio, v.Cum = values["maintMarginRatio"], values["cum"]
	return nil
}

func (v *NegativeBalanceExchangeDetail) UnmarshalJSON(data []byte) error {
	type alias NegativeBalanceExchangeDetail
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"negativeBalance": exactjson.NumberMode, "negativeMaxThreshold": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.NegativeBalance = values["negativeBalance"]
	v.NegativeMaxThreshold = values["negativeMaxThreshold"]
	return nil
}

func (v *Bracket) UnmarshalJSON(data []byte) error {
	type alias Bracket
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"notionalCap": exactjson.NumberMode, "notionalFloor": exactjson.NumberMode,
		"maintMarginRatio": exactjson.NumberMode, "cum": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.NotionalCap, v.NotionalFloor = values["notionalCap"], values["notionalFloor"]
	v.MaintMarginRatio, v.Cum = values["maintMarginRatio"], values["cum"]
	return nil
}

func (v *Indicator) UnmarshalJSON(data []byte) error {
	type alias Indicator
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"value": exactjson.NumberMode, "triggerValue": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.Value, v.TriggerValue = values["value"], values["triggerValue"]
	return nil
}

func (v *WsPosition) UnmarshalJSON(data []byte) error {
	type alias WsPosition
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{"bep": exactjson.NumberOrStringMode})
	if err != nil {
		return err
	}
	v.BreakEvenPrice = values["bep"]
	return nil
}
