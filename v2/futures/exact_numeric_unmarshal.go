package futures

import (
	"github.com/btcnash/go-binance/v2/internal/exactjson"
)

func (v *IndicatorInfo) UnmarshalJSON(data []byte) error {
	type alias IndicatorInfo
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"value": exactjson.NumberMode, "triggerValue": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.Value, v.TriggerValue = values["value"], values["triggerValue"]
	return nil
}

func (v *DeliveryPrice) UnmarshalJSON(data []byte) error {
	type alias DeliveryPrice
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{"deliveryPrice": exactjson.NumberMode})
	if err != nil {
		return err
	}
	v.DeliveryPrice = values["deliveryPrice"]
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
	v.NotionalCap = values["notionalCap"]
	v.NotionalFloor = values["notionalFloor"]
	v.MaintMarginRatio = values["maintMarginRatio"]
	v.Cum = values["cum"]
	return nil
}

func (v *WsBLVTInfoEvent) UnmarshalJSON(data []byte) error {
	type alias WsBLVTInfoEvent
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"m": exactjson.NumberOrStringMode, "n": exactjson.NumberOrStringMode,
		"l": exactjson.NumberOrStringMode, "f": exactjson.NumberOrStringMode,
	})
	if err != nil {
		return err
	}
	v.Issued, v.Nav = values["m"], values["n"]
	v.Leverage, v.FundingRate = values["l"], values["f"]
	return nil
}

func (v *WsContractInfoBracket) UnmarshalJSON(data []byte) error {
	type alias WsContractInfoBracket
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"bnf": exactjson.NumberMode, "bnc": exactjson.NumberMode,
		"mmr": exactjson.NumberMode, "cf": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.NotionalFloor, v.NotionalCap = values["bnf"], values["bnc"]
	v.MaintMarginRatio, v.Cum = values["mmr"], values["cf"]
	return nil
}
