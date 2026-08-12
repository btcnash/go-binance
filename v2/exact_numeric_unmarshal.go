package binance

import "github.com/btcnash/go-binance/v2/internal/exactjson"

func (v *AssetBalance) UnmarshalJSON(data []byte) error {
	type alias AssetBalance
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"free": exactjson.NumberMode, "locked": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.Free, v.Locked = values["free"], values["locked"]
	return nil
}

func (v *TradeInfoVo) UnmarshalJSON(data []byte) error {
	type alias TradeInfoVo
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"btc": exactjson.NumberMode, "btcFutures": exactjson.NumberMode, "btcMargin": exactjson.NumberMode,
		"busd": exactjson.NumberMode, "busdFutures": exactjson.NumberMode, "busdMargin": exactjson.NumberMode,
	})
	if err != nil {
		return err
	}
	v.Btc, v.BtcFutures, v.BtcMargin = values["btc"], values["btcFutures"], values["btcMargin"]
	v.Busd, v.BusdFutures, v.BusdMargin = values["busd"], values["busdFutures"], values["busdMargin"]
	return nil
}
