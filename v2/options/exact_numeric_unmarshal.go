package options

import "github.com/btcnash/go-binance/v2/internal/exactjson"

func (v *WsGreek) UnmarshalJSON(data []byte) error {
	type alias WsGreek
	values, err := exactjson.UnmarshalStringFields(data, (*alias)(v), map[string]exactjson.Mode{
		"d": exactjson.StringMode, "t": exactjson.StringMode,
		"g": exactjson.StringMode, "v": exactjson.StringMode,
	})
	if err != nil {
		return err
	}
	v.Delta, v.Theta = values["d"], values["t"]
	v.Gamma, v.Vega = values["g"], values["v"]
	return nil
}
