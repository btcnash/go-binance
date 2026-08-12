package binance

import "encoding/json"

// ExchangeFilter preserves exchange-level filters without decoding unknown numeric facts through float.
type ExchangeFilter struct {
	FilterType string `json:"filterType,omitempty"`
	Raw        string `json:"-"`
}

func (f *ExchangeFilter) UnmarshalJSON(data []byte) error {
	var head struct {
		FilterType string `json:"filterType"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return err
	}
	f.FilterType, f.Raw = head.FilterType, string(data)
	return nil
}

// Filter is the typed Spot symbol-filter union. Unknown filter types are retained only as raw JSON.
type Filter struct {
	FilterType            string `json:"filterType"`
	MaxQty                string `json:"maxQty,omitempty"`
	MinQty                string `json:"minQty,omitempty"`
	StepSize              string `json:"stepSize,omitempty"`
	MaxPrice              string `json:"maxPrice,omitempty"`
	MinPrice              string `json:"minPrice,omitempty"`
	TickSize              string `json:"tickSize,omitempty"`
	AvgPriceMins          int    `json:"avgPriceMins,omitempty"`
	BidMultiplierUp       string `json:"bidMultiplierUp,omitempty"`
	BidMultiplierDown     string `json:"bidMultiplierDown,omitempty"`
	AskMultiplierUp       string `json:"askMultiplierUp,omitempty"`
	AskMultiplierDown     string `json:"askMultiplierDown,omitempty"`
	MinNotional           string `json:"minNotional,omitempty"`
	ApplyMinToMarket      bool   `json:"applyMinToMarket,omitempty"`
	MaxNotional           string `json:"maxNotional,omitempty"`
	ApplyMaxToMarket      bool   `json:"applyMaxToMarket,omitempty"`
	Limit                 int    `json:"limit,omitempty"`
	MaxNumOrders          int    `json:"maxNumOrders,omitempty"`
	MaxNumAlgoOrders      int    `json:"maxNumAlgoOrders,omitempty"`
	MinTrailingAboveDelta int    `json:"minTrailingAboveDelta,omitempty"`
	MaxTrailingAboveDelta int    `json:"maxTrailingAboveDelta,omitempty"`
	MinTrailingBelowDelta int    `json:"minTrailingBelowDelta,omitempty"`
	MaxTrailingBelowDelta int    `json:"maxTrailingBelowDelta,omitempty"`
	Raw                   string `json:"-"`
}

func (f *Filter) UnmarshalJSON(data []byte) error {
	var head struct {
		FilterType string `json:"filterType"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return err
	}
	f.Raw = string(data)
	switch head.FilterType {
	case string(SymbolFilterTypeLotSize), string(SymbolFilterTypePriceFilter), string(SymbolFilterTypePercentPriceBySide),
		string(SymbolFilterTypeNotional), string(SymbolFilterTypeIcebergParts), string(SymbolFilterTypeMarketLotSize),
		string(SymbolFilterTypeMaxNumOrders), string(SymbolFilterTypeMaxNumAlgoOrders), string(SymbolFilterTypeTrailingDelta):
		type alias Filter
		var decoded alias
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		*f = Filter(decoded)
		f.Raw = string(data)
	default:
		f.FilterType = head.FilterType
	}
	return nil
}
