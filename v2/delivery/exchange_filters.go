package delivery

import "encoding/json"

type ExchangeFilter struct {
	FilterType string `json:"filterType,omitempty"`
	Raw        string `json:"-"`
}

func (f *ExchangeFilter) UnmarshalJSON(data []byte) error {
	var h struct {
		FilterType string `json:"filterType"`
	}
	if err := json.Unmarshal(data, &h); err != nil {
		return err
	}
	f.FilterType = h.FilterType
	f.Raw = string(data)
	return nil
}

type Filter struct {
	FilterType        string `json:"filterType"`
	MaxQty            string `json:"maxQty,omitempty"`
	MinQty            string `json:"minQty,omitempty"`
	StepSize          string `json:"stepSize,omitempty"`
	MaxPrice          string `json:"maxPrice,omitempty"`
	MinPrice          string `json:"minPrice,omitempty"`
	TickSize          string `json:"tickSize,omitempty"`
	MultiplierDecimal string `json:"multiplierDecimal,omitempty"`
	MultiplierUp      string `json:"multiplierUp,omitempty"`
	MultiplierDown    string `json:"multiplierDown,omitempty"`
	Limit             int64  `json:"limit,omitempty"`
	Raw               string `json:"-"`
}

func (f *Filter) UnmarshalJSON(data []byte) error {
	var h struct {
		FilterType string `json:"filterType"`
	}
	if err := json.Unmarshal(data, &h); err != nil {
		return err
	}
	f.Raw = string(data)
	switch h.FilterType {
	case string(SymbolFilterTypeLotSize), string(SymbolFilterTypePrice), string(SymbolFilterTypePercentPrice), string(SymbolFilterTypeMarketLotSize), string(SymbolFilterTypeMaxNumOrders), string(SymbolFilterTypeMaxNumAlgoOrders):
		type alias Filter
		var d alias
		if err := json.Unmarshal(data, &d); err != nil {
			return err
		}
		*f = Filter(d)
		f.Raw = string(data)
	default:
		f.FilterType = h.FilterType
	}
	return nil
}
