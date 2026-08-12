package delivery

import (
	"context"
	"encoding/json"
	"net/http"
)

// ExchangeInfoService exchange info service
type ExchangeInfoService struct {
	c *Client
}

// Do send request
func (s *ExchangeInfoService) Do(ctx context.Context, opts ...RequestOption) (res *ExchangeInfo, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/dapi/v1/exchangeInfo",
		secType:  secTypeNone,
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(ExchangeInfo)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ExchangeInfo exchange info
type ExchangeInfo struct {
	Timezone        string           `json:"timezone"`
	ServerTime      int64            `json:"serverTime"`
	RateLimits      []RateLimit      `json:"rateLimits"`
	ExchangeFilters []ExchangeFilter `json:"exchangeFilters"`
	Symbols         []Symbol         `json:"symbols"`
}

// RateLimit struct
type RateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int64  `json:"intervalNum"`
	Limit         int64  `json:"limit"`
}

// Symbol market symbol
type Symbol struct {
	OrderType             []OrderType       `json:"OrderType"`
	TimeInForce           []TimeInForceType `json:"timeInForce"`
	Filters               []Filter          `json:"filters"`
	Symbol                string            `json:"symbol"`
	Pair                  string            `json:"pair"`
	ContractType          string            `json:"contractType"`
	DeliveryDate          int64             `json:"deliveryDate"`
	OnboardDate           int64             `json:"onboardDate"`
	ContractStatus        string            `json:"contractStatus"`
	ContractSize          int               `json:"contractSize"`
	PricePrecision        int               `json:"pricePrecision"`
	QuantityPrecision     int               `json:"quantityPrecision"`
	MaintMarginPercent    string            `json:"maintMarginPercent"`
	RequiredMarginPercent string            `json:"requiredMarginPercent"`
	QuoteAsset            string            `json:"quoteAsset"`
	BaseAsset             string            `json:"baseAsset"`
	MarginAsset           string            `json:"marginAsset"`
	BaseAssetPrecision    int               `json:"baseAssetPrecision"`
	QuotePrecision        int               `json:"quotePrecision"`
	EqualQtyPrecision     int               `json:"equalQtyPrecision"`
	TriggerProtect        string            `json:"triggerProtect"`
	UnderlyingType        string            `json:"underlyingType"`
	UnderlyingSubType     []string          `json:"underlyingSubType"`
}

// LotSizeFilter define lot size filter of symbol
type LotSizeFilter struct {
	MaxQuantity string `json:"maxQty"`
	MinQuantity string `json:"minQty"`
	StepSize    string `json:"stepSize"`
}

// PriceFilter define price filter of symbol
type PriceFilter struct {
	MaxPrice string `json:"maxPrice"`
	MinPrice string `json:"minPrice"`
	TickSize string `json:"tickSize"`
}

// PercentPriceFilter define percent price filter of symbol
type PercentPriceFilter struct {
	MultiplierDecimal string `json:"multiplierDecimal"`
	MultiplierUp      string `json:"multiplierUp"`
	MultiplierDown    string `json:"multiplierDown"`
}

// MarketLotSizeFilter define market lot size filter of symbol
type MarketLotSizeFilter struct {
	MaxQuantity string `json:"maxQty"`
	MinQuantity string `json:"minQty"`
	StepSize    string `json:"stepSize"`
}

// MaxNumOrdersFilter define max num orders filter of symbol
type MaxNumOrdersFilter struct {
	Limit int64 `json:"limit"`
}

// MaxNumAlgoOrdersFilter define max num orders filter of symbol of algo
type MaxNumAlgoOrdersFilter struct {
	Limit int64 `json:"limit"`
}

// LotSizeFilter return lot size filter of symbol
func (s *Symbol) LotSizeFilter() *LotSizeFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeLotSize) {
			return &LotSizeFilter{MaxQuantity: f.MaxQty, MinQuantity: f.MinQty, StepSize: f.StepSize}
		}
	}
	return nil
}
func (s *Symbol) PriceFilter() *PriceFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypePrice) {
			return &PriceFilter{MaxPrice: f.MaxPrice, MinPrice: f.MinPrice, TickSize: f.TickSize}
		}
	}
	return nil
}
func (s *Symbol) PercentPriceFilter() *PercentPriceFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypePercentPrice) {
			return &PercentPriceFilter{MultiplierDecimal: f.MultiplierDecimal, MultiplierUp: f.MultiplierUp, MultiplierDown: f.MultiplierDown}
		}
	}
	return nil
}
func (s *Symbol) MarketLotSizeFilter() *MarketLotSizeFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeMarketLotSize) {
			return &MarketLotSizeFilter{MaxQuantity: f.MaxQty, MinQuantity: f.MinQty, StepSize: f.StepSize}
		}
	}
	return nil
}
func (s *Symbol) MaxNumOrdersFilter() *MaxNumOrdersFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeMaxNumOrders) {
			return &MaxNumOrdersFilter{Limit: f.Limit}
		}
	}
	return nil
}
func (s *Symbol) MaxNumAlgoOrdersFilter() *MaxNumAlgoOrdersFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeMaxNumAlgoOrders) {
			return &MaxNumAlgoOrdersFilter{Limit: f.Limit}
		}
	}
	return nil
}
