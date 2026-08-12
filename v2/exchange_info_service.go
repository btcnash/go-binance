package binance

import (
	"context"
	"encoding/json"
	"net/http"
)

// ExchangeInfoService exchange info service
type ExchangeInfoService struct {
	c                  *Client
	symbol             string
	symbols            []string
	permissions        []string
	showPermissionSets *bool
}

// Symbol set symbol
func (s *ExchangeInfoService) Symbol(symbol string) *ExchangeInfoService {
	s.symbol = symbol
	return s
}

// Symbols set symbol
func (s *ExchangeInfoService) Symbols(symbols ...string) *ExchangeInfoService {
	s.symbols = symbols

	return s
}

// Permissions set permission
func (s *ExchangeInfoService) Permissions(permissions ...string) *ExchangeInfoService {
	s.permissions = permissions

	return s
}

// ShowPermissionSets set showPermissionSets
func (s *ExchangeInfoService) ShowPermissionSets(showPermissionSets *bool) *ExchangeInfoService {
	s.showPermissionSets = showPermissionSets

	return s
}

// Do send request
func (s *ExchangeInfoService) Do(ctx context.Context, opts ...RequestOption) (res *ExchangeInfo, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/api/v3/exchangeInfo",
		secType:  secTypeNone,
	}
	m := params{}
	if s.symbol != "" {
		m["symbol"] = s.symbol
	}
	if len(s.symbols) != 0 {
		m["symbols"] = s.symbols
	}
	if len(s.permissions) != 0 {
		m["permissions"] = s.permissions
	}
	if s.showPermissionSets != nil {
		m["showPermissionSets"] = *s.showPermissionSets
	}
	r.setParams(m)
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
	Symbol                     string     `json:"symbol"`
	Status                     string     `json:"status"`
	BaseAsset                  string     `json:"baseAsset"`
	BaseAssetPrecision         int        `json:"baseAssetPrecision"`
	QuoteAsset                 string     `json:"quoteAsset"`
	QuotePrecision             int        `json:"quotePrecision"`
	QuoteAssetPrecision        int        `json:"quoteAssetPrecision"`
	BaseCommissionPrecision    int32      `json:"baseCommissionPrecision"`
	QuoteCommissionPrecision   int32      `json:"quoteCommissionPrecision"`
	OrderTypes                 []string   `json:"orderTypes"`
	IcebergAllowed             bool       `json:"icebergAllowed"`
	OcoAllowed                 bool       `json:"ocoAllowed"`
	QuoteOrderQtyMarketAllowed bool       `json:"quoteOrderQtyMarketAllowed"`
	IsSpotTradingAllowed       bool       `json:"isSpotTradingAllowed"`
	IsMarginTradingAllowed     bool       `json:"isMarginTradingAllowed"`
	Filters                    []Filter   `json:"filters"`
	Permissions                []string   `json:"permissions"`
	PermissionSets             [][]string `json:"permissionSets"`
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

// PERCENT_PRICE_BY_SIDE define percent price filter of symbol by side
type PercentPriceBySideFilter struct {
	AveragePriceMins  int    `json:"avgPriceMins"`
	BidMultiplierUp   string `json:"bidMultiplierUp"`
	BidMultiplierDown string `json:"bidMultiplierDown"`
	AskMultiplierUp   string `json:"askMultiplierUp"`
	AskMultiplierDown string `json:"askMultiplierDown"`
}

// NotionalFilter define notional filter of symbol
type NotionalFilter struct {
	MinNotional      string `json:"minNotional"`
	ApplyMinToMarket bool   `json:"applyMinToMarket"`
	MaxNotional      string `json:"maxNotional"`
	ApplyMaxToMarket bool   `json:"applyMaxToMarket"`
	AvgPriceMins     int    `json:"avgPriceMins"`
}

// IcebergPartsFilter define iceberg part filter of symbol
type IcebergPartsFilter struct {
	Limit int `json:"limit"`
}

// MarketLotSizeFilter define market lot size filter of symbol
type MarketLotSizeFilter struct {
	MaxQuantity string `json:"maxQty"`
	MinQuantity string `json:"minQty"`
	StepSize    string `json:"stepSize"`
}

// Spot trading supports tracking stop orders
// Tracking stop loss sets an automatic trigger price based on market price using a new parameter trailingDelta
type TrailingDeltaFilter struct {
	MinTrailingAboveDelta int `json:"minTrailingAboveDelta"`
	MaxTrailingAboveDelta int `json:"maxTrailingAboveDelta"`
	MinTrailingBelowDelta int `json:"minTrailingBelowDelta"`
	MaxTrailingBelowDelta int `json:"maxTrailingBelowDelta"`
}

// The "Algo" order is STOP_ LOSS, STOP_ LOS_ LIMITED, TAKE_ PROFIT and TAKE_ PROFIT_ Limit Stop Loss Order.
// Therefore, orders other than the above types are non conditional(Algo) orders, and MaxNumOrders defines the maximum
// number of orders placed for these types of orders
type MaxNumOrdersFilter struct {
	MaxNumOrders int `json:"maxNumOrders"`
}

// MaxNumAlgoOrdersFilter define max num algo orders filter of symbol
type MaxNumAlgoOrdersFilter struct {
	MaxNumAlgoOrders int `json:"maxNumAlgoOrders"`
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
		if f.FilterType == string(SymbolFilterTypePriceFilter) {
			return &PriceFilter{MaxPrice: f.MaxPrice, MinPrice: f.MinPrice, TickSize: f.TickSize}
		}
	}
	return nil
}

func (s *Symbol) PercentPriceBySideFilter() *PercentPriceBySideFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypePercentPriceBySide) {
			return &PercentPriceBySideFilter{AveragePriceMins: f.AvgPriceMins, BidMultiplierUp: f.BidMultiplierUp, BidMultiplierDown: f.BidMultiplierDown, AskMultiplierUp: f.AskMultiplierUp, AskMultiplierDown: f.AskMultiplierDown}
		}
	}
	return nil
}

func (s *Symbol) NotionalFilter() *NotionalFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeNotional) {
			return &NotionalFilter{MinNotional: f.MinNotional, ApplyMinToMarket: f.ApplyMinToMarket, MaxNotional: f.MaxNotional, ApplyMaxToMarket: f.ApplyMaxToMarket, AvgPriceMins: f.AvgPriceMins}
		}
	}
	return nil
}

func (s *Symbol) IcebergPartsFilter() *IcebergPartsFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeIcebergParts) {
			return &IcebergPartsFilter{Limit: f.Limit}
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
			return &MaxNumOrdersFilter{MaxNumOrders: f.MaxNumOrders}
		}
	}
	return nil
}

func (s *Symbol) MaxNumAlgoOrdersFilter() *MaxNumAlgoOrdersFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeMaxNumAlgoOrders) {
			return &MaxNumAlgoOrdersFilter{MaxNumAlgoOrders: f.MaxNumAlgoOrders}
		}
	}
	return nil
}

func (s *Symbol) TrailingDeltaFilter() *TrailingDeltaFilter {
	for _, f := range s.Filters {
		if f.FilterType == string(SymbolFilterTypeTrailingDelta) {
			return &TrailingDeltaFilter{MinTrailingAboveDelta: f.MinTrailingAboveDelta, MaxTrailingAboveDelta: f.MaxTrailingAboveDelta, MinTrailingBelowDelta: f.MinTrailingBelowDelta, MaxTrailingBelowDelta: f.MaxTrailingBelowDelta}
		}
	}
	return nil
}
