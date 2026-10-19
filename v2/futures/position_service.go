package futures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/btcnash/go-binance/v2/common"
	"net/http"
)

// ChangeLeverageService change user's initial leverage of specific symbol market
type ChangeLeverageService struct {
	c        *Client
	symbol   string
	leverage int
}

// Symbol set symbol
func (s *ChangeLeverageService) Symbol(symbol string) *ChangeLeverageService {
	s.symbol = symbol
	return s
}

// Leverage set leverage
func (s *ChangeLeverageService) Leverage(leverage int) *ChangeLeverageService {
	s.leverage = leverage
	return s
}

// Do send request
func (s *ChangeLeverageService) Do(ctx context.Context, opts ...RequestOption) (res *SymbolLeverage, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/leverage",
		secType:  secTypeSigned,
	}
	r.setFormParams(params{
		"symbol":   s.symbol,
		"leverage": s.leverage,
	})
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = new(SymbolLeverage)
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	if res == nil || strings.TrimSpace(res.Symbol) == "" || !strings.EqualFold(res.Symbol, s.symbol) || res.Leverage <= 0 {
		return nil, ErrInvalidAccountConfigurationResponse
	}
	return res, nil
}

// SymbolLeverage define leverage info of symbol
type SymbolLeverage struct {
	Leverage         int    `json:"leverage"`
	MaxNotionalValue string `json:"maxNotionalValue"`
	Symbol           string `json:"symbol"`
}

// ChangeMarginTypeService change user's margin type of specific symbol market
type ChangeMarginTypeService struct {
	c          *Client
	symbol     string
	marginType MarginType
}

// Symbol set symbol
func (s *ChangeMarginTypeService) Symbol(symbol string) *ChangeMarginTypeService {
	s.symbol = symbol
	return s
}

// MarginType set margin type
func (s *ChangeMarginTypeService) MarginType(marginType MarginType) *ChangeMarginTypeService {
	s.marginType = marginType
	return s
}

// Do send request
func (s *ChangeMarginTypeService) Do(ctx context.Context, opts ...RequestOption) (err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/marginType",
		secType:  secTypeSigned,
	}
	r.setFormParams(params{
		"symbol":     s.symbol,
		"marginType": s.marginType,
	})
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return err
	}
	return validateAccountConfigurationSuccess(data)
}

// UpdatePositionMarginService update isolated position margin
type UpdatePositionMarginService struct {
	c            *Client
	symbol       string
	positionSide *PositionSideType
	amount       string
	actionType   int
}

// Symbol set symbol
func (s *UpdatePositionMarginService) Symbol(symbol string) *UpdatePositionMarginService {
	s.symbol = symbol
	return s
}

// Side set side
func (s *UpdatePositionMarginService) PositionSide(positionSide PositionSideType) *UpdatePositionMarginService {
	s.positionSide = &positionSide
	return s
}

// Amount set position margin amount
func (s *UpdatePositionMarginService) Amount(amount string) *UpdatePositionMarginService {
	s.amount = amount
	return s
}

// Type set action type: 1: Add position margin，2: Reduce position margin
func (s *UpdatePositionMarginService) Type(actionType int) *UpdatePositionMarginService {
	s.actionType = actionType
	return s
}

// Do send request
func (s *UpdatePositionMarginService) Do(ctx context.Context, opts ...RequestOption) (err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/positionMargin",
		secType:  secTypeSigned,
	}
	m := params{
		"symbol": s.symbol,
		"amount": s.amount,
		"type":   s.actionType,
	}
	if s.positionSide != nil {
		m["positionSide"] = *s.positionSide
	}
	r.setFormParams(m)

	_, _, err = s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return err
	}
	return nil
}

// ChangePositionModeService change user's position mode
type ChangePositionModeService struct {
	c        *Client
	dualSide bool
}

// Change user's position mode: true - Hedge Mode, false - One-way Mode
func (s *ChangePositionModeService) DualSide(dualSide bool) *ChangePositionModeService {
	s.dualSide = dualSide
	return s
}

// Do send request
func (s *ChangePositionModeService) Do(ctx context.Context, opts ...RequestOption) (err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/positionSide/dual",
		secType:  secTypeSigned,
	}
	r.setFormParams(params{
		"dualSidePosition": s.dualSide,
	})
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return err
	}
	return validateAccountConfigurationSuccess(data)
}

// GetPositionModeService get user's position mode
type GetPositionModeService struct {
	c *Client
}

// Response of user's position mode
type PositionMode struct {
	DualSidePosition bool `json:"dualSidePosition"`
}

// UnmarshalJSON requires an explicit mode; a missing or null field is not false.
func (m *PositionMode) UnmarshalJSON(data []byte) error {
	var response struct {
		DualSidePosition *bool `json:"dualSidePosition"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return err
	}
	if response.DualSidePosition == nil {
		return fmt.Errorf("%w: missing or null dualSidePosition", ErrInvalidAccountConfigurationResponse)
	}
	m.DualSidePosition = *response.DualSidePosition
	return nil
}

// Do send request
func (s *GetPositionModeService) Do(ctx context.Context, opts ...RequestOption) (res *PositionMode, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/positionSide/dual",
		secType:  secTypeSigned,
	}
	r.setFormParams(params{})
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = &PositionMode{}
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ChangeMultiAssetModeService change user's multi-asset mode
type ChangeMultiAssetModeService struct {
	c                 *Client
	multiAssetsMargin bool
}

// MultiAssetsMargin set multiAssetsMargin
func (s *ChangeMultiAssetModeService) MultiAssetsMargin(multiAssetsMargin bool) *ChangeMultiAssetModeService {
	s.multiAssetsMargin = multiAssetsMargin
	return s
}

// Do send request
func (s *ChangeMultiAssetModeService) Do(ctx context.Context, opts ...RequestOption) (err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/multiAssetsMargin",
		secType:  secTypeSigned,
	}
	r.setFormParams(params{
		"multiAssetsMargin": s.multiAssetsMargin,
	})
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return err
	}
	return validateAccountConfigurationSuccess(data)
}

// GetMultiAssetModeService get user's multi-asset mode
type GetMultiAssetModeService struct {
	c *Client
}

// Response of user's multi-asset mode
type MultiAssetMode struct {
	MultiAssetsMargin bool `json:"multiAssetsMargin"`
}

// UnmarshalJSON requires an explicit mode; a missing or null field is not false.
func (m *MultiAssetMode) UnmarshalJSON(data []byte) error {
	var response struct {
		MultiAssetsMargin *bool `json:"multiAssetsMargin"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return err
	}
	if response.MultiAssetsMargin == nil {
		return fmt.Errorf("%w: missing or null multiAssetsMargin", ErrInvalidAccountConfigurationResponse)
	}
	m.MultiAssetsMargin = *response.MultiAssetsMargin
	return nil
}

// Do send request
func (s *GetMultiAssetModeService) Do(ctx context.Context, opts ...RequestOption) (res *MultiAssetMode, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/multiAssetsMargin",
		secType:  secTypeSigned,
	}
	r.setFormParams(params{})
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = &MultiAssetMode{}
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ErrInvalidAccountConfigurationResponse means a configuration response lacks
// usable evidence. Query callers must not interpret absent modes as false.
// For submitted changes, callers must reconcile rather than retry the side
// effect or assume that it failed.
var ErrInvalidAccountConfigurationResponse = errors.New("futures: invalid account configuration response")

func validateAccountConfigurationSuccess(data []byte) error {
	var response struct {
		Code    *int64 `json:"code"`
		Message string `json:"msg"`
	}
	if err := json.Unmarshal(data, &response); err != nil || response.Code == nil {
		return ErrInvalidAccountConfigurationResponse
	}
	if *response.Code == 200 {
		return nil
	}
	if *response.Code < 0 {
		return &common.APIError{Code: *response.Code, Message: response.Message}
	}
	return ErrInvalidAccountConfigurationResponse
}
