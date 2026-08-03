package futures

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/common/websocket"
)

var (
	ErrAlgoOrderPlaceRequestNil        = errors.New("algo order place ws: request is nil")
	ErrAlgoOrderSymbolRequired         = errors.New("algo order place ws: symbol is required")
	ErrAlgoOrderSideRequired           = errors.New("algo order place ws: side is required")
	ErrAlgoOrderTypeRequired           = errors.New("algo order place ws: type is required")
	ErrAlgoOrderQuantityRequired       = errors.New("algo order place ws: quantity is required unless closePosition is true")
	ErrAlgoOrderTriggerPriceRequired   = errors.New("algo order place ws: triggerPrice is required for stop and take-profit orders")
	ErrAlgoOrderPriceRequired          = errors.New("algo order place ws: price or priceMatch is required for STOP and TAKE_PROFIT")
	ErrAlgoOrderPriceConflict          = errors.New("algo order place ws: price and priceMatch cannot be sent together")
	ErrAlgoOrderPriceMatchTypeConflict = errors.New("algo order place ws: priceMatch is only supported for STOP and TAKE_PROFIT")
	ErrAlgoOrderClosePositionConflict  = errors.New("algo order place ws: closePosition cannot be sent with quantity or reduceOnly")
	ErrAlgoOrderClosePositionType      = errors.New("algo order place ws: closePosition is only supported for STOP_MARKET and TAKE_PROFIT_MARKET")
	ErrAlgoOrderPriceProtectType       = errors.New("algo order place ws: priceProtect is only supported for STOP_MARKET and TAKE_PROFIT_MARKET")
	ErrAlgoOrderReduceOnlyHedgeMode    = errors.New("algo order place ws: reduceOnly cannot be sent in Hedge Mode")
	ErrAlgoOrderTrailingParamConflict  = errors.New("algo order place ws: activatePrice and callbackRate are only supported for TRAILING_STOP_MARKET")
	ErrAlgoOrderGoodTillDateRequired   = errors.New("algo order place ws: goodTillDate is required when timeInForce is GTD")
	ErrAlgoOrderGoodTillDateConflict   = errors.New("algo order place ws: goodTillDate is only supported when timeInForce is GTD")
	ErrAlgoOrderGoodTillDateOutOfRange = errors.New("algo order place ws: goodTillDate must be more than 600 seconds in the future and less than 253402300799000")
	ErrAlgoOrderCallbackRateRequired   = errors.New("algo order place ws: callbackRate is required for TRAILING_STOP_MARKET")
	ErrAlgoOrderCallbackRateOutOfRange = errors.New("algo order place ws: callbackRate must be between 0.1 and 10")
	ErrAlgoOrderClientIDInvalid        = errors.New("algo order place ws: clientAlgoId must match ^[.A-Z:/a-z0-9_-]{1,36}$")
	ErrAlgoOrderCancelIdentityNeeded   = errors.New("algo order cancel ws: algoId or clientAlgoId is required")
)

// WsRateLimit describes the rate-limit snapshot returned by Futures WSAPI.
type WsRateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int64  `json:"intervalNum"`
	Limit         int64  `json:"limit"`
	Count         int64  `json:"count"`
}

// AlgoOrderPlaceWsService creates an Algo order through USDⓈ-M Futures WSAPI.
type AlgoOrderPlaceWsService struct {
	c          websocket.Client
	ApiKey     string
	SecretKey  string
	KeyType    string
	TimeOffset int64
}

// NewAlgoOrderPlaceWsService initializes AlgoOrderPlaceWsService.
func NewAlgoOrderPlaceWsService(apiKey, secretKey string) (*AlgoOrderPlaceWsService, error) {
	client, err := newManagedLegacyWSAPIClient()
	if err != nil {
		return nil, err
	}
	return &AlgoOrderPlaceWsService{
		c:         client,
		ApiKey:    apiKey,
		SecretKey: secretKey,
		KeyType:   common.KeyTypeHmac,
	}, nil
}

// AlgoOrderPlaceWsRequest contains parameters for algoOrder.place.
type AlgoOrderPlaceWsRequest struct {
	algoType                OrderAlgoType
	symbol                  string
	side                    SideType
	orderType               AlgoOrderType
	positionSide            *PositionSideType
	timeInForce             *TimeInForceType
	quantity                *string
	price                   *string
	triggerPrice            *string
	workingType             *WorkingType
	priceMatch              *PriceMatchType
	closePosition           *bool
	priceProtect            *bool
	reduceOnly              *bool
	activatePrice           *string
	callbackRate            *string
	clientAlgoID            *string
	newOrderRespType        NewOrderRespType
	selfTradePreventionMode *SelfTradePreventionMode
	goodTillDate            *int64
	recvWindow              *int64
}

// NewAlgoOrderPlaceWsRequest initializes an Algo place request using Binance defaults.
func NewAlgoOrderPlaceWsRequest() *AlgoOrderPlaceWsRequest {
	return &AlgoOrderPlaceWsRequest{
		algoType:         OrderAlgoTypeConditional,
		newOrderRespType: NewOrderRespTypeACK,
	}
}

func (r *AlgoOrderPlaceWsRequest) AlgoType(value OrderAlgoType) *AlgoOrderPlaceWsRequest {
	r.algoType = value
	return r
}
func (r *AlgoOrderPlaceWsRequest) Symbol(value string) *AlgoOrderPlaceWsRequest {
	r.symbol = value
	return r
}
func (r *AlgoOrderPlaceWsRequest) Side(value SideType) *AlgoOrderPlaceWsRequest {
	r.side = value
	return r
}
func (r *AlgoOrderPlaceWsRequest) Type(value AlgoOrderType) *AlgoOrderPlaceWsRequest {
	r.orderType = value
	return r
}
func (r *AlgoOrderPlaceWsRequest) PositionSide(value PositionSideType) *AlgoOrderPlaceWsRequest {
	r.positionSide = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) TimeInForce(value TimeInForceType) *AlgoOrderPlaceWsRequest {
	r.timeInForce = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) Quantity(value string) *AlgoOrderPlaceWsRequest {
	r.quantity = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) Price(value string) *AlgoOrderPlaceWsRequest {
	r.price = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) TriggerPrice(value string) *AlgoOrderPlaceWsRequest {
	r.triggerPrice = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) WorkingType(value WorkingType) *AlgoOrderPlaceWsRequest {
	r.workingType = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) PriceMatch(value PriceMatchType) *AlgoOrderPlaceWsRequest {
	r.priceMatch = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) ClosePosition(value bool) *AlgoOrderPlaceWsRequest {
	r.closePosition = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) PriceProtect(value bool) *AlgoOrderPlaceWsRequest {
	r.priceProtect = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) ReduceOnly(value bool) *AlgoOrderPlaceWsRequest {
	r.reduceOnly = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) ActivationPrice(value string) *AlgoOrderPlaceWsRequest {
	return r.ActivatePrice(value)
}
func (r *AlgoOrderPlaceWsRequest) ActivatePrice(value string) *AlgoOrderPlaceWsRequest {
	r.activatePrice = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) CallbackRate(value string) *AlgoOrderPlaceWsRequest {
	r.callbackRate = &value
	return r
}

// NewClientOrderID is retained for source compatibility.
// Deprecated: use ClientAlgoID.
func (r *AlgoOrderPlaceWsRequest) NewClientOrderID(value string) *AlgoOrderPlaceWsRequest {
	return r.ClientAlgoID(value)
}
func (r *AlgoOrderPlaceWsRequest) ClientAlgoID(value string) *AlgoOrderPlaceWsRequest {
	r.clientAlgoID = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) NewOrderResponseType(value NewOrderRespType) *AlgoOrderPlaceWsRequest {
	r.newOrderRespType = value
	return r
}
func (r *AlgoOrderPlaceWsRequest) SelfTradePreventionMode(value SelfTradePreventionMode) *AlgoOrderPlaceWsRequest {
	r.selfTradePreventionMode = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) GoodTillDate(value int64) *AlgoOrderPlaceWsRequest {
	r.goodTillDate = &value
	return r
}
func (r *AlgoOrderPlaceWsRequest) RecvWindow(value int64) *AlgoOrderPlaceWsRequest {
	r.recvWindow = &value
	return r
}

// Validate checks the parameter combinations documented for algoOrder.place.
func (r *AlgoOrderPlaceWsRequest) Validate() error {
	if r == nil {
		return ErrAlgoOrderPlaceRequestNil
	}
	if r.symbol == "" {
		return ErrAlgoOrderSymbolRequired
	}
	if r.side == "" {
		return ErrAlgoOrderSideRequired
	}
	if r.orderType == "" {
		return ErrAlgoOrderTypeRequired
	}
	if r.price != nil && r.priceMatch != nil {
		return ErrAlgoOrderPriceConflict
	}
	if r.priceMatch != nil && r.orderType != AlgoOrderTypeStop && r.orderType != AlgoOrderTypeTakeProfit {
		return ErrAlgoOrderPriceMatchTypeConflict
	}
	if r.closePosition != nil {
		if r.orderType != AlgoOrderTypeStopMarket && r.orderType != AlgoOrderTypeTakeProfitMarket {
			return ErrAlgoOrderClosePositionType
		}
		if *r.closePosition && (r.quantity != nil || r.reduceOnly != nil) {
			return ErrAlgoOrderClosePositionConflict
		}
	}
	if r.priceProtect != nil && r.orderType != AlgoOrderTypeStopMarket && r.orderType != AlgoOrderTypeTakeProfitMarket {
		return ErrAlgoOrderPriceProtectType
	}
	if r.reduceOnly != nil && r.positionSide != nil && *r.positionSide != PositionSideTypeBoth {
		return ErrAlgoOrderReduceOnlyHedgeMode
	}
	if r.orderType != AlgoOrderTypeTrailingStopMarket && (r.activatePrice != nil || r.callbackRate != nil) {
		return ErrAlgoOrderTrailingParamConflict
	}
	if r.timeInForce != nil && *r.timeInForce == TimeInForceTypeGTD {
		if r.goodTillDate == nil {
			return ErrAlgoOrderGoodTillDateRequired
		}
		nowWithMinimumLead := time.Now().Add(600 * time.Second).UnixMilli()
		if *r.goodTillDate <= nowWithMinimumLead || *r.goodTillDate >= 253402300799000 {
			return ErrAlgoOrderGoodTillDateOutOfRange
		}
	} else if r.goodTillDate != nil {
		return ErrAlgoOrderGoodTillDateConflict
	}
	if (r.closePosition == nil || !*r.closePosition) && r.quantity == nil {
		return ErrAlgoOrderQuantityRequired
	}
	if r.clientAlgoID != nil && !validClientAlgoID(*r.clientAlgoID) {
		return ErrAlgoOrderClientIDInvalid
	}

	switch r.orderType {
	case AlgoOrderTypeStop, AlgoOrderTypeTakeProfit:
		if r.triggerPrice == nil {
			return ErrAlgoOrderTriggerPriceRequired
		}
		if r.price == nil && r.priceMatch == nil {
			return ErrAlgoOrderPriceRequired
		}
	case AlgoOrderTypeStopMarket, AlgoOrderTypeTakeProfitMarket:
		if r.triggerPrice == nil {
			return ErrAlgoOrderTriggerPriceRequired
		}
	case AlgoOrderTypeTrailingStopMarket:
		if r.callbackRate == nil {
			return ErrAlgoOrderCallbackRateRequired
		}
		rate, err := strconv.ParseFloat(*r.callbackRate, 64)
		if err != nil || rate < 0.1 || rate > 10 {
			return fmt.Errorf("%w: %q", ErrAlgoOrderCallbackRateOutOfRange, *r.callbackRate)
		}
	}
	return nil
}

func validClientAlgoID(value string) bool {
	if len(value) < 1 || len(value) > 36 {
		return false
	}
	for _, char := range value {
		switch {
		case char >= 'A' && char <= 'Z':
		case char >= 'a' && char <= 'z':
		case char >= '0' && char <= '9':
		case char == '.', char == ':', char == '/', char == '_', char == '-':
		default:
			return false
		}
	}
	return true
}

// GetParams returns the exact signed parameter contract sent to Binance.
func (r *AlgoOrderPlaceWsRequest) GetParams() map[string]any {
	return r.buildParams()
}

func (r *AlgoOrderPlaceWsRequest) buildParams() params {
	m := params{
		"algoType":         r.algoType,
		"symbol":           r.symbol,
		"side":             r.side,
		"type":             r.orderType,
		"newOrderRespType": r.newOrderRespType,
	}
	if r.positionSide != nil {
		m["positionSide"] = *r.positionSide
	}
	if r.timeInForce != nil {
		m["timeInForce"] = *r.timeInForce
	}
	if r.quantity != nil {
		m["quantity"] = *r.quantity
	}
	if r.price != nil {
		m["price"] = *r.price
	}
	if r.triggerPrice != nil {
		m["triggerPrice"] = *r.triggerPrice
	}
	if r.workingType != nil {
		m["workingType"] = *r.workingType
	}
	if r.priceMatch != nil {
		m["priceMatch"] = *r.priceMatch
	}
	if r.closePosition != nil {
		m["closePosition"] = *r.closePosition
	}
	if r.priceProtect != nil {
		m["priceProtect"] = *r.priceProtect
	}
	if r.reduceOnly != nil {
		m["reduceOnly"] = *r.reduceOnly
	}
	if r.activatePrice != nil {
		m["activatePrice"] = *r.activatePrice
	}
	if r.callbackRate != nil {
		m["callbackRate"] = *r.callbackRate
	}
	if r.clientAlgoID != nil {
		m["clientAlgoId"] = *r.clientAlgoID
	}
	if r.selfTradePreventionMode != nil {
		m["selfTradePreventionMode"] = *r.selfTradePreventionMode
	}
	if r.goodTillDate != nil {
		m["goodTillDate"] = *r.goodTillDate
	}
	if r.recvWindow != nil {
		m["recvWindow"] = *r.recvWindow
	}
	return m
}

// CreateAlgoOrderResult is the typed result returned by algoOrder.place.
type CreateAlgoOrderResult struct {
	AlgoId                  int64                   `json:"algoId"`
	ClientAlgoId            string                  `json:"clientAlgoId"`
	AlgoType                OrderAlgoType           `json:"algoType"`
	OrderType               AlgoOrderType           `json:"orderType"`
	Symbol                  string                  `json:"symbol"`
	Side                    SideType                `json:"side"`
	PositionSide            PositionSideType        `json:"positionSide"`
	TimeInForce             TimeInForceType         `json:"timeInForce"`
	Quantity                string                  `json:"quantity"`
	AlgoStatus              AlgoOrderStatusType     `json:"algoStatus"`
	TriggerPrice            string                  `json:"triggerPrice"`
	Price                   string                  `json:"price"`
	IcebergQuantity         *string                 `json:"icebergQuantity"`
	SelfTradePreventionMode SelfTradePreventionMode `json:"selfTradePreventionMode"`
	WorkingType             WorkingType             `json:"workingType"`
	PriceMatch              PriceMatchType          `json:"priceMatch"`
	ClosePosition           bool                    `json:"closePosition"`
	PriceProtect            bool                    `json:"priceProtect"`
	ReduceOnly              bool                    `json:"reduceOnly"`
	CreateTime              int64                   `json:"createTime"`
	UpdateTime              int64                   `json:"updateTime"`
	TriggerTime             int64                   `json:"triggerTime"`
	GoodTillDate            int64                   `json:"goodTillDate"`
}

// CreateAlgoOrderWsResponse is the algoOrder.place response envelope.
type CreateAlgoOrderWsResponse struct {
	Id         string                `json:"id"`
	Status     int                   `json:"status"`
	Result     CreateAlgoOrderResult `json:"result"`
	RateLimits []WsRateLimit         `json:"rateLimits"`
	Error      *common.APIError      `json:"error,omitempty"`
}

func (s *AlgoOrderPlaceWsService) buildRequest(requestID string, request *AlgoOrderPlaceWsRequest) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	return websocket.CreateRequest(
		websocket.NewRequestData(requestID, s.ApiKey, s.SecretKey, s.TimeOffset, s.KeyType),
		websocket.AlgoOrderPlaceFuturesWsApiMethod,
		request.buildParams(),
	)
}

// Do sends algoOrder.place asynchronously.
func (s *AlgoOrderPlaceWsService) Do(requestID string, request *AlgoOrderPlaceWsRequest) error {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return err
	}
	return s.c.Write(requestID, rawData)
}

// SyncDo sends algoOrder.place and waits for its response.
func (s *AlgoOrderPlaceWsService) SyncDo(requestID string, request *AlgoOrderPlaceWsRequest) (*CreateAlgoOrderWsResponse, error) {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return nil, err
	}
	response, err := s.c.WriteSync(requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}
	result := &CreateAlgoOrderWsResponse{}
	if err := json.Unmarshal(response, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *AlgoOrderPlaceWsService) ReceiveAllDataBeforeStop(timeout time.Duration) { s.c.Wait(timeout) }
func (s *AlgoOrderPlaceWsService) GetReadChannel() <-chan []byte                  { return s.c.GetReadChannel() }
func (s *AlgoOrderPlaceWsService) GetReadErrorChannel() <-chan error {
	return s.c.GetReadErrorChannel()
}
func (s *AlgoOrderPlaceWsService) GetReconnectCount() int64 { return s.c.GetReconnectCount() }
