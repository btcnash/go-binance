package futures

import (
	"context"
	"encoding/json"
	"time"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/common/websocket"
	managedfutures "github.com/btcnash/go-binance/v2/futures/wsapi"
)

// NewOrderCancelRequest init OrderCancelRequest
func NewOrderCancelRequest() *OrderCancelRequest {
	return &OrderCancelRequest{}
}

// OrderCancelRequest parameters for 'order.cancel' websocket API
type OrderCancelRequest struct {
	symbol            string
	orderID           *int64
	origClientOrderID *string
}

// Symbol set symbol
func (s *OrderCancelRequest) Symbol(symbol string) *OrderCancelRequest {
	s.symbol = symbol
	return s
}

// OrderID set orderID
func (s *OrderCancelRequest) OrderID(orderID int64) *OrderCancelRequest {
	s.orderID = &orderID
	return s
}

// OrigClientOrderID set origClientOrderID
func (s *OrderCancelRequest) OrigClientOrderID(origClientOrderID string) *OrderCancelRequest {
	s.origClientOrderID = &origClientOrderID
	return s
}

func (r *OrderCancelRequest) GetParams() map[string]any {
	return r.buildParams()
}

// buildParams builds params
func (s *OrderCancelRequest) buildParams() params {
	m := params{
		"symbol": s.symbol,
	}

	if s.orderID != nil {
		m["orderId"] = *s.orderID
	}

	if s.origClientOrderID != nil {
		m["origClientOrderId"] = *s.origClientOrderID
	}

	return m
}

// CancelOrderResult define order cancel result
type CancelOrderResult struct {
	CancelOrderResponse
}

// OrderCancelWsResponse define 'order.cancel' websocket API response
type OrderCancelWsResponse struct {
	Id     string            `json:"id"`
	Status int               `json:"status"`
	Result CancelOrderResult `json:"result"`

	// error response
	Error *common.APIError `json:"error,omitempty"`
}

// OrderCancelWsService cancel order
type OrderCancelWsService struct {
	c          websocket.Client
	ApiKey     string
	SecretKey  string
	KeyType    string
	TimeOffset int64
}

// NewOrderCancelWsService init OrderCancelWsService
func NewOrderCancelWsService(apiKey, secretKey string) (*OrderCancelWsService, error) {
	client, err := newManagedLegacyWSAPIClient()
	if err != nil {
		return nil, err
	}

	return &OrderCancelWsService{
		c:         client,
		ApiKey:    apiKey,
		SecretKey: secretKey,
		KeyType:   common.KeyTypeHmac,
	}, nil
}

// NewOrderCancelWsServiceWithSession initializes OrderCancelWsService with an externally managed shared Session.
func NewOrderCancelWsServiceWithSession(session *managedfutures.Session, apiKey, secretKey string) (*OrderCancelWsService, error) {
	client, err := newBorrowedLegacyWSAPIClient(session)
	if err != nil {
		return nil, err
	}

	return &OrderCancelWsService{
		c:         client,
		ApiKey:    apiKey,
		SecretKey: secretKey,
		KeyType:   common.KeyTypeHmac,
	}, nil
}

func (s *OrderCancelWsService) buildRequest(requestID string, request *OrderCancelRequest) ([]byte, error) {
	return websocket.CreateRequest(
		websocket.NewRequestData(
			requestID,
			s.ApiKey,
			s.SecretKey,
			s.TimeOffset,
			s.KeyType,
		),
		websocket.CancelFuturesWsApiMethod,
		request.buildParams(),
	)
}

// Do - sends 'order.cancel' request
func (s *OrderCancelWsService) Do(requestID string, request *OrderCancelRequest) error {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return err
	}

	if err := s.c.Write(requestID, rawData); err != nil {
		return err
	}

	return nil
}

// SyncDo - sends 'order.cancel' request and receives response
func (s *OrderCancelWsService) SyncDo(requestID string, request *OrderCancelRequest) (*OrderCancelWsResponse, error) {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return nil, err
	}

	response, err := s.c.WriteSync(requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}

	cancelOrderWsResponse := &OrderCancelWsResponse{}
	if err := json.Unmarshal(response, cancelOrderWsResponse); err != nil {
		return nil, err
	}

	return cancelOrderWsResponse, nil
}

// SyncDoContext sends 'order.cancel' with caller cancellation/deadline semantics.
func (s *OrderCancelWsService) SyncDoContext(ctx context.Context, requestID string, request *OrderCancelRequest) (*OrderCancelWsResponse, error) {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return nil, err
	}
	response, err := writeLegacyWSAPISyncContext(ctx, s.c, requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}
	result := &OrderCancelWsResponse{}
	if err := json.Unmarshal(response, result); err != nil {
		return nil, err
	}
	return result, nil
}

// ReceiveAllDataBeforeStop waits until all responses will be received from websocket until timeout expired
func (s *OrderCancelWsService) ReceiveAllDataBeforeStop(timeout time.Duration) {
	s.c.Wait(timeout)
}

// GetReadChannel returns channel with API response data (including API errors)
func (s *OrderCancelWsService) GetReadChannel() <-chan []byte {
	return s.c.GetReadChannel()
}

// GetReadErrorChannel returns channel with errors which are occurred while reading websocket connection
func (s *OrderCancelWsService) GetReadErrorChannel() <-chan error {
	return s.c.GetReadErrorChannel()
}

// GetReconnectCount returns count of reconnect attempts by client
func (s *OrderCancelWsService) GetReconnectCount() int64 {
	return s.c.GetReconnectCount()
}
