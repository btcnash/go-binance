package futures

import (
	"context"
	"encoding/json"
	"time"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/common/websocket"
	managedfutures "github.com/btcnash/go-binance/v2/futures/wsapi"
)

// AlgoOrderCancelWsRequest contains parameters for algoOrder.cancel.
type AlgoOrderCancelWsRequest struct {
	algoID       *int64
	clientAlgoID *string
	recvWindow   *int64
}

func NewAlgoOrderCancelWsRequest() *AlgoOrderCancelWsRequest { return &AlgoOrderCancelWsRequest{} }
func (r *AlgoOrderCancelWsRequest) AlgoID(value int64) *AlgoOrderCancelWsRequest {
	r.algoID = &value
	return r
}
func (r *AlgoOrderCancelWsRequest) ClientAlgoID(value string) *AlgoOrderCancelWsRequest {
	r.clientAlgoID = &value
	return r
}
func (r *AlgoOrderCancelWsRequest) RecvWindow(value int64) *AlgoOrderCancelWsRequest {
	r.recvWindow = &value
	return r
}

func (r *AlgoOrderCancelWsRequest) Validate() error {
	if r == nil || (r.algoID == nil && r.clientAlgoID == nil) {
		return ErrAlgoOrderCancelIdentityNeeded
	}
	if r.algoID != nil && *r.algoID <= 0 {
		return ErrAlgoOrderCancelAlgoIDInvalid
	}
	if r.clientAlgoID != nil && *r.clientAlgoID == "" {
		return ErrAlgoOrderCancelClientIDInvalid
	}
	return nil
}

func (r *AlgoOrderCancelWsRequest) GetParams() map[string]any { return r.buildParams() }
func (r *AlgoOrderCancelWsRequest) buildParams() params {
	m := params{}
	if r.algoID != nil {
		m["algoId"] = *r.algoID
	}
	if r.clientAlgoID != nil {
		m["clientAlgoId"] = *r.clientAlgoID
	}
	if r.recvWindow != nil {
		m["recvWindow"] = *r.recvWindow
	}
	return m
}

// CancelAlgoOrderResult is the typed result returned by algoOrder.cancel.
type CancelAlgoOrderResult struct {
	AlgoId       int64  `json:"algoId"`
	ClientAlgoId string `json:"clientAlgoId"`
	Code         string `json:"code"`
	Message      string `json:"msg"`
}

type CancelAlgoOrderWsResponse struct {
	Id         string                `json:"id"`
	Status     int                   `json:"status"`
	Result     CancelAlgoOrderResult `json:"result"`
	RateLimits []WsRateLimit         `json:"rateLimits"`
	Error      *common.APIError      `json:"error,omitempty"`
}

type AlgoOrderCancelWsService struct {
	c          websocket.Client
	ApiKey     string
	SecretKey  string
	KeyType    string
	TimeOffset int64
}

func NewAlgoOrderCancelWsService(apiKey, secretKey string) (*AlgoOrderCancelWsService, error) {
	client, err := newManagedLegacyWSAPIClient()
	if err != nil {
		return nil, err
	}
	return &AlgoOrderCancelWsService{c: client, ApiKey: apiKey, SecretKey: secretKey, KeyType: common.KeyTypeHmac}, nil
}

// NewAlgoOrderCancelWsServiceWithSession initializes AlgoOrderCancelWsService with an externally managed shared Session.
func NewAlgoOrderCancelWsServiceWithSession(session *managedfutures.Session, apiKey, secretKey string) (*AlgoOrderCancelWsService, error) {
	client, err := newBorrowedLegacyWSAPIClient(session)
	if err != nil {
		return nil, err
	}
	return &AlgoOrderCancelWsService{c: client, ApiKey: apiKey, SecretKey: secretKey, KeyType: common.KeyTypeHmac}, nil
}

func (s *AlgoOrderCancelWsService) buildRequest(requestID string, request *AlgoOrderCancelWsRequest) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	return websocket.CreateRequest(
		websocket.NewRequestData(requestID, s.ApiKey, s.SecretKey, s.TimeOffset, s.KeyType),
		websocket.AlgoOrderCancelFuturesWsApiMethod,
		request.buildParams(),
	)
}

func (s *AlgoOrderCancelWsService) Do(requestID string, request *AlgoOrderCancelWsRequest) error {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return err
	}
	return s.c.Write(requestID, rawData)
}

func (s *AlgoOrderCancelWsService) SyncDo(requestID string, request *AlgoOrderCancelWsRequest) (*CancelAlgoOrderWsResponse, error) {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return nil, err
	}
	response, err := s.c.WriteSync(requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}
	result := &CancelAlgoOrderWsResponse{}
	if err := json.Unmarshal(response, result); err != nil {
		return nil, err
	}
	return result, nil
}

// SyncDoContext sends algoOrder.cancel with caller cancellation/deadline semantics.
func (s *AlgoOrderCancelWsService) SyncDoContext(ctx context.Context, requestID string, request *AlgoOrderCancelWsRequest) (*CancelAlgoOrderWsResponse, error) {
	rawData, err := s.buildRequest(requestID, request)
	if err != nil {
		return nil, err
	}
	response, err := writeLegacyWSAPISyncContext(ctx, s.c, requestID, rawData, websocket.WriteSyncWsTimeout)
	if err != nil {
		return nil, err
	}
	result := &CancelAlgoOrderWsResponse{}
	if err := json.Unmarshal(response, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *AlgoOrderCancelWsService) ReceiveAllDataBeforeStop(timeout time.Duration) { s.c.Wait(timeout) }
func (s *AlgoOrderCancelWsService) GetReadChannel() <-chan []byte                  { return s.c.GetReadChannel() }
func (s *AlgoOrderCancelWsService) GetReadErrorChannel() <-chan error {
	return s.c.GetReadErrorChannel()
}
func (s *AlgoOrderCancelWsService) GetReconnectCount() int64 { return s.c.GetReconnectCount() }
