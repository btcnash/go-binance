package futures

import (
	"context"
	"encoding/json"
	"time"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/common/websocket"
	managedfutures "github.com/btcnash/go-binance/v2/futures/wsapi"
)

type OrderModifyWsRequest struct {
	symbol            string
	side              SideType
	quantity          string
	price             string
	orderID           *int64
	origClientOrderID *string
	modifyID          *int64
	recvWindow        *int64
}

func NewOrderModifyWsRequest() *OrderModifyWsRequest                    { return &OrderModifyWsRequest{} }
func (r *OrderModifyWsRequest) Symbol(v string) *OrderModifyWsRequest   { r.symbol = v; return r }
func (r *OrderModifyWsRequest) Side(v SideType) *OrderModifyWsRequest   { r.side = v; return r }
func (r *OrderModifyWsRequest) Quantity(v string) *OrderModifyWsRequest { r.quantity = v; return r }
func (r *OrderModifyWsRequest) Price(v string) *OrderModifyWsRequest    { r.price = v; return r }
func (r *OrderModifyWsRequest) OrderID(v int64) *OrderModifyWsRequest   { r.orderID = &v; return r }
func (r *OrderModifyWsRequest) OrigClientOrderID(v string) *OrderModifyWsRequest {
	r.origClientOrderID = &v
	return r
}
func (r *OrderModifyWsRequest) ModifyID(v int64) *OrderModifyWsRequest   { r.modifyID = &v; return r }
func (r *OrderModifyWsRequest) RecvWindow(v int64) *OrderModifyWsRequest { r.recvWindow = &v; return r }
func (r *OrderModifyWsRequest) GetParams() map[string]any                { return r.buildParams() }
func (r *OrderModifyWsRequest) buildParams() params {
	m := params{"symbol": r.symbol, "side": r.side, "quantity": r.quantity, "price": r.price}
	if r.orderID != nil {
		m["orderId"] = *r.orderID
	}
	if r.origClientOrderID != nil {
		m["origClientOrderId"] = *r.origClientOrderID
	}
	if r.modifyID != nil {
		m["modifyId"] = *r.modifyID
	}
	if r.recvWindow != nil {
		m["recvWindow"] = *r.recvWindow
	}
	return m
}

type ModifyOrderResult struct{ ModifyOrderResponse }
type OrderModifyWsResponse struct {
	Id         string            `json:"id"`
	Status     int               `json:"status"`
	Result     ModifyOrderResult `json:"result"`
	RateLimits []WsRateLimit     `json:"rateLimits"`
	Error      *common.APIError  `json:"error,omitempty"`
}

type OrderModifyWsService struct {
	c          websocket.Client
	ApiKey     string
	SecretKey  string
	KeyType    string
	TimeOffset int64
}

func NewOrderModifyWsService(apiKey, secretKey string) (*OrderModifyWsService, error) {
	client, err := newManagedLegacyWSAPIClient()
	if err != nil {
		return nil, err
	}
	return &OrderModifyWsService{c: client, ApiKey: apiKey, SecretKey: secretKey, KeyType: common.KeyTypeHmac}, nil
}
func NewOrderModifyWsServiceWithSession(session *managedfutures.Session, apiKey, secretKey string) (*OrderModifyWsService, error) {
	client, err := newBorrowedLegacyWSAPIClient(session)
	if err != nil {
		return nil, err
	}
	return &OrderModifyWsService{c: client, ApiKey: apiKey, SecretKey: secretKey, KeyType: common.KeyTypeHmac}, nil
}
func (s *OrderModifyWsService) buildRequest(id string, r *OrderModifyWsRequest) ([]byte, error) {
	return websocket.CreateRequest(websocket.NewRequestData(id, s.ApiKey, s.SecretKey, s.TimeOffset, s.KeyType), websocket.OrderModifyFuturesWsApiMethod, r.buildParams())
}
func (s *OrderModifyWsService) Do(id string, r *OrderModifyWsRequest) error {
	b, e := s.buildRequest(id, r)
	if e != nil {
		return e
	}
	return s.c.Write(id, b)
}
func (s *OrderModifyWsService) SyncDo(id string, r *OrderModifyWsRequest) (*OrderModifyWsResponse, error) {
	b, e := s.buildRequest(id, r)
	if e != nil {
		return nil, e
	}
	raw, e := s.c.WriteSync(id, b, websocket.WriteSyncWsTimeout)
	if e != nil {
		return nil, e
	}
	out := &OrderModifyWsResponse{}
	if e = json.Unmarshal(raw, out); e != nil {
		return nil, e
	}
	return out, nil
}

// SyncDoContext never automatically replays a transport-unknown side effect.
func (s *OrderModifyWsService) SyncDoContext(ctx context.Context, id string, r *OrderModifyWsRequest) (*OrderModifyWsResponse, error) {
	b, e := s.buildRequest(id, r)
	if e != nil {
		return nil, e
	}
	raw, e := writeLegacyWSAPISyncContext(ctx, s.c, id, b, websocket.WriteSyncWsTimeout)
	if e != nil {
		return nil, e
	}
	out := &OrderModifyWsResponse{}
	if e = json.Unmarshal(raw, out); e != nil {
		return nil, e
	}
	return out, nil
}
func (s *OrderModifyWsService) ReceiveAllDataBeforeStop(t time.Duration) { s.c.Wait(t) }
func (s *OrderModifyWsService) GetReadChannel() <-chan []byte            { return s.c.GetReadChannel() }
func (s *OrderModifyWsService) GetReadErrorChannel() <-chan error        { return s.c.GetReadErrorChannel() }
func (s *OrderModifyWsService) GetReconnectCount() int64                 { return s.c.GetReconnectCount() }
