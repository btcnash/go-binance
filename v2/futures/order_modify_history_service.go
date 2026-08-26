package futures

import (
	"context"
	"encoding/json"
	"net/http"
)

// OrderAmendmentValue contains one before/after amendment value.
type OrderAmendmentValue struct {
	Before string `json:"before"`
	After  string `json:"after"`
}

// OrderAmendment contains the fields changed by one order modification.
type OrderAmendment struct {
	Price    OrderAmendmentValue `json:"price"`
	OrigQty  OrderAmendmentValue `json:"origQty"`
	Count    int64               `json:"count"`
	ModifyID int64               `json:"modifyId"`
}

// OrderModifyHistory records one Binance order amendment.
type OrderModifyHistory struct {
	AmendmentID   int64          `json:"amendmentId"`
	Symbol        string         `json:"symbol"`
	Pair          string         `json:"pair"`
	OrderID       int64          `json:"orderId"`
	ClientOrderID string         `json:"clientOrderId"`
	Time          int64          `json:"time"`
	Amendment     OrderAmendment `json:"amendment"`
}

// GetOrderModifyHistoryService gets USDⓈ-M order modification history.
type GetOrderModifyHistoryService struct {
	c                 *Client
	symbol            string
	orderID           *int64
	origClientOrderID *string
	startTime         *int64
	endTime           *int64
	limit             *int
}

func (s *GetOrderModifyHistoryService) Symbol(symbol string) *GetOrderModifyHistoryService {
	s.symbol = symbol
	return s
}
func (s *GetOrderModifyHistoryService) OrderID(orderID int64) *GetOrderModifyHistoryService {
	s.orderID = &orderID
	return s
}
func (s *GetOrderModifyHistoryService) OrigClientOrderID(v string) *GetOrderModifyHistoryService {
	s.origClientOrderID = &v
	return s
}
func (s *GetOrderModifyHistoryService) StartTime(v int64) *GetOrderModifyHistoryService {
	s.startTime = &v
	return s
}
func (s *GetOrderModifyHistoryService) EndTime(v int64) *GetOrderModifyHistoryService {
	s.endTime = &v
	return s
}
func (s *GetOrderModifyHistoryService) Limit(v int) *GetOrderModifyHistoryService {
	s.limit = &v
	return s
}

// Do sends GET /fapi/v1/orderAmendment.
func (s *GetOrderModifyHistoryService) Do(ctx context.Context, opts ...RequestOption) (res []*OrderModifyHistory, err error) {
	r := &request{method: http.MethodGet, endpoint: "/fapi/v1/orderAmendment", secType: secTypeSigned}
	m := params{"symbol": s.symbol}
	if s.orderID != nil {
		m["orderId"] = *s.orderID
	}
	if s.origClientOrderID != nil {
		m["origClientOrderId"] = *s.origClientOrderID
	}
	if s.startTime != nil {
		m["startTime"] = *s.startTime
	}
	if s.endTime != nil {
		m["endTime"] = *s.endTime
	}
	if s.limit != nil {
		m["limit"] = *s.limit
	}
	r.setParams(m)
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	res = make([]*OrderModifyHistory, 0)
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	return res, nil
}
