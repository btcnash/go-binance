package futures

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// StartUserStreamService create listen key for user stream service
type StartUserStreamService struct {
	c *Client
}

// Do send request
func (s *StartUserStreamService) Do(ctx context.Context, opts ...RequestOption) (listenKey string, err error) {
	r := &request{
		method:   http.MethodPost,
		endpoint: "/fapi/v1/listenKey",
		secType:  secTypeAPIKey,
	}
	data, _, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return "", err
	}
	j, err := newJSON(data)
	if err != nil {
		return "", err
	}
	listenKey = j.Get("listenKey").MustString()
	return listenKey, nil
}

// KeepaliveUserStreamService update listen key
type KeepaliveUserStreamService struct {
	c         *Client
	listenKey string
}

// ListenKey set listen key
func (s *KeepaliveUserStreamService) ListenKey(listenKey string) *KeepaliveUserStreamService {
	s.listenKey = listenKey
	return s
}

// KeepaliveUserStreamResponse is the active key returned by a successful renewal.
type KeepaliveUserStreamResponse struct {
	ListenKey string `json:"listenKey"`
}

// Do preserves the original error-only API, including empty success bodies.
func (s *KeepaliveUserStreamService) Do(ctx context.Context, opts ...RequestOption) error {
	_, err := s.do(ctx, opts...)
	return err
}

// DoWithResponse returns the effective key without a second HTTP request.
func (s *KeepaliveUserStreamService) DoWithResponse(ctx context.Context, opts ...RequestOption) (*KeepaliveUserStreamResponse, error) {
	data, err := s.do(ctx, opts...)
	if err != nil {
		return nil, err
	}
	var response KeepaliveUserStreamResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("decode listen key renewal response: %w", err)
	}
	response.ListenKey = strings.TrimSpace(response.ListenKey)
	if response.ListenKey == "" {
		return nil, fmt.Errorf("listen key renewal response missing active key")
	}
	return &response, nil
}
func (s *KeepaliveUserStreamService) do(ctx context.Context, opts ...RequestOption) ([]byte, error) {
	r := &request{method: http.MethodPut, endpoint: "/fapi/v1/listenKey", secType: secTypeAPIKey}
	data, _, err := s.c.callAPI(ctx, r, opts...)
	return data, err
}

// CloseUserStreamService delete listen key
type CloseUserStreamService struct {
	c         *Client
	listenKey string
}

// ListenKey set listen key
func (s *CloseUserStreamService) ListenKey(listenKey string) *CloseUserStreamService {
	s.listenKey = listenKey
	return s
}

// Do send request
func (s *CloseUserStreamService) Do(ctx context.Context, opts ...RequestOption) (err error) {
	r := &request{
		method:   http.MethodDelete,
		endpoint: "/fapi/v1/listenKey",
		secType:  secTypeAPIKey,
	}
	_, _, err = s.c.callAPI(ctx, r, opts...)
	return err
}
