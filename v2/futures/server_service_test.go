package futures

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/stretchr/testify/suite"
)

type serverServiceTestSuite struct {
	baseTestSuite
}

func TestServerService(t *testing.T) {
	suite.Run(t, new(serverServiceTestSuite))
}

func (s *serverServiceTestSuite) TestPing() {
	data := []byte(`{}`)
	s.mockDo(data, nil)
	defer s.assertDo()

	s.assertReq(func(r *request) {
		e := newRequest()
		s.assertRequestEqual(e, r)
	})

	err := s.client.NewPingService().Do(newContext())
	s.r().NoError(err)
}

func (s *serverServiceTestSuite) TestServerTime() {
	data := []byte(`{
        "serverTime": 1499827319559
    }`)
	s.mockDo(data, nil)
	defer s.assertDo()

	s.assertReq(func(r *request) {
		e := newRequest()
		s.assertRequestEqual(e, r)
	})

	serverTime, err := s.client.NewServerTimeService().Do(newContext())
	s.r().NoError(err)
	s.r().EqualValues(1499827319559, serverTime)
}

func (s *serverServiceTestSuite) TestServerTimeError() {
	s.mockDo([]byte("{}"), fmt.Errorf("dummy error"), http.StatusInternalServerError)
	defer s.assertDo()

	s.assertReq(func(r *request) {
		e := newRequest()
		s.assertRequestEqual(e, r)
	})
	_, err := s.client.NewServerTimeService().Do(newContext())
	s.r().Error(err)
	s.r().Contains(err.Error(), "dummy error")
}

func (s *serverServiceTestSuite) TestServerTimeBadRequest() {
	s.mockDo([]byte(`{
        "code": -1121,
        "msg": "Invalid symbol."
    }`), nil, http.StatusBadRequest)
	defer s.assertDo()

	s.assertReq(func(r *request) {
		e := newRequest()
		s.assertRequestEqual(e, r)
	})
	_, err := s.client.NewServerTimeService().Do(newContext())
	s.r().Error(err)
	s.r().True(common.IsAPIError(err))
}

func (s *serverServiceTestSuite) TestInvalidResponseBody() {
	s.mockDo([]byte(``), nil)
	defer s.assertDo()

	s.assertReq(func(r *request) {
		e := newRequest()
		s.assertRequestEqual(e, r)
	})
	_, err := s.client.NewServerTimeService().Do(newContext())
	s.r().Error(err)
	s.r().False(common.IsAPIError(err))
}

func (s *serverServiceTestSuite) TestSetServerTime() {
	data := []byte(`1399827319559`)
	s.mockDo(data, nil)
	defer s.assertDo()

	s.assertReq(func(r *request) {
		e := newRequest()
		s.assertRequestEqual(e, r)
	})

	timeOffset, err := s.client.NewSetServerTimeService().Do(newContext())
	s.r().NoError(err)
	s.r().NotZero(s.client.TimeOffset)
	s.r().EqualValues(timeOffset, s.client.TimeOffset)
}

func (s *serverServiceTestSuite) TestAPIErrorPreservesHTTPResponseMetadata() {
	body := []byte(`{
        "code": -1007,
        "msg": "Timeout waiting for response from backend server. Send status unknown; execution status unknown."
    }`)
	response := newHTTPResponse(body, http.StatusServiceUnavailable)
	response.Header = http.Header{
		"X-Mbx-Used-Weight-1m":  []string{"123"},
		"X-Mbx-Order-Count-10s": []string{"7"},
	}

	s.client.Client.do = s.client.do
	s.client.On("do", anyHTTPRequest()).Return(response, nil)
	defer s.assertDo()

	_, err := s.client.NewServerTimeService().Do(newContext())
	s.r().Error(err)

	apiErr, ok := err.(*common.APIError)
	s.r().True(ok)
	s.r().EqualValues(-1007, apiErr.Code)
	s.r().Equal("Timeout waiting for response from backend server. Send status unknown; execution status unknown.", apiErr.Message)
	s.r().Equal(body, apiErr.Response)
	s.r().Equal(http.StatusServiceUnavailable, apiErr.StatusCode)
	s.r().Equal("123", apiErr.Header.Get("X-MBX-USED-WEIGHT-1M"))
	s.r().Equal("7", apiErr.Header.Get("X-MBX-ORDER-COUNT-10S"))

	response.Header.Set("X-MBX-USED-WEIGHT-1M", "999")
	s.r().Equal("123", apiErr.Header.Get("X-MBX-USED-WEIGHT-1M"))
}

func (s *serverServiceTestSuite) TestAPIErrorStandardJSONPreservesRawBody() {
	body := []byte(`{"code":-1125,"msg":"This listenKey does not exist."}`)
	response := newHTTPResponse(body, http.StatusBadRequest)
	response.Header = http.Header{"X-Test": []string{"diagnostic"}}

	s.client.Client.do = s.client.do
	s.client.On("do", anyHTTPRequest()).Return(response, nil)
	defer s.assertDo()

	_, err := s.client.NewServerTimeService().Do(newContext())
	s.r().Error(err)

	apiErr, ok := err.(*common.APIError)
	s.r().True(ok)
	s.r().EqualValues(-1125, apiErr.Code)
	s.r().Equal("This listenKey does not exist.", apiErr.Message)
	s.r().Equal(http.StatusBadRequest, apiErr.StatusCode)
	s.r().Equal("diagnostic", apiErr.Header.Get("X-Test"))
	s.r().Equal(body, apiErr.Response)
}

func (s *serverServiceTestSuite) TestAPIErrorPreservesNonJSONResponseBody() {
	body := []byte("upstream unavailable")
	response := newHTTPResponse(body, http.StatusBadGateway)
	response.Header = http.Header{"X-Test": []string{"diagnostic"}}

	s.client.Client.do = s.client.do
	s.client.On("do", anyHTTPRequest()).Return(response, nil)
	defer s.assertDo()

	_, err := s.client.NewServerTimeService().Do(newContext())
	s.r().Error(err)

	apiErr, ok := err.(*common.APIError)
	s.r().True(ok)
	s.r().Zero(apiErr.Code)
	s.r().Empty(apiErr.Message)
	s.r().Equal(body, apiErr.Response)
	s.r().Equal(http.StatusBadGateway, apiErr.StatusCode)
	s.r().Equal("diagnostic", apiErr.Header.Get("X-Test"))
}
