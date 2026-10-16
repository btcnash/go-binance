package futures

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/suite"
)

type userStreamServiceTestSuite struct {
	baseTestSuite
}

func TestUserStreamService(t *testing.T) {
	suite.Run(t, new(userStreamServiceTestSuite))
}

func (s *userStreamServiceTestSuite) TestStartUserStream() {
	data := []byte(`{
        "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
    }`)
	s.mockDo(data, nil)
	defer s.assertDo()

	s.assertReq(func(r *request) {
		s.assertRequestEqual(newRequest(), r)
	})
	s.assertHTTPReq(func(r *http.Request) {
		s.r().Equal(http.MethodPost, r.Method)
		s.r().Equal("/fapi/v1/listenKey", r.URL.Path)
		s.r().Equal(s.apiKey, r.Header.Get("X-MBX-APIKEY"))
	})

	listenKey, err := s.client.NewStartUserStreamService().Do(newContext())
	s.r().NoError(err)
	s.r().Equal("pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1", listenKey)
}

func (s *userStreamServiceTestSuite) TestKeepaliveUserStream() {
	data := []byte(`{}`)
	s.mockDo(data, nil)
	defer s.assertDo()

	listenKey := "dummykey"
	s.assertReq(func(r *request) {
		s.assertRequestEqual(newRequest(), r)
	})
	s.assertHTTPReq(func(r *http.Request) {
		s.r().Equal(http.MethodPut, r.Method)
		s.r().Equal("/fapi/v1/listenKey", r.URL.Path)
		s.r().Equal(s.apiKey, r.Header.Get("X-MBX-APIKEY"))
	})

	err := s.client.NewKeepaliveUserStreamService().ListenKey(listenKey).Do(newContext())
	s.r().NoError(err)
}

func (s *userStreamServiceTestSuite) TestCloseUserStream() {
	data := []byte(`{}`)
	s.mockDo(data, nil)
	defer s.assertDo()

	listenKey := "dummykey"
	s.assertReq(func(r *request) {
		s.assertRequestEqual(newRequest(), r)
	})
	s.assertHTTPReq(func(r *http.Request) {
		s.r().Equal(http.MethodDelete, r.Method)
		s.r().Equal("/fapi/v1/listenKey", r.URL.Path)
		s.r().Equal(s.apiKey, r.Header.Get("X-MBX-APIKEY"))
	})

	err := s.client.NewCloseUserStreamService().ListenKey(listenKey).Do(newContext())
	s.r().NoError(err)
}

func (s *userStreamServiceTestSuite) TestKeepaliveUserStreamResponse() {
	s.mockDo([]byte(`{"listenKey":"renewed-key"}`), nil)
	defer s.assertDo()
	calls := 0
	s.assertHTTPReq(func(r *http.Request) {
		calls++
		s.r().Equal(http.MethodPut, r.Method)
		s.r().Equal("/fapi/v1/listenKey", r.URL.Path)
	})
	response, err := s.client.NewKeepaliveUserStreamService().ListenKey("old-key").DoWithResponse(newContext())
	s.r().NoError(err)
	s.r().Equal("renewed-key", response.ListenKey)
	s.r().Equal(1, calls)
}

func (s *userStreamServiceTestSuite) TestKeepaliveUserStreamRejectsInvalidResponse() {
	for _, body := range []string{`{}`, `{"listenKey":" "}`, `{"listenKey":null}`, `{"listenKey":123}`, `null`, `{"listenKey":`} {
		s.mockDo([]byte(body), nil)
		response, err := s.client.NewKeepaliveUserStreamService().ListenKey("old-key").DoWithResponse(newContext())
		s.r().Error(err, body)
		s.r().Nil(response, body)
		s.assertDo()
	}
}
