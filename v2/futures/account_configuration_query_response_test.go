package futures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/btcnash/go-binance/v2/common"
)

type configurationQueryCase struct {
	name     string
	field    string
	endpoint string
	read     func(*Client) (bool, bool, error)
}

func configurationQueryCases() []configurationQueryCase {
	return []configurationQueryCase{
		{"position", "dualSidePosition", "/fapi/v1/positionSide/dual", func(c *Client) (bool, bool, error) {
			result, err := c.NewGetPositionModeService().Do(context.Background())
			if result == nil {
				return false, false, err
			}
			return result.DualSidePosition, true, err
		}},
		{"multi_asset", "multiAssetsMargin", "/fapi/v1/multiAssetsMargin", func(c *Client) (bool, bool, error) {
			result, err := c.NewGetMultiAssetModeService().Do(context.Background())
			if result == nil {
				return false, false, err
			}
			return result.MultiAssetsMargin, true, err
		}},
		{"account", "multiAssetsMargin", "/fapi/v1/accountConfig", func(c *Client) (bool, bool, error) {
			result, err := c.NewGetAccountConfigService().Do(context.Background())
			if result == nil {
				return false, false, err
			}
			return result.MultiAssetsMargin, true, err
		}},
	}
}

func TestConfigurationQueryRejectsIncompleteMode(t *testing.T) {
	for _, query := range configurationQueryCases() {
		bodies := []string{`{}`, `null`, `{"unrelated":false}`, fmt.Sprintf(`{"%s":null}`, query.field)}
		for _, body := range bodies {
			t.Run(query.name+"/"+body, func(t *testing.T) {
				_, present, err := query.read(configurationResponseClient(body))
				if err == nil || present {
					t.Fatalf("incomplete mode accepted: result_present=%t error=%v", present, err)
				}
				if !errors.Is(err, ErrInvalidAccountConfigurationResponse) {
					t.Fatalf("missing mode must have a stable error identity, got %T", err)
				}
			})
		}
	}
}

func TestConfigurationQueryRejectsMalformedMode(t *testing.T) {
	for _, query := range configurationQueryCases() {
		bodies := []string{"", " ", `[]`, `true`, `"false"`, `invalid`, `{"`, fmt.Sprintf(`{"%s":"false"}`, query.field), fmt.Sprintf(`{"%s":0}`, query.field), fmt.Sprintf(`{"%s":[]}`, query.field), fmt.Sprintf(`{"%s":{}}`, query.field)}
		for _, body := range bodies {
			t.Run(query.name+"/"+body, func(t *testing.T) {
				_, present, err := query.read(configurationResponseClient(body))
				if err == nil || present {
					t.Fatalf("malformed mode accepted: result_present=%t error=%v", present, err)
				}
			})
		}
	}
}

func TestConfigurationQueryAcceptsExplicitTrueAndFalse(t *testing.T) {
	for _, query := range configurationQueryCases() {
		for _, want := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/%t", query.name, want), func(t *testing.T) {
				body := fmt.Sprintf(`{"%s":%t,"futureField":{"enabled":true}}`, query.field, want)
				got, present, err := query.read(configurationResponseClient(body))
				if err != nil || !present || got != want {
					t.Fatalf("explicit mode changed: got=%t present=%t error=%v", got, present, err)
				}
			})
		}
	}
}

func TestConfigurationQueryPreservesAccountFields(t *testing.T) {
	body := `{"feeTier":3,"canTrade":true,"canDeposit":true,"canWithdraw":false,"dualSidePosition":true,"updateTime":1724416653850,"multiAssetsMargin":false,"tradeGroupId":-1}`
	got, err := configurationResponseClient(body).NewGetAccountConfigService().Do(context.Background())
	want := AccountConfig{FeeTier: 3, CanTrade: true, CanDeposit: true, CanWithdraw: false, DualSidePosition: true, UpdateTime: 1724416653850, MultiAssetsMargin: false, TradeGroupId: -1}
	if err != nil || got == nil || *got != want {
		t.Fatalf("account fields changed: got=%+v error=%v", got, err)
	}
}

func TestConfigurationQueryCanRecoverWithoutAutomaticRetry(t *testing.T) {
	for _, query := range configurationQueryCases() {
		t.Run(query.name, func(t *testing.T) {
			calls := 0
			client := NewClient("test", "test")
			client.HTTPClient = &http.Client{Transport: configurationResponseTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodGet || r.URL.Path != query.endpoint {
					t.Errorf("query route changed: %s %s", r.Method, r.URL.Path)
				}
				body := `{}`
				if calls > 1 {
					body = fmt.Sprintf(`{"%s":false}`, query.field)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: r}, nil
			})}
			if _, present, err := query.read(client); err == nil || present || calls != 1 {
				t.Fatalf("invalid query must fail once: present=%t error=%v calls=%d", present, err, calls)
			}
			if got, present, err := query.read(client); err != nil || !present || got || calls != 2 {
				t.Fatalf("later valid false must recover: value=%t present=%t error=%v calls=%d", got, present, err, calls)
			}
		})
	}
}

func TestConfigurationQueryPreservesTransportAndAPIErrors(t *testing.T) {
	for _, query := range configurationQueryCases() {
		t.Run(query.name, func(t *testing.T) {
			transportErr := errors.New("test transport unavailable")
			client := NewClient("test", "test")
			client.HTTPClient = &http.Client{Transport: configurationResponseTransport(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			})}
			if _, present, err := query.read(client); present || !errors.Is(err, transportErr) {
				t.Fatalf("transport error changed: present=%t error=%v", present, err)
			}
			client.HTTPClient = &http.Client{Transport: configurationResponseTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader(`{"code":-2015,"msg":"test API rejection"}`)), Header: make(http.Header), Request: r}, nil
			})}
			_, present, err := query.read(client)
			var apiErr *common.APIError
			if present || !errors.As(err, &apiErr) || apiErr.Code != -2015 || apiErr.StatusCode != http.StatusBadRequest {
				t.Fatalf("API error changed: present=%t error=%v", present, err)
			}
		})
	}
}

func TestConfigurationQueryJSONFailureDoesNotOverwriteExistingMode(t *testing.T) {
	t.Run("position", func(t *testing.T) {
		position := PositionMode{DualSidePosition: true}
		if err := json.Unmarshal([]byte(`{}`), &position); !errors.Is(err, ErrInvalidAccountConfigurationResponse) || !position.DualSidePosition {
			t.Fatalf("invalid position mode changed a known value or lacked an error: %+v error=%v", position, err)
		}
	})
	t.Run("multi_asset", func(t *testing.T) {
		assets := MultiAssetMode{MultiAssetsMargin: true}
		if err := json.Unmarshal([]byte(`{"multiAssetsMargin":null}`), &assets); !errors.Is(err, ErrInvalidAccountConfigurationResponse) || !assets.MultiAssetsMargin {
			t.Fatalf("invalid asset mode changed a known value or lacked an error: %+v error=%v", assets, err)
		}
	})
	t.Run("account", func(t *testing.T) {
		account := AccountConfig{CanTrade: true, MultiAssetsMargin: true}
		if err := json.Unmarshal([]byte(`{"canTrade":false}`), &account); !errors.Is(err, ErrInvalidAccountConfigurationResponse) || !account.CanTrade || !account.MultiAssetsMargin {
			t.Fatalf("invalid account config changed known values or lacked an error: %+v error=%v", account, err)
		}
	})
}

func TestConfigurationQueryJSONRoundTripPreservesFalse(t *testing.T) {
	for name, original := range map[string]any{
		"position":    &PositionMode{},
		"multi_asset": &MultiAssetMode{},
		"account":     &AccountConfig{CanTrade: true},
	} {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			var decoded any
			switch original.(type) {
			case *PositionMode:
				decoded = &PositionMode{}
			case *MultiAssetMode:
				decoded = &MultiAssetMode{}
			case *AccountConfig:
				decoded = &AccountConfig{}
			}
			if err := json.Unmarshal(data, decoded); err != nil {
				t.Fatal(err)
			}
			again, err := json.Marshal(decoded)
			if err != nil || string(again) != string(data) {
				t.Fatalf("public JSON contract changed: error=%v", err)
			}
		})
	}
}
