package futures

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type configurationResponseTransport func(*http.Request) (*http.Response, error)

func (f configurationResponseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestConfigurationMutationRequiresSuccessfulResponse(t *testing.T) {
	for _, family := range []string{"margin", "position", "assets"} {
		for _, body := range []string{`{}`, `null`, `[]`, `invalid`, `{"code":null}`, `{"code":0}`, `{"code":201}`, `{"code":"200"}`, `{"code":-4059,"msg":"No need to change position side."}`} {
			t.Run(family+"/"+body, func(t *testing.T) {
				client := configurationResponseClient(body)
				var err error
				switch family {
				case "margin":
					err = client.NewChangeMarginTypeService().Symbol("BTCUSDT").MarginType(MarginTypeCrossed).Do(context.Background())
				case "position":
					err = client.NewChangePositionModeService().DualSide(true).Do(context.Background())
				case "assets":
					err = client.NewChangeMultiAssetModeService().MultiAssetsMargin(true).Do(context.Background())
				}
				if err == nil {
					t.Fatal("invalid mutation response was accepted as success")
				}
			})
		}
	}
}

func TestConfigurationLeverageRequiresUsableResponse(t *testing.T) {
	for _, body := range []string{`null`, `{}`, `{"symbol":"BTCUSDT"}`, `{"leverage":20}`, `{"symbol":"BTCUSDT","leverage":0}`} {
		t.Run(body, func(t *testing.T) {
			_, err := configurationResponseClient(body).NewChangeLeverageService().Symbol("BTCUSDT").Leverage(20).Do(context.Background())
			if err == nil {
				t.Fatal("incomplete leverage response was accepted as success")
			}
		})
	}
}

func configurationResponseClient(body string) *Client {
	client := NewClient("test", "test")
	client.HTTPClient = &http.Client{Transport: configurationResponseTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: r}, nil
	})}
	return client
}
