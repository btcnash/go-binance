package private

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/futures"
)

func TestRESTListenKeyProviderLifecycle(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	var bodies []string
	var apiKeys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		methods = append(methods, r.Method)
		bodies = append(bodies, string(body))
		apiKeys = append(apiKeys, r.Header.Get("X-MBX-APIKEY"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"listenKey":"managed-key"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := futures.NewClient("api-key", "secret")
	client.BaseURL = server.URL
	client.HTTPClient = server.Client()
	provider := RESTListenKeyProvider{Client: client}

	key, err := provider.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if key != "managed-key" {
		t.Fatalf("Acquire() key = %q", key)
	}
	if err := provider.KeepAlive(context.Background(), key); err != nil {
		t.Fatalf("KeepAlive() error = %v", err)
	}
	if err := provider.Release(context.Background(), key); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	wantMethods := []string{http.MethodPost, http.MethodPut, http.MethodDelete}
	if len(methods) != len(wantMethods) {
		t.Fatalf("methods = %v", methods)
	}
	for i, want := range wantMethods {
		if methods[i] != want {
			t.Fatalf("methods[%d] = %q, want %q", i, methods[i], want)
		}
	}
	for i := range methods {
		if bodies[i] != "" {
			t.Fatalf("request body[%d] = %q, want empty", i, bodies[i])
		}
		if apiKeys[i] != "api-key" {
			t.Fatalf("X-MBX-APIKEY[%d] = %q, want api-key", i, apiKeys[i])
		}
	}
}

func TestRESTListenKeyProviderClassifiesOnlyInvalidListenKey(t *testing.T) {
	provider := RESTListenKeyProvider{}
	if !provider.IsInvalidListenKey(&common.APIError{Code: -1125, Message: "listenKey does not exist"}) {
		t.Fatal("-1125 should be classified as invalid listen key")
	}
	if provider.IsInvalidListenKey(&common.APIError{Code: -1001, Message: "disconnected"}) {
		t.Fatal("transient API failure must not be classified as invalid listen key")
	}
	if provider.IsInvalidListenKey(errors.New("network timeout")) {
		t.Fatal("plain transport failure must not be classified as invalid listen key")
	}
}

func TestRESTListenKeyProviderRejectsNilClient(t *testing.T) {
	provider := RESTListenKeyProvider{}
	if _, err := provider.Acquire(context.Background()); !errors.Is(err, ErrListenKeyAcquire) {
		t.Fatalf("Acquire() error = %v", err)
	}
	if err := provider.KeepAlive(context.Background(), "key"); !errors.Is(err, ErrListenKeyKeepAlive) {
		t.Fatalf("KeepAlive() error = %v", err)
	}
	if _, err := provider.KeepAliveWithResponse(context.Background(), "key"); !errors.Is(err, ErrListenKeyKeepAlive) {
		t.Fatalf("KeepAliveWithResponse() error = %v", err)
	}
	if err := provider.Release(context.Background(), "key"); !errors.Is(err, ErrListenKeyRelease) {
		t.Fatalf("Release() error = %v", err)
	}
}

func TestRESTListenKeyProviderReturnsRenewalResponseAndAPICause(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodPut || r.URL.Path != "/fapi/v1/listenKey" {
					t.Errorf("unexpected renewal: %v", r.URL)
				}
				w.WriteHeader(status)
				if status == http.StatusOK {
					_, _ = w.Write([]byte(`{"listenKey":"key-new"}`))
				} else {
					_, _ = w.Write([]byte(`{"code":-1125,"msg":"This listenKey does not exist."}`))
				}
			}))
			defer server.Close()
			client := futures.NewClient("api-key", "secret")
			client.BaseURL, client.HTTPClient = server.URL, server.Client()
			provider := RESTListenKeyProvider{Client: client}
			key, err := provider.KeepAliveWithResponse(context.Background(), "key-old")
			if status == http.StatusOK {
				if err != nil || key != "key-new" {
					t.Fatalf("key=%q err=%v", key, err)
				}
			} else {
				var apiErr *common.APIError
				if !errors.As(err, &apiErr) || apiErr.Code != -1125 || !provider.IsInvalidListenKey(err) {
					t.Fatalf("lost API error: %v", err)
				}
			}
			if calls != 1 {
				t.Fatalf("PUT count=%d", calls)
			}
		})
	}
}
