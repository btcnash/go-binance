package private

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/btcnash/go-binance/v2/common"
	"github.com/btcnash/go-binance/v2/futures"
)

func TestRESTListenKeyProviderLifecycle(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	var values []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		form, err := url.ParseQuery(string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		value := form.Get("listenKey")
		if value == "" {
			value = r.URL.Query().Get("listenKey")
		}
		mu.Lock()
		methods = append(methods, r.Method)
		values = append(values, value)
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
	if values[0] != "" || values[1] != key || values[2] != key {
		t.Fatalf("listenKey form values = %v", values)
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
	if err := provider.Release(context.Background(), "key"); !errors.Is(err, ErrListenKeyRelease) {
		t.Fatalf("Release() error = %v", err)
	}
}
