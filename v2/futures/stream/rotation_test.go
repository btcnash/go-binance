package stream

import (
	"context"
	"testing"
	"time"

	managedws "github.com/adshao/go-binance/v2/common/websocket/managed"
)

func TestStreamSessionDefaultsProactiveRotation(t *testing.T) {
	opts, err := normalizeStreamOptions(StreamSessionOptions{
		Class:             StreamClassPublic,
		ConnectionOptions: managedws.Options{Dialer: testDialer{}},
	})
	if err != nil {
		t.Fatalf("normalizeStreamOptions() error = %v", err)
	}
	want := 23*time.Hour + 50*time.Minute
	if opts.ConnectionOptions.MaxConnectionAge != want {
		t.Fatalf("max age = %s, want %s", opts.ConnectionOptions.MaxConnectionAge, want)
	}
}

func TestStreamSessionCanDisableProactiveRotation(t *testing.T) {
	opts, err := normalizeStreamOptions(StreamSessionOptions{
		Class:             StreamClassPublic,
		DisableRotation:   true,
		ConnectionOptions: managedws.Options{Dialer: testDialer{}},
	})
	if err != nil {
		t.Fatalf("normalizeStreamOptions() error = %v", err)
	}
	if opts.ConnectionOptions.MaxConnectionAge != 0 {
		t.Fatalf("max age = %s, want disabled", opts.ConnectionOptions.MaxConnectionAge)
	}
}

type testDialer struct{}

func (testDialer) Dial(context.Context) (managedws.Socket, error) { return nil, nil }
