package managed

import (
	"context"
	"testing"
	"time"
)

func TestConnectionMaxAgeProactivelyReconnects(t *testing.T) {
	first := newFakeSocket()
	second := newFakeSocket()
	dialer := &sequenceDialer{sockets: []Socket{first, second}}

	conn := mustNewConnection(t, dialer, Options{
		Heartbeat:        HeartbeatOptions{Enabled: false},
		Reconnect:        ReconnectPolicy{Enabled: true, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1},
		MaxConnectionAge: 20 * time.Millisecond,
	})
	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForGenerationReady(t, conn.States(), 2, time.Second)
	if first.CloseCount() == 0 {
		t.Fatal("first socket was not closed after max age")
	}
	waitForErrorKind(t, conn.Errors(), ErrorMaxAgeReached, time.Second)
}

func TestConnectionRejectsNegativeMaxAge(t *testing.T) {
	_, err := NewConnection(Options{
		Dialer:           &sequenceDialer{sockets: []Socket{newFakeSocket()}},
		MaxConnectionAge: -time.Second,
	})
	if err == nil {
		t.Fatal("NewConnection() error = nil, want invalid options")
	}
}
