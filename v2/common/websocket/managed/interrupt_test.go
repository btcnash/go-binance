package managed

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectionInterruptReconnectsCurrentGeneration(t *testing.T) {
	first := newFakeSocket()
	second := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{first, second}}, Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
		Reconnect: ReconnectPolicy{
			Enabled:      true,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Millisecond,
			Multiplier:   1,
		},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForGenerationReady(t, conn.States(), 1, time.Second)

	cause := errors.New("upper layer protocol stalled")
	if err := conn.Interrupt(cause); err != nil {
		t.Fatalf("Interrupt() error = %v", err)
	}

	waitForGenerationReady(t, conn.States(), 2, time.Second)
	event := waitForErrorKind(t, conn.Errors(), ErrorInterrupted, time.Second)
	if !errors.Is(event.Err, cause) {
		t.Fatalf("interrupt error = %v, want cause %v", event.Err, cause)
	}
}

func TestConnectionInterruptRequiresReadySession(t *testing.T) {
	conn := mustNewConnection(t, DialFunc(func(context.Context) (Socket, error) {
		return nil, errors.New("dial blocked")
	}), Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
		Reconnect: ReconnectPolicy{Enabled: false},
	})

	if err := conn.Interrupt(errors.New("test")); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Interrupt() error = %v, want ErrNotReady", err)
	}
}
