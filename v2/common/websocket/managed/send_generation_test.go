package managed

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSendTextOnGenerationRejectsStaleGeneration(t *testing.T) {
	socket := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
		Reconnect: ReconnectPolicy{Enabled: false},
	})
	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)
	generation := conn.Generation()
	if generation == 0 {
		t.Fatal("generation is zero")
	}
	if err := conn.SendTextOnGeneration(context.Background(), generation+1, []byte("stale")); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("error = %v, want ErrGenerationChanged", err)
	}
	if got := socket.TextWrites(); len(got) != 0 {
		t.Fatalf("unexpected writes: %q", got)
	}
}
