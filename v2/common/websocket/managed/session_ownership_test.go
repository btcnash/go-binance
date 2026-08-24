package managed

import (
	"context"
	"testing"
	"time"
)

func TestConnectionFrameDeliveryTransfersReadPayloadWithoutCopy(t *testing.T) {
	socket := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat:   HeartbeatOptions{Enabled: false},
		Reconnect:   ReconnectPolicy{Enabled: false},
		FrameBuffer: 1,
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)

	payload := []byte(`{"e":"bookTicker","s":"BTCUSDT"}`)
	socket.readCh <- fakeRead{messageType: TextMessage, payload: payload}

	select {
	case frame := <-conn.Frames():
		if string(frame.Payload) != string(payload) {
			t.Fatalf("payload = %q, want %q", frame.Payload, payload)
		}
		if frame.Generation != 1 {
			t.Fatalf("frame generation = %d, want 1", frame.Generation)
		}
		if len(frame.Payload) == 0 || &frame.Payload[0] != &payload[0] {
			t.Fatal("frame payload was copied; expected ownership transfer")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for frame")
	}
}
