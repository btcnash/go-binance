package stream

import (
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
)

func TestHandleFrameTransfersManagedRawBuffer(t *testing.T) {
	session := &StreamSession{
		events: make(chan StreamEvent, 1),
		errors: make(chan StreamErrorEvent, 1),
		gaps:   make(chan GapEvent, 1),
	}
	payload := []byte(`{"e":"bookTicker","s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}`)
	session.handleFrame(managedws.Frame{
		Generation: 3,
		Type:       managedws.TextMessage,
		Payload:    payload,
		ReceivedAt: time.Now(),
	})
	event := <-session.events
	if &event.Raw[0] != &payload[0] {
		t.Fatal("stream session copied the managed frame raw payload")
	}
	if &event.Data[0] != &payload[0] {
		t.Fatal("unwrapped stream event did not reuse the raw payload")
	}
}
