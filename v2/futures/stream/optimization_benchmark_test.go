package stream

import (
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
)

func BenchmarkHandleUnwrappedEvent(b *testing.B) {
	session := &StreamSession{
		events: make(chan StreamEvent, 1),
		errors: make(chan StreamErrorEvent, 1),
		gaps:   make(chan GapEvent, 1),
	}
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"e":"bookTicker","s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		<-session.events
	}
}

func BenchmarkHandleCombinedEvent(b *testing.B) {
	session := &StreamSession{
		events: make(chan StreamEvent, 1),
		errors: make(chan StreamErrorEvent, 1),
		gaps:   make(chan GapEvent, 1),
	}
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		<-session.events
	}
}
