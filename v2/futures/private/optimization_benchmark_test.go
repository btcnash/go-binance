package private

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	"github.com/btcnash/go-binance/v2/futures"
)

func BenchmarkHandleSharedAccountEvent100Sources(b *testing.B) {
	bindings := make([]sourceBinding, 0, 100)
	for i := 0; i < 100; i++ {
		event := futures.UserDataEventTypeOrderTradeUpdate
		if i == 0 {
			event = futures.UserDataEventTypeAccountUpdate
		}
		bindings = append(bindings, sourceBinding{
			SourceID:  fmt.Sprintf("account-%03d", i),
			ListenKey: fmt.Sprintf("key-%03d", i),
			Events:    []futures.UserDataEventType{event},
			eventSet:  map[futures.UserDataEventType]struct{}{event: {}},
		})
	}
	session := &Session{
		generation:      1,
		currentBindings: cloneBindings(bindings),
		events:          make(chan Event, 1),
		states:          make(chan StateEvent, 1),
		errors:          make(chan ErrorEvent, 1),
		gaps:            make(chan GapEvent, 1),
		listenKeys:      make(chan ListenKeyEvent, 1),
		observations:    make(chan observation, 1),
	}
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"m":"ORDER","B":[],"P":[]}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		<-session.events
	}
}

func BenchmarkWsUserDataEventUnmarshalAccountUpdate(b *testing.B) {
	payload := []byte(`{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"m":"ORDER","B":[],"P":[]}}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var event futures.WsUserDataEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			b.Fatal(err)
		}
	}
}
