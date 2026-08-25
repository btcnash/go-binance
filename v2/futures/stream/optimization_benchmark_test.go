package stream

import (
	"encoding/json"
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

func BenchmarkManagedBookTickerGenericDTO(b *testing.B) {
	session := &StreamSession{
		events: make(chan StreamEvent, 1),
		errors: make(chan StreamErrorEvent, 1),
		gaps:   make(chan GapEvent, 1),
	}
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BTCUSDT","b":"100.1","B":"2.3","a":"100.2","A":"4.5"}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		event := <-session.events
		var dto WsBookTickerEvent
		if err := json.Unmarshal(event.Data, &dto); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManagedBookTickerTypedDTO(b *testing.B) {
	session := newTypedTestSession(TypedDeliveryBookTicker, DeliveryPolicyStrict, 1)
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BTCUSDT","b":"100.1","B":"2.3","a":"100.2","A":"4.5"}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		event := <-session.typedEvents
		if event.BookTicker.UpdateID == 0 {
			b.Fatal("decoded zero bookTicker")
		}
	}
}

func BenchmarkManagedAggTradeGenericDTO(b *testing.B) {
	session := &StreamSession{
		events: make(chan StreamEvent, 1),
		errors: make(chan StreamErrorEvent, 1),
		gaps:   make(chan GapEvent, 1),
	}
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":123456789,"s":"BTCUSDT","a":5933014,"p":"0.001","q":"100","nq":"0.014","f":100,"l":105,"T":123456785,"m":true,"st":1}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		event := <-session.events
		var dto WsAggTradeEvent
		if err := json.Unmarshal(event.Data, &dto); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManagedAggTradeTypedDTO(b *testing.B) {
	session := newTypedTestSession(TypedDeliveryAggTrade, DeliveryPolicyStrict, 1)
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":123456789,"s":"BTCUSDT","a":5933014,"p":"0.001","q":"100","nq":"0.014","f":100,"l":105,"T":123456785,"m":true,"st":1}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		event := <-session.typedEvents
		if event.AggTrade.AggregateTradeID == 0 || !event.AggTrade.NormalQuantityPresent || !event.AggTrade.SymbolTypePresent {
			b.Fatal("decoded invalid aggTrade")
		}
	}
}

func BenchmarkManagedKlineGenericDTO(b *testing.B) {
	session := &StreamSession{
		events: make(chan StreamEvent, 1),
		errors: make(chan StreamErrorEvent, 1),
		gaps:   make(chan GapEvent, 1),
	}
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1499404907056,"s":"BTCUSDT","k":{"t":1499404860000,"T":1499404919999,"s":"BTCUSDT","i":"1m","f":77462,"L":77465,"o":"0.10278577","c":"0.10278645","h":"0.10278712","l":"0.10278518","v":"17.47929838","n":4,"x":false,"q":"1.79662878","V":"2.34879839","Q":"0.24142166"}}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		event := <-session.events
		var dto WsKlineEvent
		if err := json.Unmarshal(event.Data, &dto); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManagedKlineTypedDTO(b *testing.B) {
	session := newTypedTestSession(TypedDeliveryKline, DeliveryPolicyStrict, 1)
	frame := managedws.Frame{
		Generation: 1,
		Type:       managedws.TextMessage,
		Payload:    []byte(`{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1499404907056,"s":"BTCUSDT","k":{"t":1499404860000,"T":1499404919999,"s":"BTCUSDT","i":"1m","f":77462,"L":77465,"o":"0.10278577","c":"0.10278645","h":"0.10278712","l":"0.10278518","v":"17.47929838","n":4,"x":false,"q":"1.79662878","V":"2.34879839","Q":"0.24142166"}}}`),
		ReceivedAt: time.Unix(1, 0),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.handleFrame(frame)
		event := <-session.typedEvents
		if event.Kline.Kline.Interval == "" || !event.Kline.Kline.IsFinalPresent {
			b.Fatal("decoded invalid kline")
		}
	}
}
