package m5compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/btcnash/go-binance/v2/futures"
	"github.com/gorilla/websocket"
)

func TestLegacyFuturesStreamUsesManagedReconnect(t *testing.T) {
	var connections atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		n := connections.Add(1)
		_, requestPayload, err := conn.ReadMessage()
		if err != nil {
			_ = conn.Close()
			return
		}
		var request struct {
			ID uint64 `json:"id"`
		}
		if err := json.Unmarshal(requestPayload, &request); err != nil {
			_ = conn.Close()
			return
		}
		ack := []byte(fmt.Sprintf(`{"result":null,"id":%d}`, request.ID))
		_ = conn.WriteMessage(websocket.TextMessage, ack)
		payload := []byte(fmt.Sprintf(`{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":%d,"s":"BTCUSDT","a":%d,"p":"1","q":"1","f":1,"l":1,"T":1,"m":false}}`, n, n))
		_ = conn.WriteMessage(websocket.TextMessage, payload)
		if n == 1 {
			_ = conn.Close()
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	oldBase := futures.BaseWsMarketMainUrl
	oldTestnet, oldDemo := futures.UseTestnet, futures.UseDemo
	oldKeepalive := futures.WebsocketKeepalive
	futures.BaseWsMarketMainUrl = "ws" + strings.TrimPrefix(server.URL, "http") + "/market/ws"
	futures.UseTestnet, futures.UseDemo = false, false
	futures.WebsocketKeepalive = false
	defer func() {
		futures.BaseWsMarketMainUrl = oldBase
		futures.UseTestnet, futures.UseDemo = oldTestnet, oldDemo
		futures.WebsocketKeepalive = oldKeepalive
	}()

	events := make(chan int64, 2)
	doneC, stopC, err := futures.WsAggTradeServe("BTCUSDT", func(event *futures.WsAggTradeEvent) {
		events <- event.AggregateTradeID
	}, func(error) {})
	if err != nil {
		t.Fatalf("WsAggTradeServe() error = %v", err)
	}

	for _, want := range []int64{1, 2} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("aggregate trade id = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for event %d", want)
		}
	}
	stopC <- struct{}{}
	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatal("legacy done channel did not close")
	}
}

func TestLegacyCombinedStreamPreservesCombinedEnvelope(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, requestPayload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var request struct {
			ID     uint64   `json:"id"`
			Params []string `json:"params"`
		}
		if err := json.Unmarshal(requestPayload, &request); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"result":null,"id":%d}`, request.ID)))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"stream":"ethusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"ETHUSDT","a":7,"p":"1","q":"1","f":1,"l":1,"T":1,"m":false}}`))
	}))
	defer server.Close()

	oldBase := futures.BaseCombinedMarketMainURL
	oldTestnet, oldDemo := futures.UseTestnet, futures.UseDemo
	oldKeepalive := futures.WebsocketKeepalive
	futures.BaseCombinedMarketMainURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/market/stream?streams="
	futures.UseTestnet, futures.UseDemo = false, false
	futures.WebsocketKeepalive = false
	defer func() {
		futures.BaseCombinedMarketMainURL = oldBase
		futures.UseTestnet, futures.UseDemo = oldTestnet, oldDemo
		futures.WebsocketKeepalive = oldKeepalive
	}()

	events := make(chan *futures.WsAggTradeEvent, 1)
	doneC, stopC, err := futures.WsCombinedAggTradeServe([]string{"BTCUSDT", "ETHUSDT"}, func(event *futures.WsAggTradeEvent) {
		events <- event
	}, func(error) {})
	if err != nil {
		t.Fatalf("WsCombinedAggTradeServe() error = %v", err)
	}
	select {
	case event := <-events:
		if event.Symbol != "ETHUSDT" || event.AggregateTradeID != 7 {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for combined event")
	}
	close(stopC)
	<-doneC
}

func TestLegacyPrivateStreamUsesManagedReconnect(t *testing.T) {
	var connections atomic.Int32
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		n := connections.Add(1)
		payload := []byte(fmt.Sprintf(`{"e":"ACCOUNT_UPDATE","E":%d,"T":%d,"a":{"m":"ORDER","B":[],"P":[]}}`, n, n))
		_ = conn.WriteMessage(websocket.TextMessage, payload)
		if n == 1 {
			_ = conn.Close()
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	oldBase := futures.BaseWsPrivateMainUrl
	oldTestnet, oldDemo := futures.UseTestnet, futures.UseDemo
	oldKeepalive := futures.WebsocketKeepalive
	futures.BaseWsPrivateMainUrl = "ws" + strings.TrimPrefix(server.URL, "http") + "/private/ws"
	futures.UseTestnet, futures.UseDemo = false, false
	futures.WebsocketKeepalive = false
	defer func() {
		futures.BaseWsPrivateMainUrl = oldBase
		futures.UseTestnet, futures.UseDemo = oldTestnet, oldDemo
		futures.WebsocketKeepalive = oldKeepalive
	}()

	events := make(chan int64, 2)
	doneC, stopC, err := futures.WsUserDataServe("listen-key", func(event *futures.WsUserDataEvent) {
		events <- event.Time
	}, func(error) {})
	if err != nil {
		t.Fatalf("WsUserDataServe() error = %v", err)
	}
	for _, want := range []int64{1, 2} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event time = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for private event %d", want)
		}
	}
	close(stopC)
	<-doneC
}
