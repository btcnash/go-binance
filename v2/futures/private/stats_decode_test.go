package private

import (
    "encoding/json"
    "testing"

    "github.com/btcnash/go-binance/v2/futures"
)

func TestWsUserDataEventUnmarshalResetsReusedReceiver(t *testing.T) {
    event := futures.WsUserDataEvent{
        WsUserDataOrderTradeUpdate: futures.WsUserDataOrderTradeUpdate{
            OrderTradeUpdate: futures.WsOrderTradeUpdate{Symbol: "STALE"},
        },
    }
    payload := []byte(`{"e":"ACCOUNT_UPDATE","E":1,"T":2,"a":{"m":"ORDER","B":[],"P":[]}}`)
    if err := json.Unmarshal(payload, &event); err != nil {
        t.Fatalf("Unmarshal: %v", err)
    }
    if event.OrderTradeUpdate.Symbol != "" {
        t.Fatalf("stale order payload survived reuse: %#v", event.OrderTradeUpdate)
    }
    if event.AccountUpdate.Reason != "ORDER" {
        t.Fatalf("account update = %#v", event.AccountUpdate)
    }
}

func TestPrivateStatsSnapshotIncludesTransport(t *testing.T) {
    session := &Session{}
    session.stats.eventsDelivered.Add(3)
    session.stats.decodeErrors.Add(2)
    stats := session.Stats()
    if stats.EventsDelivered != 3 || stats.DecodeErrors != 2 {
        t.Fatalf("stats = %+v", stats)
    }
}
