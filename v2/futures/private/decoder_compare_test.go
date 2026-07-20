package private

import (
	"encoding/json"
	"testing"

	"github.com/btcnash/go-binance/v2/futures"
)

func legacyAccountUpdateUnmarshal(data []byte, event *futures.WsUserDataEvent) error {
	var header struct {
		Event           futures.UserDataEventType `json:"e"`
		Time            int64                     `json:"E"`
		TransactionTime int64                     `json:"T"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}
	event.Event = header.Event
	event.Time = header.Time
	event.TransactionTime = header.TransactionTime
	return json.Unmarshal(data, &event.WsUserDataAccountUpdate)
}

func BenchmarkLegacyWsUserDataEventUnmarshalAccountUpdate(b *testing.B) {
	payload := []byte(`{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"m":"ORDER","B":[],"P":[]}}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var event futures.WsUserDataEvent
		if err := legacyAccountUpdateUnmarshal(payload, &event); err != nil {
			b.Fatal(err)
		}
	}
}
