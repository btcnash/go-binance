package futures

import (
	"encoding/json"
	"testing"
)

func TestWsKlineEventPresence(t *testing.T) {
	tests := []struct {
		name           string
		x              string
		isFinal        bool
		isFinalPresent bool
	}{
		{name: "absent"},
		{name: "explicit false", x: `,"x":false`, isFinalPresent: true},
		{name: "explicit true", x: `,"x":true`, isFinal: true, isFinalPresent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event WsKlineEvent
			payload := []byte(`{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":1,"T":2,"s":"BTCUSDT","i":"1m"` + tt.x + `}}`)
			if err := json.Unmarshal(payload, &event); err != nil {
				t.Fatalf("UnmarshalJSON: %v", err)
			}
			if event.Kline.IsFinal != tt.isFinal || event.Kline.IsFinalPresent != tt.isFinalPresent {
				t.Fatalf("x = %v present=%v, want %v present=%v", event.Kline.IsFinal, event.Kline.IsFinalPresent, tt.isFinal, tt.isFinalPresent)
			}
		})
	}
}

func TestWsKlineEventPresenceTypeError(t *testing.T) {
	var event WsKlineEvent
	if err := json.Unmarshal([]byte(`{"e":"kline","k":{"x":"false"}}`), &event); err == nil {
		t.Fatal("expected x type error")
	}
}
