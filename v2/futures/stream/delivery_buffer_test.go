package stream

import "testing"

func TestStreamSessionAllocatesEventBufferForSelectedDeliveryMode(t *testing.T) {
	const eventBuffer = 7

	tests := []struct {
		name          string
		class         StreamClass
		typedDelivery TypedDeliveryMode
		wantEventsCap int
		wantTypedCap  int
	}{
		{
			name:          "generic",
			class:         StreamClassMarket,
			wantEventsCap: eventBuffer,
		},
		{
			name:          "typed book ticker",
			class:         StreamClassPublic,
			typedDelivery: TypedDeliveryBookTicker,
			wantTypedCap:  eventBuffer,
		},
		{
			name:          "typed aggregate trade",
			class:         StreamClassMarket,
			typedDelivery: TypedDeliveryAggTrade,
			wantTypedCap:  eventBuffer,
		},
		{
			name:          "typed kline",
			class:         StreamClassMarket,
			typedDelivery: TypedDeliveryKline,
			wantTypedCap:  eventBuffer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session, err := NewStreamSession(StreamSessionOptions{
				Class:         tt.class,
				EventBuffer:   eventBuffer,
				TypedDelivery: tt.typedDelivery,
			})
			if err != nil {
				t.Fatalf("NewStreamSession() error = %v", err)
			}

			if got := cap(session.Events()); got != tt.wantEventsCap {
				t.Errorf("cap(Events()) = %d, want %d", got, tt.wantEventsCap)
			}
			if got := cap(session.TypedEvents()); got != tt.wantTypedCap {
				t.Errorf("cap(TypedEvents()) = %d, want %d", got, tt.wantTypedCap)
			}

			if err := session.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			assertClosedEventChannel(t, "Events()", session.Events())
			assertClosedEventChannel(t, "TypedEvents()", session.TypedEvents())
		})
	}
}

func assertClosedEventChannel[T any](t *testing.T, name string, events <-chan T) {
	t.Helper()
	if _, ok := <-events; ok {
		t.Fatalf("%s remained open after Close()", name)
	}
}
