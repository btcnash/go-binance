package api

import (
	"context"
	"testing"
)

func BenchmarkNormalizeRequest(b *testing.B) {
	payload := []byte(`{"id":"benchmark","method":"time","params":{}}`)
	request := Request{ID: "benchmark", Method: "time", Payload: payload, Outcome: OutcomeSafe}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := normalizeRequest(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompletePendingSteadyState(b *testing.B) {
	session := &Session{
		pending: make(map[string]*pendingRequest, 1),
		changed: make(chan struct{}),
	}
	pending := &pendingRequest{
		request: Request{ID: "benchmark"},
		slot:    &transportSlot{},
		result:  make(chan pendingResult, 1),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		session.pending[pending.request.ID] = pending
		if !session.completePending(pending, pendingResult{}) {
			b.Fatal("pending request was not completed")
		}
		<-pending.result
	}
}
