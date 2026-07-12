package stream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLatestValuePolicyRejectsOrderedStreams(t *testing.T) {
	_, err := normalizeStreamOptions(StreamSessionOptions{
		Class:                StreamClassPublic,
		DeliveryPolicy:       DeliveryPolicyLatestByStream,
		InitialSubscriptions: []Subscription{DiffDepth("BTCUSDT", DepthSpeed100ms)},
	})
	if !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("error = %v, want ErrInvalidDeliveryPolicy", err)
	}
}

func TestLatestValuePolicyCoalescesBlockedStream(t *testing.T) {
	session := &StreamSession{
		opts:         StreamSessionOptions{DeliveryPolicy: DeliveryPolicyLatestByStream},
		events:       make(chan StreamEvent, 1),
		coalesced:    make(map[string]coalescedEvent),
		coalesceWake: make(chan struct{}, 1),
	}
	session.events <- StreamEvent{Stream: "blocker"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.workers.Add(1)
	go session.coalescingLoop(ctx)

	first := StreamEvent{Stream: "btcusdt@ticker", Data: []byte(`{"c":"1"}`), ReceivedAt: time.Now()}
	latest := StreamEvent{Stream: "btcusdt@ticker", Data: []byte(`{"c":"2"}`), ReceivedAt: time.Now().Add(time.Millisecond)}
	if !session.publishEvent(first) || !session.publishEvent(latest) {
		t.Fatal("latest-value events were rejected")
	}

	<-session.events // release the blocked output
	select {
	case got := <-session.events:
		if string(got.Data) != string(latest.Data) {
			t.Fatalf("delivered data = %s, want latest %s", got.Data, latest.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced event was not delivered")
	}

	cancel()
	session.workers.Wait()
	stats := session.Stats()
	if stats.EventsCoalesced != 2 || stats.EventsReplaced != 1 {
		t.Fatalf("coalescing stats = %+v", stats)
	}
}

func TestLatestValuePolicyDoesNotDeliverStalePendingAfterNewerEvent(t *testing.T) {
	session := &StreamSession{
		opts:         StreamSessionOptions{DeliveryPolicy: DeliveryPolicyLatestByStream},
		events:       make(chan StreamEvent, 2),
		coalesced:    make(map[string]coalescedEvent),
		coalesceWake: make(chan struct{}, 1),
	}
	first := StreamEvent{Stream: "btcusdt@ticker", Data: []byte(`{"c":"1"}`), ReceivedAt: time.Now()}
	latest := StreamEvent{Stream: "btcusdt@ticker", Data: []byte(`{"c":"2"}`), ReceivedAt: time.Now().Add(time.Millisecond)}
	session.coalesceEvent(first)
	if !session.publishEvent(latest) {
		t.Fatal("newer event was rejected")
	}

	ctx, cancel := context.WithCancel(context.Background())
	session.workers.Add(1)
	go session.coalescingLoop(ctx)
	select {
	case got := <-session.events:
		if string(got.Data) != string(latest.Data) {
			t.Fatalf("delivered stale data = %s, want %s", got.Data, latest.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("latest event was not delivered")
	}
	select {
	case extra := <-session.events:
		t.Fatalf("unexpected stale event delivered after latest: %s", extra.Data)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	session.workers.Wait()
}

func TestLatestValuePolicyRotatesReplacedStreamBehindOtherPendingStreams(t *testing.T) {
	session := &StreamSession{
		coalesced: make(map[string]coalescedEvent),
	}
	firstA := StreamEvent{Stream: "btcusdt@ticker", Data: []byte(`{"c":"1"}`)}
	latestA := StreamEvent{Stream: "btcusdt@ticker", Data: []byte(`{"c":"2"}`)}
	firstB := StreamEvent{Stream: "ethusdt@ticker", Data: []byte(`{"c":"3"}`)}
	session.coalesceEvent(firstA)
	session.coalesceEvent(firstB)

	streamName, pending, ok := session.peekCoalesced()
	if !ok || streamName != firstA.Stream {
		t.Fatalf("first pending stream = %q, ok=%v", streamName, ok)
	}
	session.replacePendingCoalesced(latestA)
	session.completeCoalescedDelivery(streamName, pending.version)

	streamName, _, ok = session.peekCoalesced()
	if !ok || streamName != firstB.Stream {
		t.Fatalf("next pending stream = %q, want %q", streamName, firstB.Stream)
	}
}
