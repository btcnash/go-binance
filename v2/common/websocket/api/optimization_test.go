package api

import (
	"context"
	"errors"
	"testing"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	"github.com/gorilla/websocket"
)

func TestNormalizeRequestRetainsPayloadBuffer(t *testing.T) {
	payload := []byte(`{"id":"same-buffer","method":"time","params":{}}`)
	normalized, err := normalizeRequest(context.Background(), Request{
		ID:      "same-buffer",
		Method:  "time",
		Payload: payload,
		Outcome: OutcomeSafe,
	})
	if err != nil {
		t.Fatalf("normalizeRequest: %v", err)
	}
	if &normalized.Payload[0] != &payload[0] {
		t.Fatal("normalizeRequest copied a payload that managed transport copies before enqueue")
	}
}

func TestHandleFrameTransfersManagedPayloadBuffer(t *testing.T) {
	resultC := make(chan pendingResult, 1)
	slot := &transportSlot{transportGeneration: 7, apiGeneration: 11, ready: true}
	pending := &pendingRequest{
		request:             Request{ID: "response-1", Method: "time", Outcome: OutcomeSafe},
		slot:                slot,
		transportGeneration: 7,
		apiGeneration:       11,
		result:              resultC,
	}
	session := &Session{
		pending:      map[string]*pendingRequest{"response-1": pending},
		changed:      make(chan struct{}),
		drainChanged: make(chan struct{}, 1),
		errors:       make(chan ErrorEvent, 1),
		unsolicited:  make(chan UnsolicitedFrame, 1),
		observations: make(chan observation, 1),
		observerDone: make(chan struct{}),
	}
	payload := []byte(`{"id":"response-1","status":200,"result":{"ok":true}}`)
	session.handleFrame(slot, managedws.Frame{
		Generation: 7,
		Type:       managedws.TextMessage,
		Payload:    payload,
		ReceivedAt: time.Now(),
	})
	result := <-resultC
	if result.err != nil {
		t.Fatalf("handleFrame result: %v", result.err)
	}
	if &result.response.Payload[0] != &payload[0] {
		t.Fatal("handleFrame copied a managed frame payload")
	}
}

func TestCompletedRequestDoesNotNotifyLifecycleWaiters(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		writeAPIResponse(t, conn, req.ID, "ok")
	})
	session := newTestSession(t, server.endpoint, nil)
	startReady(t, session)

	session.mu.Lock()
	before := session.changed
	session.mu.Unlock()

	if _, err := session.Do(context.Background(), Request{ID: "steady", Method: "time", Payload: requestPayload(t, "steady", "time"), Outcome: OutcomeSafe}); err != nil {
		t.Fatalf("Do: %v", err)
	}

	session.mu.Lock()
	after := session.changed
	session.mu.Unlock()
	if before != after {
		t.Fatal("steady-state request churned the lifecycle notification channel")
	}
}

func TestRequestIDHistoryCapacityFailsClosedWithoutRotation(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		writeAPIResponse(t, conn, req.ID, "ok")
	})
	session := newTestSession(t, server.endpoint, func(o *Options) {
		o.MaxRequestIDsPerGeneration = 2
		o.Rotation.Enabled = false
	})
	startReady(t, session)
	for _, id := range []string{"id-1", "id-2"} {
		if _, err := session.Do(context.Background(), Request{ID: id, Method: "time", Payload: requestPayload(t, id, "time"), Outcome: OutcomeSafe}); err != nil {
			t.Fatalf("request %s: %v", id, err)
		}
	}
	_, err := session.Do(context.Background(), Request{ID: "id-3", Method: "time", Payload: requestPayload(t, "id-3", "time"), Outcome: OutcomeSafe})
	if !errors.Is(err, ErrRequestIDCapacity) {
		t.Fatalf("third request error = %v, want ErrRequestIDCapacity", err)
	}
}

func TestRequestIDHistoryCapacityProactivelyRotates(t *testing.T) {
	server := newAPITestServer(t, func(_ *apiTestServer, _ int64, conn *websocket.Conn, req apiWireRequest) {
		writeAPIResponse(t, conn, req.ID, "ok")
	})
	session := newTestSession(t, server.endpoint, func(o *Options) {
		o.MaxRequestIDsPerGeneration = 2
		o.Rotation = RotationOptions{Enabled: true, MaxAge: time.Hour, DrainTimeout: time.Second}
	})
	startReady(t, session)
	firstGeneration := session.Generation()
	for _, id := range []string{"rotate-1", "rotate-2"} {
		if _, err := session.Do(context.Background(), Request{ID: id, Method: "time", Payload: requestPayload(t, id, "time"), Outcome: OutcomeSafe}); err != nil {
			t.Fatalf("request %s: %v", id, err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for session.Generation() == firstGeneration && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if session.Generation() == firstGeneration {
		t.Fatalf("request ID history did not trigger proactive rotation from generation %d", firstGeneration)
	}
	if _, err := session.Do(context.Background(), Request{ID: "rotate-3", Method: "time", Payload: requestPayload(t, "rotate-3", "time"), Outcome: OutcomeSafe}); err != nil {
		t.Fatalf("request after proactive rotation: %v", err)
	}
}

func TestNormalizeOptionsRejectsNegativeRequestIDCapacity(t *testing.T) {
	_, err := normalizeOptions(Options{MaxRequestIDsPerGeneration: -1})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("normalizeOptions error = %v, want ErrInvalidOptions", err)
	}
}

func TestNormalizeOptionsDefaultsRequestIDCapacity(t *testing.T) {
	options, err := normalizeOptions(Options{})
	if err != nil {
		t.Fatalf("normalizeOptions: %v", err)
	}
	if options.MaxRequestIDsPerGeneration != defaultMaxRequestIDsPerGeneration {
		t.Fatalf("request ID capacity = %d, want %d", options.MaxRequestIDsPerGeneration, defaultMaxRequestIDsPerGeneration)
	}
}

func TestWaitRotationTriggerIgnoresStaleSlotSignal(t *testing.T) {
	session := &Session{rotationNow: make(chan *transportSlot, 1)}
	stale := &transportSlot{id: 1}
	active := &transportSlot{id: 2}
	session.rotationNow <- stale

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !session.waitRotationTrigger(ctx, active, 20*time.Millisecond) {
		t.Fatal("rotation trigger was canceled")
	}
	if elapsed := time.Since(started); elapsed < 10*time.Millisecond {
		t.Fatalf("stale slot signal triggered rotation after %s", elapsed)
	}
}
