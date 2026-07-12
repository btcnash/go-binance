package managed

import (
    "errors"
    "testing"
)

func TestStatsSnapshotTracksDroppedObservations(t *testing.T) {
    c := &Connection{
        opts: Options{Observer: ObserverFuncs{}},
        states: make(chan StateEvent, 1),
        heartbeats: make(chan HeartbeatEvent, 1),
        errors: make(chan ErrorEvent, 1),
        observations: make(chan observation, 1),
    }

    c.transition(StateConnecting, ReasonStart, 0, nil)
    c.transition(StateConnected, ReasonDialSucceeded, 0, nil)
    c.emitHeartbeat(HeartbeatEvent{})
    c.emitHeartbeat(HeartbeatEvent{})
    c.emitError(connectionError(ErrorRead, 1, "read", errors.New("boom")))
    c.emitError(connectionError(ErrorRead, 1, "read", errors.New("boom")))
    c.publishObservation(observation{kind: observationState})
    c.publishObservation(observation{kind: observationState})

    stats := c.Stats()
    if stats.StateEventsDropped != 1 {
        t.Fatalf("StateEventsDropped = %d, want 1", stats.StateEventsDropped)
    }
    if stats.HeartbeatEventsDropped != 1 {
        t.Fatalf("HeartbeatEventsDropped = %d, want 1", stats.HeartbeatEventsDropped)
    }
    if stats.ErrorEventsDropped != 1 {
        t.Fatalf("ErrorEventsDropped = %d, want 1", stats.ErrorEventsDropped)
    }
    if stats.ObserverEventsDropped == 0 {
        t.Fatal("ObserverEventsDropped was not recorded")
    }
}
