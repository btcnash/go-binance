package managed

import (
	"testing"
	"time"
)

func TestReconnectBackoffGrowsAndCaps(t *testing.T) {
	backoff := newReconnectBackoff(ReconnectPolicy{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     25 * time.Millisecond,
		Multiplier:   2,
	})

	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	for i, expected := range want {
		if got := backoff.duration(i + 1); got != expected {
			t.Fatalf("duration(%d) = %s, want %s", i+1, got, expected)
		}
	}
}

func TestReconnectBackoffJitterStaysWithinConfiguredRange(t *testing.T) {
	backoff := newReconnectBackoff(ReconnectPolicy{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   1,
		Jitter:       0.2,
	})

	for i := 0; i < 100; i++ {
		got := backoff.duration(1)
		if got < 80*time.Millisecond || got > 120*time.Millisecond {
			t.Fatalf("jittered duration = %s, want [80ms,120ms]", got)
		}
	}
}
