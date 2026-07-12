package managed

import (
	"math"
	"math/rand"
	"time"
)

type reconnectBackoff struct {
	policy ReconnectPolicy
	rng    *rand.Rand
}

func newReconnectBackoff(policy ReconnectPolicy) *reconnectBackoff {
	return &reconnectBackoff{
		policy: policy,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (b *reconnectBackoff) duration(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	factor := math.Pow(b.policy.Multiplier, float64(attempt-1))
	delay := time.Duration(float64(b.policy.InitialDelay) * factor)
	if delay > b.policy.MaxDelay {
		delay = b.policy.MaxDelay
	}
	if b.policy.Jitter <= 0 || delay <= 0 {
		return delay
	}
	spread := (b.rng.Float64()*2 - 1) * b.policy.Jitter
	jittered := time.Duration(float64(delay) * (1 + spread))
	if jittered < 0 {
		return 0
	}
	return jittered
}
