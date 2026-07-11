package stream

import (
	"context"
	"sync"
	"time"
)

type requestPacer struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newRequestPacer(interval time.Duration) *requestPacer {
	return &requestPacer{interval: interval}
}

func (p *requestPacer) Wait(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.interval <= 0 {
		return nil
	}
	now := time.Now()
	if p.next.Before(now) {
		p.next = now
	}
	wait := time.Until(p.next)
	p.next = p.next.Add(p.interval)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
