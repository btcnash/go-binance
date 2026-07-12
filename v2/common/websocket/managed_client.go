package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	apiws "github.com/btcnash/go-binance/v2/common/websocket/api"
)

// ManagedClient adapts the M4 managed API Session to the legacy Client
// interface. It exists for source compatibility; new code should use
// common/websocket/api.Session directly.
type ManagedClient struct {
	session *apiws.Session
	ctx     context.Context
	cancel  context.CancelFunc

	readC chan []byte
	errC  chan error

	mu      sync.Mutex
	pending map[string]struct{}
	count   int64

	reconnectCount int64
	closeOnce      sync.Once
}

// NewManagedClient starts session and returns a legacy Client adapter.
func NewManagedClient(session *apiws.Session) (Client, error) {
	if session == nil {
		return nil, errors.New("ws managed client: nil session")
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := session.Start(ctx); err != nil {
		cancel()
		return nil, err
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, 45*time.Second)
	err := session.WaitReady(readyCtx)
	readyCancel()
	if err != nil {
		cancel()
		_ = session.Close()
		return nil, err
	}
	client := &ManagedClient{
		session: session,
		ctx:     ctx,
		cancel:  cancel,
		readC:   make(chan []byte, 64),
		errC:    make(chan error, 64),
		pending: make(map[string]struct{}),
	}
	go client.observeStates()
	go client.forwardUnsolicited()
	return client, nil
}

func (c *ManagedClient) Write(id string, data []byte) error {
	request, err := legacyAPIRequest(id, data)
	if err != nil {
		return err
	}
	if c.session.State() != apiws.StateReady {
		return apiws.ErrSessionNotReady
	}
	c.mu.Lock()
	if _, exists := c.pending[id]; exists {
		c.mu.Unlock()
		return ErrorWsIdAlreadySent
	}
	c.pending[id] = struct{}{}
	atomic.AddInt64(&c.count, 1)
	c.mu.Unlock()

	go func() {
		defer c.finish(id)
		response, doErr := c.session.Do(c.ctx, request)
		if doErr != nil {
			c.publishError(doErr)
			return
		}
		c.publishRead(response.Payload)
	}()
	return nil
}

func (c *ManagedClient) WriteSync(id string, data []byte, timeout time.Duration) ([]byte, error) {
	request, err := legacyAPIRequest(id, data)
	if err != nil {
		return nil, err
	}
	ctx := c.ctx
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(c.ctx, timeout)
	}
	defer cancel()
	response, err := c.session.Do(ctx, request)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), response.Payload...), nil
}

func legacyAPIRequest(id string, data []byte) (apiws.Request, error) {
	if id == "" || len(data) == 0 {
		return apiws.Request{}, apiws.ErrInvalidRequest
	}
	var envelope struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return apiws.Request{}, err
	}
	if envelope.Method == "" {
		return apiws.Request{}, apiws.ErrInvalidRequest
	}
	return apiws.Request{
		ID:      id,
		Method:  envelope.Method,
		Payload: append([]byte(nil), data...),
		Outcome: legacyOutcome(envelope.Method),
	}, nil
}

func legacyOutcome(method string) apiws.OutcomePolicy {
	switch method {
	case "account.status", "account.balance", "v2/account.status", "v2/account.balance", "order.status", "session.status", "time", "exchangeInfo":
		return apiws.OutcomeSafe
	default:
		return apiws.OutcomeUnknown
	}
}

func (c *ManagedClient) GetReadChannel() <-chan []byte     { return c.readC }
func (c *ManagedClient) GetReadErrorChannel() <-chan error { return c.errC }
func (c *ManagedClient) GetReconnectCount() int64          { return atomic.LoadInt64(&c.reconnectCount) }

func (c *ManagedClient) Wait(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for atomic.LoadInt64(&c.count) > 0 {
		if timeout > 0 && time.Now().After(deadline) {
			return
		}
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *ManagedClient) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.cancel()
		err = c.session.Close()
	})
	return err
}

func (c *ManagedClient) finish(id string) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
	atomic.AddInt64(&c.count, -1)
}

func (c *ManagedClient) publishRead(payload []byte) {
	select {
	case c.readC <- append([]byte(nil), payload...):
	case <-c.ctx.Done():
	}
}

func (c *ManagedClient) publishError(err error) {
	select {
	case c.errC <- err:
	default:
	}
}

func (c *ManagedClient) observeStates() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case event, ok := <-c.session.States():
			if !ok {
				return
			}
			if event.State == apiws.StateReconnecting {
				atomic.AddInt64(&c.reconnectCount, 1)
			}
		}
	}
}

func (c *ManagedClient) forwardUnsolicited() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case frame, ok := <-c.session.Unsolicited():
			if !ok {
				return
			}
			c.publishRead(frame.Payload)
		}
	}
}

var _ Client = (*ManagedClient)(nil)
