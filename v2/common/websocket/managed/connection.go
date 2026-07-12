package managed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPingInterval    = 5 * time.Second
	defaultPongTimeout     = 3 * time.Second
	defaultWriteTimeout    = 2 * time.Second
	defaultReconnectMin    = 100 * time.Millisecond
	defaultReconnectMax    = 10 * time.Second
	defaultReconnectFactor = 2.0
	defaultStableReset     = time.Minute
	defaultWriteQueue      = 128
	defaultFrameBuffer     = 256
	defaultEventBuffer     = 64
)

// Connection owns one logical WebSocket connection across physical reconnects.
type Connection struct {
	// Keep 64-bit atomics first for correct alignment on 32-bit platforms.
	reconnectCount uint64

	opts Options

	lifecycleMu sync.Mutex
	started     bool
	closed      bool
	cancel      context.CancelFunc
	done        chan struct{}

	stateMu     sync.RWMutex
	state       State
	generation  uint64
	current     *physicalSession
	terminalErr error

	states     chan StateEvent
	frames     chan Frame
	heartbeats chan HeartbeatEvent
	errors     chan ErrorEvent
	firstReady chan struct{}
	readyOnce  sync.Once

	observations chan observation
	observerDone chan struct{}
}

// NewConnection validates options and creates an idle managed connection.
func NewConnection(opts Options) (*Connection, error) {
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Connection{
		opts:         normalized,
		done:         make(chan struct{}),
		state:        StateIdle,
		states:       make(chan StateEvent, normalized.StateBuffer),
		frames:       make(chan Frame, normalized.FrameBuffer),
		heartbeats:   make(chan HeartbeatEvent, normalized.HeartbeatBuffer),
		errors:       make(chan ErrorEvent, normalized.ErrorBuffer),
		firstReady:   make(chan struct{}),
		observations: make(chan observation, normalized.ObserverBuffer),
		observerDone: make(chan struct{}),
	}, nil
}

func normalizeOptions(opts Options) (Options, error) {
	if opts.MaxConnectionAge < 0 {
		return Options{}, fmt.Errorf("%w: max connection age must not be negative", ErrInvalidOptions)
	}
	if opts.Dialer == nil {
		return Options{}, fmt.Errorf("%w: dialer is required", ErrInvalidOptions)
	}

	if opts.Heartbeat.Enabled {
		allZero := opts.Heartbeat.PingInterval == 0 && opts.Heartbeat.PongTimeout == 0 && opts.Heartbeat.WriteTimeout == 0
		if allZero {
			opts.Heartbeat.PingInterval = defaultPingInterval
			opts.Heartbeat.PongTimeout = defaultPongTimeout
			opts.Heartbeat.WriteTimeout = defaultWriteTimeout
		} else if opts.Heartbeat.PingInterval <= 0 || opts.Heartbeat.PongTimeout <= 0 || opts.Heartbeat.WriteTimeout <= 0 {
			return Options{}, fmt.Errorf("%w: enabled heartbeat requires positive ping, pong, and write durations", ErrInvalidOptions)
		}
	} else {
		if opts.Heartbeat.PingInterval < 0 || opts.Heartbeat.PongTimeout < 0 || opts.Heartbeat.WriteTimeout < 0 {
			return Options{}, fmt.Errorf("%w: heartbeat durations must not be negative", ErrInvalidOptions)
		}
		if opts.Heartbeat.WriteTimeout == 0 {
			opts.Heartbeat.WriteTimeout = defaultWriteTimeout
		}
	}

	if opts.Reconnect.InitialDelay < 0 || opts.Reconnect.MaxDelay < 0 || opts.Reconnect.Multiplier < 0 || opts.Reconnect.Jitter < 0 || opts.Reconnect.Jitter > 1 || opts.Reconnect.MaxAttempts < 0 || opts.Reconnect.StableResetTime < 0 {
		return Options{}, fmt.Errorf("%w: invalid reconnect policy", ErrInvalidOptions)
	}
	if opts.Reconnect.Enabled {
		if opts.Reconnect.InitialDelay == 0 {
			opts.Reconnect.InitialDelay = defaultReconnectMin
		}
		if opts.Reconnect.MaxDelay == 0 {
			opts.Reconnect.MaxDelay = defaultReconnectMax
		}
		if opts.Reconnect.MaxDelay < opts.Reconnect.InitialDelay {
			return Options{}, fmt.Errorf("%w: reconnect max delay must be >= initial delay", ErrInvalidOptions)
		}
		if opts.Reconnect.Multiplier == 0 {
			opts.Reconnect.Multiplier = defaultReconnectFactor
		}
		if opts.Reconnect.Multiplier < 1 {
			return Options{}, fmt.Errorf("%w: reconnect multiplier must be >= 1", ErrInvalidOptions)
		}
		if opts.Reconnect.StableResetTime == 0 {
			opts.Reconnect.StableResetTime = defaultStableReset
		}
	}

	if opts.WriteQueue < 0 || opts.FrameBuffer < 0 || opts.StateBuffer < 0 || opts.HeartbeatBuffer < 0 || opts.ErrorBuffer < 0 || opts.ObserverBuffer < 0 {
		return Options{}, fmt.Errorf("%w: buffer sizes must not be negative", ErrInvalidOptions)
	}
	if opts.WriteQueue == 0 {
		opts.WriteQueue = defaultWriteQueue
	}
	if opts.FrameBuffer == 0 {
		opts.FrameBuffer = defaultFrameBuffer
	}
	if opts.StateBuffer == 0 {
		opts.StateBuffer = defaultEventBuffer
	}
	if opts.HeartbeatBuffer == 0 {
		opts.HeartbeatBuffer = defaultEventBuffer
	}
	if opts.ErrorBuffer == 0 {
		opts.ErrorBuffer = defaultEventBuffer
	}
	if opts.ObserverBuffer == 0 {
		opts.ObserverBuffer = defaultEventBuffer
	}
	return opts, nil
}

// Start launches the managed lifecycle. It is non-blocking; callers can use
// WaitReady or States to observe the first successful connection.
func (c *Connection) Start(parent context.Context) error {
	if parent == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOptions)
	}
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.started {
		return ErrAlreadyStarted
	}
	ctx, cancel := context.WithCancel(parent)
	c.started = true
	c.cancel = cancel
	go c.observerLoop()
	go c.run(ctx)
	return nil
}

// WaitReady waits for the first physical connection to become ready.
func (c *Connection) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.firstReady:
		return nil
	case <-c.done:
		if c.State() == StateReady {
			return nil
		}
		if err := c.TerminalError(); err != nil {
			return err
		}
		return ErrClosed
	}
}

// Close permanently stops the logical connection. It is idempotent.
func (c *Connection) Close() error {
	c.lifecycleMu.Lock()
	if c.closed {
		c.lifecycleMu.Unlock()
		return nil
	}
	c.closed = true
	cancel := c.cancel
	started := c.started
	c.lifecycleMu.Unlock()

	if started {
		c.stateMu.RLock()
		session := c.current
		c.stateMu.RUnlock()
		if session != nil {
			ctx, closeCancel := context.WithTimeout(context.Background(), c.opts.Heartbeat.WriteTimeout)
			_ = session.writeControl(ctx, CloseMessage, nil, c.opts.Heartbeat.WriteTimeout)
			closeCancel()
		}
	}
	if cancel != nil {
		cancel()
	}
	if !started {
		c.transition(StateClosed, ReasonUserClosed, 0, nil)
		close(c.observations)
		close(c.observerDone)
		close(c.done)
		close(c.states)
		close(c.frames)
		close(c.heartbeats)
		close(c.errors)
	}
	return nil
}

// Done closes after the lifecycle and all physical-connection goroutines stop.
func (c *Connection) Done() <-chan struct{} { return c.done }

// States returns lifecycle state events.
func (c *Connection) States() <-chan StateEvent { return c.states }

// Frames returns application frames from the current generation.
func (c *Connection) Frames() <-chan Frame { return c.frames }

// Heartbeats returns active and server-initiated heartbeat events.
func (c *Connection) Heartbeats() <-chan HeartbeatEvent { return c.heartbeats }

// Errors returns classified connection errors.
func (c *Connection) Errors() <-chan ErrorEvent { return c.errors }

// State returns the current lifecycle state.
func (c *Connection) State() State {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

// Generation returns the current physical connection generation.
func (c *Connection) Generation() uint64 {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.generation
}

// ReconnectCount returns the number of reconnect dial attempts. The initial
// dial is not counted.
func (c *Connection) ReconnectCount() uint64 {
	return atomic.LoadUint64(&c.reconnectCount)
}

// TerminalError returns the error that ended the connection in StateFailed.
func (c *Connection) TerminalError() error {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.terminalErr
}

// Interrupt fails the current physical connection and lets the configured
// reconnect policy rebuild it. Higher-level protocol sessions use this when
// the transport remains open but its application protocol can no longer be
// trusted, for example after an acknowledgement timeout or event overflow.
func (c *Connection) Interrupt(cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: interrupt cause is required", ErrInvalidOptions)
	}
	c.stateMu.RLock()
	session := c.current
	state := c.state
	generation := c.generation
	c.stateMu.RUnlock()
	if session == nil || state != StateReady {
		return ErrNotReady
	}
	session.fail(connectionError(ErrorInterrupted, generation, "interrupt", cause))
	return nil
}

// SendText writes one application text frame through the single writer loop.
func (c *Connection) SendText(ctx context.Context, payload []byte) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOptions)
	}
	c.stateMu.RLock()
	session := c.current
	state := c.state
	generation := c.generation
	c.stateMu.RUnlock()
	if session == nil || state != StateReady {
		return ErrNotReady
	}
	return c.sendTextOnSession(ctx, session, generation, payload)
}

// SendTextOnGeneration writes a text frame only when generation is still the
// current physical connection generation. Higher-level request protocols use
// this to prevent a request registered for one connection from being written
// to a newly reconnected socket.
func (c *Connection) SendTextOnGeneration(ctx context.Context, generation uint64, payload []byte) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOptions)
	}
	c.stateMu.RLock()
	session := c.current
	state := c.state
	currentGeneration := c.generation
	c.stateMu.RUnlock()
	if session == nil || state != StateReady {
		return ErrNotReady
	}
	if generation == 0 || generation != currentGeneration || session.generation != generation {
		return ErrGenerationChanged
	}
	return c.sendTextOnSession(ctx, session, generation, payload)
}

func (c *Connection) sendTextOnSession(ctx context.Context, session *physicalSession, generation uint64, payload []byte) error {
	if !c.isCurrentGeneration(generation) || session.generation != generation {
		return ErrGenerationChanged
	}
	return session.writeText(ctx, payload)
}

func (c *Connection) run(ctx context.Context) {
	defer func() {
		c.clearCurrent(nil)
		reason := ReasonContextCanceled
		c.lifecycleMu.Lock()
		closedByUser := c.closed
		c.lifecycleMu.Unlock()
		if closedByUser {
			reason = ReasonUserClosed
		}
		if c.State() != StateFailed {
			c.transition(StateClosed, reason, 0, nil)
		}
		close(c.observations)
		<-c.observerDone
		close(c.done)
		close(c.states)
		close(c.frames)
		close(c.heartbeats)
		close(c.errors)
	}()

	backoff := newReconnectBackoff(c.opts.Reconnect)
	reconnectAttempt := 0
	initial := true

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		if initial {
			c.transition(StateConnecting, ReasonStart, 0, nil)
		} else {
			atomic.AddUint64(&c.reconnectCount, 1)
			c.transition(StateReconnecting, ReasonReconnectScheduled, reconnectAttempt, nil)
		}

		socket, err := c.opts.Dialer.Dial(ctx)
		if err != nil {
			wrapped := connectionError(ErrorDial, c.Generation(), "dial", err)
			c.emitError(wrapped)
			if !c.opts.Reconnect.Enabled {
				c.transition(StateFailed, ReasonDialFailed, reconnectAttempt, wrapped)
				return
			}
			reconnectAttempt++
			if c.reconnectExhausted(reconnectAttempt) {
				exhausted := connectionError(ErrorReconnectExhausted, c.Generation(), "reconnect", wrapped)
				c.emitError(exhausted)
				c.transition(StateFailed, ReasonReconnectExhausted, reconnectAttempt, exhausted)
				return
			}
			if !waitContext(ctx, backoff.duration(reconnectAttempt)) {
				return
			}
			initial = false
			continue
		}

		generation := c.nextGeneration()
		c.transition(StateConnected, ReasonDialSucceeded, reconnectAttempt, nil)
		session := newPhysicalSession(c, ctx, generation, socket)
		c.setCurrent(session)
		session.start()
		c.transition(StateReady, ReasonSessionReady, reconnectAttempt, nil)
		c.readyOnce.Do(func() { close(c.firstReady) })

		select {
		case <-ctx.Done():
			session.stop()
			return
		case sessionErr := <-session.failureC:
			session.stop()
			c.clearCurrent(session)
			c.emitError(sessionErr)
			reason := reasonForError(sessionErr)
			c.transition(StateDisconnected, reason, reconnectAttempt, sessionErr)
			if !c.opts.Reconnect.Enabled {
				c.transition(StateFailed, reason, reconnectAttempt, sessionErr)
				return
			}
			if time.Since(session.startedAt) >= c.opts.Reconnect.StableResetTime {
				reconnectAttempt = 0
			}
			reconnectAttempt++
			if c.reconnectExhausted(reconnectAttempt) {
				exhausted := connectionError(ErrorReconnectExhausted, generation, "reconnect", sessionErr)
				c.emitError(exhausted)
				c.transition(StateFailed, ReasonReconnectExhausted, reconnectAttempt, exhausted)
				return
			}
			initial = false
			if !waitContext(ctx, backoff.duration(reconnectAttempt)) {
				return
			}
		}
	}
}

func (c *Connection) reconnectExhausted(attempt int) bool {
	return c.opts.Reconnect.MaxAttempts > 0 && attempt > c.opts.Reconnect.MaxAttempts
}

func (c *Connection) nextGeneration() uint64 {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.generation++
	return c.generation
}

func (c *Connection) setCurrent(session *physicalSession) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.current = session
}

func (c *Connection) clearCurrent(session *physicalSession) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if session == nil || c.current == session {
		c.current = nil
	}
}

func (c *Connection) isCurrentGeneration(generation uint64) bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.current != nil && c.current.generation == generation && c.generation == generation
}

func (c *Connection) transition(next State, reason StateReason, attempt int, err error) {
	c.stateMu.Lock()
	previous := c.state
	c.state = next
	if next == StateFailed {
		c.terminalErr = err
	}
	generation := c.generation
	c.stateMu.Unlock()

	event := StateEvent{
		Previous:   previous,
		State:      next,
		Reason:     reason,
		Generation: generation,
		Attempt:    attempt,
		At:         time.Now(),
		Err:        err,
	}
	select {
	case c.states <- event:
	default:
		// Observation must never block transport liveness.
	}
	c.publishObservation(observation{kind: observationState, state: event})
}

func (c *Connection) emitHeartbeat(event HeartbeatEvent) {
	select {
	case c.heartbeats <- event:
	default:
	}
	c.publishObservation(observation{kind: observationHeartbeat, heartbeat: event})
}

func (c *Connection) emitError(err error) {
	var typed *ConnectionError
	if !errors.As(err, &typed) {
		typed = connectionError(ErrorWrite, c.Generation(), "unknown", err)
	}
	event := ErrorEvent{
		Kind:       typed.Kind,
		Generation: typed.Generation,
		Operation:  typed.Operation,
		At:         time.Now(),
		Err:        typed,
	}
	select {
	case c.errors <- event:
	default:
	}
	c.publishObservation(observation{kind: observationError, err: event})
}

type observationKind uint8

const (
	observationState observationKind = iota + 1
	observationHeartbeat
	observationError
)

type observation struct {
	kind      observationKind
	state     StateEvent
	heartbeat HeartbeatEvent
	err       ErrorEvent
}

func (c *Connection) publishObservation(event observation) {
	if c.opts.Observer == nil {
		return
	}
	select {
	case c.observations <- event:
	default:
	}
}

func (c *Connection) observerLoop() {
	defer close(c.observerDone)
	if c.opts.Observer == nil {
		for range c.observations {
		}
		return
	}
	for event := range c.observations {
		func() {
			defer func() { _ = recover() }()
			switch event.kind {
			case observationState:
				c.opts.Observer.OnState(event.state)
			case observationHeartbeat:
				c.opts.Observer.OnHeartbeat(event.heartbeat)
			case observationError:
				c.opts.Observer.OnError(event.err)
			}
		}()
	}
}

func reasonForError(err error) StateReason {
	switch errorKind(err) {
	case ErrorRead:
		return ReasonReadFailed
	case ErrorPingWrite:
		return ReasonPingWriteFailed
	case ErrorPongTimeout:
		return ReasonPongTimeout
	case ErrorInterrupted:
		return ReasonInterrupted
	case ErrorMaxAgeReached:
		return ReasonMaxAgeReached
	case ErrorFrameBufferFull:
		return ReasonFrameBufferFull
	default:
		return ReasonWriteFailed
	}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
