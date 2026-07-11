package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	managedws "github.com/adshao/go-binance/v2/common/websocket/managed"
)

const (
	defaultRequestTimeout = 5 * time.Second
	defaultBuffer         = 64
	defaultMaxPending     = 1024
	defaultDrainTimeout   = 5 * time.Second
)

type pendingResult struct {
	response Response
	err      error
}

type pendingRequest struct {
	request             Request
	slot                *transportSlot
	transportGeneration uint64
	apiGeneration       uint64
	sentAt              time.Time
	result              chan pendingResult
}

type transportSlot struct {
	id                  uint64
	conn                *managedws.Connection
	transportGeneration uint64
	apiGeneration       uint64
	ready               bool
	readyAt             time.Time
	draining            bool
	closed              bool
	failedErr           error
}

type observation struct {
	state *StateEvent
	err   *ErrorEvent
}

// Session multiplexes concurrent WebSocket API requests by ID across managed
// physical connection generations. Requests are never replayed automatically.
type Session struct {
	opts        Options
	initialConn *managedws.Connection

	lifecycleMu sync.Mutex
	started     bool
	closed      bool
	userClosed  bool
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}

	mu             sync.Mutex
	state          State
	terminalErr    error
	active         *transportSlot
	slots          map[uint64]*transportSlot
	nextSlotID     uint64
	nextGeneration uint64
	pending        map[string]*pendingRequest
	usedIDs        map[uint64]map[string]struct{}
	changed        chan struct{}

	states       chan StateEvent
	errors       chan ErrorEvent
	unsolicited  chan UnsolicitedFrame
	observations chan observation
	observerDone chan struct{}

	workerMu sync.Mutex
	stopping bool
	workers  sync.WaitGroup
}

// NewSession validates options and creates an idle API session.
func NewSession(opts Options) (*Session, error) {
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	conn, err := managedws.NewConnection(normalized.ConnectionOptions)
	if err != nil {
		return nil, fmt.Errorf("%w: create managed connection: %v", ErrInvalidOptions, err)
	}
	return &Session{
		opts:         normalized,
		initialConn:  conn,
		done:         make(chan struct{}),
		state:        StateIdle,
		slots:        make(map[uint64]*transportSlot),
		pending:      make(map[string]*pendingRequest),
		usedIDs:      make(map[uint64]map[string]struct{}),
		changed:      make(chan struct{}),
		states:       make(chan StateEvent, normalized.StateBuffer),
		errors:       make(chan ErrorEvent, normalized.ErrorBuffer),
		unsolicited:  make(chan UnsolicitedFrame, normalized.UnsolicitedBuffer),
		observations: make(chan observation, normalized.ObserverBuffer),
		observerDone: make(chan struct{}),
	}, nil
}

func normalizeOptions(opts Options) (Options, error) {
	if opts.RequestTimeout < 0 || opts.MaxPendingRequests < 0 || opts.UnsolicitedBuffer < 0 || opts.StateBuffer < 0 || opts.ErrorBuffer < 0 || opts.ObserverBuffer < 0 {
		return Options{}, fmt.Errorf("%w: negative timeout or buffer", ErrInvalidOptions)
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = defaultRequestTimeout
	}
	if opts.MaxPendingRequests == 0 {
		opts.MaxPendingRequests = defaultMaxPending
	}
	if opts.UnsolicitedBuffer == 0 {
		opts.UnsolicitedBuffer = defaultBuffer
	}
	if opts.StateBuffer == 0 {
		opts.StateBuffer = defaultBuffer
	}
	if opts.ErrorBuffer == 0 {
		opts.ErrorBuffer = defaultBuffer
	}
	if opts.ObserverBuffer == 0 {
		opts.ObserverBuffer = defaultBuffer
	}
	if opts.Rotation.Enabled {
		if opts.Rotation.MaxAge <= 0 || opts.Rotation.DrainTimeout < 0 {
			return Options{}, fmt.Errorf("%w: enabled rotation requires positive max age", ErrInvalidOptions)
		}
		if opts.Rotation.DrainTimeout == 0 {
			opts.Rotation.DrainTimeout = defaultDrainTimeout
		}
	}
	return opts, nil
}

// Start starts the initial managed connection and lifecycle workers.
func (s *Session) Start(parent context.Context) error {
	if parent == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOptions)
	}
	s.lifecycleMu.Lock()
	if s.closed {
		s.lifecycleMu.Unlock()
		return ErrSessionClosed
	}
	if s.started {
		s.lifecycleMu.Unlock()
		return nil
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(parent)
	ctx := s.ctx
	s.lifecycleMu.Unlock()

	go s.observerLoop()
	s.transition(StateConnecting, ReasonStart, 0, nil)

	s.mu.Lock()
	initial := s.addSlotLocked(s.initialConn)
	s.active = initial
	s.notifyLocked()
	s.mu.Unlock()
	if err := s.startSlot(ctx, initial); err != nil {
		s.failTerminal(err)
	}
	go s.run(ctx)
	return nil
}

func (s *Session) run(ctx context.Context) {
	if s.opts.Rotation.Enabled {
		s.launchWorker(func() { s.rotationLoop(ctx) })
	}
	<-ctx.Done()

	s.workerMu.Lock()
	s.stopping = true
	s.workerMu.Unlock()

	s.mu.Lock()
	slots := make([]*transportSlot, 0, len(s.slots))
	for _, slot := range s.slots {
		slots = append(slots, slot)
	}
	s.mu.Unlock()
	for _, slot := range slots {
		_ = slot.conn.Close()
	}
	s.workers.Wait()

	s.mu.Lock()
	pending := make([]*pendingRequest, 0, len(s.pending))
	for _, p := range s.pending {
		pending = append(pending, p)
	}
	failed := s.state == StateFailed
	s.mu.Unlock()
	for _, p := range pending {
		s.completePending(p, pendingResult{err: s.failureForRequest(p, ErrSessionClosed)})
	}
	if !failed {
		reason := ReasonContextCanceled
		s.lifecycleMu.Lock()
		if s.userClosed {
			reason = ReasonUserClosed
		}
		s.lifecycleMu.Unlock()
		s.transition(StateClosed, reason, 0, nil)
	}
	close(s.observations)
	<-s.observerDone
	close(s.states)
	close(s.errors)
	close(s.unsolicited)
	close(s.done)
}

func (s *Session) addSlotLocked(conn *managedws.Connection) *transportSlot {
	s.nextSlotID++
	slot := &transportSlot{id: s.nextSlotID, conn: conn}
	s.slots[slot.id] = slot
	return slot
}

func (s *Session) startSlot(ctx context.Context, slot *transportSlot) error {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	if s.stopping {
		return ErrSessionClosed
	}
	if err := slot.conn.Start(ctx); err != nil {
		return err
	}
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		s.slotLoop(ctx, slot)
	}()
	return nil
}

func (s *Session) launchWorker(fn func()) bool {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	if s.stopping {
		return false
	}
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		fn()
	}()
	return true
}

func (s *Session) slotLoop(ctx context.Context, slot *transportSlot) {
	states := slot.conn.States()
	frames := slot.conn.Frames()
	for states != nil || frames != nil {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-states:
			if !ok {
				states = nil
				continue
			}
			s.handleTransportState(ctx, slot, event)
		case frame, ok := <-frames:
			if !ok {
				frames = nil
				continue
			}
			s.handleFrame(slot, frame)
		}
	}
	s.mu.Lock()
	slot.closed = true
	s.notifyLocked()
	s.mu.Unlock()
}

func (s *Session) handleTransportState(ctx context.Context, slot *transportSlot, event managedws.StateEvent) {
	switch event.State {
	case managedws.StateReady:
		s.mu.Lock()
		if slot.closed {
			s.mu.Unlock()
			return
		}
		s.nextGeneration++
		slot.transportGeneration = event.Generation
		slot.apiGeneration = s.nextGeneration
		slot.ready = false
		slot.failedErr = nil
		apiGeneration := slot.apiGeneration
		isActive := s.active == slot
		s.notifyLocked()
		s.mu.Unlock()
		if isActive && s.opts.Authenticator != nil {
			s.transition(StateAuthenticating, ReasonAuthenticationPending, apiGeneration, nil)
		}
		s.launchWorker(func() {
			s.authenticateGeneration(ctx, slot, event.Generation, apiGeneration)
		})
	case managedws.StateDisconnected:
		s.handleDisconnect(slot, event.Generation, event.Err)
	case managedws.StateConnecting:
		s.mu.Lock()
		isActive := s.active == slot && !slot.draining
		apiGeneration := slot.apiGeneration
		s.mu.Unlock()
		if isActive && apiGeneration == 0 {
			s.transition(StateConnecting, ReasonStart, 0, event.Err)
		} else if isActive {
			s.transition(StateReconnecting, ReasonTransportReconnecting, apiGeneration, event.Err)
		}
	case managedws.StateReconnecting:
		s.mu.Lock()
		isActive := s.active == slot && !slot.draining
		apiGeneration := slot.apiGeneration
		s.mu.Unlock()
		if isActive {
			s.transition(StateReconnecting, ReasonTransportReconnecting, apiGeneration, event.Err)
		}
	case managedws.StateFailed:
		s.mu.Lock()
		slot.failedErr = event.Err
		isActive := s.active == slot && !slot.draining
		s.notifyLocked()
		s.mu.Unlock()
		if isActive {
			s.failTerminal(event.Err)
		}
	case managedws.StateClosed:
		s.mu.Lock()
		slot.closed = true
		if s.active != slot {
			delete(s.slots, slot.id)
			delete(s.usedIDs, slot.apiGeneration)
		}
		s.notifyLocked()
		s.mu.Unlock()
	}
}

func (s *Session) authenticateGeneration(ctx context.Context, slot *transportSlot, transportGeneration, apiGeneration uint64) {
	if s.opts.Authenticator == nil {
		s.markSlotReady(slot, transportGeneration, apiGeneration, ReasonTransportReady)
		return
	}
	request, err := s.opts.Authenticator.BuildRequest(apiGeneration)
	if err != nil {
		s.failAuthentication(slot, apiGeneration, err, true)
		return
	}
	request.Outcome = OutcomeSafe
	request, err = normalizeRequest(ctx, request)
	if err != nil {
		s.failAuthentication(slot, apiGeneration, err, true)
		return
	}
	response, err := s.doOnSlot(ctx, slot, transportGeneration, apiGeneration, request, true)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.failAuthentication(slot, apiGeneration, err, false)
		return
	}
	if err := s.opts.Authenticator.ValidateResponse(response); err != nil {
		s.failAuthentication(slot, apiGeneration, err, true)
		return
	}
	s.markSlotReady(slot, transportGeneration, apiGeneration, ReasonAuthenticationReady)
}

func (s *Session) failAuthentication(slot *transportSlot, generation uint64, cause error, terminal bool) {
	err := &RequestError{Kind: ErrorAuthentication, Method: "session.logon", Generation: generation, Err: fmt.Errorf("%w: %v", ErrAuthenticationFailed, cause)}
	s.emitError(ErrorEvent{Kind: ErrorAuthentication, Method: "session.logon", Generation: generation, At: time.Now(), Err: err})
	if terminal {
		s.failTerminal(err)
		return
	}
	_ = slot.conn.Interrupt(err)
}

func (s *Session) markSlotReady(slot *transportSlot, transportGeneration, apiGeneration uint64, reason StateReason) {
	s.mu.Lock()
	if slot.closed || slot.transportGeneration != transportGeneration || slot.apiGeneration != apiGeneration {
		s.mu.Unlock()
		return
	}
	slot.ready = true
	slot.readyAt = time.Now()
	isActive := s.active == slot
	s.notifyLocked()
	s.mu.Unlock()
	if isActive {
		s.transition(StateReady, reason, apiGeneration, nil)
	}
}

func (s *Session) handleDisconnect(slot *transportSlot, transportGeneration uint64, cause error) {
	s.mu.Lock()
	if slot.transportGeneration != transportGeneration {
		s.mu.Unlock()
		return
	}
	slot.ready = false
	apiGeneration := slot.apiGeneration
	isActive := s.active == slot && !slot.draining
	pending := make([]*pendingRequest, 0)
	for _, p := range s.pending {
		if p.slot == slot && p.transportGeneration == transportGeneration {
			pending = append(pending, p)
		}
	}
	delete(s.usedIDs, apiGeneration)
	s.notifyLocked()
	s.mu.Unlock()
	for _, p := range pending {
		s.completePending(p, pendingResult{err: s.failureForRequest(p, fmt.Errorf("%w: %v", ErrDisconnected, cause))})
	}
	if isActive {
		s.transition(StateDisconnected, ReasonTransportDisconnected, apiGeneration, cause)
	}
}

// Do sends a request on the current Ready generation and waits for its exact
// response ID. It never automatically retries or replays a request.
func (s *Session) Do(ctx context.Context, request Request) (Response, error) {
	request, err := normalizeRequest(ctx, request)
	if err != nil {
		return Response{}, err
	}
	s.mu.Lock()
	if !s.startedLocked() {
		s.mu.Unlock()
		return Response{}, ErrSessionNotStarted
	}
	if s.closedLocked() {
		s.mu.Unlock()
		return Response{}, ErrSessionClosed
	}
	slot := s.active
	if slot == nil || !slot.ready || slot.draining {
		s.mu.Unlock()
		return Response{}, ErrSessionNotReady
	}
	transportGeneration, apiGeneration := slot.transportGeneration, slot.apiGeneration
	s.mu.Unlock()
	return s.doOnSlot(ctx, slot, transportGeneration, apiGeneration, request, false)
}

func (s *Session) doOnSlot(ctx context.Context, slot *transportSlot, transportGeneration, apiGeneration uint64, request Request, internal bool) (Response, error) {
	requestCtx, cancel := context.WithTimeout(ctx, s.opts.RequestTimeout)
	defer cancel()
	p := &pendingRequest{request: request, slot: slot, transportGeneration: transportGeneration, apiGeneration: apiGeneration, sentAt: time.Now(), result: make(chan pendingResult, 1)}

	s.mu.Lock()
	if existing := s.pending[request.ID]; existing != nil {
		s.mu.Unlock()
		return Response{}, ErrDuplicateRequestID
	}
	if len(s.pending) >= s.opts.MaxPendingRequests {
		s.mu.Unlock()
		return Response{}, ErrTooManyPendingRequests
	}
	used := s.usedIDs[apiGeneration]
	if used == nil {
		used = make(map[string]struct{})
		s.usedIDs[apiGeneration] = used
	}
	if _, exists := used[request.ID]; exists {
		s.mu.Unlock()
		return Response{}, ErrDuplicateRequestID
	}
	if slot.closed || slot.transportGeneration != transportGeneration || slot.apiGeneration != apiGeneration || (!internal && !slot.ready) {
		s.mu.Unlock()
		return Response{}, ErrSessionNotReady
	}
	used[request.ID] = struct{}{}
	s.pending[request.ID] = p
	s.notifyLocked()
	s.mu.Unlock()

	if err := slot.conn.SendTextOnGeneration(requestCtx, transportGeneration, request.Payload); err != nil {
		failure := s.failureForRequest(p, fmt.Errorf("%w: %v", ErrDisconnected, err))
		if s.completePending(p, pendingResult{err: failure}) {
			return Response{}, failure
		}
		result := <-p.result
		return result.response, result.err
	}

	select {
	case result := <-p.result:
		return result.response, result.err
	case <-requestCtx.Done():
		cause := error(ErrRequestTimeout)
		if ctx.Err() != nil {
			cause = ctx.Err()
		}
		failure := s.failureForRequest(p, cause)
		if s.completePending(p, pendingResult{err: failure}) {
			return Response{}, failure
		}
		result := <-p.result
		return result.response, result.err
	case <-s.done:
		failure := s.failureForRequest(p, ErrSessionClosed)
		if s.completePending(p, pendingResult{err: failure}) {
			return Response{}, failure
		}
		result := <-p.result
		return result.response, result.err
	}
}

func normalizeRequest(ctx context.Context, request Request) (Request, error) {
	if ctx == nil {
		return Request{}, fmt.Errorf("%w: context is nil", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return Request{}, err
	}
	if request.ID == "" || request.Method == "" || len(request.Payload) == 0 {
		return Request{}, ErrInvalidRequest
	}
	if request.Outcome == "" {
		request.Outcome = OutcomeUnknown
	}
	if request.Outcome != OutcomeSafe && request.Outcome != OutcomeUnknown {
		return Request{}, ErrInvalidRequest
	}
	var envelope struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(request.Payload, &envelope); err != nil {
		return Request{}, fmt.Errorf("%w: invalid JSON: %v", ErrInvalidRequest, err)
	}
	id, err := normalizeWireID(envelope.ID)
	if err != nil || id != request.ID {
		return Request{}, fmt.Errorf("%w: payload id does not match request id", ErrInvalidRequest)
	}
	request.Payload = append([]byte(nil), request.Payload...)
	return request, nil
}

func (s *Session) handleFrame(slot *transportSlot, frame managedws.Frame) {
	if frame.Type != managedws.TextMessage {
		return
	}
	var envelope struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(frame.Payload, &envelope); err != nil {
		s.emitError(ErrorEvent{Kind: ErrorProtocol, Generation: slot.apiGeneration, At: time.Now(), Err: fmt.Errorf("%w: %v", ErrUnexpectedResponse, err)})
		return
	}
	id, err := normalizeWireID(envelope.ID)
	if err != nil || id == "" {
		s.emitUnsolicited(slot, frame)
		return
	}
	s.mu.Lock()
	p := s.pending[id]
	apiGeneration := slot.apiGeneration
	transportGeneration := slot.transportGeneration
	s.mu.Unlock()
	if p == nil || p.slot != slot || p.transportGeneration != frame.Generation || transportGeneration != frame.Generation {
		s.emitUnsolicited(slot, frame)
		return
	}
	response := Response{ID: id, Payload: append(json.RawMessage(nil), frame.Payload...), Generation: apiGeneration, ReceivedAt: frame.ReceivedAt}
	s.completePending(p, pendingResult{response: response})
}

func (s *Session) emitUnsolicited(slot *transportSlot, frame managedws.Frame) {
	s.mu.Lock()
	generation := slot.apiGeneration
	s.mu.Unlock()
	event := UnsolicitedFrame{Payload: append(json.RawMessage(nil), frame.Payload...), Generation: generation, ReceivedAt: frame.ReceivedAt}
	select {
	case s.unsolicited <- event:
	default:
	}
}

func normalizeWireID(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var text string
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &text); err != nil {
			return "", err
		}
		return text, nil
	}
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return "", err
	}
	if number.String() == "" {
		return "", ErrUnexpectedResponse
	}
	if _, err := strconv.ParseFloat(number.String(), 64); err != nil {
		return "", err
	}
	return number.String(), nil
}

func (s *Session) completePending(p *pendingRequest, result pendingResult) bool {
	s.mu.Lock()
	if s.pending[p.request.ID] != p {
		s.mu.Unlock()
		return false
	}
	delete(s.pending, p.request.ID)
	s.notifyLocked()
	s.mu.Unlock()
	p.result <- result
	if result.err != nil {
		s.emitError(errorEventForRequest(p, result.err))
	}
	return true
}

func errorEventForRequest(p *pendingRequest, err error) ErrorEvent {
	kind := ErrorDisconnected
	var unknown *UnknownOutcomeError
	var requestErr *RequestError
	if errors.As(err, &unknown) {
		kind = ErrorOutcomeUnknown
	}
	if errors.As(err, &requestErr) {
		kind = requestErr.Kind
	}
	return ErrorEvent{Kind: kind, RequestID: p.request.ID, Method: p.request.Method, Generation: p.apiGeneration, At: time.Now(), Err: err}
}

func (s *Session) failureForRequest(p *pendingRequest, cause error) error {
	kind := ErrorDisconnected
	if errors.Is(cause, ErrRequestTimeout) || errors.Is(cause, context.DeadlineExceeded) {
		kind = ErrorTimeout
		cause = fmt.Errorf("%w: %v", ErrRequestTimeout, cause)
	}
	if p.request.Outcome == OutcomeUnknown {
		return &UnknownOutcomeError{RequestID: p.request.ID, Method: p.request.Method, Generation: p.apiGeneration, SentAt: p.sentAt, Err: cause}
	}
	return &RequestError{Kind: kind, RequestID: p.request.ID, Method: p.request.Method, Generation: p.apiGeneration, Err: cause}
}

// WaitReady waits for the current active connection generation to complete any
// configured authentication. It works across repeated disconnect/reconnects.
func (s *Session) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	for {
		s.mu.Lock()
		if s.active != nil && s.active.ready && !s.active.draining {
			s.mu.Unlock()
			return nil
		}
		if s.state == StateFailed {
			err := s.terminalErr
			s.mu.Unlock()
			if err != nil {
				return err
			}
			return ErrSessionClosed
		}
		if s.closedLocked() {
			s.mu.Unlock()
			return ErrSessionClosed
		}
		if !s.startedLocked() {
			s.mu.Unlock()
			return ErrSessionNotStarted
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return ErrSessionClosed
		case <-changed:
		}
	}
}

func (s *Session) rotationLoop(ctx context.Context) {
	for {
		s.mu.Lock()
		active := s.active
		if active == nil || !active.ready || active.draining {
			changed := s.changed
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-changed:
				continue
			}
		}
		wait := time.Until(active.readyAt.Add(s.opts.Rotation.MaxAge))
		s.mu.Unlock()
		if wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
		if err := s.rotate(ctx, active); err != nil && ctx.Err() == nil {
			s.emitError(ErrorEvent{Kind: ErrorRotation, Generation: active.apiGeneration, At: time.Now(), Err: err})
			s.mu.Lock()
			current := s.active
			ready := current != nil && current.ready && !current.draining
			generation := uint64(0)
			if current != nil {
				generation = current.apiGeneration
			}
			s.mu.Unlock()
			if ready {
				s.transition(StateReady, ReasonRotationFailed, generation, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

func (s *Session) rotate(ctx context.Context, old *transportSlot) error {
	s.transition(StateRotating, ReasonRotationStarted, old.apiGeneration, nil)
	conn, err := managedws.NewConnection(s.opts.ConnectionOptions)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRotationFailed, err)
	}
	s.mu.Lock()
	if s.active != old || !old.ready || old.draining {
		s.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	candidate := s.addSlotLocked(conn)
	s.notifyLocked()
	s.mu.Unlock()
	if err := s.startSlot(ctx, candidate); err != nil {
		_ = candidate.conn.Close()
		return fmt.Errorf("%w: %v", ErrRotationFailed, err)
	}
	if err := s.waitSlotReady(ctx, candidate); err != nil {
		_ = candidate.conn.Close()
		return fmt.Errorf("%w: %v", ErrRotationFailed, err)
	}

	s.mu.Lock()
	if s.active != old || !candidate.ready {
		s.mu.Unlock()
		_ = candidate.conn.Close()
		return nil
	}
	s.active = candidate
	old.draining = true
	s.notifyLocked()
	newGeneration := candidate.apiGeneration
	s.mu.Unlock()
	s.transition(StateDraining, ReasonRotationDraining, old.apiGeneration, nil)
	s.transition(StateReady, ReasonRotationSwitched, newGeneration, nil)

	drainCtx, cancel := context.WithTimeout(ctx, s.opts.Rotation.DrainTimeout)
	defer cancel()
	drainTimedOut := false
DrainLoop:
	for {
		s.mu.Lock()
		count := 0
		for _, p := range s.pending {
			if p.slot == old {
				count++
			}
		}
		changed := s.changed
		s.mu.Unlock()
		if count == 0 {
			break
		}
		select {
		case <-drainCtx.Done():
			drainTimedOut = true
			break DrainLoop
		case <-changed:
		}
	}
	if drainTimedOut {
		s.failSlotPending(old, fmt.Errorf("%w: rotation drain timeout", ErrDisconnected))
	}
	_ = old.conn.Close()
	return nil
}

func (s *Session) failSlotPending(slot *transportSlot, cause error) {
	s.mu.Lock()
	pending := make([]*pendingRequest, 0)
	for _, p := range s.pending {
		if p.slot == slot {
			pending = append(pending, p)
		}
	}
	s.mu.Unlock()
	for _, p := range pending {
		s.completePending(p, pendingResult{err: s.failureForRequest(p, cause)})
	}
}

func (s *Session) waitSlotReady(ctx context.Context, slot *transportSlot) error {
	for {
		s.mu.Lock()
		if slot.ready {
			s.mu.Unlock()
			return nil
		}
		if slot.failedErr != nil {
			err := slot.failedErr
			s.mu.Unlock()
			return err
		}
		if slot.closed {
			s.mu.Unlock()
			return ErrSessionClosed
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Close permanently closes the session. It is idempotent.
func (s *Session) Close() error {
	s.lifecycleMu.Lock()
	if s.closed {
		s.lifecycleMu.Unlock()
		return nil
	}
	s.closed = true
	s.userClosed = true
	cancel := s.cancel
	started := s.started
	s.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if !started {
		_ = s.initialConn.Close()
		s.transition(StateClosed, ReasonUserClosed, 0, nil)
		close(s.observations)
		close(s.observerDone)
		close(s.states)
		close(s.errors)
		close(s.unsolicited)
		close(s.done)
	}
	return nil
}

func (s *Session) failTerminal(err error) {
	s.mu.Lock()
	if s.terminalErr != nil {
		s.mu.Unlock()
		return
	}
	s.terminalErr = err
	s.notifyLocked()
	s.mu.Unlock()
	s.transition(StateFailed, ReasonTerminalFailure, s.Generation(), err)
	s.lifecycleMu.Lock()
	cancel := s.cancel
	s.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Session) transition(next State, reason StateReason, generation uint64, err error) {
	s.mu.Lock()
	previous := s.state
	s.state = next
	s.notifyLocked()
	s.mu.Unlock()
	event := StateEvent{Previous: previous, State: next, Reason: reason, Generation: generation, At: time.Now(), Err: err}
	select {
	case s.states <- event:
	default:
	}
	s.publishObservation(observation{state: &event})
}

func (s *Session) emitError(event ErrorEvent) {
	select {
	case s.errors <- event:
	default:
	}
	s.publishObservation(observation{err: &event})
}

func (s *Session) publishObservation(event observation) {
	if s.opts.Observer == nil {
		return
	}
	select {
	case s.observations <- event:
	default:
	}
}

func (s *Session) observerLoop() {
	defer close(s.observerDone)
	if s.opts.Observer == nil {
		for range s.observations {
		}
		return
	}
	for event := range s.observations {
		func() {
			defer func() { _ = recover() }()
			if event.state != nil {
				s.opts.Observer.OnState(*event.state)
			}
			if event.err != nil {
				s.opts.Observer.OnError(*event.err)
			}
		}()
	}
}

func (s *Session) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }
func (s *Session) startedLocked() bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.started
}
func (s *Session) closedLocked() bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.closed
}

// Done closes after all connection, rotation, and observer workers stop.
func (s *Session) Done() <-chan struct{}                { return s.done }
func (s *Session) States() <-chan StateEvent            { return s.states }
func (s *Session) Errors() <-chan ErrorEvent            { return s.errors }
func (s *Session) Unsolicited() <-chan UnsolicitedFrame { return s.unsolicited }
func (s *Session) State() State                         { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *Session) TerminalError() error                 { s.mu.Lock(); defer s.mu.Unlock(); return s.terminalErr }
func (s *Session) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		return 0
	}
	return s.active.apiGeneration
}
