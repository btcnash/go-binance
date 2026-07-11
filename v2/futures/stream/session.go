package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	managedws "github.com/adshao/go-binance/v2/common/websocket/managed"
	managedgorilla "github.com/adshao/go-binance/v2/common/websocket/managed/gorilla"
)

const (
	defaultAckTimeout      = 5 * time.Second
	defaultRequestInterval = 200 * time.Millisecond
	defaultMaxBatchSize    = 100
	defaultMaxStreams      = 1024
	defaultEventBuffer     = 256
	defaultSessionBuffer   = 64
)

const (
	methodSubscribe         = "SUBSCRIBE"
	methodUnsubscribe       = "UNSUBSCRIBE"
	methodListSubscriptions = "LIST_SUBSCRIPTIONS"
	methodSetProperty       = "SET_PROPERTY"
	methodGetProperty       = "GET_PROPERTY"
)

var dynamicEndpoints = map[Environment]map[StreamClass]string{
	EnvironmentMainnet: {
		StreamClassPublic: "wss://fstream.binance.com/public/stream",
		StreamClassMarket: "wss://fstream.binance.com/market/stream",
	},
	EnvironmentTestnet: {
		StreamClassPublic: "wss://stream.binancefuture.com/public/stream",
		StreamClassMarket: "wss://stream.binancefuture.com/market/stream",
	},
	EnvironmentDemo: {
		StreamClassPublic: "wss://fstream.binancefuture.com/public/stream",
		StreamClassMarket: "wss://fstream.binancefuture.com/market/stream",
	},
}

type pendingRequest struct {
	generation uint64
	method     string
	result     chan protocolResponse
}

type protocolResponse struct {
	result json.RawMessage
	code   int
	msg    string
	err    error
}

type waiterKind uint8

const (
	waiterPresent waiterKind = iota + 1
	waiterAbsent
	waiterExact
)

type subscriptionWaiter struct {
	kind   waiterKind
	set    map[string]struct{}
	result chan error
}

type wireRequest struct {
	Method string      `json:"method"`
	Params interface{} `json:"params,omitempty"`
	ID     uint64      `json:"id"`
}

type wireEnvelope struct {
	ID     *uint64         `json:"id"`
	Result json.RawMessage `json:"result"`
	Code   *int            `json:"code"`
	Msg    string          `json:"msg"`
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// NewStreamSession creates an idle dynamic subscription session.
func NewStreamSession(opts StreamSessionOptions) (*StreamSession, error) {
	normalized, err := normalizeStreamOptions(opts)
	if err != nil {
		return nil, err
	}
	conn, err := managedws.NewConnection(normalized.ConnectionOptions)
	if err != nil {
		return nil, fmt.Errorf("%w: create managed connection: %v", ErrInvalidStreamOptions, err)
	}
	desired := make(map[string]Subscription, len(normalized.InitialSubscriptions))
	for _, sub := range normalized.InitialSubscriptions {
		desired[sub.String()] = sub
	}
	return &StreamSession{
		opts:         normalized,
		conn:         conn,
		done:         make(chan struct{}),
		state:        StreamStateIdle,
		desired:      desired,
		active:       make(map[string]Subscription),
		pending:      make(map[uint64]*pendingRequest),
		waiters:      make(map[uint64]*subscriptionWaiter),
		changed:      make(chan struct{}),
		reconcileC:   make(chan struct{}, 1),
		events:       make(chan StreamEvent, normalized.EventBuffer),
		states:       make(chan StreamStateEvent, normalized.StateBuffer),
		errors:       make(chan StreamErrorEvent, normalized.ErrorBuffer),
		gaps:         make(chan GapEvent, normalized.GapBuffer),
		observations: make(chan streamObservation, normalized.ObserverBuffer),
		firstReady:   make(chan struct{}),
		pacer:        newRequestPacer(normalized.RequestInterval),
	}, nil
}

func normalizeStreamOptions(opts StreamSessionOptions) (StreamSessionOptions, error) {
	if opts.Class != StreamClassPublic && opts.Class != StreamClassMarket {
		return StreamSessionOptions{}, fmt.Errorf("%w: class must be public or market", ErrInvalidStreamOptions)
	}
	if opts.Environment == "" {
		opts.Environment = EnvironmentMainnet
	}
	if _, ok := dynamicEndpoints[opts.Environment]; !ok {
		return StreamSessionOptions{}, fmt.Errorf("%w: unsupported environment %q", ErrInvalidStreamOptions, opts.Environment)
	}
	if opts.AckTimeout < 0 || opts.RequestInterval < 0 || opts.MaxBatchSize < 0 || opts.MaxStreams < 0 || opts.EventBuffer < 0 || opts.StateBuffer < 0 || opts.ErrorBuffer < 0 || opts.GapBuffer < 0 || opts.ObserverBuffer < 0 {
		return StreamSessionOptions{}, fmt.Errorf("%w: durations and capacities must not be negative", ErrInvalidStreamOptions)
	}
	if opts.AckTimeout == 0 {
		opts.AckTimeout = defaultAckTimeout
	}
	if opts.RequestInterval == 0 {
		opts.RequestInterval = defaultRequestInterval
	}
	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = defaultMaxBatchSize
	}
	if opts.MaxStreams == 0 {
		opts.MaxStreams = defaultMaxStreams
	}
	if opts.EventBuffer == 0 {
		opts.EventBuffer = defaultEventBuffer
	}
	if opts.StateBuffer == 0 {
		opts.StateBuffer = defaultSessionBuffer
	}
	if opts.ErrorBuffer == 0 {
		opts.ErrorBuffer = defaultSessionBuffer
	}
	if opts.GapBuffer == 0 {
		opts.GapBuffer = defaultSessionBuffer
	}
	if opts.ObserverBuffer == 0 {
		opts.ObserverBuffer = defaultSessionBuffer
	}

	seen := make(map[string]struct{}, len(opts.InitialSubscriptions))
	for _, sub := range opts.InitialSubscriptions {
		if err := sub.ValidateFor(opts.Class); err != nil {
			return StreamSessionOptions{}, err
		}
		seen[sub.String()] = struct{}{}
	}
	if len(seen) > opts.MaxStreams {
		return StreamSessionOptions{}, fmt.Errorf("%w: %d > %d", ErrTooManySubscriptions, len(seen), opts.MaxStreams)
	}

	if opts.ConnectionOptions.Dialer == nil {
		endpoint := strings.TrimSpace(opts.Endpoint)
		if endpoint == "" {
			endpoint = dynamicEndpoints[opts.Environment][opts.Class]
		}
		opts.ConnectionOptions.Dialer = managedgorilla.Dialer{
			Endpoint:  endpoint,
			ReadLimit: 655350,
		}
	}
	if !opts.DisableHeartbeat && !opts.ConnectionOptions.Heartbeat.Enabled {
		opts.ConnectionOptions.Heartbeat.Enabled = true
	}
	if !opts.DisableReconnect && !opts.ConnectionOptions.Reconnect.Enabled {
		opts.ConnectionOptions.Reconnect.Enabled = true
	}
	return opts, nil
}

// Start launches transport, protocol, and reconciliation loops.
func (s *StreamSession) Start(parent context.Context) error {
	if parent == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidStreamOptions)
	}
	s.lifecycleMu.Lock()
	if s.closed || s.terminated {
		s.lifecycleMu.Unlock()
		return ErrSessionClosed
	}
	if s.started {
		s.lifecycleMu.Unlock()
		return managedws.ErrAlreadyStarted
	}
	ctx, cancel := context.WithCancel(parent)
	s.started = true
	s.cancel = cancel
	s.lifecycleMu.Unlock()

	s.transition(StreamStateConnecting, StreamReasonStart, 0, nil)
	if err := s.conn.Start(ctx); err != nil {
		cancel()
		return err
	}

	s.workers.Add(4)
	go s.transportStateLoop(ctx)
	go s.frameLoop(ctx)
	go s.reconcileLoop(ctx)
	go s.observerLoop(ctx)
	go s.finalize(ctx)
	return nil
}

// WaitReady waits for the first generation whose desired subscriptions have
// all been acknowledged.
func (s *StreamSession) WaitReady(ctx context.Context) error {
	if err := validateCallContext(ctx); err != nil {
		return err
	}
	s.lifecycleMu.Lock()
	started := s.started
	closed := s.closed || s.terminated
	s.lifecycleMu.Unlock()
	if closed {
		return ErrSessionClosed
	}
	if !started {
		return ErrSessionNotStarted
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.firstReady:
		return nil
	case <-s.done:
		if err := s.TerminalError(); err != nil {
			return err
		}
		return ErrSessionClosed
	}
}

// Close permanently stops the logical session. It is idempotent.
func (s *StreamSession) Close() error {
	s.lifecycleMu.Lock()
	if s.closed || s.terminated {
		s.lifecycleMu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.cancel
	started := s.started
	s.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if started {
		return s.conn.Close()
	}
	s.lifecycleMu.Lock()
	s.terminated = true
	s.lifecycleMu.Unlock()
	s.transition(StreamStateClosed, StreamReasonUserClosed, 0, nil)
	s.finish()
	return nil
}

func (s *StreamSession) Done() <-chan struct{}           { return s.done }
func (s *StreamSession) Events() <-chan StreamEvent      { return s.events }
func (s *StreamSession) States() <-chan StreamStateEvent { return s.states }
func (s *StreamSession) Errors() <-chan StreamErrorEvent { return s.errors }
func (s *StreamSession) Gaps() <-chan GapEvent           { return s.gaps }
func (s *StreamSession) State() StreamSessionState       { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *StreamSession) Generation() uint64              { s.mu.Lock(); defer s.mu.Unlock(); return s.generation }
func (s *StreamSession) TerminalError() error            { s.mu.Lock(); defer s.mu.Unlock(); return s.terminalErr }
func (s *StreamSession) DesiredSubscriptions() []Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sortedSubscriptions(s.desired)
}
func (s *StreamSession) ActiveSubscriptions() []Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sortedSubscriptions(s.active)
}

// Subscribe adds streams to desired state and waits until they are active.
func (s *StreamSession) Subscribe(ctx context.Context, subscriptions ...Subscription) error {
	if err := validateCallContext(ctx); err != nil {
		return err
	}
	if len(subscriptions) == 0 {
		return fmt.Errorf("%w: at least one subscription is required", ErrInvalidSubscription)
	}
	if err := s.validateSubscriptions(subscriptions); err != nil {
		return err
	}
	required := make(map[string]struct{}, len(subscriptions))
	s.mu.Lock()
	if err := s.ensureMutableLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	prospective := len(s.desired)
	for _, sub := range subscriptions {
		if _, exists := s.desired[sub.String()]; !exists {
			prospective++
		}
		required[sub.String()] = struct{}{}
	}
	if prospective > s.opts.MaxStreams {
		s.mu.Unlock()
		return fmt.Errorf("%w: %d > %d", ErrTooManySubscriptions, prospective, s.opts.MaxStreams)
	}
	for _, sub := range subscriptions {
		s.desired[sub.String()] = sub
	}
	waiterID, waiter := s.addWaiterLocked(waiterPresent, required)
	s.supersedeWaitersLocked(waiterID)
	s.completeWaitersLocked()
	s.mu.Unlock()
	s.markSubscriptionsPending()
	s.signalReconcile()
	return s.waitForWaiter(ctx, waiterID, waiter)
}

// Unsubscribe removes streams from desired state and waits until they are no
// longer active on the current generation.
func (s *StreamSession) Unsubscribe(ctx context.Context, subscriptions ...Subscription) error {
	if err := validateCallContext(ctx); err != nil {
		return err
	}
	if len(subscriptions) == 0 {
		return fmt.Errorf("%w: at least one subscription is required", ErrInvalidSubscription)
	}
	if err := s.validateSubscriptions(subscriptions); err != nil {
		return err
	}
	required := make(map[string]struct{}, len(subscriptions))
	s.mu.Lock()
	if err := s.ensureMutableLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	for _, sub := range subscriptions {
		delete(s.desired, sub.String())
		required[sub.String()] = struct{}{}
	}
	waiterID, waiter := s.addWaiterLocked(waiterAbsent, required)
	s.supersedeWaitersLocked(waiterID)
	s.completeWaitersLocked()
	s.mu.Unlock()
	s.markSubscriptionsPending()
	s.signalReconcile()
	return s.waitForWaiter(ctx, waiterID, waiter)
}

// ReplaceSubscriptions atomically changes desired state and waits for exact
// convergence on the current generation.
func (s *StreamSession) ReplaceSubscriptions(ctx context.Context, subscriptions []Subscription) error {
	if err := validateCallContext(ctx); err != nil {
		return err
	}
	if err := s.validateSubscriptions(subscriptions); err != nil {
		return err
	}
	if len(uniqueSubscriptions(subscriptions)) > s.opts.MaxStreams {
		return fmt.Errorf("%w: %d > %d", ErrTooManySubscriptions, len(subscriptions), s.opts.MaxStreams)
	}
	target := make(map[string]struct{}, len(subscriptions))
	s.mu.Lock()
	if err := s.ensureMutableLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.desired = make(map[string]Subscription, len(subscriptions))
	for _, sub := range subscriptions {
		s.desired[sub.String()] = sub
		target[sub.String()] = struct{}{}
	}
	waiterID, waiter := s.addWaiterLocked(waiterExact, target)
	s.supersedeWaitersLocked(waiterID)
	s.completeWaitersLocked()
	s.mu.Unlock()
	s.markSubscriptionsPending()
	s.signalReconcile()
	return s.waitForWaiter(ctx, waiterID, waiter)
}

// ListSubscriptions asks Binance for the active subscription names.
func (s *StreamSession) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	if err := validateCallContext(ctx); err != nil {
		return nil, err
	}
	response, err := s.sendProtocolRequest(ctx, methodListSubscriptions, nil)
	if err != nil {
		return nil, err
	}
	var names []string
	if err := json.Unmarshal(response.result, &names); err != nil {
		return nil, newStreamError(StreamErrorProtocol, methodListSubscriptions, 0, s.Generation(), fmt.Errorf("%w: %v", ErrUnexpectedResponse, err))
	}
	result := make([]Subscription, 0, len(names))
	for _, name := range names {
		result = append(result, RawSubscription(s.opts.Class, name))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result, nil
}

// SetCombined changes Binance's combined-stream property.
func (s *StreamSession) SetCombined(ctx context.Context, enabled bool) error {
	if err := validateCallContext(ctx); err != nil {
		return err
	}
	_, err := s.sendProtocolRequest(ctx, methodSetProperty, []interface{}{"combined", enabled})
	return err
}

// GetCombined reads Binance's combined-stream property.
func (s *StreamSession) GetCombined(ctx context.Context) (bool, error) {
	if err := validateCallContext(ctx); err != nil {
		return false, err
	}
	response, err := s.sendProtocolRequest(ctx, methodGetProperty, []interface{}{"combined"})
	if err != nil {
		return false, err
	}
	var enabled bool
	if err := json.Unmarshal(response.result, &enabled); err != nil {
		return false, newStreamError(StreamErrorProtocol, methodGetProperty, 0, s.Generation(), fmt.Errorf("%w: %v", ErrUnexpectedResponse, err))
	}
	return enabled, nil
}

func (s *StreamSession) validateSubscriptions(subscriptions []Subscription) error {
	for _, sub := range subscriptions {
		if err := sub.ValidateFor(s.opts.Class); err != nil {
			return err
		}
	}
	return nil
}

func (s *StreamSession) ensureMutableLocked() error {
	s.lifecycleMu.Lock()
	started := s.started
	closed := s.closed || s.terminated
	s.lifecycleMu.Unlock()
	if closed {
		return ErrSessionClosed
	}
	if !started {
		return ErrSessionNotStarted
	}
	return nil
}

func (s *StreamSession) addWaiterLocked(kind waiterKind, set map[string]struct{}) (uint64, *subscriptionWaiter) {
	id := atomic.AddUint64(&s.waiterID, 1)
	waiter := &subscriptionWaiter{kind: kind, set: cloneStringSet(set), result: make(chan error, 1)}
	s.waiters[id] = waiter
	return id, waiter
}

func (s *StreamSession) waitForWaiter(ctx context.Context, id uint64, waiter *subscriptionWaiter) error {
	select {
	case err := <-waiter.result:
		return err
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, id)
		s.mu.Unlock()
		return ctx.Err()
	case <-s.done:
		return ErrSessionClosed
	}
}

func (s *StreamSession) supersedeWaitersLocked(except uint64) {
	for id, waiter := range s.waiters {
		if id == except || waiterCompatibleWithDesired(waiter, s.desired) {
			continue
		}
		delete(s.waiters, id)
		waiter.result <- newStreamError(StreamErrorSuperseded, "", 0, s.generation, ErrOperationSuperseded)
	}
}

func waiterCompatibleWithDesired(waiter *subscriptionWaiter, desired map[string]Subscription) bool {
	switch waiter.kind {
	case waiterPresent:
		for name := range waiter.set {
			if _, ok := desired[name]; !ok {
				return false
			}
		}
		return true
	case waiterAbsent:
		for name := range waiter.set {
			if _, ok := desired[name]; ok {
				return false
			}
		}
		return true
	case waiterExact:
		if len(waiter.set) != len(desired) {
			return false
		}
		for name := range waiter.set {
			if _, ok := desired[name]; !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (s *StreamSession) completeWaitersLocked() {
	if !s.transportReady || !subscriptionMapsEqual(s.desired, s.active) || s.hasPendingMutationLocked() {
		return
	}
	for id, waiter := range s.waiters {
		if !waiterSatisfied(waiter, s.active) {
			continue
		}
		delete(s.waiters, id)
		waiter.result <- nil
	}
}

func (s *StreamSession) hasPendingMutationLocked() bool {
	for _, pending := range s.pending {
		if pending.generation == s.generation && (pending.method == methodSubscribe || pending.method == methodUnsubscribe) {
			return true
		}
	}
	return false
}

func subscriptionMapsEqual(left, right map[string]Subscription) bool {
	if len(left) != len(right) {
		return false
	}
	for name := range left {
		if _, ok := right[name]; !ok {
			return false
		}
	}
	return true
}

func waiterSatisfied(waiter *subscriptionWaiter, active map[string]Subscription) bool {
	switch waiter.kind {
	case waiterPresent:
		for name := range waiter.set {
			if _, ok := active[name]; !ok {
				return false
			}
		}
		return true
	case waiterAbsent:
		for name := range waiter.set {
			if _, ok := active[name]; ok {
				return false
			}
		}
		return true
	case waiterExact:
		if len(waiter.set) != len(active) {
			return false
		}
		for name := range waiter.set {
			if _, ok := active[name]; !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func validateCallContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidStreamOptions)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (s *StreamSession) markSubscriptionsPending() {
	s.mu.Lock()
	if !s.transportReady || subscriptionMapsEqual(s.desired, s.active) {
		s.mu.Unlock()
		return
	}
	state := StreamStateSubscribing
	if s.everReady || s.generation > 1 {
		state = StreamStateResubscribing
	}
	event, publish := s.transitionLocked(state, StreamReasonSubscriptionsPending, s.generation, nil)
	s.mu.Unlock()
	if publish {
		s.publishState(event)
	}
}

func (s *StreamSession) transportStateLoop(ctx context.Context) {
	defer s.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-s.conn.States():
			if !ok {
				return
			}
			s.handleTransportState(event)
		}
	}
}

func (s *StreamSession) handleTransportState(event managedws.StateEvent) {
	switch event.State {
	case managedws.StateConnecting:
		s.transition(StreamStateConnecting, StreamReasonStart, event.Generation, event.Err)
	case managedws.StateReady:
		s.mu.Lock()
		s.generation = event.Generation
		s.transportReady = true
		s.active = make(map[string]Subscription)
		s.notifyChangedLocked()
		hasDesired := len(s.desired) > 0
		everReady := s.everReady
		s.mu.Unlock()
		if hasDesired {
			state := StreamStateSubscribing
			if everReady || event.Generation > 1 {
				state = StreamStateResubscribing
			}
			s.transition(state, StreamReasonSubscriptionsPending, event.Generation, nil)
			s.signalReconcile()
		} else {
			s.markReady(event.Generation)
		}
	case managedws.StateDisconnected:
		s.mu.Lock()
		fromGeneration := s.generation
		hadContinuity := s.everReady || len(s.active) > 0
		s.transportReady = false
		s.active = make(map[string]Subscription)
		s.failPendingLocked(newStreamError(StreamErrorGenerationChanged, "", 0, fromGeneration, ErrGenerationChanged))
		s.notifyChangedLocked()
		s.mu.Unlock()
		if hadContinuity {
			s.emitGap(GapEvent{Reason: GapReasonDisconnected, FromGeneration: fromGeneration, At: time.Now(), Err: event.Err})
		}
		s.transition(StreamStateDisconnected, StreamReasonTransportDisconnected, fromGeneration, event.Err)
	case managedws.StateReconnecting:
		s.transition(StreamStateReconnecting, StreamReasonTransportReconnecting, event.Generation, event.Err)
	case managedws.StateFailed:
		s.mu.Lock()
		s.terminalErr = event.Err
		s.transportReady = false
		s.failPendingLocked(event.Err)
		s.failWaitersLocked(event.Err)
		s.notifyChangedLocked()
		s.mu.Unlock()
		s.transition(StreamStateFailed, StreamReasonTransportFailed, event.Generation, event.Err)
	}
}

func (s *StreamSession) frameLoop(ctx context.Context) {
	defer s.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-s.conn.Frames():
			if !ok {
				return
			}
			s.handleFrame(frame)
		}
	}
}

func (s *StreamSession) handleFrame(frame managedws.Frame) {
	var envelope wireEnvelope
	if err := json.Unmarshal(frame.Payload, &envelope); err != nil {
		s.emitError(newStreamError(StreamErrorProtocol, "", 0, frame.Generation, fmt.Errorf("decode websocket payload: %w", err)))
		return
	}
	if envelope.ID != nil {
		s.deliverResponse(frame.Generation, *envelope.ID, envelope)
		return
	}
	if envelope.Code != nil {
		err := &StreamError{Kind: StreamErrorRejected, Generation: frame.Generation, Code: *envelope.Code, Message: envelope.Msg, Err: ErrRequestRejected}
		s.emitError(err)
		s.mu.Lock()
		s.failPendingLocked(err)
		s.mu.Unlock()
		return
	}

	raw := append(json.RawMessage(nil), frame.Payload...)
	data := raw
	if envelope.Stream != "" && len(envelope.Data) > 0 {
		data = append(json.RawMessage(nil), envelope.Data...)
	}
	event := StreamEvent{
		Generation: frame.Generation,
		Stream:     envelope.Stream,
		Data:       data,
		Raw:        raw,
		ReceivedAt: frame.ReceivedAt,
	}
	select {
	case s.events <- event:
	default:
		err := newStreamError(StreamErrorEventOverflow, "", 0, frame.Generation, ErrEventBufferFull)
		s.emitError(err)
		s.emitGap(GapEvent{Reason: GapReasonEventOverflow, FromGeneration: frame.Generation, At: time.Now(), Err: err})
		_ = s.conn.Interrupt(err)
	}
}

func (s *StreamSession) deliverResponse(generation, id uint64, envelope wireEnvelope) {
	s.mu.Lock()
	pending, ok := s.pending[id]
	if ok && pending.generation == generation {
		delete(s.pending, id)
	}
	s.mu.Unlock()
	if !ok || pending.generation != generation {
		return
	}
	response := protocolResponse{result: append(json.RawMessage(nil), envelope.Result...)}
	if envelope.Code != nil {
		response.code = *envelope.Code
		response.msg = envelope.Msg
	}
	pending.result <- response
}

func (s *StreamSession) reconcileLoop(ctx context.Context) {
	defer s.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.reconcileC:
		}
		for {
			progress, err := s.reconcileOnce(ctx)
			if err != nil {
				if !errors.Is(err, ErrGenerationChanged) && !errors.Is(err, managedws.ErrNotReady) && !errors.Is(err, context.Canceled) {
					s.mu.Lock()
					s.failWaitersLocked(err)
					s.mu.Unlock()
					s.emitError(err)
				}
				break
			}
			if !progress {
				break
			}
		}
	}
}

func (s *StreamSession) reconcileOnce(ctx context.Context) (bool, error) {
	s.mu.Lock()
	if !s.transportReady {
		s.mu.Unlock()
		return false, nil
	}
	generation := s.generation
	additions, removals := subscriptionDiff(s.desired, s.active)
	if len(additions) == 0 && len(removals) == 0 {
		s.completeWaitersLocked()
		s.mu.Unlock()
		s.markReady(generation)
		return false, nil
	}
	everReady := s.everReady
	s.mu.Unlock()

	state := StreamStateSubscribing
	if everReady || generation > 1 {
		state = StreamStateResubscribing
	}
	s.transition(state, StreamReasonSubscriptionsPending, generation, nil)

	method := methodSubscribe
	batch := additions
	if len(removals) > 0 {
		method = methodUnsubscribe
		batch = removals
	}
	if len(batch) > s.opts.MaxBatchSize {
		batch = batch[:s.opts.MaxBatchSize]
	}
	params := make([]string, len(batch))
	for i, sub := range batch {
		params[i] = sub.String()
	}
	if _, err := s.sendProtocolRequestForGeneration(ctx, generation, method, params); err != nil {
		return false, err
	}

	s.mu.Lock()
	if !s.transportReady || s.generation != generation {
		s.mu.Unlock()
		return false, ErrGenerationChanged
	}
	for _, sub := range batch {
		if method == methodSubscribe {
			s.active[sub.String()] = sub
		} else {
			delete(s.active, sub.String())
		}
	}
	s.completeWaitersLocked()
	s.mu.Unlock()
	return true, nil
}

func (s *StreamSession) sendProtocolRequest(ctx context.Context, method string, params interface{}) (protocolResponse, error) {
	generation, err := s.waitTransportReady(ctx)
	if err != nil {
		return protocolResponse{}, err
	}
	return s.sendProtocolRequestForGeneration(ctx, generation, method, params)
}

func (s *StreamSession) sendProtocolRequestForGeneration(ctx context.Context, generation uint64, method string, params interface{}) (protocolResponse, error) {
	if err := s.pacer.Wait(ctx); err != nil {
		return protocolResponse{}, err
	}
	id := atomic.AddUint64(&s.requestID, 1)
	payload, err := json.Marshal(wireRequest{Method: method, Params: params, ID: id})
	if err != nil {
		return protocolResponse{}, err
	}
	pending := &pendingRequest{generation: generation, method: method, result: make(chan protocolResponse, 1)}

	s.mu.Lock()
	if !s.transportReady || s.generation != generation {
		s.mu.Unlock()
		return protocolResponse{}, newStreamError(StreamErrorGenerationChanged, method, id, generation, ErrGenerationChanged)
	}
	s.pending[id] = pending
	s.mu.Unlock()

	if err := s.conn.SendText(ctx, payload); err != nil {
		s.removePending(id, pending)
		if errors.Is(err, managedws.ErrNotReady) {
			return protocolResponse{}, newStreamError(StreamErrorGenerationChanged, method, id, generation, ErrGenerationChanged)
		}
		return protocolResponse{}, newStreamError(StreamErrorNotReady, method, id, generation, err)
	}

	timer := time.NewTimer(s.opts.AckTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		s.removePending(id, pending)
		return protocolResponse{}, ctx.Err()
	case <-s.done:
		s.removePending(id, pending)
		return protocolResponse{}, ErrSessionClosed
	case response := <-pending.result:
		if response.err != nil {
			return protocolResponse{}, response.err
		}
		if response.code != 0 || response.msg != "" {
			return protocolResponse{}, &StreamError{Kind: StreamErrorRejected, Method: method, RequestID: id, Generation: generation, Code: response.code, Message: response.msg, Err: ErrRequestRejected}
		}
		return response, nil
	case <-timer.C:
		s.removePending(id, pending)
		timeout := newStreamError(StreamErrorACKTimeout, method, id, generation, ErrSubscriptionACKTimeout)
		_ = s.conn.Interrupt(timeout)
		return protocolResponse{}, timeout
	}
}

func (s *StreamSession) removePending(id uint64, pending *pendingRequest) {
	s.mu.Lock()
	if current, ok := s.pending[id]; ok && current == pending {
		delete(s.pending, id)
	}
	s.mu.Unlock()
}

func (s *StreamSession) waitTransportReady(ctx context.Context) (uint64, error) {
	for {
		s.lifecycleMu.Lock()
		started := s.started
		closed := s.closed || s.terminated
		s.lifecycleMu.Unlock()
		if closed {
			return 0, ErrSessionClosed
		}
		if !started {
			return 0, ErrSessionNotStarted
		}
		s.mu.Lock()
		if s.transportReady {
			generation := s.generation
			s.mu.Unlock()
			return generation, nil
		}
		changed := s.changed
		terminal := s.terminalErr
		s.mu.Unlock()
		if terminal != nil {
			return 0, terminal
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.done:
			return 0, ErrSessionClosed
		case <-changed:
		}
	}
}

func (s *StreamSession) finalize(ctx context.Context) {
	<-s.conn.Done()
	if s.cancel != nil {
		s.cancel()
	}

	s.lifecycleMu.Lock()
	closedByUser := s.closed
	s.terminated = true
	s.lifecycleMu.Unlock()

	s.workers.Wait()

	s.mu.Lock()
	transportErr := s.conn.TerminalError()
	if transportErr != nil {
		s.terminalErr = transportErr
	}
	terminal := s.terminalErr
	s.failPendingLocked(ErrSessionClosed)
	s.failWaitersLocked(ErrSessionClosed)
	s.notifyChangedLocked()
	s.mu.Unlock()

	if terminal != nil {
		s.transition(StreamStateFailed, StreamReasonTransportFailed, s.Generation(), terminal)
	} else if closedByUser {
		s.transition(StreamStateClosed, StreamReasonUserClosed, s.Generation(), nil)
	} else {
		s.transition(StreamStateClosed, StreamReasonContextCanceled, s.Generation(), ctx.Err())
	}
	s.finish()
}

func (s *StreamSession) finish() {
	s.finishOnce.Do(func() {
		s.outputMu.Lock()
		s.outputClosed = true
		close(s.done)
		close(s.events)
		close(s.states)
		close(s.errors)
		close(s.gaps)
		s.outputMu.Unlock()
	})
}

func (s *StreamSession) markReady(generation uint64) {
	s.mu.Lock()
	if !s.transportReady || s.generation != generation || !subscriptionMapsEqual(s.desired, s.active) || s.hasPendingMutationLocked() {
		s.mu.Unlock()
		return
	}
	s.everReady = true
	s.completeWaitersLocked()
	event, publish := s.transitionLocked(StreamStateReady, StreamReasonSubscriptionsReady, generation, nil)
	s.mu.Unlock()
	if publish {
		s.publishState(event)
	}
	s.readyOnce.Do(func() { close(s.firstReady) })
}

func (s *StreamSession) transition(next StreamSessionState, reason StreamStateReason, generation uint64, err error) {
	s.mu.Lock()
	event, publish := s.transitionLocked(next, reason, generation, err)
	s.mu.Unlock()
	if publish {
		s.publishState(event)
	}
}

func (s *StreamSession) transitionLocked(next StreamSessionState, reason StreamStateReason, generation uint64, err error) (StreamStateEvent, bool) {
	previous := s.state
	if generation == 0 {
		generation = s.generation
	}
	if previous == next && generation == s.generation {
		return StreamStateEvent{}, false
	}
	s.state = next
	if next == StreamStateFailed {
		s.terminalErr = err
	}
	return StreamStateEvent{Previous: previous, State: next, Reason: reason, Generation: generation, At: time.Now(), Err: err}, true
}

func (s *StreamSession) publishState(event StreamStateEvent) {
	s.outputMu.Lock()
	if !s.outputClosed {
		select {
		case s.states <- event:
		default:
		}
	}
	s.outputMu.Unlock()
	s.publishObservation(streamObservation{kind: observationState, state: event})
}

func (s *StreamSession) emitError(err error) {
	var typed *StreamError
	if !errors.As(err, &typed) {
		typed = newStreamError(StreamErrorProtocol, "", 0, s.Generation(), err)
	}
	event := StreamErrorEvent{Kind: typed.Kind, Method: typed.Method, RequestID: typed.RequestID, Generation: typed.Generation, At: time.Now(), Err: typed}
	s.outputMu.Lock()
	if !s.outputClosed {
		select {
		case s.errors <- event:
		default:
		}
	}
	s.outputMu.Unlock()
	s.publishObservation(streamObservation{kind: observationError, err: event})
}

func (s *StreamSession) emitGap(event GapEvent) {
	s.outputMu.Lock()
	if !s.outputClosed {
		select {
		case s.gaps <- event:
		default:
		}
	}
	s.outputMu.Unlock()
	s.publishObservation(streamObservation{kind: observationGap, gap: event})
}

type observationKind uint8

const (
	observationState observationKind = iota + 1
	observationError
	observationGap
)

type streamObservation struct {
	kind  observationKind
	state StreamStateEvent
	err   StreamErrorEvent
	gap   GapEvent
}

func (s *StreamSession) publishObservation(event streamObservation) {
	if s.opts.Observer == nil {
		return
	}
	select {
	case s.observations <- event:
	default:
	}
}

func (s *StreamSession) observerLoop(ctx context.Context) {
	defer s.workers.Done()
	if s.opts.Observer == nil {
		<-ctx.Done()
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-s.observations:
			switch event.kind {
			case observationState:
				safeObserverCall(func() { s.opts.Observer.OnState(event.state) })
			case observationError:
				safeObserverCall(func() { s.opts.Observer.OnError(event.err) })
			case observationGap:
				safeObserverCall(func() { s.opts.Observer.OnGap(event.gap) })
			}
		}
	}
}

func safeObserverCall(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

func (s *StreamSession) signalReconcile() {
	select {
	case s.reconcileC <- struct{}{}:
	default:
	}
}

func (s *StreamSession) notifyChangedLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *StreamSession) failPendingLocked(err error) {
	for id, pending := range s.pending {
		delete(s.pending, id)
		select {
		case pending.result <- protocolResponse{err: err}:
		default:
		}
	}
}

func (s *StreamSession) failWaitersLocked(err error) {
	for id, waiter := range s.waiters {
		delete(s.waiters, id)
		select {
		case waiter.result <- err:
		default:
		}
	}
}

func subscriptionDiff(desired, active map[string]Subscription) (additions, removals []Subscription) {
	for name, sub := range desired {
		if _, ok := active[name]; !ok {
			additions = append(additions, sub)
		}
	}
	for name, sub := range active {
		if _, ok := desired[name]; !ok {
			removals = append(removals, sub)
		}
	}
	sort.Slice(additions, func(i, j int) bool { return additions[i].String() < additions[j].String() })
	sort.Slice(removals, func(i, j int) bool { return removals[i].String() < removals[j].String() })
	return additions, removals
}

func sortedSubscriptions(values map[string]Subscription) []Subscription {
	result := make([]Subscription, 0, len(values))
	for _, sub := range values {
		result = append(result, sub)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result
}

func uniqueSubscriptions(values []Subscription) map[string]Subscription {
	result := make(map[string]Subscription, len(values))
	for _, sub := range values {
		result[sub.String()] = sub
	}
	return result
}

func cloneStringSet(values map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for value := range values {
		result[value] = struct{}{}
	}
	return result
}
