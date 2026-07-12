package private

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	"github.com/btcnash/go-binance/v2/futures"
)

const (
	defaultKeepAliveInterval = 30 * time.Minute
	defaultKeepAliveTimeout  = 10 * time.Second
	defaultKeepAliveRetry    = time.Second
	defaultKeepAliveRetryMax = 30 * time.Second
	defaultKeepAliveFactor   = 2.0
	defaultKeepAliveAttempts = 3
	defaultPrivateBuffer     = 64
	defaultEventBuffer       = 256
	defaultConnectionMaxAge  = 23*time.Hour + 50*time.Minute
)

type observationKind uint8

const (
	observationState observationKind = iota + 1
	observationError
	observationGap
	observationListenKey
)

type observation struct {
	kind      observationKind
	state     StateEvent
	err       ErrorEvent
	gap       GapEvent
	listenKey ListenKeyEvent
}

// Session owns one logical private user-data stream across physical reconnects.
type Session struct {
	opts       SessionOptions
	conn       *managedws.Connection
	dialer     *privateDialer
	sources    []*sourceRuntime
	sourceByID map[string]*sourceRuntime

	lifecycleMu sync.Mutex
	started     bool
	closed      bool
	terminated  bool
	cancel      context.CancelFunc
	done        chan struct{}
	finishOnce  sync.Once

	outputMu     sync.Mutex
	outputClosed bool

	mu              sync.Mutex
	state           State
	generation      uint64
	currentBindings []sourceBinding
	bindingIndex    atomic.Pointer[bindingSnapshot]
	terminalErr     error
	everReady       bool

	events     chan Event
	states     chan StateEvent
	errors     chan ErrorEvent
	gaps       chan GapEvent
	listenKeys chan ListenKeyEvent
	firstReady chan struct{}
	readyOnce  sync.Once

	observations chan observation
	observerDone chan struct{}
	workers      sync.WaitGroup
}

// NewSession creates an idle managed private user-data session.
func NewSession(opts SessionOptions) (*Session, error) {
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	session := &Session{
		opts:         normalized,
		done:         make(chan struct{}),
		state:        StateIdle,
		events:       make(chan Event, normalized.EventBuffer),
		states:       make(chan StateEvent, normalized.StateBuffer),
		errors:       make(chan ErrorEvent, normalized.ErrorBuffer),
		gaps:         make(chan GapEvent, normalized.GapBuffer),
		listenKeys:   make(chan ListenKeyEvent, normalized.LifecycleBuffer),
		firstReady:   make(chan struct{}),
		observations: make(chan observation, normalized.ObserverBuffer),
		observerDone: make(chan struct{}),
		sourceByID:   make(map[string]*sourceRuntime, len(normalized.Sources)),
	}
	session.sources = make([]*sourceRuntime, 0, len(normalized.Sources))
	for _, source := range normalized.Sources {
		runtime := newSourceRuntime(source, session.emitListenKey)
		session.sources = append(session.sources, runtime)
		session.sourceByID[source.ID] = runtime
	}
	session.dialer = &privateDialer{
		mode:             normalized.Mode,
		root:             normalized.Endpoint,
		endpointDialer:   normalized.EndpointDialer,
		invalidDialError: normalized.InvalidListenKeyDialError,
		sources:          session.sources,
	}
	connectionOptions := managedConnectionOptions(normalized.Connection)
	connectionOptions.Dialer = session.dialer
	conn, err := managedws.NewConnection(connectionOptions)
	if err != nil {
		return nil, privateError(ErrorInvalidOptions, "", 0, "connection", err)
	}
	session.conn = conn
	return session, nil
}

func normalizeOptions(opts SessionOptions) (SessionOptions, error) {
	if opts.Mode == "" {
		opts.Mode = ModeIsolated
	}
	if opts.Mode != ModeIsolated && opts.Mode != ModeShared {
		return SessionOptions{}, invalidOption("mode must be isolated or shared")
	}
	if opts.Environment == "" {
		opts.Environment = EnvironmentMainnet
	}
	root, ok := privateRoots[opts.Environment]
	if !ok {
		return SessionOptions{}, invalidOption("unsupported environment %q", opts.Environment)
	}
	if strings.TrimSpace(opts.Endpoint) == "" {
		opts.Endpoint = root
	}
	if opts.EndpointDialer == nil {
		opts.EndpointDialer = gorillaEndpointDialer{}
	}
	if opts.InvalidListenKeyDialError == nil {
		opts.InvalidListenKeyDialError = defaultInvalidListenKeyDialError
	}
	if len(opts.Sources) == 0 {
		return SessionOptions{}, invalidOption("at least one source is required")
	}
	if opts.Mode == ModeIsolated && len(opts.Sources) != 1 {
		return SessionOptions{}, invalidOption("isolated mode requires exactly one source")
	}
	seenIDs := make(map[string]struct{}, len(opts.Sources))
	normalizedSources := make([]Source, 0, len(opts.Sources))
	for _, source := range opts.Sources {
		source.ID = strings.TrimSpace(source.ID)
		if source.ID == "" {
			return SessionOptions{}, privateError(ErrorInvalidSource, "", 0, "validate", fmt.Errorf("%w: source ID is required", ErrInvalidSource))
		}
		if _, exists := seenIDs[source.ID]; exists {
			return SessionOptions{}, privateError(ErrorInvalidSource, source.ID, 0, "validate", fmt.Errorf("%w: duplicate source ID", ErrInvalidSource))
		}
		seenIDs[source.ID] = struct{}{}
		if source.Provider == nil {
			return SessionOptions{}, privateError(ErrorInvalidSource, source.ID, 0, "validate", fmt.Errorf("%w: provider is required", ErrInvalidSource))
		}
		eventSeen := make(map[futures.UserDataEventType]struct{}, len(source.Events))
		events := make([]futures.UserDataEventType, 0, len(source.Events))
		for _, event := range source.Events {
			name := strings.TrimSpace(string(event))
			if name == "" || strings.ContainsAny(name, "/?&= \t\r\n") {
				return SessionOptions{}, privateError(ErrorInvalidSource, source.ID, 0, "validate", fmt.Errorf("%w: invalid event %q", ErrInvalidSource, event))
			}
			normalizedEvent := futures.UserDataEventType(name)
			if _, exists := eventSeen[normalizedEvent]; exists {
				continue
			}
			eventSeen[normalizedEvent] = struct{}{}
			events = append(events, normalizedEvent)
		}
		source.Events = events
		normalizedSources = append(normalizedSources, source)
	}
	opts.Sources = normalizedSources

	if opts.KeepAlive.Interval < 0 || opts.KeepAlive.Timeout < 0 || opts.KeepAlive.RetryInitial < 0 || opts.KeepAlive.RetryMax < 0 || opts.KeepAlive.Multiplier < 0 || opts.KeepAlive.MaxAttempts < 0 {
		return SessionOptions{}, invalidOption("keepalive durations and attempts must not be negative")
	}
	if opts.KeepAlive.Interval == 0 {
		opts.KeepAlive.Interval = defaultKeepAliveInterval
	}
	if opts.KeepAlive.Timeout == 0 {
		opts.KeepAlive.Timeout = defaultKeepAliveTimeout
	}
	if opts.KeepAlive.RetryInitial == 0 {
		opts.KeepAlive.RetryInitial = defaultKeepAliveRetry
	}
	if opts.KeepAlive.RetryMax == 0 {
		opts.KeepAlive.RetryMax = defaultKeepAliveRetryMax
	}
	if opts.KeepAlive.RetryMax < opts.KeepAlive.RetryInitial {
		return SessionOptions{}, invalidOption("keepalive retry max must be >= retry initial")
	}
	if opts.KeepAlive.Multiplier == 0 {
		opts.KeepAlive.Multiplier = defaultKeepAliveFactor
	}
	if opts.KeepAlive.Multiplier < 1 {
		return SessionOptions{}, invalidOption("keepalive multiplier must be >= 1")
	}
	if opts.KeepAlive.MaxAttempts == 0 {
		opts.KeepAlive.MaxAttempts = defaultKeepAliveAttempts
	}

	buffers := []*int{&opts.EventBuffer, &opts.StateBuffer, &opts.ErrorBuffer, &opts.GapBuffer, &opts.LifecycleBuffer, &opts.ObserverBuffer}
	for _, buffer := range buffers {
		if *buffer < 0 {
			return SessionOptions{}, invalidOption("buffer sizes must not be negative")
		}
	}
	if opts.EventBuffer == 0 {
		opts.EventBuffer = defaultEventBuffer
	}
	if opts.StateBuffer == 0 {
		opts.StateBuffer = defaultPrivateBuffer
	}
	if opts.ErrorBuffer == 0 {
		opts.ErrorBuffer = defaultPrivateBuffer
	}
	if opts.GapBuffer == 0 {
		opts.GapBuffer = defaultPrivateBuffer
	}
	if opts.LifecycleBuffer == 0 {
		opts.LifecycleBuffer = defaultPrivateBuffer
	}
	if opts.ObserverBuffer == 0 {
		opts.ObserverBuffer = defaultPrivateBuffer
	}

	c := opts.Connection
	durations := []time.Duration{c.HeartbeatPingInterval, c.HeartbeatPongTimeout, c.HeartbeatWriteTimeout, c.ReconnectInitialDelay, c.ReconnectMaxDelay, c.StableResetTime}
	for _, duration := range durations {
		if duration < 0 {
			return SessionOptions{}, invalidOption("connection durations must not be negative")
		}
	}
	if c.ReconnectJitter < 0 || c.ReconnectJitter > 1 || c.ReconnectMultiplier < 0 || c.ReconnectMaxAttempts < 0 {
		return SessionOptions{}, invalidOption("invalid reconnect policy")
	}
	return opts, nil
}

func managedConnectionOptions(options ConnectionOptions) managedws.Options {
	heartbeat := managedws.HeartbeatOptions{Enabled: !options.DisableHeartbeat, PingInterval: options.HeartbeatPingInterval, PongTimeout: options.HeartbeatPongTimeout, WriteTimeout: options.HeartbeatWriteTimeout}
	reconnect := managedws.ReconnectPolicy{Enabled: !options.DisableReconnect, InitialDelay: options.ReconnectInitialDelay, MaxDelay: options.ReconnectMaxDelay, Multiplier: options.ReconnectMultiplier, Jitter: options.ReconnectJitter, MaxAttempts: options.ReconnectMaxAttempts, StableResetTime: options.StableResetTime}
	maxAge := options.MaxConnectionAge
	if !options.DisableRotation && maxAge == 0 {
		maxAge = defaultConnectionMaxAge
	}
	return managedws.Options{
		Heartbeat:        heartbeat,
		Reconnect:        reconnect,
		MaxConnectionAge: maxAge,
		WriteQueue:       options.WriteQueue,
		FrameBuffer:      options.FrameBuffer,
		StateBuffer:      options.StateBuffer,
		HeartbeatBuffer:  options.HeartbeatBuffer,
		ErrorBuffer:      options.ErrorBuffer,
		ObserverBuffer:   options.ObserverBuffer,
		Observer:         options.Observer,
	}
}

// Start launches transport, event, keepalive, and observation loops.
func (s *Session) Start(parent context.Context) error {
	if parent == nil {
		return invalidOption("context is nil")
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

	go s.observerLoop()
	s.transition(StateConnecting, ReasonStart, 0, nil)
	if err := s.conn.Start(ctx); err != nil {
		cancel()
		close(s.observations)
		<-s.observerDone
		return err
	}

	s.workers.Add(3 + len(s.sources))
	go s.transportStateLoop(ctx)
	go s.transportErrorLoop(ctx)
	go s.frameLoop(ctx)
	for _, source := range s.sources {
		go s.keepAliveLoop(ctx, source)
	}
	go s.finalize(ctx)
	return nil
}

// WaitReady waits until a physical private connection using the current listen
// keys is established.
func (s *Session) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return invalidOption("context is nil")
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
func (s *Session) Close() error {
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
	// Start the observer drain for the never-started lifecycle so transition
	// and release observations cannot deadlock Close.
	go s.observerLoop()
	s.transition(StateClosed, ReasonUserClosed, 0, nil)
	if !s.opts.RetainListenKeys {
		s.releaseSources(context.Background())
	}
	close(s.observations)
	<-s.observerDone
	s.finish()
	return nil
}

func (s *Session) Done() <-chan struct{}                  { return s.done }
func (s *Session) Events() <-chan Event                   { return s.events }
func (s *Session) States() <-chan StateEvent              { return s.states }
func (s *Session) Errors() <-chan ErrorEvent              { return s.errors }
func (s *Session) Gaps() <-chan GapEvent                  { return s.gaps }
func (s *Session) ListenKeyEvents() <-chan ListenKeyEvent { return s.listenKeys }
func (s *Session) State() State                           { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *Session) Generation() uint64                     { s.mu.Lock(); defer s.mu.Unlock(); return s.generation }
func (s *Session) TerminalError() error                   { s.mu.Lock(); defer s.mu.Unlock(); return s.terminalErr }

func (s *Session) transportStateLoop(ctx context.Context) {
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

func (s *Session) handleTransportState(event managedws.StateEvent) {
	switch event.State {
	case managedws.StateConnecting:
		s.transition(StateConnecting, ReasonStart, event.Generation, event.Err)
	case managedws.StateReconnecting:
		s.transition(StateReconnecting, ReasonTransportReconnecting, event.Generation, event.Err)
	case managedws.StateDisconnected:
		ids := s.currentSourceIDs()
		s.transition(StateDisconnected, ReasonTransportDisconnected, event.Generation, event.Err)
		if s.hasEverBeenReady() {
			s.emitGap(GapEvent{Reason: GapReasonDisconnected, FromGeneration: event.Generation, SourceIDs: ids, At: time.Now(), Err: event.Err})
		}
	case managedws.StateReady:
		snapshot := s.dialer.Snapshot()
		if !s.snapshotCurrent(snapshot) {
			s.transition(StateRefreshing, ReasonListenKeyRefresh, event.Generation, ErrListenKeyExpired)
			_ = s.conn.Interrupt(ErrListenKeyExpired)
			return
		}
		bindings := cloneBindings(snapshot.Bindings)
		s.mu.Lock()
		s.generation = event.Generation
		s.currentBindings = bindings
		s.bindingIndex.Store(newBindingSnapshot(bindings))
		s.everReady = true
		s.mu.Unlock()
		s.transition(StateReady, ReasonTransportReady, event.Generation, nil)
		s.readyOnce.Do(func() { close(s.firstReady) })
	case managedws.StateFailed:
		s.mu.Lock()
		s.terminalErr = event.Err
		s.mu.Unlock()
		s.transition(StateFailed, ReasonTransportFailed, event.Generation, event.Err)
		s.cancelSession()
	}
}

func (s *Session) transportErrorLoop(ctx context.Context) {
	defer s.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-s.conn.Errors():
			if !ok {
				return
			}
			var protocolErr *PrivateError
			if errors.As(event.Err, &protocolErr) {
				s.emitError(event.Err)
				continue
			}
			s.emitError(privateError(ErrorTransport, "", event.Generation, event.Operation, event.Err))
		}
	}
}

func (s *Session) frameLoop(ctx context.Context) {
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

type eventEnvelope struct {
	Stream    string          `json:"stream"`
	ListenKey string          `json:"listenKey"`
	Data      json.RawMessage `json:"data"`
}

func (s *Session) handleFrame(frame managedws.Frame) {
	if frame.Type != managedws.TextMessage && frame.Type != managedws.BinaryMessage {
		return
	}
	s.mu.Lock()
	if frame.Generation != s.generation {
		s.mu.Unlock()
		// Transport state and frame delivery use independent channels. A fast
		// server can send the first event before the logical state loop observes
		// managed.StateReady. Adopt the current dial snapshot only when it still
		// matches every source runtime; otherwise the frame belongs to a stale
		// or invalidated generation and must be fenced.
		snapshot := s.dialer.Snapshot()
		if frame.Generation != s.conn.Generation() || !s.snapshotCurrent(snapshot) {
			return
		}
		s.mu.Lock()
		if frame.Generation > s.generation {
			bindings := cloneBindings(snapshot.Bindings)
			s.generation = frame.Generation
			s.currentBindings = bindings
			s.bindingIndex.Store(newBindingSnapshot(bindings))
			s.everReady = true
		}
	}
	bindingIndex := s.bindingIndex.Load()
	if bindingIndex == nil {
		bindingIndex = newBindingSnapshot(s.currentBindings)
		s.bindingIndex.Store(bindingIndex)
	}
	s.mu.Unlock()

	raw := json.RawMessage(frame.Payload)
	payload := raw
	var envelope eventEnvelope
	if json.Unmarshal(raw, &envelope) == nil && len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		payload = envelope.Data
	}
	decoded := new(futures.WsUserDataEvent)
	decodeErr := json.Unmarshal(payload, decoded)
	eventType := decoded.Event
	if eventType == "" {
		if code, message, rejected := exactPrivateRejection(payload); rejected && isInvalidListenKeyCode(code) {
			affected := bindingIndex.allSourceIDs()
			wrapped := privateError(ErrorExpired, "", frame.Generation, "private_rejected", fmt.Errorf("%w: code=%d message=%q", ErrListenKeyExpired, code, message))
			s.emitError(wrapped)
			s.emitGap(GapEvent{Reason: GapReasonListenKeyExpired, FromGeneration: frame.Generation, SourceIDs: affected, At: time.Now(), Err: wrapped})
			s.invalidateSources(affected, wrapped)
			s.transition(StateRefreshing, ReasonListenKeyRefresh, frame.Generation, wrapped)
			_ = s.conn.Interrupt(wrapped)
			return
		}
		wrapped := privateError(ErrorProtocol, "", frame.Generation, "decode_header", fmt.Errorf("%w: %v", ErrMalformedEvent, decodeErr))
		s.emitError(wrapped)
		s.emitGap(GapEvent{Reason: GapReasonMalformedEvent, FromGeneration: frame.Generation, SourceIDs: bindingIndex.allSourceIDs(), At: time.Now(), Err: wrapped})
		_ = s.conn.Interrupt(wrapped)
		return
	}

	sourceID, candidates, resolution := bindingIndex.resolve(eventType, envelope.ListenKey, envelope.Stream)
	event := Event{
		Generation:         frame.Generation,
		SourceID:           sourceID,
		CandidateSourceIDs: candidates,
		SourceResolution:   resolution,
		Type:               eventType,
		Decoded:            decoded,
		DecodeError:        decodeErr,
		Raw:                payload,
		ReceivedAt:         frame.ReceivedAt,
	}
	if decodeErr != nil {
		event.Decoded = nil
		s.emitError(privateError(ErrorProtocol, sourceID, frame.Generation, "decode_event", decodeErr))
	}
	if sourceID == "" && len(candidates) > 1 {
		s.emitError(privateError(ErrorAmbiguousSource, "", frame.Generation, "attribute_event", fmt.Errorf("%w: event=%s candidates=%v", ErrAmbiguousSource, eventType, candidates)))
	}

	if !s.publishEvent(event) {
		wrapped := privateError(ErrorEventOverflow, sourceID, frame.Generation, "deliver_event", ErrEventBufferFull)
		s.emitError(wrapped)
		s.emitGap(GapEvent{Reason: GapReasonEventOverflow, FromGeneration: frame.Generation, SourceIDs: candidateOrAll(sourceID, candidates, bindingIndex), At: time.Now(), Err: wrapped})
		_ = s.conn.Interrupt(wrapped)
		return
	}

	if eventType == futures.UserDataEventTypeListenKeyExpired {
		affected := candidateOrAll(sourceID, candidates, bindingIndex)
		wrapped := privateError(ErrorExpired, sourceID, frame.Generation, "listen_key_expired", ErrListenKeyExpired)
		s.emitGap(GapEvent{Reason: GapReasonListenKeyExpired, FromGeneration: frame.Generation, SourceIDs: affected, At: time.Now(), Err: wrapped})
		s.invalidateSources(affected, wrapped)
		s.transition(StateRefreshing, ReasonListenKeyRefresh, frame.Generation, wrapped)
		_ = s.conn.Interrupt(wrapped)
	}
}

func exactPrivateRejection(payload []byte) (int, string, bool) {
	var rejection struct {
		Code *int   `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(payload, &rejection); err != nil || rejection.Code == nil {
		return 0, "", false
	}
	return *rejection.Code, rejection.Msg, true
}

func isInvalidListenKeyCode(code int) bool {
	switch code {
	case -1125, -2015:
		return true
	default:
		return false
	}
}

func candidateOrAll(sourceID string, candidates []string, bindings *bindingSnapshot) []string {
	if sourceID != "" {
		return []string{sourceID}
	}
	if len(candidates) > 0 {
		return append([]string(nil), candidates...)
	}
	return bindings.allSourceIDs()
}

func (s *Session) keepAliveLoop(ctx context.Context, source *sourceRuntime) {
	defer s.workers.Done()
	for {
		key, version, err := source.waitForKey(ctx)
		if err != nil {
			return
		}
		timer := time.NewTimer(s.opts.KeepAlive.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		current, currentVersion := source.current()
		if current != key || currentVersion != version {
			continue
		}
		if s.keepAliveWithRetry(ctx, source, key, version) {
			continue
		}
		// The source was invalidated; reconnect will reacquire it.
	}
}

func (s *Session) keepAliveWithRetry(ctx context.Context, source *sourceRuntime, key string, version uint64) bool {
	delay := s.opts.KeepAlive.RetryInitial
	var lastErr error
	for attempt := 1; attempt <= s.opts.KeepAlive.MaxAttempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, s.opts.KeepAlive.Timeout)
		err := source.keepAlive(callCtx, key)
		cancel()
		if err == nil {
			s.emitListenKey(ListenKeyEvent{Kind: ListenKeyKeepAliveSucceeded, SourceID: source.spec.ID, Version: version, Attempt: attempt, At: time.Now()})
			return true
		}
		lastErr = privateError(ErrorKeepAlive, source.spec.ID, s.Generation(), "keepalive", fmt.Errorf("%w: %v", ErrListenKeyKeepAlive, err))
		s.emitListenKey(ListenKeyEvent{Kind: ListenKeyKeepAliveFailed, SourceID: source.spec.ID, Version: version, Attempt: attempt, At: time.Now(), Err: lastErr})
		s.emitError(lastErr)
		if classifier, ok := source.spec.Provider.(InvalidListenKeyClassifier); ok && classifier.IsInvalidListenKey(err) {
			break
		}
		if attempt == s.opts.KeepAlive.MaxAttempts {
			break
		}
		if !waitContext(ctx, delay) {
			return false
		}
		delay = nextDelay(delay, s.opts.KeepAlive.RetryMax, s.opts.KeepAlive.Multiplier)
	}
	wrapped := lastErr
	if wrapped == nil {
		wrapped = privateError(ErrorKeepAlive, source.spec.ID, s.Generation(), "keepalive", ErrListenKeyKeepAlive)
	}
	source.invalidate(wrapped)
	s.emitGap(GapEvent{Reason: GapReasonKeepAliveFailed, FromGeneration: s.Generation(), SourceIDs: []string{source.spec.ID}, At: time.Now(), Err: wrapped})
	s.transition(StateRefreshing, ReasonListenKeyRefresh, s.Generation(), wrapped)
	_ = s.conn.Interrupt(wrapped)
	return false
}

func nextDelay(current, max time.Duration, multiplier float64) time.Duration {
	next := time.Duration(float64(current) * multiplier)
	if next <= 0 || next > max {
		return max
	}
	return next
}
func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Session) snapshotCurrent(snapshot dialSnapshot) bool {
	if len(snapshot.Bindings) != len(s.sources) {
		return false
	}
	for _, binding := range snapshot.Bindings {
		source := s.sourceByID[binding.SourceID]
		if source == nil {
			return false
		}
		key, version := source.current()
		if key == "" || key != binding.ListenKey || version != binding.Version {
			return false
		}
	}
	return true
}

func (s *Session) invalidateSources(ids []string, err error) {
	for _, id := range ids {
		if source := s.sourceByID[id]; source != nil {
			source.invalidate(err)
		}
	}
}

func (s *Session) currentSourceIDs() []string {
	if bindings := s.bindingIndex.Load(); bindings != nil && len(bindings.sourceIDs) > 0 {
		return bindings.allSourceIDs()
	}
	if len(s.sources) > 0 {
		ids := make([]string, 0, len(s.sources))
		for _, source := range s.sources {
			ids = append(ids, source.spec.ID)
		}
		sort.Strings(ids)
		return ids
	}
	return nil
}
func (s *Session) hasEverBeenReady() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.everReady }

func (s *Session) finalize(ctx context.Context) {
	<-s.conn.Done()
	s.cancelSession()
	s.lifecycleMu.Lock()
	closedByUser := s.closed
	s.terminated = true
	s.lifecycleMu.Unlock()
	s.workers.Wait()

	if !s.opts.RetainListenKeys {
		s.releaseSources(context.Background())
	}

	s.mu.Lock()
	if err := s.conn.TerminalError(); err != nil {
		s.terminalErr = err
	}
	terminal := s.terminalErr
	generation := s.generation
	s.mu.Unlock()
	if terminal != nil {
		s.transition(StateFailed, ReasonTransportFailed, generation, terminal)
	} else if closedByUser {
		s.transition(StateClosed, ReasonUserClosed, generation, nil)
	} else {
		s.transition(StateClosed, ReasonContextCanceled, generation, ctx.Err())
	}
	close(s.observations)
	<-s.observerDone
	s.finish()
}

func (s *Session) releaseSources(parent context.Context) {
	seen := make(map[string]struct{})
	for _, source := range s.sources {
		key, _ := source.current()
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			source.invalidate(nil)
			continue
		}
		seen[key] = struct{}{}
		ctx, cancel := context.WithTimeout(parent, s.opts.KeepAlive.Timeout)
		err := source.release(ctx)
		cancel()
		if err != nil {
			s.emitError(privateError(ErrorRelease, source.spec.ID, s.Generation(), "release", fmt.Errorf("%w: %v", ErrListenKeyRelease, err)))
		}
	}
}

func (s *Session) cancelSession() {
	s.lifecycleMu.Lock()
	cancel := s.cancel
	s.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Session) finish() {
	s.finishOnce.Do(func() {
		s.outputMu.Lock()
		s.outputClosed = true
		close(s.done)
		close(s.events)
		close(s.states)
		close(s.errors)
		close(s.gaps)
		close(s.listenKeys)
		s.outputMu.Unlock()
	})
}

func (s *Session) transition(next State, reason StateReason, generation uint64, err error) {
	s.mu.Lock()
	previous := s.state
	if previous == next && generation == s.generation {
		s.mu.Unlock()
		return
	}
	if generation != 0 {
		s.generation = generation
	}
	s.state = next
	event := StateEvent{Previous: previous, State: next, Reason: reason, Generation: s.generation, SourceIDs: s.currentSourceIDsLocked(), At: time.Now(), Err: err}
	s.mu.Unlock()
	s.publishState(event)
}
func (s *Session) currentSourceIDsLocked() []string {
	if len(s.currentBindings) > 0 {
		return sortedSourceIDs(s.currentBindings)
	}
	ids := make([]string, 0, len(s.sources))
	for _, source := range s.sources {
		ids = append(ids, source.spec.ID)
	}
	sort.Strings(ids)
	return ids
}

func (s *Session) publishEvent(event Event) bool {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	if s.outputClosed {
		return false
	}
	select {
	case s.events <- event:
		return true
	default:
		return false
	}
}
func (s *Session) publishState(event StateEvent) {
	s.outputMu.Lock()
	if !s.outputClosed {
		select {
		case s.states <- event:
		default:
		}
	}
	s.outputMu.Unlock()
	s.publishObservation(observation{kind: observationState, state: event})
}
func (s *Session) emitError(err error) {
	if err == nil {
		return
	}
	event := ErrorEvent{Kind: ErrorProtocol, At: time.Now(), Err: err}
	var privateErr *PrivateError
	if errors.As(err, &privateErr) {
		event.Kind = privateErr.Kind
		event.SourceID = privateErr.SourceID
		event.Generation = privateErr.Generation
		event.Operation = privateErr.Operation
	}
	s.outputMu.Lock()
	if !s.outputClosed {
		select {
		case s.errors <- event:
		default:
		}
	}
	s.outputMu.Unlock()
	s.publishObservation(observation{kind: observationError, err: event})
}
func (s *Session) emitGap(event GapEvent) {
	event.SourceIDs = sortedUnique(event.SourceIDs)
	s.outputMu.Lock()
	if !s.outputClosed {
		select {
		case s.gaps <- event:
		default:
		}
	}
	s.outputMu.Unlock()
	s.publishObservation(observation{kind: observationGap, gap: event})
}
func (s *Session) emitListenKey(event ListenKeyEvent) {
	s.outputMu.Lock()
	if !s.outputClosed {
		select {
		case s.listenKeys <- event:
		default:
		}
	}
	s.outputMu.Unlock()
	s.publishObservation(observation{kind: observationListenKey, listenKey: event})
}
func sortedUnique(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
func (s *Session) publishObservation(event observation) {
	if s.opts.Observer == nil {
		return
	}
	defer func() { _ = recover() }()
	select {
	case s.observations <- event:
	default:
	}
}
func (s *Session) observerLoop() {
	defer close(s.observerDone)
	for event := range s.observations {
		switch event.kind {
		case observationState:
			s.opts.Observer.OnState(event.state)
		case observationError:
			s.opts.Observer.OnError(event.err)
		case observationGap:
			s.opts.Observer.OnGap(event.gap)
		case observationListenKey:
			s.opts.Observer.OnListenKey(event.listenKey)
		}
	}
}
