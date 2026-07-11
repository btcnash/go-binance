package managed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionActivePingPongKeepsReadyAndReportsRTT(t *testing.T) {
	socket := newFakeSocket()
	socket.autoPong = true
	dialer := &sequenceDialer{sockets: []Socket{socket}}

	conn := mustNewConnection(t, dialer, Options{
		Heartbeat: HeartbeatOptions{
			Enabled:      true,
			PingInterval: 20 * time.Millisecond,
			PongTimeout:  15 * time.Millisecond,
			WriteTimeout: 10 * time.Millisecond,
		},
		Reconnect: ReconnectPolicy{Enabled: false},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := conn.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)

	heartbeat := waitForHeartbeatKind(t, conn.Heartbeats(), HeartbeatPongReceived, time.Second)
	if heartbeat.Generation != 1 {
		t.Fatalf("heartbeat generation = %d, want 1", heartbeat.Generation)
	}
	if heartbeat.RTT <= 0 {
		t.Fatalf("heartbeat RTT = %s, want positive", heartbeat.RTT)
	}
	if heartbeat.Payload == "" {
		t.Fatal("heartbeat payload is empty")
	}
	if got := conn.Generation(); got != 1 {
		t.Fatalf("Generation() = %d, want 1", got)
	}
	if got := conn.State(); got != StateReady {
		t.Fatalf("State() = %s, want %s", got, StateReady)
	}
}

func TestConnectionPongTimeoutReconnectsWithinBudget(t *testing.T) {
	first := newFakeSocket()
	second := newFakeSocket()
	second.autoPong = true
	dialer := &sequenceDialer{sockets: []Socket{first, second}}

	conn := mustNewConnection(t, dialer, Options{
		Heartbeat: HeartbeatOptions{
			Enabled:      true,
			PingInterval: 20 * time.Millisecond,
			PongTimeout:  15 * time.Millisecond,
			WriteTimeout: 10 * time.Millisecond,
		},
		Reconnect: ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Multiplier:   1,
		},
	})

	started := time.Now()
	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForGenerationReady(t, conn.States(), 2, time.Second)

	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("reconnection took %s, want <= 250ms", elapsed)
	}
	if dialer.Count() < 2 {
		t.Fatalf("dial count = %d, want at least 2", dialer.Count())
	}
	if got := conn.ReconnectCount(); got < 1 {
		t.Fatalf("ReconnectCount() = %d, want at least 1", got)
	}
	if first.CloseCount() == 0 {
		t.Fatal("first socket was not closed after pong timeout")
	}
}

func TestConnectionPingWriteFailureReconnects(t *testing.T) {
	first := newFakeSocket()
	first.pingWriteErr = errors.New("ping write failed")
	second := newFakeSocket()
	second.autoPong = true

	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{first, second}}, Options{
		Heartbeat: HeartbeatOptions{
			Enabled:      true,
			PingInterval: 10 * time.Millisecond,
			PongTimeout:  20 * time.Millisecond,
			WriteTimeout: 10 * time.Millisecond,
		},
		Reconnect: ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Multiplier:   1,
		},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForGenerationReady(t, conn.States(), 2, time.Second)
	waitForErrorKind(t, conn.Errors(), ErrorPingWrite, time.Second)
}

func TestConnectionReadFailureReconnectsAndFencesOldGeneration(t *testing.T) {
	first := newFakeSocket()
	second := newFakeSocket()
	dialer := &sequenceDialer{sockets: []Socket{first, second}}

	conn := mustNewConnection(t, dialer, Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
		Reconnect: ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Multiplier:   1,
		},
		FrameBuffer: 4,
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForGenerationReady(t, conn.States(), 1, time.Second)
	first.failRead(errors.New("network down"))
	waitForGenerationReady(t, conn.States(), 2, time.Second)

	// A late frame from the old physical connection must never reach consumers.
	first.injectFrame(TextMessage, []byte("stale"))
	second.injectFrame(TextMessage, []byte("current"))

	select {
	case frame := <-conn.Frames():
		if string(frame.Payload) != "current" {
			t.Fatalf("frame payload = %q, want current", frame.Payload)
		}
		if frame.Generation != 2 {
			t.Fatalf("frame generation = %d, want 2", frame.Generation)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for current frame")
	}
}

func TestConnectionRespondsToServerPingWithMatchingPong(t *testing.T) {
	socket := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat: HeartbeatOptions{
			Enabled:      false,
			WriteTimeout: 20 * time.Millisecond,
		},
		Reconnect: ReconnectPolicy{Enabled: false},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)

	if err := socket.serverPing("binance-payload"); err != nil {
		t.Fatalf("serverPing() error = %v", err)
	}
	control := socket.waitControl(t, time.Second)
	if control.messageType != PongMessage {
		t.Fatalf("control type = %d, want PongMessage", control.messageType)
	}
	if string(control.payload) != "binance-payload" {
		t.Fatalf("pong payload = %q, want binance-payload", control.payload)
	}
}

func TestConnectionSerializesApplicationAndControlWrites(t *testing.T) {
	socket := newFakeSocket()
	socket.autoPong = true
	socket.writeDelay = 2 * time.Millisecond
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat: HeartbeatOptions{
			Enabled:      true,
			PingInterval: 10 * time.Millisecond,
			PongTimeout:  500 * time.Millisecond,
			WriteTimeout: 100 * time.Millisecond,
		},
		Reconnect:  ReconnectPolicy{Enabled: false},
		WriteQueue: 64,
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := conn.SendText(ctx, []byte(fmt.Sprintf("message-%d", i))); err != nil {
				t.Errorf("SendText() error = %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := socket.MaxConcurrentWrites(); got != 1 {
		t.Fatalf("max concurrent socket writes = %d, want 1", got)
	}
}

func TestConnectionCloseStopsReconnectAndGoroutines(t *testing.T) {
	first := newFakeSocket()
	dialer := &sequenceDialer{sockets: []Socket{first}, repeatErr: errors.New("dial unavailable")}
	conn := mustNewConnection(t, dialer, Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
		Reconnect: ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Multiplier:   1,
		},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)
	first.failRead(errors.New("disconnect"))
	waitForState(t, conn.States(), StateReconnecting, time.Second)

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("connection did not stop after Close")
	}

	count := dialer.Count()
	time.Sleep(30 * time.Millisecond)
	if got := dialer.Count(); got != count {
		t.Fatalf("dial count changed after Close: before=%d after=%d", count, got)
	}
	if got := conn.State(); got != StateClosed {
		t.Fatalf("State() = %s, want %s", got, StateClosed)
	}
}

func TestConnectionDoesNotSendQueuedFrameAfterCallerTimeout(t *testing.T) {
	socket := newFakeSocket()
	socket.writeBlock = make(chan struct{})
	socket.writeStarted = make(chan struct{}, 2)
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat:  HeartbeatOptions{Enabled: false, WriteTimeout: time.Second},
		Reconnect:  ReconnectPolicy{Enabled: false},
		WriteQueue: 4,
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- conn.SendText(context.Background(), []byte("first"))
	}()
	select {
	case <-socket.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := conn.SendText(ctx, []byte("expired"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second SendText() error = %v, want context deadline", err)
	}

	close(socket.writeBlock)
	if err := <-firstDone; err != nil {
		t.Fatalf("first SendText() error = %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	writes := socket.TextWrites()
	if len(writes) != 1 || string(writes[0]) != "first" {
		t.Fatalf("text writes = %q, want only first", writes)
	}
	if got := conn.State(); got != StateReady {
		t.Fatalf("State() = %s, canceled queued write must not fail connection", got)
	}
}

func TestConnectionCloseUsesSerializedCloseControlFrame(t *testing.T) {
	socket := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat: HeartbeatOptions{Enabled: false, WriteTimeout: 20 * time.Millisecond},
		Reconnect: ReconnectPolicy{Enabled: false},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	control := socket.waitControl(t, time.Second)
	if control.messageType != CloseMessage {
		t.Fatalf("control type = %d, want CloseMessage", control.messageType)
	}
}

func TestConnectionReconnectExhaustionIsTyped(t *testing.T) {
	dialer := &sequenceDialer{repeatErr: errors.New("dial unavailable")}
	conn := mustNewConnection(t, dialer, Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
		Reconnect: ReconnectPolicy{
			Enabled:      true,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Millisecond,
			Multiplier:   1,
			MaxAttempts:  2,
		},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateFailed, time.Second)
	waitForErrorKind(t, conn.Errors(), ErrorReconnectExhausted, time.Second)
	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("connection did not finish after reconnect exhaustion")
	}
	if got := conn.State(); got != StateFailed {
		t.Fatalf("State() = %s, want terminal %s", got, StateFailed)
	}
	if conn.TerminalError() == nil {
		t.Fatal("TerminalError() = nil, want reconnect exhaustion error")
	}
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := conn.WaitReady(readyCtx); err == nil {
		t.Fatal("WaitReady() error = nil after terminal failure")
	}
	if got := dialer.Count(); got != 3 {
		t.Fatalf("dial count = %d, want initial dial plus 2 retries", got)
	}
}

func TestConnectionFrameOverflowFailsInsteadOfDroppingSilently(t *testing.T) {
	socket := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat:   HeartbeatOptions{Enabled: false},
		Reconnect:   ReconnectPolicy{Enabled: false},
		FrameBuffer: 1,
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)
	socket.injectFrame(TextMessage, []byte("first"))
	socket.injectFrame(TextMessage, []byte("second"))
	waitForErrorKind(t, conn.Errors(), ErrorFrameBufferFull, time.Second)
	waitForState(t, conn.States(), StateFailed, time.Second)
}

func TestConnectionIgnoresOldGenerationControlCallbacks(t *testing.T) {
	first := newFakeSocket()
	second := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{first, second}}, Options{
		Heartbeat: HeartbeatOptions{Enabled: false, WriteTimeout: 20 * time.Millisecond},
		Reconnect: ReconnectPolicy{
			Enabled:      true,
			InitialDelay: time.Millisecond,
			MaxDelay:     time.Millisecond,
			Multiplier:   1,
		},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForGenerationReady(t, conn.States(), 1, time.Second)
	first.failRead(errors.New("disconnect"))
	waitForGenerationReady(t, conn.States(), 2, time.Second)

	if err := first.serverPing("stale"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("old generation ping callback error = %v, want ErrNotReady", err)
	}
	select {
	case control := <-first.controls:
		t.Fatalf("old generation emitted control frame: %+v", control)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestConnectionObserverReceivesLifecycleAndHeartbeatEvents(t *testing.T) {
	socket := newFakeSocket()
	socket.autoPong = true
	stateC := make(chan StateEvent, 8)
	heartbeatC := make(chan HeartbeatEvent, 8)
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat: HeartbeatOptions{
			Enabled:      true,
			PingInterval: 10 * time.Millisecond,
			PongTimeout:  10 * time.Millisecond,
			WriteTimeout: 10 * time.Millisecond,
		},
		Reconnect: ReconnectPolicy{Enabled: false},
		Observer: ObserverFuncs{
			State:     func(event StateEvent) { stateC <- event },
			Heartbeat: func(event HeartbeatEvent) { heartbeatC <- event },
		},
	})

	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case event := <-stateC:
		for event.State != StateReady {
			select {
			case event = <-stateC:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for observer ready state")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for observer state")
	}
	select {
	case event := <-heartbeatC:
		for event.Kind != HeartbeatPongReceived {
			select {
			case event = <-heartbeatC:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for observer pong")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for observer heartbeat")
	}
}

func TestConnectionStartIsSingleUse(t *testing.T) {
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{newFakeSocket()}}, Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
	})
	if err := conn.Start(context.Background()); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	if err := conn.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start() error = %v, want ErrAlreadyStarted", err)
	}
}

func TestConnectionContextCancellationStops(t *testing.T) {
	socket := newFakeSocket()
	conn := mustNewConnection(t, &sequenceDialer{sockets: []Socket{socket}}, Options{
		Heartbeat: HeartbeatOptions{Enabled: false},
		Reconnect: ReconnectPolicy{Enabled: true},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := conn.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForState(t, conn.States(), StateReady, time.Second)
	cancel()

	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("connection did not stop after context cancellation")
	}
	if socket.CloseCount() == 0 {
		t.Fatal("socket was not closed after context cancellation")
	}
}

func TestNewConnectionRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{
			name: "missing dialer",
			opts: Options{},
		},
		{
			name: "pong timeout without ping interval",
			opts: Options{
				Dialer: &sequenceDialer{},
				Heartbeat: HeartbeatOptions{
					Enabled:     true,
					PongTimeout: time.Second,
				},
			},
		},
		{
			name: "negative reconnect delay",
			opts: Options{
				Dialer: &sequenceDialer{},
				Reconnect: ReconnectPolicy{
					Enabled:      true,
					InitialDelay: -time.Second,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnection(tt.opts)
			if err == nil {
				t.Fatal("NewConnection() error = nil, want error")
			}
		})
	}
}

func mustNewConnection(t *testing.T, dialer Dialer, opts Options) *Connection {
	t.Helper()
	opts.Dialer = dialer
	conn, err := NewConnection(opts)
	if err != nil {
		t.Fatalf("NewConnection() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func waitForState(t *testing.T, states <-chan StateEvent, want State, timeout time.Duration) StateEvent {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-states:
			if !ok {
				t.Fatalf("state channel closed before %s", want)
			}
			if event.State == want {
				return event
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for state %s", want)
		}
	}
}

func waitForGenerationReady(t *testing.T, states <-chan StateEvent, generation uint64, timeout time.Duration) StateEvent {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-states:
			if !ok {
				t.Fatalf("state channel closed before ready generation %d", generation)
			}
			if event.State == StateReady && event.Generation == generation {
				return event
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for ready generation %d", generation)
		}
	}
}

func waitForHeartbeatKind(t *testing.T, events <-chan HeartbeatEvent, want HeartbeatKind, timeout time.Duration) HeartbeatEvent {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("heartbeat channel closed before %s", want)
			}
			if event.Kind == want {
				return event
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for heartbeat %s", want)
		}
	}
}

func waitForErrorKind(t *testing.T, events <-chan ErrorEvent, want ErrorKind, timeout time.Duration) ErrorEvent {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatalf("error channel closed before %s", want)
			}
			if event.Kind == want {
				return event
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for error kind %s", want)
		}
	}
}

type sequenceDialer struct {
	mu        sync.Mutex
	sockets   []Socket
	errors    []error
	repeatErr error
	count     int
}

func (d *sequenceDialer) Dial(ctx context.Context) (Socket, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.count++
	if len(d.errors) > 0 {
		err := d.errors[0]
		d.errors = d.errors[1:]
		return nil, err
	}
	if len(d.sockets) > 0 {
		socket := d.sockets[0]
		d.sockets = d.sockets[1:]
		return socket, nil
	}
	if d.repeatErr != nil {
		return nil, d.repeatErr
	}
	return nil, errors.New("no socket configured")
}

func (d *sequenceDialer) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

type fakeRead struct {
	messageType int
	payload     []byte
	err         error
}

type fakeControl struct {
	messageType int
	payload     []byte
}

type fakeSocket struct {
	readCh chan fakeRead

	mu            sync.Mutex
	pingHandler   func(string) error
	pongHandler   func(string) error
	controls      chan fakeControl
	autoPong      bool
	pingWriteErr  error
	writeDelay    time.Duration
	writeBlock    chan struct{}
	writeStarted  chan struct{}
	textWrites    [][]byte
	closeCount    int
	closed        chan struct{}
	closeOnce     sync.Once
	activeWrites  int32
	maxConcurrent int32
}

func newFakeSocket() *fakeSocket {
	return &fakeSocket{
		readCh:   make(chan fakeRead, 16),
		controls: make(chan fakeControl, 32),
		closed:   make(chan struct{}),
	}
}

func (s *fakeSocket) ReadMessage() (int, []byte, error) {
	select {
	case item := <-s.readCh:
		return item.messageType, item.payload, item.err
	case <-s.closed:
		return 0, nil, errors.New("socket closed")
	}
}

func (s *fakeSocket) WriteMessage(messageType int, data []byte) error {
	s.beginWrite()
	defer s.endWrite()
	if s.writeStarted != nil {
		select {
		case s.writeStarted <- struct{}{}:
		default:
		}
	}
	if s.writeBlock != nil {
		select {
		case <-s.writeBlock:
		case <-s.closed:
			return errors.New("socket closed")
		}
	}
	if s.writeDelay > 0 {
		time.Sleep(s.writeDelay)
	}
	select {
	case <-s.closed:
		return errors.New("socket closed")
	default:
	}
	if messageType == TextMessage {
		s.mu.Lock()
		s.textWrites = append(s.textWrites, append([]byte(nil), data...))
		s.mu.Unlock()
	}
	return nil
}

func (s *fakeSocket) WriteControl(messageType int, data []byte, deadline time.Time) error {
	s.beginWrite()
	defer s.endWrite()
	if s.writeDelay > 0 {
		time.Sleep(s.writeDelay)
	}
	if messageType == PingMessage && s.pingWriteErr != nil {
		return s.pingWriteErr
	}
	payload := append([]byte(nil), data...)
	select {
	case s.controls <- fakeControl{messageType: messageType, payload: payload}:
	default:
	}
	if messageType == PingMessage && s.autoPong {
		s.mu.Lock()
		handler := s.pongHandler
		s.mu.Unlock()
		if handler != nil {
			go func() {
				time.Sleep(time.Millisecond)
				_ = handler(string(payload))
			}()
		}
	}
	return nil
}

func (s *fakeSocket) SetPingHandler(handler func(string) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pingHandler = handler
}

func (s *fakeSocket) SetPongHandler(handler func(string) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pongHandler = handler
}

func (s *fakeSocket) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closeCount++
		s.mu.Unlock()
		close(s.closed)
	})
	return nil
}

func (s *fakeSocket) injectFrame(messageType int, payload []byte) {
	select {
	case s.readCh <- fakeRead{messageType: messageType, payload: append([]byte(nil), payload...)}:
	default:
	}
}

func (s *fakeSocket) failRead(err error) {
	s.readCh <- fakeRead{err: err}
}

func (s *fakeSocket) serverPing(payload string) error {
	s.mu.Lock()
	handler := s.pingHandler
	s.mu.Unlock()
	if handler == nil {
		return errors.New("ping handler not installed")
	}
	return handler(payload)
}

func (s *fakeSocket) waitControl(t *testing.T, timeout time.Duration) fakeControl {
	t.Helper()
	select {
	case control := <-s.controls:
		return control
	case <-time.After(timeout):
		t.Fatal("timeout waiting for control frame")
		return fakeControl{}
	}
}

func (s *fakeSocket) CloseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

func (s *fakeSocket) TextWrites() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([][]byte, len(s.textWrites))
	for i := range s.textWrites {
		result[i] = append([]byte(nil), s.textWrites[i]...)
	}
	return result
}

func (s *fakeSocket) beginWrite() {
	current := atomic.AddInt32(&s.activeWrites, 1)
	for {
		max := atomic.LoadInt32(&s.maxConcurrent)
		if current <= max || atomic.CompareAndSwapInt32(&s.maxConcurrent, max, current) {
			break
		}
	}
}

func (s *fakeSocket) endWrite() {
	atomic.AddInt32(&s.activeWrites, -1)
}

func (s *fakeSocket) MaxConcurrentWrites() int32 {
	return atomic.LoadInt32(&s.maxConcurrent)
}
