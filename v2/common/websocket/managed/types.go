// Package managed provides a transport-level managed WebSocket connection.
//
// It owns the physical connection lifecycle, serializes all writes, performs
// active Ping/Pong liveness checks, reconnects failed connections, and fences
// frames from older physical connection generations. Higher-level stream
// subscription and WebSocket API request semantics intentionally live above
// this package.
package managed

import (
	"context"
	"time"
)

// RFC 6455 message type values. They intentionally match gorilla/websocket
// without making the managed core depend on a concrete WebSocket library.
const (
	TextMessage   = 1
	BinaryMessage = 2
	CloseMessage  = 8
	PingMessage   = 9
	PongMessage   = 10
)

// Socket is the minimum physical WebSocket surface required by Connection.
// Implementations must allow one concurrent reader and one concurrent writer,
// and Close must unblock an in-progress ReadMessage. Connection itself
// guarantees that every write is serialized.
type Socket interface {
	ReadMessage() (messageType int, payload []byte, err error)
	WriteMessage(messageType int, payload []byte) error
	WriteControl(messageType int, payload []byte, deadline time.Time) error
	SetPingHandler(handler func(appData string) error)
	SetPongHandler(handler func(appData string) error)
	Close() error
}

// Dialer creates a new physical WebSocket connection.
type Dialer interface {
	Dial(ctx context.Context) (Socket, error)
}

// DialFunc adapts a function to Dialer.
type DialFunc func(ctx context.Context) (Socket, error)

// Dial implements Dialer.
func (f DialFunc) Dial(ctx context.Context) (Socket, error) {
	return f(ctx)
}

// State is the managed connection lifecycle state.
type State string

const (
	StateIdle         State = "idle"
	StateConnecting   State = "connecting"
	StateConnected    State = "connected"
	StateReady        State = "ready"
	StateDisconnected State = "disconnected"
	StateReconnecting State = "reconnecting"
	StateClosed       State = "closed"
	StateFailed       State = "failed"
)

// StateReason explains why a lifecycle transition occurred.
type StateReason string

const (
	ReasonStart              StateReason = "start"
	ReasonDialSucceeded      StateReason = "dial_succeeded"
	ReasonSessionReady       StateReason = "session_ready"
	ReasonDialFailed         StateReason = "dial_failed"
	ReasonReadFailed         StateReason = "read_failed"
	ReasonWriteFailed        StateReason = "write_failed"
	ReasonPingWriteFailed    StateReason = "ping_write_failed"
	ReasonPongTimeout        StateReason = "pong_timeout"
	ReasonInterrupted        StateReason = "interrupted"
	ReasonFrameBufferFull    StateReason = "frame_buffer_full"
	ReasonReconnectScheduled StateReason = "reconnect_scheduled"
	ReasonReconnectExhausted StateReason = "reconnect_exhausted"
	ReasonMaxAgeReached      StateReason = "max_age_reached"
	ReasonContextCanceled    StateReason = "context_canceled"
	ReasonUserClosed         StateReason = "user_closed"
)

// StateEvent describes one state transition.
type StateEvent struct {
	Previous   State
	State      State
	Reason     StateReason
	Generation uint64
	Attempt    int
	At         time.Time
	Err        error
}

// Frame is an application data frame read from the current physical
// connection. Generation lets consumers fence stale data across reconnects.
type Frame struct {
	Generation uint64
	Type       int
	Payload    []byte
	ReceivedAt time.Time
}

// HeartbeatKind identifies heartbeat activity.
type HeartbeatKind string

const (
	HeartbeatPingSent       HeartbeatKind = "ping_sent"
	HeartbeatPongReceived   HeartbeatKind = "pong_received"
	HeartbeatServerPing     HeartbeatKind = "server_ping_received"
	HeartbeatServerPongSent HeartbeatKind = "server_pong_sent"
)

// HeartbeatEvent describes active or server-initiated heartbeat activity.
type HeartbeatEvent struct {
	Kind       HeartbeatKind
	Generation uint64
	Payload    string
	RTT        time.Duration
	At         time.Time
}

// ErrorEvent exposes a classified connection error without requiring callers
// to parse error strings.
type ErrorEvent struct {
	Kind       ErrorKind
	Generation uint64
	Operation  string
	At         time.Time
	Err        error
}

// Stats is a lock-free snapshot of managed transport activity. Counters are
// monotonic for the lifetime of a Connection and may be sampled concurrently.
type Stats struct {
	FramesRead             uint64
	FramesDelivered        uint64
	FramesWritten          uint64
	BytesRead              uint64
	BytesWritten           uint64
	Reconnects             uint64
	FrameBufferOverflows   uint64
	StateEventsDropped     uint64
	HeartbeatEventsDropped uint64
	ErrorEventsDropped     uint64
	ObserverEventsDropped  uint64
}

// Observer receives lifecycle events without coupling the SDK to a logging or
// metrics framework. Callbacks run on a dedicated observer goroutine and must
// return promptly. A bounded observation queue prevents observer work from
// blocking WebSocket I/O; observations may be dropped when that queue is full.
type Observer interface {
	OnState(StateEvent)
	OnHeartbeat(HeartbeatEvent)
	OnError(ErrorEvent)
}

// ObserverFuncs is a convenient Observer adapter.
type ObserverFuncs struct {
	State     func(StateEvent)
	Heartbeat func(HeartbeatEvent)
	Error     func(ErrorEvent)
}

func (o ObserverFuncs) OnState(event StateEvent) {
	if o.State != nil {
		o.State(event)
	}
}

func (o ObserverFuncs) OnHeartbeat(event HeartbeatEvent) {
	if o.Heartbeat != nil {
		o.Heartbeat(event)
	}
}

func (o ObserverFuncs) OnError(event ErrorEvent) {
	if o.Error != nil {
		o.Error(event)
	}
}

// HeartbeatOptions configure active liveness detection. When Enabled is true
// and all durations are zero, the defaults are 5s/3s/2s.
type HeartbeatOptions struct {
	Enabled      bool
	PingInterval time.Duration
	PongTimeout  time.Duration
	WriteTimeout time.Duration
}

// ReconnectPolicy configures automatic reconnect. MaxAttempts == 0 means no
// limit. StableResetTime resets the backoff after a sufficiently stable
// physical connection.
type ReconnectPolicy struct {
	Enabled         bool
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	Multiplier      float64
	Jitter          float64
	MaxAttempts     int
	StableResetTime time.Duration
}

// Options configure a Connection.
type Options struct {
	Dialer Dialer

	Heartbeat HeartbeatOptions
	Reconnect ReconnectPolicy

	WriteQueue      int
	FrameBuffer     int
	StateBuffer     int
	HeartbeatBuffer int
	ErrorBuffer     int
	ObserverBuffer  int
	Observer        Observer

	// MaxConnectionAge proactively recycles a physical connection before an
	// exchange-enforced lifetime. Zero disables age-based recycling.
	MaxConnectionAge time.Duration
}
