// Package private provides managed USDⓈ-M Futures user-data WebSocket
// sessions. It combines the managed transport from common/websocket/managed
// with listen-key acquisition, keepalive, invalidation, reconnect, and
// conservative source attribution for single- and multi-account streams.
package private

import (
	"context"
	"encoding/json"
	"time"

	managedws "github.com/adshao/go-binance/v2/common/websocket/managed"
	"github.com/adshao/go-binance/v2/futures"
)

// Environment selects the Binance deployment used when Endpoint is empty.
type Environment string

const (
	EnvironmentMainnet Environment = "mainnet"
	EnvironmentTestnet Environment = "testnet"
	EnvironmentDemo    Environment = "demo"
)

// Mode controls whether one physical connection represents exactly one source
// or multiple listen keys. Isolated mode deliberately requires one source so
// callers can contain one account's failures to one session.
type Mode string

const (
	ModeIsolated Mode = "isolated"
	ModeShared   Mode = "shared"
)

// ListenKeyProvider owns the REST or WSAPI lifecycle for one account's
// listenKey. Acquire may return an existing active key. KeepAlive extends the
// supplied key. Release closes it when the session owns the lifecycle.
type ListenKeyProvider interface {
	Acquire(ctx context.Context) (listenKey string, err error)
	KeepAlive(ctx context.Context, listenKey string) error
	Release(ctx context.Context, listenKey string) error
}

// InvalidListenKeyClassifier is optional. Providers that can distinguish an
// expired/invalid listen key from a transient network failure should implement
// it so the session can reacquire immediately instead of exhausting retries.
type InvalidListenKeyClassifier interface {
	IsInvalidListenKey(err error) bool
}

// ListenKeyProviderFuncs adapts functions to ListenKeyProvider.
type ListenKeyProviderFuncs struct {
	AcquireFunc   func(context.Context) (string, error)
	KeepAliveFunc func(context.Context, string) error
	ReleaseFunc   func(context.Context, string) error
	InvalidFunc   func(error) bool
}

func (p ListenKeyProviderFuncs) Acquire(ctx context.Context) (string, error) {
	if p.AcquireFunc == nil {
		return "", ErrListenKeyAcquireUnsupported
	}
	return p.AcquireFunc(ctx)
}
func (p ListenKeyProviderFuncs) KeepAlive(ctx context.Context, key string) error {
	if p.KeepAliveFunc == nil {
		return nil
	}
	return p.KeepAliveFunc(ctx, key)
}
func (p ListenKeyProviderFuncs) Release(ctx context.Context, key string) error {
	if p.ReleaseFunc == nil {
		return nil
	}
	return p.ReleaseFunc(ctx, key)
}
func (p ListenKeyProviderFuncs) IsInvalidListenKey(err error) bool {
	return p.InvalidFunc != nil && p.InvalidFunc(err)
}

// Source describes one account/user-data subscription.
type Source struct {
	ID       string
	Provider ListenKeyProvider
	Events   []futures.UserDataEventType
}

// ConnectionOptions configure the shared M1 managed transport without exposing
// a static Dialer. Private sessions build a fresh endpoint from the current
// listen keys on every physical reconnect.
type ConnectionOptions struct {
	DisableHeartbeat bool
	DisableReconnect bool

	HeartbeatPingInterval time.Duration
	HeartbeatPongTimeout  time.Duration
	HeartbeatWriteTimeout time.Duration

	ReconnectInitialDelay time.Duration
	ReconnectMaxDelay     time.Duration
	ReconnectMultiplier   float64
	ReconnectJitter       float64
	ReconnectMaxAttempts  int
	StableResetTime       time.Duration

	WriteQueue      int
	FrameBuffer     int
	StateBuffer     int
	HeartbeatBuffer int
	ErrorBuffer     int
	ObserverBuffer  int
	Observer        managedws.Observer
}

// KeepAliveOptions configure the listenKey lifecycle. They are independent of
// WebSocket Ping/Pong, which only proves transport liveness.
type KeepAliveOptions struct {
	Interval     time.Duration
	Timeout      time.Duration
	RetryInitial time.Duration
	RetryMax     time.Duration
	Multiplier   float64
	MaxAttempts  int
}

// EndpointDialer permits deterministic local protocol tests and custom
// transports. The default implementation uses Gorilla WebSocket.
type EndpointDialer interface {
	Dial(ctx context.Context, endpoint string) (managedws.Socket, error)
}

// EndpointDialFunc adapts a function to EndpointDialer.
type EndpointDialFunc func(context.Context, string) (managedws.Socket, error)

func (f EndpointDialFunc) Dial(ctx context.Context, endpoint string) (managedws.Socket, error) {
	return f(ctx, endpoint)
}

// SessionOptions configure a private user-data session.
type SessionOptions struct {
	Mode        Mode
	Environment Environment

	// Endpoint is a root such as wss://fstream.binance.com or a local test
	// server. The session appends /private/ws or /private/stream.
	Endpoint       string
	EndpointDialer EndpointDialer

	// InvalidListenKeyDialError classifies WebSocket handshake failures that
	// require all URL listen keys to be reacquired. The default recognizes
	// HTTP 400/401/403 handshake rejections from the Gorilla adapter.
	InvalidListenKeyDialError func(error) bool

	Sources []Source

	Connection ConnectionOptions
	KeepAlive  KeepAliveOptions

	// RetainListenKeys skips Provider.Release during Close. The default is to
	// release keys acquired and owned by this session.
	RetainListenKeys bool

	EventBuffer     int
	StateBuffer     int
	ErrorBuffer     int
	GapBuffer       int
	LifecycleBuffer int
	ObserverBuffer  int
	Observer        Observer
}

// State is the logical private-session lifecycle.
type State string

const (
	StateIdle         State = "idle"
	StateConnecting   State = "connecting"
	StateReady        State = "ready"
	StateDisconnected State = "disconnected"
	StateReconnecting State = "reconnecting"
	StateRefreshing   State = "refreshing_listen_key"
	StateClosed       State = "closed"
	StateFailed       State = "failed"
)

// StateReason explains a private-session transition.
type StateReason string

const (
	ReasonStart                 StateReason = "start"
	ReasonTransportReady        StateReason = "transport_ready"
	ReasonTransportDisconnected StateReason = "transport_disconnected"
	ReasonTransportReconnecting StateReason = "transport_reconnecting"
	ReasonListenKeyRefresh      StateReason = "listen_key_refresh"
	ReasonTransportFailed       StateReason = "transport_failed"
	ReasonContextCanceled       StateReason = "context_canceled"
	ReasonUserClosed            StateReason = "user_closed"
)

// StateEvent describes one logical session transition.
type StateEvent struct {
	Previous   State
	State      State
	Reason     StateReason
	Generation uint64
	SourceIDs  []string
	At         time.Time
	Err        error
}

// SourceResolution explains how an event was attributed without overstating
// certainty in shared sessions.
type SourceResolution string

const (
	SourceResolutionExplicit    SourceResolution = "explicit_listen_key"
	SourceResolutionIsolated    SourceResolution = "isolated_session"
	SourceResolutionEventFilter SourceResolution = "unique_event_filter"
	SourceResolutionAmbiguous   SourceResolution = "ambiguous_candidates"
	SourceResolutionUnmatched   SourceResolution = "unmatched"
)

// Event is one user-data event. SourceID is only set when attribution is
// certain. Shared sessions preserve CandidateSourceIDs when Binance's payload
// does not identify one account and event filters overlap.
type Event struct {
	Generation         uint64
	SourceID           string
	CandidateSourceIDs []string
	SourceResolution   SourceResolution
	Type               futures.UserDataEventType
	Decoded            *futures.WsUserDataEvent
	DecodeError        error
	Raw                json.RawMessage
	ReceivedAt         time.Time
}

// GapReason identifies why callers must assume account/order state may have
// changed while no trustworthy user-data stream was available.
type GapReason string

const (
	GapReasonDisconnected     GapReason = "disconnected"
	GapReasonEventOverflow    GapReason = "event_overflow"
	GapReasonListenKeyExpired GapReason = "listen_key_expired"
	GapReasonKeepAliveFailed  GapReason = "keepalive_failed"
	GapReasonMalformedEvent   GapReason = "malformed_event"
)

// GapEvent explicitly tells callers to reconcile account state.
type GapEvent struct {
	Reason         GapReason
	FromGeneration uint64
	SourceIDs      []string
	At             time.Time
	Err            error
}

// ListenKeyEventKind identifies lifecycle activity without exposing the raw
// listen key in logs or metrics.
type ListenKeyEventKind string

const (
	ListenKeyAcquired           ListenKeyEventKind = "acquired"
	ListenKeyAcquireFailed      ListenKeyEventKind = "acquire_failed"
	ListenKeyKeepAliveSucceeded ListenKeyEventKind = "keepalive_succeeded"
	ListenKeyKeepAliveFailed    ListenKeyEventKind = "keepalive_failed"
	ListenKeyInvalidated        ListenKeyEventKind = "invalidated"
	ListenKeyReleased           ListenKeyEventKind = "released"
	ListenKeyReleaseFailed      ListenKeyEventKind = "release_failed"
)

// ListenKeyEvent is safe for observation because it contains no key material.
type ListenKeyEvent struct {
	Kind     ListenKeyEventKind
	SourceID string
	Version  uint64
	Attempt  int
	At       time.Time
	Err      error
}

// Observer receives session observations on a dedicated bounded goroutine.
type Observer interface {
	OnState(StateEvent)
	OnError(ErrorEvent)
	OnGap(GapEvent)
	OnListenKey(ListenKeyEvent)
}

// ObserverFuncs adapts functions to Observer.
type ObserverFuncs struct {
	State     func(StateEvent)
	Error     func(ErrorEvent)
	Gap       func(GapEvent)
	ListenKey func(ListenKeyEvent)
}

func (o ObserverFuncs) OnState(event StateEvent) {
	if o.State != nil {
		o.State(event)
	}
}
func (o ObserverFuncs) OnError(event ErrorEvent) {
	if o.Error != nil {
		o.Error(event)
	}
}
func (o ObserverFuncs) OnGap(event GapEvent) {
	if o.Gap != nil {
		o.Gap(event)
	}
}
func (o ObserverFuncs) OnListenKey(event ListenKeyEvent) {
	if o.ListenKey != nil {
		o.ListenKey(event)
	}
}
