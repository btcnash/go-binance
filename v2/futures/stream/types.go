// Package stream provides managed USDⓈ-M Futures public and market data
// WebSocket sessions with dynamic subscriptions and automatic restoration.
package stream

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
)

// StreamClass selects the Binance Futures WebSocket entry point.
type StreamClass string

const (
	StreamClassPublic StreamClass = "public"
	StreamClassMarket StreamClass = "market"
)

// Environment selects the Binance deployment used when Endpoint is empty.
type Environment string

const (
	EnvironmentMainnet Environment = "mainnet"
	EnvironmentTestnet Environment = "testnet"
	EnvironmentDemo    Environment = "demo"
)

// StreamSessionState is the logical subscription-session lifecycle. Transport
// readiness alone is insufficient: Ready is emitted only after desired
// subscriptions have been acknowledged for the current physical generation.
type StreamSessionState string

const (
	StreamStateIdle          StreamSessionState = "idle"
	StreamStateConnecting    StreamSessionState = "connecting"
	StreamStateSubscribing   StreamSessionState = "subscribing"
	StreamStateReady         StreamSessionState = "ready"
	StreamStateDisconnected  StreamSessionState = "disconnected"
	StreamStateReconnecting  StreamSessionState = "reconnecting"
	StreamStateResubscribing StreamSessionState = "resubscribing"
	StreamStateClosed        StreamSessionState = "closed"
	StreamStateFailed        StreamSessionState = "failed"
)

// StreamStateReason explains a logical session transition.
type StreamStateReason string

const (
	StreamReasonStart                 StreamStateReason = "start"
	StreamReasonTransportReady        StreamStateReason = "transport_ready"
	StreamReasonSubscriptionsPending  StreamStateReason = "subscriptions_pending"
	StreamReasonSubscriptionsReady    StreamStateReason = "subscriptions_ready"
	StreamReasonTransportDisconnected StreamStateReason = "transport_disconnected"
	StreamReasonTransportReconnecting StreamStateReason = "transport_reconnecting"
	StreamReasonContextCanceled       StreamStateReason = "context_canceled"
	StreamReasonUserClosed            StreamStateReason = "user_closed"
	StreamReasonTransportFailed       StreamStateReason = "transport_failed"
)

// StreamStateEvent describes one session lifecycle transition.
type StreamStateEvent struct {
	Previous   StreamSessionState
	State      StreamSessionState
	Reason     StreamStateReason
	Generation uint64
	At         time.Time
	Err        error
}

// StreamEvent is one Binance application event. For combined streams, Stream
// and Data contain the envelope fields. Raw always preserves the original JSON.
type StreamEvent struct {
	Generation uint64
	Stream     string
	Data       json.RawMessage
	Raw        json.RawMessage
	ReceivedAt time.Time
}

// GapReason identifies why callers must assume a market-data gap.
type GapReason string

const (
	GapReasonDisconnected  GapReason = "disconnected"
	GapReasonEventOverflow GapReason = "event_overflow"
)

// GapEvent explicitly reports that data continuity cannot be guaranteed.
type GapEvent struct {
	Reason         GapReason
	FromGeneration uint64
	At             time.Time
	Err            error
}

// StreamObserver receives lifecycle observations on a dedicated bounded
// observer loop. Callbacks should still return promptly because observations
// may be dropped when the observer queue is full.
type StreamObserver interface {
	OnState(StreamStateEvent)
	OnError(StreamErrorEvent)
	OnGap(GapEvent)
}

// StreamObserverFuncs adapts functions to StreamObserver.
type StreamObserverFuncs struct {
	State func(StreamStateEvent)
	Error func(StreamErrorEvent)
	Gap   func(GapEvent)
}

func (o StreamObserverFuncs) OnState(event StreamStateEvent) {
	if o.State != nil {
		o.State(event)
	}
}
func (o StreamObserverFuncs) OnError(event StreamErrorEvent) {
	if o.Error != nil {
		o.Error(event)
	}
}
func (o StreamObserverFuncs) OnGap(event GapEvent) {
	if o.Gap != nil {
		o.Gap(event)
	}
}

// StreamSessionOptions configure a Public or Market dynamic stream session.
type StreamSessionOptions struct {
	Class       StreamClass
	Environment Environment
	Endpoint    string

	InitialSubscriptions []Subscription

	// ConnectionOptions are passed to the M1 managed transport. When Dialer is
	// nil, a Gorilla dialer is created for Endpoint/Class/Environment.
	ConnectionOptions managedws.Options
	DisableHeartbeat  bool
	DisableReconnect  bool
	DisableRotation   bool

	AckTimeout      time.Duration
	RequestInterval time.Duration
	MaxBatchSize    int
	MaxStreams      int

	EventBuffer int
	StateBuffer int
	ErrorBuffer int
	GapBuffer   int

	ObserverBuffer int
	Observer       StreamObserver
}

// StreamSession is a logical dynamic subscription session.
type StreamSession struct {
	// Keep 64-bit atomics first for correct alignment on 32-bit platforms.
	requestID uint64
	waiterID  uint64

	opts StreamSessionOptions
	conn *managedws.Connection

	lifecycleMu sync.Mutex
	started     bool
	closed      bool
	terminated  bool
	cancel      context.CancelFunc
	done        chan struct{}
	finishOnce  sync.Once

	outputMu     sync.Mutex
	outputClosed bool

	mu             sync.Mutex
	state          StreamSessionState
	generation     uint64
	transportReady bool
	everReady      bool
	terminalErr    error
	desired        map[string]Subscription
	active         map[string]Subscription
	pending        map[uint64]*pendingRequest
	waiters        map[uint64]*subscriptionWaiter
	changed        chan struct{}

	reconcileC   chan struct{}
	events       chan StreamEvent
	states       chan StreamStateEvent
	errors       chan StreamErrorEvent
	gaps         chan GapEvent
	observations chan streamObservation
	firstReady   chan struct{}
	readyOnce    sync.Once

	pacer   *requestPacer
	workers sync.WaitGroup
}
