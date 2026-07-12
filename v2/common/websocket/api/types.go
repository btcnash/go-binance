// Package api provides a managed request/response session on top of the
// transport-level managed WebSocket connection.
package api

import (
	"context"
	"encoding/json"
	"time"

	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
)

// OutcomePolicy defines how a request is reported when its response can no
// longer be observed after the request may have reached Binance.
type OutcomePolicy string

const (
	// OutcomeSafe is intended for read-only or otherwise safely retryable
	// requests. The SDK still never replays it automatically.
	OutcomeSafe OutcomePolicy = "safe"
	// OutcomeUnknown is intended for trading or other side-effecting requests.
	// A disconnect, cancellation, or response timeout after send is surfaced as
	// UnknownOutcomeError and is never replayed automatically.
	OutcomeUnknown OutcomePolicy = "unknown"
)

// Request is one already-encoded WebSocket API request. Payload must contain a
// top-level id equal to ID and must not be mutated while Do is executing.
type Request struct {
	ID      string
	Method  string
	Payload []byte
	Outcome OutcomePolicy
}

// Response preserves the raw Binance response and the physical generation on
// which it arrived. Payload is immutable session-owned data; the SDK does not
// modify it after delivery.
type Response struct {
	ID         string
	Payload    json.RawMessage
	Generation uint64
	ReceivedAt time.Time
}

// UnsolicitedFrame is a text frame that does not match an outstanding request.
// Payload is immutable session-owned data after delivery.
type UnsolicitedFrame struct {
	Payload    json.RawMessage
	Generation uint64
	ReceivedAt time.Time
}

// Authenticator builds and validates an optional session authentication
// request such as session.logon. It is invoked for every physical connection
// generation before the logical API session becomes Ready.
type Authenticator interface {
	BuildRequest(generation uint64) (Request, error)
	ValidateResponse(response Response) error
}

// AuthenticatorFuncs adapts functions to Authenticator.
type AuthenticatorFuncs struct {
	Build    func(generation uint64) (Request, error)
	Validate func(response Response) error
}

func (a AuthenticatorFuncs) BuildRequest(generation uint64) (Request, error) {
	if a.Build == nil {
		return Request{}, ErrAuthenticationFailed
	}
	return a.Build(generation)
}
func (a AuthenticatorFuncs) ValidateResponse(response Response) error {
	if a.Validate == nil {
		return nil
	}
	return a.Validate(response)
}

// RotationOptions configure proactive physical connection replacement.
type RotationOptions struct {
	Enabled      bool
	MaxAge       time.Duration
	DrainTimeout time.Duration
}

// State is the logical API session lifecycle.
type State string

const (
	StateIdle           State = "idle"
	StateConnecting     State = "connecting"
	StateAuthenticating State = "authenticating"
	StateReady          State = "ready"
	StateDisconnected   State = "disconnected"
	StateReconnecting   State = "reconnecting"
	StateRotating       State = "rotating"
	StateDraining       State = "draining"
	StateClosed         State = "closed"
	StateFailed         State = "failed"
)

// StateReason describes a logical session transition.
type StateReason string

const (
	ReasonStart                 StateReason = "start"
	ReasonTransportReady        StateReason = "transport_ready"
	ReasonAuthenticationPending StateReason = "authentication_pending"
	ReasonAuthenticationReady   StateReason = "authentication_ready"
	ReasonTransportDisconnected StateReason = "transport_disconnected"
	ReasonTransportReconnecting StateReason = "transport_reconnecting"
	ReasonRotationStarted       StateReason = "rotation_started"
	ReasonRotationSwitched      StateReason = "rotation_switched"
	ReasonRotationDraining      StateReason = "rotation_draining"
	ReasonRotationFailed        StateReason = "rotation_failed"
	ReasonContextCanceled       StateReason = "context_canceled"
	ReasonUserClosed            StateReason = "user_closed"
	ReasonTerminalFailure       StateReason = "terminal_failure"
)

// StateEvent describes one API session lifecycle transition.
type StateEvent struct {
	Previous   State
	State      State
	Reason     StateReason
	Generation uint64
	At         time.Time
	Err        error
}

// ErrorEvent exposes an observable request/session error.
type ErrorEvent struct {
	Kind       ErrorKind
	RequestID  string
	Method     string
	Generation uint64
	At         time.Time
	Err        error
}

// Observer receives observations on a dedicated bounded worker.
type Observer interface {
	OnState(StateEvent)
	OnError(ErrorEvent)
}

// ObserverFuncs adapts functions to Observer.
type ObserverFuncs struct {
	State func(StateEvent)
	Error func(ErrorEvent)
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

// Options configure a managed WebSocket API session.
type Options struct {
	ConnectionOptions managedws.Options
	Authenticator     Authenticator
	Rotation          RotationOptions

	RequestTimeout     time.Duration
	MaxPendingRequests int
	// MaxRequestIDsPerGeneration bounds the no-reuse history retained for one
	// authenticated transport generation. Zero uses the SDK default. When the
	// history approaches the limit, an enabled rotation is requested; at the
	// limit, new requests fail closed with ErrRequestIDCapacity.
	MaxRequestIDsPerGeneration int
	UnsolicitedBuffer          int
	StateBuffer                int
	ErrorBuffer                int
	ObserverBuffer             int
	Observer                   Observer
}

// RequestSender is the public request surface used by wrappers.
type RequestSender interface {
	Do(ctx context.Context, request Request) (Response, error)
}
