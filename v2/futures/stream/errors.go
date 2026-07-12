package stream

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidStreamOptions   = errors.New("futures stream: invalid options")
	ErrInvalidSubscription    = errors.New("futures stream: invalid subscription")
	ErrWrongStreamClass       = errors.New("futures stream: wrong stream class")
	ErrSessionNotStarted      = errors.New("futures stream: session not started")
	ErrSessionClosed          = errors.New("futures stream: session closed")
	ErrSessionNotReady        = errors.New("futures stream: session not ready")
	ErrTooManySubscriptions   = errors.New("futures stream: too many subscriptions")
	ErrSubscriptionACKTimeout = errors.New("futures stream: subscription acknowledgement timeout")
	ErrRequestRejected        = errors.New("futures stream: request rejected")
	ErrGenerationChanged      = errors.New("futures stream: connection generation changed")
	ErrEventBufferFull        = errors.New("futures stream: event buffer full")
	ErrOperationSuperseded    = errors.New("futures stream: operation superseded by newer desired state")
	ErrUnexpectedResponse     = errors.New("futures stream: unexpected response")
	ErrInvalidDeliveryPolicy  = errors.New("futures stream: invalid delivery policy")
)

// StreamErrorKind is a machine-readable session failure category.
type StreamErrorKind string

const (
	StreamErrorInvalidOptions      StreamErrorKind = "invalid_options"
	StreamErrorInvalidSubscription StreamErrorKind = "invalid_subscription"
	StreamErrorWrongClass          StreamErrorKind = "wrong_stream_class"
	StreamErrorNotReady            StreamErrorKind = "not_ready"
	StreamErrorTooManyStreams      StreamErrorKind = "too_many_streams"
	StreamErrorACKTimeout          StreamErrorKind = "ack_timeout"
	StreamErrorRejected            StreamErrorKind = "request_rejected"
	StreamErrorGenerationChanged   StreamErrorKind = "generation_changed"
	StreamErrorProtocol            StreamErrorKind = "protocol"
	StreamErrorEventOverflow       StreamErrorKind = "event_overflow"
	StreamErrorSuperseded          StreamErrorKind = "superseded"
	StreamErrorClosed              StreamErrorKind = "closed"
)

// StreamError adds protocol and connection context to an error.
type StreamError struct {
	Kind       StreamErrorKind
	Method     string
	RequestID  uint64
	Generation uint64
	Code       int
	Message    string
	Err        error
}

func (e *StreamError) Error() string {
	if e == nil {
		return "<nil>"
	}
	base := "futures stream"
	if e.Method != "" {
		base += " " + e.Method
	}
	if e.RequestID != 0 {
		base += fmt.Sprintf(" request %d", e.RequestID)
	}
	if e.Code != 0 || e.Message != "" {
		base += fmt.Sprintf(" rejected: code=%d message=%q", e.Code, e.Message)
	} else if e.Err != nil {
		base += ": " + e.Err.Error()
	}
	return base
}

func (e *StreamError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// StreamErrorEvent is emitted for observable non-terminal session failures.
type StreamErrorEvent struct {
	Kind       StreamErrorKind
	Method     string
	RequestID  uint64
	Generation uint64
	At         time.Time
	Err        error
}

func newStreamError(kind StreamErrorKind, method string, requestID, generation uint64, cause error) *StreamError {
	return &StreamError{
		Kind:       kind,
		Method:     method,
		RequestID:  requestID,
		Generation: generation,
		Err:        cause,
	}
}
