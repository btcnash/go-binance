package api

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidOptions         = errors.New("websocket api: invalid options")
	ErrInvalidRequest         = errors.New("websocket api: invalid request")
	ErrSessionNotStarted      = errors.New("websocket api: session not started")
	ErrSessionClosed          = errors.New("websocket api: session closed")
	ErrSessionNotReady        = errors.New("websocket api: session not ready")
	ErrDuplicateRequestID     = errors.New("websocket api: duplicate request id")
	ErrTooManyPendingRequests = errors.New("websocket api: too many pending requests")
	ErrRequestTimeout         = errors.New("websocket api: request timeout")
	ErrDisconnected           = errors.New("websocket api: disconnected")
	ErrOutcomeUnknown         = errors.New("websocket api: outcome unknown")
	ErrAuthenticationFailed   = errors.New("websocket api: authentication failed")
	ErrUnexpectedResponse     = errors.New("websocket api: unexpected response")
	ErrRotationFailed         = errors.New("websocket api: rotation failed")
)

// ErrorKind is a machine-readable API session failure category.
type ErrorKind string

const (
	ErrorInvalidRequest ErrorKind = "invalid_request"
	ErrorNotReady       ErrorKind = "not_ready"
	ErrorDuplicateID    ErrorKind = "duplicate_request_id"
	ErrorTooManyPending ErrorKind = "too_many_pending_requests"
	ErrorTimeout        ErrorKind = "request_timeout"
	ErrorDisconnected   ErrorKind = "disconnected"
	ErrorOutcomeUnknown ErrorKind = "outcome_unknown"
	ErrorAuthentication ErrorKind = "authentication_failed"
	ErrorProtocol       ErrorKind = "protocol"
	ErrorRotation       ErrorKind = "rotation_failed"
	ErrorClosed         ErrorKind = "closed"
)

// RequestError adds request and transport context to a safe request failure.
type RequestError struct {
	Kind       ErrorKind
	RequestID  string
	Method     string
	Generation uint64
	Err        error
}

func (e *RequestError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("websocket api request %q method %q generation %d failed: %v", e.RequestID, e.Method, e.Generation, e.Err)
}
func (e *RequestError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// UnknownOutcomeError means a side-effecting request may have been accepted by
// Binance but no terminal response was observed. The SDK never replays it.
type UnknownOutcomeError struct {
	RequestID  string
	Method     string
	Generation uint64
	SentAt     time.Time
	Err        error
}

func (e *UnknownOutcomeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("websocket api request %q method %q outcome unknown on generation %d: %v", e.RequestID, e.Method, e.Generation, e.Err)
}
func (e *UnknownOutcomeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
func (e *UnknownOutcomeError) Is(target error) bool {
	return target == ErrOutcomeUnknown || errors.Is(e.Err, target)
}
