package private

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidOptions              = errors.New("futures private stream: invalid options")
	ErrInvalidSource               = errors.New("futures private stream: invalid source")
	ErrSessionNotStarted           = errors.New("futures private stream: session not started")
	ErrSessionClosed               = errors.New("futures private stream: session closed")
	ErrSessionNotReady             = errors.New("futures private stream: session not ready")
	ErrListenKeyAcquire            = errors.New("futures private stream: listen key acquire failed")
	ErrListenKeyAcquireUnsupported = errors.New("futures private stream: listen key acquire unsupported")
	ErrListenKeyKeepAlive          = errors.New("futures private stream: listen key keepalive failed")
	ErrListenKeyRelease            = errors.New("futures private stream: listen key release failed")
	ErrListenKeyExpired            = errors.New("futures private stream: listen key expired")
	ErrEventBufferFull             = errors.New("futures private stream: event buffer full")
	ErrMalformedEvent              = errors.New("futures private stream: malformed event")
	ErrAmbiguousSource             = errors.New("futures private stream: ambiguous event source")
)

// ErrorKind is a machine-readable private-session failure category.
type ErrorKind string

const (
	ErrorInvalidOptions  ErrorKind = "invalid_options"
	ErrorInvalidSource   ErrorKind = "invalid_source"
	ErrorAcquire         ErrorKind = "listen_key_acquire"
	ErrorKeepAlive       ErrorKind = "listen_key_keepalive"
	ErrorRelease         ErrorKind = "listen_key_release"
	ErrorExpired         ErrorKind = "listen_key_expired"
	ErrorTransport       ErrorKind = "transport"
	ErrorProtocol        ErrorKind = "protocol"
	ErrorEventOverflow   ErrorKind = "event_overflow"
	ErrorAmbiguousSource ErrorKind = "ambiguous_source"
	ErrorClosed          ErrorKind = "closed"
)

// PrivateError adds account and generation context to an error.
type PrivateError struct {
	Kind       ErrorKind
	SourceID   string
	Generation uint64
	Operation  string
	Err        error
}

func (e *PrivateError) Error() string {
	if e == nil {
		return "<nil>"
	}
	base := "futures private stream"
	if e.SourceID != "" {
		base += " source " + e.SourceID
	}
	if e.Operation != "" {
		base += " " + e.Operation
	}
	if e.Err != nil {
		base += ": " + e.Err.Error()
	}
	return base
}
func (e *PrivateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ErrorEvent is emitted for observable non-terminal failures.
type ErrorEvent struct {
	Kind       ErrorKind
	SourceID   string
	Generation uint64
	Operation  string
	At         time.Time
	Err        error
}

func privateError(kind ErrorKind, sourceID string, generation uint64, operation string, cause error) *PrivateError {
	return &PrivateError{Kind: kind, SourceID: sourceID, Generation: generation, Operation: operation, Err: cause}
}

func invalidOption(format string, args ...any) error {
	return privateError(ErrorInvalidOptions, "", 0, "validate", fmt.Errorf("%w: %s", ErrInvalidOptions, fmt.Sprintf(format, args...)))
}
