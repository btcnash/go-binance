package managed

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidOptions = errors.New("managed websocket: invalid options")
	ErrAlreadyStarted = errors.New("managed websocket: already started")
	ErrClosed         = errors.New("managed websocket: closed")
	ErrNotReady       = errors.New("managed websocket: not ready")
)

// ErrorKind is a machine-readable failure category.
type ErrorKind string

const (
	ErrorInvalidOptions     ErrorKind = "invalid_options"
	ErrorDial               ErrorKind = "dial"
	ErrorRead               ErrorKind = "read"
	ErrorWrite              ErrorKind = "write"
	ErrorPingWrite          ErrorKind = "ping_write"
	ErrorPongWrite          ErrorKind = "pong_write"
	ErrorPongTimeout        ErrorKind = "pong_timeout"
	ErrorFrameBufferFull    ErrorKind = "frame_buffer_full"
	ErrorReconnectExhausted ErrorKind = "reconnect_exhausted"
)

// ConnectionError adds transport context to an underlying error.
type ConnectionError struct {
	Kind       ErrorKind
	Generation uint64
	Operation  string
	Err        error
}

func (e *ConnectionError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return fmt.Sprintf("managed websocket %s failed", e.Operation)
	}
	return fmt.Sprintf("managed websocket %s failed: %v", e.Operation, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func connectionError(kind ErrorKind, generation uint64, operation string, err error) *ConnectionError {
	return &ConnectionError{
		Kind:       kind,
		Generation: generation,
		Operation:  operation,
		Err:        err,
	}
}

func errorKind(err error) ErrorKind {
	var target *ConnectionError
	if errors.As(err, &target) {
		return target.Kind
	}
	return ErrorWrite
}
