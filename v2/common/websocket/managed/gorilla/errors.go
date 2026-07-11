package gorilla

import "fmt"

// HandshakeError preserves the HTTP status returned when a WebSocket upgrade
// is rejected. Higher layers can classify authentication/listen-key failures
// without parsing error strings.
type HandshakeError struct {
	Endpoint   string
	StatusCode int
	Status     string
	Err        error
}

func (e *HandshakeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("managed websocket gorilla dial %s: handshake status=%d %s: %v", e.Endpoint, e.StatusCode, e.Status, e.Err)
	}
	return fmt.Sprintf("managed websocket gorilla dial %s: %v", e.Endpoint, e.Err)
}
func (e *HandshakeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
