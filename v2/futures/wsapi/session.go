// Package wsapi provides managed USDⓈ-M Futures WebSocket API sessions.
package wsapi

import (
	"time"

	apiws "github.com/btcnash/go-binance/v2/common/websocket/api"
	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	managedgorilla "github.com/btcnash/go-binance/v2/common/websocket/managed/gorilla"
	"github.com/btcnash/go-binance/v2/internal/networkenv"
)

// Re-export the managed API request surface for Futures callers.
type Session = apiws.Session
type Request = apiws.Request
type Response = apiws.Response
type Stats = apiws.Stats
type APIOptions = apiws.Options
type Authenticator = apiws.Authenticator
type AuthenticatorFuncs = apiws.AuthenticatorFuncs
type UnknownOutcomeError = apiws.UnknownOutcomeError
type OutcomePolicy = apiws.OutcomePolicy

const (
	OutcomeSafe    = apiws.OutcomeSafe
	OutcomeUnknown = apiws.OutcomeUnknown
)

const (
	defaultRotationAge  = 23*time.Hour + 50*time.Minute
	defaultDrainTimeout = 30 * time.Second
)

// Options configure a managed Futures WebSocket API session.
type Options struct {
	Endpoint string

	API apiws.Options

	DisableHeartbeat bool
	DisableReconnect bool
	DisableRotation  bool
}

// NewSession creates an idle Futures WSAPI session. The default configuration
// enables M1 active heartbeat (5s Ping / 3s Pong timeout / 2s write timeout),
// automatic reconnect, and proactive replacement before Binance's 24-hour
// connection lifetime.
func NewSession(opts Options) (*Session, error) {
	endpoint := resolveEndpoint(opts.Endpoint)
	apiOptions := opts.API
	if apiOptions.ConnectionOptions.Dialer == nil {
		apiOptions.ConnectionOptions.Dialer = managedgorilla.Dialer{Endpoint: endpoint}
	}
	if !opts.DisableHeartbeat && !apiOptions.ConnectionOptions.Heartbeat.Enabled {
		apiOptions.ConnectionOptions.Heartbeat = managedws.HeartbeatOptions{Enabled: true}
	}
	if !opts.DisableReconnect && !apiOptions.ConnectionOptions.Reconnect.Enabled {
		apiOptions.ConnectionOptions.Reconnect = managedws.ReconnectPolicy{Enabled: true}
	}
	if !opts.DisableRotation && !apiOptions.Rotation.Enabled {
		apiOptions.Rotation = apiws.RotationOptions{Enabled: true, MaxAge: defaultRotationAge, DrainTimeout: defaultDrainTimeout}
	}
	return apiws.NewSession(apiOptions)
}

func resolveEndpoint(explicit string) string {
	if explicit != "" {
		return explicit
	}
	return networkenv.USDM(networkenv.Current()).WSAPI
}
