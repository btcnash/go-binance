package futures

import (
	"context"
	"errors"
	"time"

	legacyws "github.com/btcnash/go-binance/v2/common/websocket"
	managedfutures "github.com/btcnash/go-binance/v2/futures/wsapi"
)

func newManagedLegacyWSAPIClient() (legacyws.Client, error) {
	session, err := managedfutures.NewSession(managedfutures.Options{Endpoint: getWsApiEndpoint()})
	if err != nil {
		return nil, err
	}
	return legacyws.NewManagedClient(session)
}

func newBorrowedLegacyWSAPIClient(session *managedfutures.Session) (legacyws.Client, error) {
	return legacyws.NewBorrowedManagedClient(session)
}

var errWSAPIContextSyncUnsupported = errors.New("futures wsapi: client does not support context-aware sync")

type contextSyncClient interface {
	WriteSyncContext(context.Context, string, []byte, time.Duration) ([]byte, error)
}

func writeLegacyWSAPISyncContext(ctx context.Context, client legacyws.Client, requestID string, rawData []byte, timeout time.Duration) ([]byte, error) {
	contextClient, ok := client.(contextSyncClient)
	if !ok {
		return nil, errWSAPIContextSyncUnsupported
	}
	return contextClient.WriteSyncContext(ctx, requestID, rawData, timeout)
}
