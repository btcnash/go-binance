package futures

import (
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
