package private

import (
	"context"
	"errors"
	"fmt"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
)

// RESTListenKeyProvider adapts the existing Futures REST client to the managed
// private-session provider contract.
type RESTListenKeyProvider struct {
	Client *futures.Client
}

func (p RESTListenKeyProvider) Acquire(ctx context.Context) (string, error) {
	if p.Client == nil {
		return "", fmt.Errorf("%w: nil futures client", ErrListenKeyAcquire)
	}
	key, err := p.Client.NewStartUserStreamService().Do(ctx)
	if err != nil {
		return "", err
	}
	return key, nil
}

func (p RESTListenKeyProvider) KeepAlive(ctx context.Context, listenKey string) error {
	if p.Client == nil {
		return fmt.Errorf("%w: nil futures client", ErrListenKeyKeepAlive)
	}
	return p.Client.NewKeepaliveUserStreamService().ListenKey(listenKey).Do(ctx)
}

func (p RESTListenKeyProvider) Release(ctx context.Context, listenKey string) error {
	if p.Client == nil {
		return fmt.Errorf("%w: nil futures client", ErrListenKeyRelease)
	}
	return p.Client.NewCloseUserStreamService().ListenKey(listenKey).Do(ctx)
}

func (RESTListenKeyProvider) IsInvalidListenKey(err error) bool {
	var apiErr *common.APIError
	return errors.As(err, &apiErr) && apiErr.Code == -1125
}

var _ ListenKeyProvider = RESTListenKeyProvider{}
var _ InvalidListenKeyClassifier = RESTListenKeyProvider{}
