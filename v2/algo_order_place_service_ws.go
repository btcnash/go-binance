package binance

import "github.com/btcnash/go-binance/v2/futures"

// Compatibility aliases. USDⓈ-M Algo WSAPI belongs to the futures package.
type AlgoOrderPlaceWsService = futures.AlgoOrderPlaceWsService
type AlgoOrderPlaceWsRequest = futures.AlgoOrderPlaceWsRequest
type CreateAlgoOrderResult = futures.CreateAlgoOrderResult
type CreateAlgoOrderWsResponse = futures.CreateAlgoOrderWsResponse
type WsRateLimit = futures.WsRateLimit

func NewAlgoOrderPlaceWsService(apiKey, secretKey string) (*AlgoOrderPlaceWsService, error) {
	return futures.NewAlgoOrderPlaceWsService(apiKey, secretKey)
}

func NewAlgoOrderPlaceWsRequest() *AlgoOrderPlaceWsRequest {
	// Preserve the legacy top-level behavior; the canonical futures request defaults to ACK.
	return futures.NewAlgoOrderPlaceWsRequest().NewOrderResponseType(futures.NewOrderRespTypeRESULT)
}
