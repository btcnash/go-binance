package binance

import "github.com/btcnash/go-binance/v2/futures"

// Compatibility aliases. USDⓈ-M Algo WSAPI belongs to the futures package.
type AlgoOrderCancelWsService = futures.AlgoOrderCancelWsService
type AlgoOrderCancelWsRequest = futures.AlgoOrderCancelWsRequest
type CancelAlgoOrderResult = futures.CancelAlgoOrderResult
type CancelAlgoOrderWsResponse = futures.CancelAlgoOrderWsResponse

func NewAlgoOrderCancelWsService(apiKey, secretKey string) (*AlgoOrderCancelWsService, error) {
	return futures.NewAlgoOrderCancelWsService(apiKey, secretKey)
}

func NewAlgoOrderCancelWsRequest() *AlgoOrderCancelWsRequest {
	return futures.NewAlgoOrderCancelWsRequest()
}
