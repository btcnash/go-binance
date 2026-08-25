package futures

import managedstream "github.com/btcnash/go-binance/v2/futures/stream"

// WsAggTradeEvent remains available from the futures package while sharing the
// managed-stream wire contract and presence decoder.
type WsAggTradeEvent = managedstream.WsAggTradeEvent

// WsBookTickerEvent remains available from the futures package while sharing
// the managed-stream typed delivery DTO.
type WsBookTickerEvent = managedstream.WsBookTickerEvent
