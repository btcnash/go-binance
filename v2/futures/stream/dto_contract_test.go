package stream_test

import (
	"encoding/json"
	"testing"

	"github.com/btcnash/go-binance/v2/futures"
	"github.com/btcnash/go-binance/v2/futures/stream"
)

func TestDepthStreamEventDataDecodesIntoSDKDTO(t *testing.T) {
	event := stream.StreamEvent{Data: json.RawMessage(`{
		"e":"depthUpdate",
		"E":1628847118038,
		"T":1628847117814,
		"s":"BTCUSDT",
		"U":21925649843,
		"u":21925649849,
		"pu":21925649651,
		"b":[["46248.03","0.000"]],
		"a":[["46249.88","71.870"]]
	}`)}

	var dto futures.WsDepthEvent
	if err := json.Unmarshal(event.Data, &dto); err != nil {
		t.Fatalf("StreamEvent.Data must decode into futures.WsDepthEvent: %v", err)
	}
	if len(dto.Bids) != 1 || dto.Bids[0].Price != "46248.03" || dto.Bids[0].Quantity != "0.000" {
		t.Fatalf("bids = %#v", dto.Bids)
	}
	if len(dto.Asks) != 1 || dto.Asks[0].Price != "46249.88" || dto.Asks[0].Quantity != "71.870" {
		t.Fatalf("asks = %#v", dto.Asks)
	}
}

func TestContractInfoStreamEventDataDecodesIntoSDKDTO(t *testing.T) {
	event := stream.StreamEvent{Data: json.RawMessage(`{
		"e":"contractInfo",
		"E":1669356423908,
		"s":"IOTAUSDT",
		"ct":"PERPETUAL",
		"dt":4133404800000,
		"ot":1569398400000,
		"cs":"TRADING",
		"bks":[{"bs":1,"bnf":0,"bnc":5000,"mmr":0.01,"cf":0,"mi":21,"ma":50}],
		"st":1
	}`)}

	var dto futures.WsContractInfoEvent
	if err := json.Unmarshal(event.Data, &dto); err != nil {
		t.Fatalf("StreamEvent.Data must decode into futures.WsContractInfoEvent: %v", err)
	}
	if dto.Symbol != "IOTAUSDT" || dto.ContractState != "TRADING" || dto.SymbolType != 1 {
		t.Fatalf("contractInfo DTO = %#v", dto)
	}
	if len(dto.Brackets) != 1 || dto.Brackets[0].MaxLeverage != 50 {
		t.Fatalf("contractInfo brackets = %#v", dto.Brackets)
	}
}
