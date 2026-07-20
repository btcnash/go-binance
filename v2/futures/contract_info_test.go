package futures

import (
	"encoding/json"
	"testing"
)

const contractInfoOfficialFixture = `{
	"e":"contractInfo",
	"E":1669356423908,
	"s":"IOTAUSDT",
	"ct":"PERPETUAL",
	"dt":4133404800000,
	"ot":1569398400000,
	"cs":"TRADING",
	"bks":[{"bs":1,"bnf":0,"bnc":5000,"mmr":0.01,"cf":0,"mi":21,"ma":50}],
	"st":1
}`

func TestWsContractInfoEventUnmarshalOfficialFixture(t *testing.T) {
	var event WsContractInfoEvent
	if err := json.Unmarshal([]byte(contractInfoOfficialFixture), &event); err != nil {
		t.Fatalf("unmarshal official contractInfo fixture: %v", err)
	}

	if event.Event != "contractInfo" {
		t.Fatalf("Event = %q", event.Event)
	}
	if event.Time != 1669356423908 {
		t.Fatalf("Time = %d", event.Time)
	}
	if event.Symbol != "IOTAUSDT" {
		t.Fatalf("Symbol = %q", event.Symbol)
	}
	if event.ContractType != "PERPETUAL" {
		t.Fatalf("ContractType = %q", event.ContractType)
	}
	if event.DeliveryDate != 4133404800000 {
		t.Fatalf("DeliveryDate = %d", event.DeliveryDate)
	}
	if event.OnboardDate != 1569398400000 {
		t.Fatalf("OnboardDate = %d", event.OnboardDate)
	}
	if event.ContractState != "TRADING" {
		t.Fatalf("ContractState = %q", event.ContractState)
	}
	if event.SymbolType != 1 {
		t.Fatalf("SymbolType = %d", event.SymbolType)
	}
	if len(event.Brackets) != 1 {
		t.Fatalf("Brackets = %#v", event.Brackets)
	}

	bracket := event.Brackets[0]
	if bracket.Bracket != 1 || bracket.NotionalFloor != 0 || bracket.NotionalCap != 5000 ||
		bracket.MaintMarginRatio != 0.01 || bracket.Cum != 0 || bracket.MinLeverage != 21 || bracket.MaxLeverage != 50 {
		t.Fatalf("Bracket = %#v", bracket)
	}

	if len(event.Data) != 1 || event.Data[0].Symbol != "IOTAUSDT" {
		t.Fatalf("deprecated Data compatibility = %#v", event.Data)
	}
}

func TestWsContractInfoEventUnmarshalResetsReceiver(t *testing.T) {
	var event WsContractInfoEvent
	if err := json.Unmarshal([]byte(contractInfoOfficialFixture), &event); err != nil {
		t.Fatalf("first unmarshal: %v", err)
	}

	second := []byte(`{"e":"contractInfo","E":2,"s":"ETHUSDT","ct":"PERPETUAL","dt":0,"ot":1,"cs":"TRADING","st":2}`)
	if err := json.Unmarshal(second, &event); err != nil {
		t.Fatalf("second unmarshal: %v", err)
	}

	if event.Symbol != "ETHUSDT" || event.SymbolType != 2 {
		t.Fatalf("second event = %#v", event)
	}
	if len(event.Brackets) != 0 {
		t.Fatalf("stale brackets retained: %#v", event.Brackets)
	}
	if len(event.Data) != 1 || event.Data[0].Symbol != "ETHUSDT" {
		t.Fatalf("stale Data retained: %#v", event.Data)
	}
}

func TestWsContractInfoEventLegacyDataCompatibility(t *testing.T) {
	fixture := []byte(`{
		"e":"contractInfo",
		"E":3,
		"data":[{
			"s":"BTCUSDT",
			"ps":"BTCUSDT",
			"ct":"PERPETUAL",
			"dt":0,
			"ot":1569398400000,
			"cs":"TRADING",
			"bks":[],
			"st":1
		}]
	}`)

	var event WsContractInfoEvent
	if err := json.Unmarshal(fixture, &event); err != nil {
		t.Fatalf("unmarshal legacy contractInfo fixture: %v", err)
	}

	if event.Symbol != "BTCUSDT" || event.Pair != "BTCUSDT" {
		t.Fatalf("legacy event was not promoted: %#v", event)
	}
	if len(event.Data) != 1 || event.Data[0].Symbol != "BTCUSDT" {
		t.Fatalf("legacy Data = %#v", event.Data)
	}
}

func TestWsContractInfoEventMarshalUsesTopLevelProtocol(t *testing.T) {
	event := WsContractInfoEvent{
		Event:         "contractInfo",
		Time:          4,
		Symbol:        "BTCUSDT",
		ContractType:  "PERPETUAL",
		ContractState: "TRADING",
		SymbolType:    1,
		Data:          []WsContractInfoData{{Symbol: "LEGACY"}},
	}

	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatalf("decode marshaled event: %v", err)
	}
	if wire["s"] != "BTCUSDT" {
		t.Fatalf("top-level symbol = %#v; payload=%s", wire["s"], encoded)
	}
	if _, ok := wire["data"]; ok {
		t.Fatalf("legacy data field must not be marshaled: %s", encoded)
	}
}
