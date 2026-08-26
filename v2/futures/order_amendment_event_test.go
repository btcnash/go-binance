package futures

import (
	"encoding/json"
	"testing"
)

func TestOrderTradeUpdateAmendmentExecutionType(t *testing.T) {
	data := []byte(`{"e":"ORDER_TRADE_UPDATE","E":1770736694138,"T":1770736694137,"o":{"s":"BTCUSDT","c":"client-1","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"30005","ap":"0","sp":"0","x":"AMENDMENT","X":"NEW","i":20072994037,"l":"0","z":"0","L":"0","T":1770736694137,"t":0,"b":"0","a":"0","m":false,"R":false,"wt":"CONTRACT_PRICE","ot":"LIMIT","ps":"LONG","cp":false,"pP":false,"rp":"0","V":"NONE","pm":"NONE","gtd":0}}`)
	var event WsUserDataEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if event.OrderTradeUpdate.ExecutionType != OrderExecutionTypeAmendment {
		t.Fatalf("execution=%q", event.OrderTradeUpdate.ExecutionType)
	}
}
