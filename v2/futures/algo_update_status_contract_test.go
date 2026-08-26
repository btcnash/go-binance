package futures

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestAlgoUpdateOfficialStatusesDecode(t *testing.T) {
	statuses := []AlgoOrderStatusType{
		AlgoOrderStatusTypeNew,
		AlgoOrderStatusTypeTriggering,
		AlgoOrderStatusTypeTriggered,
		AlgoOrderStatusTypeFinished,
		AlgoOrderStatusTypeCanceled,
		AlgoOrderStatusTypeRejected,
		AlgoOrderStatusTypeExpired,
	}
	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			data := []byte(fmt.Sprintf(`{"e":"ALGO_UPDATE","E":1770736694138,"T":1770736694137,"o":{"caid":"replacement-1","aid":2000000002162519,"at":"CONDITIONAL","o":"STOP_MARKET","s":"BTCUSDT","S":"BUY","ps":"BOTH","f":"GTC","q":"0.001","X":%q,"ai":"12345","ap":"0","aq":"0","act":"MARKET","tp":"59000","p":"0","V":"NONE","wt":"CONTRACT_PRICE","pm":"NONE","cp":false,"pP":false,"R":false,"tt":0,"gtd":0,"rm":""}}`, status))
			var event WsUserDataEvent
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatal(err)
			}
			if event.AlgoUpdate.AlgoStatus != status {
				t.Fatalf("status=%q want=%q", event.AlgoUpdate.AlgoStatus, status)
			}
			if event.AlgoUpdate.ClientAlgoID != "replacement-1" || event.AlgoUpdate.ActualOrderType != "MARKET" {
				t.Fatalf("payload=%#v", event.AlgoUpdate)
			}
		})
	}
}
