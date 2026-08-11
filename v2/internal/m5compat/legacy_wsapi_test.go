package m5compat

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/btcnash/go-binance/v2/futures"
	"github.com/btcnash/go-binance/v2/internal/networkenv"
	"github.com/gorilla/websocket"
)

func TestLegacyFuturesWSAPIUsesManagedSession(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var request struct {
				ID json.RawMessage `json:"id"`
			}
			if err := json.Unmarshal(payload, &request); err != nil {
				return
			}
			response := append([]byte(`{"id":`), request.ID...)
			response = append(response, []byte(`,"status":200,"result":[]}`)...)
			if err := conn.WriteMessage(websocket.TextMessage, response); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	profile := networkenv.USDM(networkenv.Mainnet)
	profile.WSAPI = "ws" + strings.TrimPrefix(server.URL, "http")
	restoreProfile := networkenv.OverrideUSDMForTesting(networkenv.Mainnet, profile)
	previousEnv := networkenv.Current()
	_ = networkenv.Set(networkenv.Mainnet)
	defer func() {
		restoreProfile()
		_ = networkenv.Set(previousEnv)
	}()

	client := futures.NewClient("api-key", "secret-key")
	response, err := client.GetAccountBalanceWs()
	if err != nil {
		t.Fatalf("GetAccountBalanceWs() error = %v", err)
	}
	if response.Status != 200 {
		t.Fatalf("status = %d, want 200", response.Status)
	}
	if len(response.Result) != 0 {
		t.Fatalf("result len = %d, want 0", len(response.Result))
	}
}
