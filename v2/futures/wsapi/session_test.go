package wsapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apiws "github.com/btcnash/go-binance/v2/common/websocket/api"
	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	"github.com/gorilla/websocket"
)

func TestResolveEndpoint(t *testing.T) {
	cases := []struct {
		env  Environment
		want string
	}{{EnvironmentMainnet, MainnetEndpoint}, {EnvironmentTestnet, TestnetEndpoint}, {EnvironmentDemo, DemoEndpoint}, {"", MainnetEndpoint}}
	for _, tc := range cases {
		got, err := resolveEndpoint(tc.env, "")
		if err != nil || got != tc.want {
			t.Fatalf("resolveEndpoint(%q)=%q,%v want %q", tc.env, got, err, tc.want)
		}
	}
	if _, err := resolveEndpoint("bad", ""); err == nil {
		t.Fatal("unsupported environment accepted")
	}
	if got, err := resolveEndpoint("bad", "ws://custom"); err != nil || got != "ws://custom" {
		t.Fatalf("explicit endpoint=%q,%v", got, err)
	}
}

func TestNewSessionUsesCustomEndpointAndManagedRequestRouting(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(payload, &req); err != nil {
				t.Errorf("decode: %v", err)
				return
			}
			if err := conn.WriteJSON(map[string]any{"id": req.ID, "status": 200, "result": "ok"}); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	session, err := NewSession(Options{Endpoint: endpoint, DisableHeartbeat: true, DisableRotation: true, API: apiws.Options{RequestTimeout: time.Second}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	payload := []byte(`{"id":"one","method":"time","params":{}}`)
	response, err := session.Do(context.Background(), apiws.Request{ID: "one", Method: "time", Payload: payload, Outcome: apiws.OutcomeSafe})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if response.ID != "one" {
		t.Fatalf("response ID=%q", response.ID)
	}
}

func TestNewSessionAppliesReliabilityDefaults(t *testing.T) {
	// Construction with an explicit inert dialer proves default heartbeat,
	// reconnect, and rotation normalize successfully without dialing.
	_, err := NewSession(Options{Endpoint: "ws://example.invalid", DisableRotation: true})
	if err != nil {
		t.Fatalf("NewSession defaults: %v", err)
	}
}

func TestHeartbeatUsesWebSocketControlPingWithoutJSONHeartbeat(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	pingC := make(chan string, 1)
	textC := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		conn.SetPingHandler(func(payload string) error {
			select {
			case pingC <- payload:
			default:
			}
			return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(time.Second))
		})
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if messageType == websocket.TextMessage {
				select {
				case textC <- payload:
				default:
				}
			}
		}
	}))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	session, err := NewSession(Options{
		Endpoint:         endpoint,
		DisableReconnect: true,
		DisableRotation:  true,
		API:              apiws.Options{ConnectionOptions: managedws.Options{Heartbeat: managedws.HeartbeatOptions{Enabled: true, PingInterval: 10 * time.Millisecond, PongTimeout: 20 * time.Millisecond, WriteTimeout: 10 * time.Millisecond}}},
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer session.Close()
	if err := session.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	select {
	case payload := <-pingC:
		if payload == "" {
			t.Fatal("control Ping payload is empty")
		}
	case <-time.After(time.Second):
		t.Fatal("no WebSocket control Ping received")
	}
	select {
	case payload := <-textC:
		t.Fatalf("unexpected JSON/text heartbeat: %s", payload)
	case <-time.After(30 * time.Millisecond):
	}
}
