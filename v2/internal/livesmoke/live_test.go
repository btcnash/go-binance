package livesmoke

import (
	"context"
	"os"
	"testing"
	"time"

	apiws "github.com/btcnash/go-binance/v2/common/websocket/api"
	managedws "github.com/btcnash/go-binance/v2/common/websocket/managed"
	"github.com/btcnash/go-binance/v2/futures"
	privatews "github.com/btcnash/go-binance/v2/futures/private"
	streamws "github.com/btcnash/go-binance/v2/futures/stream"
	futureswsapi "github.com/btcnash/go-binance/v2/futures/wsapi"
)

func requireLive(t *testing.T) {
	t.Helper()
	if os.Getenv("BINANCE_RUN_LIVE_WS_SMOKE") != "1" {
		t.Skip("set BINANCE_RUN_LIVE_WS_SMOKE=1 to run Binance Demo/Testnet smoke tests")
	}
}

func TestDemoPublicStreamEventAndActivePong(t *testing.T) {
	requireLive(t)
	pongC := make(chan managedws.HeartbeatEvent, 1)
	session, err := streamws.NewStreamSession(streamws.StreamSessionOptions{
		Environment:          streamws.EnvironmentDemo,
		Class:                streamws.StreamClassPublic,
		InitialSubscriptions: []streamws.Subscription{streamws.BookTicker("BTCUSDT")},
		ConnectionOptions: managedws.Options{Observer: managedws.ObserverFuncs{Heartbeat: func(event managedws.HeartbeatEvent) {
			if event.Kind == managedws.HeartbeatPongReceived {
				select {
				case pongC <- event:
				default:
				}
			}
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	defer session.Close()
	if err := session.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Events():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-pongC:
	case <-ctx.Done():
		t.Fatal("client Ping did not receive matching Pong")
	}
}

func TestDemoWSAPITimeAndActivePong(t *testing.T) {
	requireLive(t)
	pongC := make(chan managedws.HeartbeatEvent, 1)
	session, err := futureswsapi.NewSession(futureswsapi.Options{
		Environment: futureswsapi.EnvironmentDemo,
		API: apiws.Options{ConnectionOptions: managedws.Options{Observer: managedws.ObserverFuncs{Heartbeat: func(event managedws.HeartbeatEvent) {
			if event.Kind == managedws.HeartbeatPongReceived {
				select {
				case pongC <- event:
				default:
				}
			}
		}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	defer session.Close()
	if err := session.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = session.Do(ctx, futureswsapi.Request{ID: "live-time", Method: "time", Payload: []byte(`{"id":"live-time","method":"time"}`), Outcome: futureswsapi.OutcomeSafe})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-pongC:
	case <-ctx.Done():
		t.Fatal("client Ping did not receive matching Pong")
	}
}

func TestDemoPrivateConnectAndActivePong(t *testing.T) {
	requireLive(t)
	apiKey, secret := os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_SECRET_KEY")
	if apiKey == "" || secret == "" {
		t.Skip("BINANCE_API_KEY and BINANCE_SECRET_KEY are required")
	}
	pongC := make(chan managedws.HeartbeatEvent, 1)
	client := futures.NewClient(apiKey, secret)
	client.BaseURL = futures.BaseApiDemoURL
	session, err := privatews.NewSession(privatews.SessionOptions{
		Mode:        privatews.ModeIsolated,
		Environment: privatews.EnvironmentDemo,
		Sources:     []privatews.Source{{ID: "demo", Provider: privatews.RESTListenKeyProvider{Client: client}}},
		Connection: privatews.ConnectionOptions{Observer: managedws.ObserverFuncs{Heartbeat: func(event managedws.HeartbeatEvent) {
			if event.Kind == managedws.HeartbeatPongReceived {
				select {
				case pongC <- event:
				default:
				}
			}
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	defer session.Close()
	if err := session.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pongC:
	case <-ctx.Done():
		t.Fatal("client Ping did not receive matching Pong")
	}
}
