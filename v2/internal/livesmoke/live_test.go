package livesmoke

import (
	"context"
	"os"
	"testing"
	"time"

	binance "github.com/btcnash/go-binance/v2"
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
		t.Skip("set BINANCE_RUN_LIVE_WS_SMOKE=1 to run Binance Testnet smoke tests")
	}
}

func TestTestnetPublicStreamEventAndActivePong(t *testing.T) {
	requireLive(t)
	if err := binance.SetEnvironment(binance.EnvironmentTestnet); err != nil {
		t.Fatal(err)
	}
	pongC := make(chan managedws.HeartbeatEvent, 1)
	session, err := streamws.NewStreamSession(streamws.StreamSessionOptions{
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

func TestTestnetWSAPITimeAndActivePong(t *testing.T) {
	requireLive(t)
	if err := binance.SetEnvironment(binance.EnvironmentTestnet); err != nil {
		t.Fatal(err)
	}
	pongC := make(chan managedws.HeartbeatEvent, 1)
	session, err := futureswsapi.NewSession(futureswsapi.Options{
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

func TestTestnetPrivateConnectAndActivePong(t *testing.T) {
	requireLive(t)
	if err := binance.SetEnvironment(binance.EnvironmentTestnet); err != nil {
		t.Fatal(err)
	}
	apiKey, secret := os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_SECRET_KEY")
	if apiKey == "" || secret == "" {
		t.Skip("BINANCE_API_KEY and BINANCE_SECRET_KEY are required")
	}
	pongC := make(chan managedws.HeartbeatEvent, 1)
	client := futures.NewClient(apiKey, secret)
	session, err := privatews.NewSession(privatews.SessionOptions{
		Mode:    privatews.ModeIsolated,
		Sources: []privatews.Source{{ID: "testnet", Provider: privatews.RESTListenKeyProvider{Client: client}}},
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
