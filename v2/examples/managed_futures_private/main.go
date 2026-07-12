package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/btcnash/go-binance/v2/futures"
	privatews "github.com/btcnash/go-binance/v2/futures/private"
)

func main() {
	apiKey := os.Getenv("BINANCE_API_KEY")
	secret := os.Getenv("BINANCE_SECRET_KEY")
	if apiKey == "" || secret == "" {
		log.Fatal("BINANCE_API_KEY and BINANCE_SECRET_KEY are required")
	}

	client := futures.NewClient(apiKey, secret)
	client.BaseURL = futures.BaseApiDemoURL
	session, err := privatews.NewSession(privatews.SessionOptions{
		Mode:        privatews.ModeIsolated,
		Environment: privatews.EnvironmentDemo,
		Sources: []privatews.Source{{
			ID:       "demo-account",
			Provider: privatews.RESTListenKeyProvider{Client: client},
			Events: []futures.UserDataEventType{
				futures.UserDataEventTypeAccountUpdate,
				futures.UserDataEventTypeOrderTradeUpdate,
				futures.UserDataEventTypeAlgoUpdate,
			},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer session.Close()
	if err := session.Start(ctx); err != nil {
		log.Fatal(err)
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readyCancel()
	if err := session.WaitReady(readyCtx); err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case event := <-session.Events():
			fmt.Printf("generation=%d source=%s type=%s raw=%s\n", event.Generation, event.SourceID, event.Type, event.Raw)
		case gap := <-session.Gaps():
			fmt.Printf("reconcile required: reason=%s sources=%v err=%v\n", gap.Reason, gap.SourceIDs, gap.Err)
		}
	}
}
