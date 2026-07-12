package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/btcnash/go-binance/v2/futures/stream"
)

func main() {
	session, err := stream.NewStreamSession(stream.StreamSessionOptions{
		Environment: stream.EnvironmentDemo,
		Class:       stream.StreamClassMarket,
		InitialSubscriptions: []stream.Subscription{
			stream.AggTrade("BTCUSDT"),
			stream.MarkPrice("BTCUSDT", time.Second),
		},
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

	readyCtx, readyCancel := context.WithTimeout(ctx, 15*time.Second)
	defer readyCancel()
	if err := session.WaitReady(readyCtx); err != nil {
		log.Fatal(err)
	}

	for event := range session.Events() {
		fmt.Printf("generation=%d stream=%s data=%s\n", event.Generation, event.Stream, event.Data)
	}
}
