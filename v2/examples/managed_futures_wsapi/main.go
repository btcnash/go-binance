package main

import (
	"context"
	"fmt"
	"log"
	"time"

	futureswsapi "github.com/adshao/go-binance/v2/futures/wsapi"
)

func main() {
	session, err := futureswsapi.NewSession(futureswsapi.Options{Environment: futureswsapi.EnvironmentDemo})
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

	requestCtx, requestCancel := context.WithTimeout(ctx, 5*time.Second)
	defer requestCancel()
	response, err := session.Do(requestCtx, futureswsapi.Request{
		ID:      "time-1",
		Method:  "time",
		Payload: []byte(`{"id":"time-1","method":"time"}`),
		Outcome: futureswsapi.OutcomeSafe,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("generation=%d response=%s\n", response.Generation, response.Payload)
}
