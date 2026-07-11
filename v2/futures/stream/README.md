# Managed USDⓈ-M Futures Stream Session — M2

Package `futures/stream` adds logical Public and Market stream sessions above
M1 `common/websocket/managed.Connection`.

It uses Binance's JSON live-subscription protocol after the WebSocket has been
established. Legacy `WsXxxServe` functions are intentionally unchanged.

## Capabilities

- instance-scoped Mainnet, Testnet, Demo, or custom endpoints;
- separate `/public/stream` and `/market/stream` routing;
- typed subscription builders with endpoint-class validation;
- `SUBSCRIBE`, `UNSUBSCRIBE`, `LIST_SUBSCRIPTIONS`, `SET_PROPERTY`, and
  `GET_PROPERTY`;
- `ReplaceSubscriptions` for desired-state convergence;
- independent `desired`, `active`, and `pending` subscription state;
- request-ID correlation for out-of-order acknowledgements;
- configurable acknowledgement timeout, batching, rate pacing, and stream cap;
- automatic restoration of the latest desired set after reconnect;
- logical `Ready` only after the current generation has acknowledged the exact
  desired set;
- old-generation response and event fencing inherited from M1;
- bounded event delivery separated from socket reads;
- explicit `GapEvent` and reconnect when the event buffer overflows;
- bounded observer delivery that cannot block socket or acknowledgement loops;
- safe output-channel closure under concurrent API calls and shutdown.

## Basic use

```go
package main

import (
    "context"
    "log"

    futuresstream "github.com/adshao/go-binance/v2/futures/stream"
)

func run(ctx context.Context) error {
    session, err := futuresstream.NewStreamSession(
        futuresstream.StreamSessionOptions{
            Class:       futuresstream.StreamClassMarket,
            Environment: futuresstream.EnvironmentMainnet,
            InitialSubscriptions: []futuresstream.Subscription{
                futuresstream.AggTrade("BTCUSDT"),
                futuresstream.MarkPrice("BTCUSDT", 0),
            },
        },
    )
    if err != nil {
        return err
    }
    defer session.Close()

    if err := session.Start(ctx); err != nil {
        return err
    }
    if err := session.WaitReady(ctx); err != nil {
        return err
    }

    if err := session.Subscribe(ctx, futuresstream.Kline("ETHUSDT", "1m")); err != nil {
        return err
    }

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case event, ok := <-session.Events():
            if !ok {
                return session.TerminalError()
            }
            log.Printf("generation=%d stream=%s data=%s", event.Generation, event.Stream, event.Data)
        case gap := <-session.Gaps():
            log.Printf("stream continuity lost: %+v", gap)
        }
    }
}
```

## Desired-state behavior

`Subscribe`, `Unsubscribe`, and `ReplaceSubscriptions` modify the desired set
and wait until Binance has acknowledged convergence for the current physical
connection generation.

A newer conflicting desired-state mutation supersedes an older waiter. The
older operation returns `ErrOperationSuperseded`; the session continues toward
the latest desired set.

After reconnect:

1. the active and pending sets are discarded;
2. the latest desired set is retained;
3. subscriptions are sent in configured batches;
4. the session enters `Ready` only after every acknowledgement has arrived.

## Event continuity

A reconnect or event-buffer overflow means continuity cannot be guaranteed.
Callers must consume `Gaps()` and rebuild any state that requires lossless
updates, such as a local depth book.

No market event is silently dropped by the default policy. When the event
buffer fills, the session emits `ErrEventBufferFull`, reports a gap, and
interrupts the current connection so that the subscription session is rebuilt.

## Protocol limits

Defaults:

- acknowledgement timeout: 5 seconds;
- subscription request interval: 200 milliseconds;
- maximum parameters per request: 100;
- maximum streams per session: 1024;
- event buffer: 256.

The request interval deliberately leaves substantial margin below Binance's
connection-level incoming-message limit.

## Deliberate exclusions

M2 does not implement:

- Private/listen-key sessions;
- dynamic Private subscription payloads;
- WSAPI request/response sessions;
- automatic migration of legacy `WsXxxServe` APIs;
- application-specific event decoding beyond combined/raw envelope extraction.

Those are M3–M5 concerns.
