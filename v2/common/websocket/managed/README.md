# Managed WebSocket Connection — M1

`managed.Connection` is the transport foundation for future dynamic stream
subscriptions, private user streams, and WebSocket API sessions.

## Capabilities

- instance-scoped configuration; no package-global runtime settings;
- `context.Context` owned lifecycle;
- explicit state machine and typed failure events;
- monotonically increasing physical connection generation;
- old-generation frame and control callback fencing;
- one writer goroutine for Text, Ping, Pong, and Close frames;
- control-frame priority over application writes;
- correct response to server Ping frames with the same Pong payload;
- active client Ping with unique payload and matching Pong verification;
- configurable low-latency liveness detection;
- automatic reconnect with exponential backoff, jitter, caps, and limits;
- reconnect attempt count and Ping RTT observation;
- no reconnect after explicit close or parent-context cancellation;
- Gorilla WebSocket adapter in `managed/gorilla`.

The default active heartbeat is:

- Ping interval: 5 seconds;
- Pong timeout: 3 seconds;
- Write timeout: 2 seconds.

A link that fails immediately after a successful Pong is therefore normally
detected within approximately eight seconds: at most five seconds before the
next Ping plus three seconds waiting for its matching Pong.

## Basic use

```go
package main

import (
    "context"
    "time"

    managedws "github.com/adshao/go-binance/v2/common/websocket/managed"
    gorilladialer "github.com/adshao/go-binance/v2/common/websocket/managed/gorilla"
)

func connect(ctx context.Context, endpoint string) (*managedws.Connection, error) {
    conn, err := managedws.NewConnection(managedws.Options{
        Dialer: gorilladialer.Dialer{
            Endpoint:  endpoint,
            ReadLimit: 655350,
        },
        Heartbeat: managedws.HeartbeatOptions{
            Enabled: true, // zero durations select the 5s/3s/2s defaults
        },
        Reconnect: managedws.ReconnectPolicy{
            Enabled:      true,
            InitialDelay: 100 * time.Millisecond,
            MaxDelay:     10 * time.Second,
            Multiplier:   2,
            Jitter:       0.2,
        },
    })
    if err != nil {
        return nil, err
    }
    if err := conn.Start(ctx); err != nil {
        return nil, err
    }
    if err := conn.WaitReady(ctx); err != nil {
        return nil, err
    }
    return conn, nil
}
```

Consumers can observe `States()`, `Heartbeats()`, and `Errors()`, or provide an
`Observer`. Observer callbacks must return promptly; the bounded observation
queue prevents them from blocking socket I/O.

Upper protocol layers may call `Interrupt(cause)` to retire only the current
physical generation and enter the configured reconnect path. This is used by
M2 when a subscription acknowledgement times out or event continuity is lost;
it is distinct from permanent `Close`.

## Write semantics

`SendText` is context-aware. A queued write whose caller context expires before
the writer starts it is discarded and is not sent later. Once the physical
socket write has started, the result depends on the socket write deadline; a
caller handling trading semantics must still distinguish a confirmed failure
from an unknown outcome at the WSAPI layer.

## Deliberate exclusions

M1 does not implement:

- `SUBSCRIBE`, `UNSUBSCRIBE`, or subscription recovery;
- private listen-key creation or keepalive;
- WSAPI request/response correlation;
- migration of legacy `WsXxxServe` APIs.

Those capabilities are built above this transport in M2–M5.
