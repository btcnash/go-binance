# Managed USDⓈ-M Futures Private Stream — M3

Package `futures/private` adds managed Futures user-data WebSocket sessions above
M1 `common/websocket/managed.Connection`.

It owns listen-key acquisition, renewal, invalidation, release, transport
reconnect, conservative source attribution, and explicit continuity gaps.
Legacy `WsUserDataServe` functions are intentionally unchanged.

## Capabilities

- isolated mode: one account/source per physical connection;
- shared mode: multiple listen keys on one `/private/stream` connection;
- Mainnet, Testnet, Demo, or custom endpoint roots;
- `ListenKeyProvider` abstraction and a REST provider for the existing Futures
  client;
- listen-key acquisition on every generation that requires a fresh key;
- default 30-minute keepalive, independent of transport Ping/Pong;
- bounded keepalive retries with typed failures;
- automatic key invalidation and reacquisition after expiration, API rejection,
  or configured handshake rejection;
- reconnect and endpoint rebuild using the latest listen keys;
- explicit `GapEvent` for disconnect, event overflow, malformed event,
  keepalive exhaustion, and listen-key expiration;
- exact event-type parsing for Binance's lowercase `e` field without confusing
  uppercase `E` event time;
- conservative source attribution in shared sessions;
- event, state, error, gap, and listen-key lifecycle channels;
- bounded observer delivery that cannot block socket reads;
- safe explicit close and parent-context cancellation;
- optional `RetainListenKeys` for externally owned key lifecycles.

## Basic isolated session

```go
package main

import (
    "context"
    "log"

    "github.com/btcnash/go-binance/v2/futures"
    futuresprivate "github.com/btcnash/go-binance/v2/futures/private"
)

func run(ctx context.Context, client *futures.Client) error {
    session, err := futuresprivate.NewSession(
        futuresprivate.SessionOptions{
            Mode:        futuresprivate.ModeIsolated,
            Environment: futuresprivate.EnvironmentMainnet,
            Sources: []futuresprivate.Source{
                {
                    ID: "account-1",
                    Provider: futuresprivate.RESTListenKeyProvider{
                        Client: client,
                    },
                },
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

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case event, ok := <-session.Events():
            if !ok {
                return session.TerminalError()
            }
            log.Printf(
                "generation=%d source=%s type=%s resolution=%s",
                event.Generation,
                event.SourceID,
                event.Type,
                event.SourceResolution,
            )
        case gap := <-session.Gaps():
            // Reconcile account, order, position, and balance truth here.
            log.Printf("private stream continuity gap: %+v", gap)
        }
    }
}
```

## Listen-key lifecycle

Transport liveness and listen-key validity are separate:

- M1 active WebSocket Ping/Pong detects a broken connection;
- M3 keepalive extends the listen-key lifetime.

The default keepalive interval is 30 minutes. A transient keepalive failure is
retried. When retries are exhausted, or the provider identifies an invalid key,
the session:

1. publishes a typed keepalive error;
2. publishes a continuity gap;
3. invalidates the current key;
4. interrupts the current physical connection;
5. reacquires a key and rebuilds the private endpoint on reconnect.

`listenKeyExpired`, API errors such as an invalid-key rejection, and configured
HTTP handshake failures use the same refresh path.

Raw listen-key material is never included in lifecycle events.

## Shared sessions and source attribution

Shared mode supports multiple source/listen-key pairs:

```go
session, err := futuresprivate.NewSession(
    futuresprivate.SessionOptions{
        Mode: futuresprivate.ModeShared,
        Sources: []futuresprivate.Source{
            {ID: "account-1", Provider: provider1},
            {ID: "account-2", Provider: provider2},
        },
    },
)
```

The package does not guess account ownership. Attribution priority is:

1. explicit listen key in the envelope;
2. one-source isolated session;
3. a unique configured event filter;
4. otherwise return sorted candidate source IDs.

For ambiguous events, `Event.SourceID` is empty and
`Event.CandidateSourceIDs` contains every possible source. Callers must not
apply such an event to a single account without another source of truth.

## Continuity contract

A WebSocket reconnect does not prove that no account event was missed. Every
post-ready transport disconnect emits `GapReasonDisconnected`.

Other explicit gap reasons include:

- `event_overflow`;
- `listen_key_expired`;
- `keepalive_failed`;
- `malformed_event`.

Consumers must reconcile affected accounts through REST or another canonical
state source before considering local private-stream state complete again.

## Deliberate exclusions

M3 does not implement:

- private JSON `SUBSCRIBE`/`UNSUBSCRIBE` payloads;
- runtime addition/removal of listen keys on an existing private connection;
- WSAPI request/response correlation;
- automatic migration of legacy private stream APIs.

Private dynamic-subscription payloads remain closed until they are verified
against Binance Demo/Testnet. Shared mode currently rebuilds the URL from the
latest listen-key set on reconnect.

## Connection rotation

Private sessions proactively recycle the physical WebSocket at `23h50m` by default and reconnect using current listen keys. Configure `Connection.MaxConnectionAge`, or set `Connection.DisableRotation` to disable this policy. Every loss of continuity still emits a GapEvent.
