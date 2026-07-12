# Managed WebSocket API Session — M4

`api.Session` adds request/response semantics above the M1 managed transport.
It is intended for Binance WebSocket API connections where multiple requests
may be in flight concurrently and responses carry a top-level request ID.

## Capabilities

- concurrent requests with per-request futures;
- response routing by top-level string or numeric ID;
- out-of-order response handling;
- request-local timeout and cancellation;
- one-use request IDs for the lifetime of a physical generation;
- typed safe-request failures and side-effecting unknown outcomes;
- no automatic request replay;
- automatic transport reconnect through M1;
- optional per-generation authentication such as `session.logon`;
- proactive connection rotation with old-connection draining;
- unsolicited frame delivery;
- bounded pending requests and observer queues;
- logical session states and typed error observations.

All physical writes, including active Ping, server Pong, application Text, and
Close frames, continue through the M1 single-writer transport.

## Request outcome policy

Every request declares an outcome policy:

- `OutcomeSafe`: read-only or safely retryable request. Disconnect and timeout
  return `RequestError`. The SDK still does not replay it automatically.
- `OutcomeUnknown`: side-effecting request. Once send is attempted, disconnect,
  timeout, cancellation, close, or rotation drain timeout returns
  `UnknownOutcomeError`.

The default is `OutcomeUnknown`. This is intentionally conservative for trading
systems.

```go
response, err := session.Do(ctx, api.Request{
    ID:      requestID,
    Method:  "order.place",
    Payload: encodedRequest,
    Outcome: api.OutcomeUnknown,
})

var unknown *api.UnknownOutcomeError
if errors.As(err, &unknown) {
    // Do not blindly submit the order again. Reconcile using request/client IDs.
}
```

`UnknownOutcomeError` matches both `ErrOutcomeUnknown` and its underlying cause,
for example `ErrDisconnected` or `ErrRequestTimeout`.

## Request IDs

The payload must contain a top-level `id` equal to `Request.ID`. A request ID is
accepted once per physical API generation. It cannot be reused after a timeout
or completed response until the connection generation changes. This prevents a
late response from an old request being delivered to a newer request with the
same ID.

## Reconnect behavior

A transport disconnect completes all pending requests assigned to that physical
generation. Requests are never replayed. After M1 reconnects and any configured
authenticator succeeds, the logical session becomes Ready and accepts new
requests.

`WaitReady` may be called after every disconnect; unlike a one-shot startup
signal, it waits for the current active generation.

## Authentication

Implement `Authenticator`, or use `AuthenticatorFuncs`, to issue a request such
as `session.logon` for every physical generation:

```go
auth := api.AuthenticatorFuncs{
    Build: func(generation uint64) (api.Request, error) {
        return buildSessionLogonRequest(generation)
    },
    Validate: func(response api.Response) error {
        return validateSessionLogonResponse(response.Payload)
    },
}
```

The session does not become Ready before authentication succeeds. Transport
failures during authentication enter the normal reconnect path. A request build
or response validation failure is terminal, preventing an invalid credential
from reconnecting forever.

## Rotation

When rotation is enabled, M4 creates a replacement managed connection before
retiring the old one:

1. establish the new connection;
2. run per-generation authentication;
3. atomically route new requests to the new connection;
4. keep reading responses for pending requests on the old connection;
5. close the old connection after pending requests drain or the drain timeout
   expires.

Side-effecting requests still pending when the drain timeout expires return
`UnknownOutcomeError` and are not replayed.

## Unsolicited frames

Frames without a matching outstanding ID are published through
`Unsolicited()`. This includes server notices and late responses. They are not
allowed to complete a different request.

## Compatibility status

Business reconciliation for unknown trading outcomes and order idempotency
remain caller responsibilities. M5 adds `common/websocket.ManagedClient` and
migrates the legacy Futures account/order WSAPI constructors to this M4
session. The generic old client remains available for non-Futures source
compatibility.
