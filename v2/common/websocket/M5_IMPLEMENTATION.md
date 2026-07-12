# M5 Implementation Record

M5 converges the Futures compatibility surfaces onto the managed WebSocket stack built in M1–M4.

## Production changes

- `futures/websocket.go`: legacy raw compatibility runner now uses ManagedConnection.
- `futures/legacy_stream_compat.go`: Public/Market legacy URLs are converted to M2 dynamic subscriptions.
- `common/websocket/managed_client.go`: M4 API Session adapter for the legacy Client interface.
- `futures/legacy_wsapi_client.go`: legacy Futures WSAPI constructors use M4.
- M1 `MaxConnectionAge`: proactive recycle before exchange hard limits.
- M2/M3 default maximum connection age: 23h50m.

## Compatibility boundary

The fixed-listenKey legacy API cannot obtain a replacement listenKey after expiry because its public signature has no provider. It reconnects transport using the supplied key. Applications requiring key refresh must use M3 `private.Session`.

## Test evidence

- Local Gorilla server verifies single and combined dynamic subscriptions, reconnect and envelope compatibility.
- Local Gorilla server verifies fixed-key Private reconnect.
- Local Gorilla server verifies legacy Futures WSAPI request/response through M4.
- M1 maximum-age tests verify proactive generation replacement.

## Environment limitation

The execution environment had no external Binance connectivity. The opt-in `internal/livesmoke` suite was compiled and its skip gate was verified, but Public/Market/Private/WSAPI Demo calls were not executed here. They remain a release gate for a network-enabled environment.
