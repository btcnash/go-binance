# USDⓈ-M Futures Managed WSAPI

`futures/wsapi.NewSession` creates the M4 managed API session for the Binance
USDⓈ-M Futures WebSocket API.

## Defaults

- Mainnet endpoint: `wss://ws-fapi.binance.com/ws-fapi/v1`
- Testnet/Demo endpoint: `wss://testnet.binancefuture.com/ws-fapi/v1`
- active control Ping/Pong: M1 defaults, 5s / 3s / 2s;
- automatic reconnect: enabled;
- proactive rotation: 23h50m;
- old-connection drain timeout: 30s.

All defaults are instance-scoped. The new API does not depend on the legacy
`UseTestnet`, `WebsocketKeepalive`, or `WebsocketTimeoutReadWriteConnection`
package globals.

```go
session, err := wsapi.NewSession(wsapi.Options{
    Environment: wsapi.EnvironmentMainnet,
    API: api.Options{
        Authenticator: mySessionLogonAuthenticator,
    },
})
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

response, err := session.Do(ctx, api.Request{
    ID:      requestID,
    Method:  "order.place",
    Payload: signedPayload,
    Outcome: api.OutcomeUnknown,
})
```

Set `Endpoint` for a custom/local server. Explicit disable flags are available
for heartbeat, reconnect, and rotation, primarily for controlled tests.

Legacy Futures WSAPI service constructors continue to use the old client until
M5 wraps them around this session.
