# M2 Implementation Record

## Scope

M2 introduces managed USDⓈ-M Futures Public and Market subscription sessions.
It is built on the M1 managed WebSocket transport and does not alter legacy
stream APIs.

## Production files

- `errors.go` — typed protocol/session errors and observable error events.
- `pacer.go` — serialized request pacing.
- `session.go` — lifecycle, protocol correlation, reconciliation, event
  dispatch, reconnect restoration, and shutdown.
- `subscription.go` — typed Public/Market subscription builders.
- `types.go` — public API, state/events, observer, and options.

M1 was extended with `Connection.Interrupt(error)`, allowing an upper protocol
layer to retire a physical generation after an acknowledgement timeout or
continuity failure without permanently closing the managed connection.

## State ownership

The session maintains:

- `desired`: the latest caller-requested subscription set;
- `active`: subscriptions acknowledged on the current generation;
- `pending`: protocol requests awaiting an acknowledgement;
- `waiters`: caller operations waiting for desired/active convergence.

`Ready` is an atomic condition under the session mutex:

- transport is ready;
- generation is current;
- desired equals active;
- no subscribe/unsubscribe request is pending.

## Reconnect behavior

On transport disconnect:

- active state is cleared;
- pending requests fail with generation-changed errors;
- continuity is reported through `GapEvent` when the session had been active;
- desired state remains unchanged.

When the new physical generation becomes ready, the reconciliation loop sends
the current desired set. Older historical operations are not replayed.

## Backpressure policy

Socket reads do not execute caller handlers. Events are copied into a bounded
channel. A full channel is treated as a continuity failure:

1. emit typed overflow error;
2. emit `GapEvent`;
3. interrupt the current physical connection;
4. reconnect and restore desired subscriptions.

This avoids silent loss for depth and user-sensitive market streams.

## Shutdown safety

Natural parent-context cancellation and explicit `Close` both terminate the
logical session. Termination is recorded before output channels close. All
state/error/gap publication uses an output lock, so an API call that raced with
shutdown cannot send to a closed channel.

## Test evidence

The local Gorilla WebSocket server suite covers:

- subscribe/unsubscribe/list/property requests;
- exact request payloads;
- out-of-order acknowledgements;
- rejected requests and typed errors;
- acknowledgement timeout and forced reconnect;
- reconnect restoration of only the latest desired set;
- no logical Ready before restoration acknowledgements;
- batching and stream limits;
- public/market routing validation;
- combined and raw event delivery;
- slow-consumer continuity failure;
- slow observer isolation;
- conflicting desired mutations;
- calls before start, canceled contexts, and shutdown races.

No external Binance network was available in the build environment. Live
Mainnet/Testnet/Demo protocol smoke remains an explicit deployment gate.
