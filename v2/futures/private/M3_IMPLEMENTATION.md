# M3 Implementation Record

## Scope

M3 introduces managed USDⓈ-M Futures private user-data sessions. It is built on
M1 managed transport and coexists with M2 Public/Market stream sessions. Legacy
private stream functions are not modified.

## Production files

- `types.go` — public API, options, events, state, gaps, observers, and provider
  contracts.
- `errors.go` — typed private-session failures.
- `provider.go` — adapter from the existing Futures REST client to the
  `ListenKeyProvider` contract.
- `dialer.go` — source runtime, key versions, endpoint reconstruction, shared
  bindings, and handshake classification.
- `session.go` — lifecycle, frame decoding, source attribution, keepalive,
  invalidation, reconnect, gap reporting, and shutdown.

M1 Gorilla dialing was extended with `HandshakeError`, preserving HTTP status
codes for upper-layer authentication/listen-key classification.

## Source and key ownership

Each source owns:

- a stable caller-defined source ID;
- a `ListenKeyProvider`;
- an optional set of expected event types;
- a current listen key and monotonically increasing key version.

The session never publishes raw keys through state, error, gap, or lifecycle
observations. A physical generation stores an immutable source-binding snapshot
containing the key versions used to build that endpoint.

Before accepting a generation as current, the session verifies every binding
against the live source runtime. A generation built with an invalidated key is
interrupted before it becomes logically ready.

## Modes

### Isolated

Exactly one source is required. Every event is attributable to that source,
which contains account failures and reconciliation scope.

### Shared

Multiple listen-key/event pairs are encoded in one `/private/stream` endpoint.
Source attribution is conservative:

- explicit envelope/listen-key evidence wins;
- a unique event filter may identify one source;
- overlapping filters return candidate IDs instead of guessing.

## Keepalive and invalidation

Listen-key keepalive runs independently for every source. Default behavior:

- interval: 30 minutes;
- request timeout: 10 seconds;
- retry delay: 1 second to 30 seconds;
- multiplier: 2;
- attempts: 3.

Transient failures are retried without rebuilding a healthy connection. An
invalid-key classification or exhausted retries invalidates the source,
publishes an error and gap, and interrupts the current generation. The next
managed reconnect reacquires missing keys before dialing.

The same invalidation path handles:

- `listenKeyExpired` events;
- private rejection frames with recognized invalid/auth codes;
- configurable WebSocket handshake rejection classification.

## Event decoding

Binance user-data events use lowercase `e` for event type and uppercase `E`
for event time. M3 reads the raw JSON object by exact key before decoding the
SDK event structure. This avoids Go's case-insensitive struct-field matching
from attempting to decode numeric `E` into the string event-type field.

Socket reads never execute caller code. Events enter a bounded channel. A full
channel is treated as a continuity failure: error, gap, interrupt, reconnect.
No private event is silently discarded as if continuity remained valid.

## Continuity and generation fencing

All events carry the M1 physical generation. Old-generation frames are dropped.
A fast first frame may arrive before the independent transport-state channel
reports Ready; it is accepted only when the current dial snapshot and every key
version still match. That accepted generation is marked as previously ready so
an immediate following disconnect still emits a gap.

Every disconnect after readiness reports affected source IDs. Reconnection does
not clear the caller's obligation to reconcile the gap.

## Shutdown

Explicit `Close` and parent-context cancellation stop transport, keepalive,
frame, error, state, and observer loops. Output-channel publication and closure
share a lock, preventing concurrent send-on-closed-channel panics.

By default, keys acquired by the session are released during shutdown.
`RetainListenKeys` leaves lifecycle ownership with the caller.

## Test evidence

Local REST and Gorilla WebSocket servers cover:

- isolated source acquisition, event delivery, keepalive, and release;
- shared endpoint construction;
- exact and ambiguous source attribution;
- listen-key expiration and key reacquisition;
- invalid private response and HTTP handshake rejection recovery;
- transient keepalive retry without reconnect;
- exhausted/invalid keepalive refresh;
- transient Acquire failure recovery;
- transport disconnect GapEvent and reconnect;
- fast first event followed by immediate disconnect;
- event-buffer overflow GapEvent and reconnect;
- retain-key and close-before-start behavior;
- REST provider POST/PUT/DELETE contract;
- invalid-key API error classification;
- concurrent lifecycle execution under the Go race detector.

No external Binance network was available in the build environment. Private
JSON dynamic-subscription payloads and live Demo/Testnet endpoint behavior
remain explicit future gates rather than inferred protocol behavior.
