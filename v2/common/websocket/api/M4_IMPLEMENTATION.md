# M4 Implementation Record

## Scope

M4 adds a new managed request/response WebSocket API client above the M1
transport and a USDⓈ-M Futures WSAPI constructor. Legacy WSAPI services remain
unchanged until M5 compatibility convergence.

## Production files

- `common/websocket/api/types.go` — request/response contracts, outcome policy,
  authentication, rotation, state, observer, and options.
- `common/websocket/api/errors.go` — typed request and unknown-outcome errors.
- `common/websocket/api/session.go` — lifecycle, pending futures, response
  routing, reconnect handling, authentication, rotation, drain, and shutdown.
- `futures/wsapi/session.go` — Futures environment endpoints and reliability
  defaults.

M1 adds `SendTextOnGeneration`. It atomically rejects a send when a request was
registered for an older physical generation, preventing a reconnect race from
writing that request to the replacement socket.

## Request correlation

Before sending, M4 registers a pending future by request ID and generation. The
reader parses the top-level response ID and completes only the matching future
on the same transport slot and physical generation.

Responses may arrive in any order. A timeout removes only its own pending
future. Other requests and responses continue normally.

IDs are remembered for the full physical API generation. Reuse within the same
generation is rejected even after timeout or completion, preventing a delayed
old response from satisfying a new request with the same ID. The remembered set
is discarded when that generation disconnects.

## Trading outcome safety

M4 never automatically replays requests.

- safe/read-only requests return `RequestError` on timeout or disconnect;
- side-effecting requests return `UnknownOutcomeError` once send was attempted;
- the conservative default is side-effecting/unknown;
- close and rotation drain timeout preserve the same outcome distinction.

The SDK reports uncertainty; business code must reconcile orders using Binance
truth and caller-provided idempotency identifiers.

## Authentication

An optional `Authenticator` builds a generation-specific request and validates
its response. The logical session is not Ready until authentication succeeds.
Authentication repeats after reconnect and on proactive rotation.

Transport timeout/disconnect during authentication retires the current physical
generation and retries through M1 reconnect. Request construction or response
validation failure is terminal, avoiding an infinite retry loop for invalid
credentials.

## Active heartbeat

The Futures constructor enables M1 heartbeat by default:

- active WebSocket Ping control frame every 5 seconds;
- matching Pong timeout after 3 seconds;
- control write timeout after 2 seconds.

No custom JSON heartbeat method is sent. A local Gorilla server test proves the
connection receives a WebSocket control Ping while receiving no Text heartbeat.

## Reconnect

M1 owns physical reconnect. M4 observes generation transitions, immediately
completes all pending requests on the failed generation, repeats optional
authentication, and accepts only new requests after the new generation becomes
Ready.

Request IDs can be reused after a generation change because old-generation
frames are fenced by M1 and M4.

## Rotation and drain

The Futures constructor enables proactive rotation at 23h50m by default, before
the exchange's 24-hour connection lifetime. A replacement connection is fully
Ready and authenticated before the active pointer switches.

New requests use the replacement connection. The old connection remains open to
receive pending responses. It closes after all old pending requests finish or
after the configured drain timeout. Drain timeout completes remaining requests
with safe-disconnect or unknown-outcome errors and never replays them.

## Lifecycle safety

A worker-start gate prevents new slot/authentication goroutines from being added
after shutdown begins. Close cancels transports, rotation, authentication, and
pending waits; all workers finish before output channels close.

Observer callbacks run on a dedicated bounded queue and cannot block socket I/O.

## Test evidence

Local Gorilla WebSocket servers cover:

- 100 concurrent requests with reverse-order responses;
- request-local timeout isolation;
- string and numeric response IDs;
- duplicate pending IDs;
- generation-lifetime ID reuse protection;
- pending-request limits;
- safe disconnect versus unknown trading outcome;
- no replay after reconnect;
- new requests after reconnect;
- per-generation authentication and terminal validation failure;
- unsolicited frame delivery;
- graceful rotation with old pending response drain;
- rotation drain timeout with unknown outcome and no replay;
- pending request completion during Close;
- WebSocket control Ping without JSON heartbeat;
- concurrent lifecycle execution under the Go race detector.

No external Binance network was available. Live Demo/Testnet verification of
active client Ping/Pong, session authentication payloads, and 24-hour lifecycle
remains a release gate for M5 rather than being inferred from local tests.
