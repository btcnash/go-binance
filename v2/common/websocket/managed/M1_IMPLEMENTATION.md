# M1 Implementation Record

## Scope

M1 adds a new transport kernel under `common/websocket/managed`. Existing
stream and WSAPI entry points are intentionally unchanged in this milestone.

## Completed checklist

- [x] Instance-scoped managed connection.
- [x] Context-controlled start, close, cancellation, and resource cleanup.
- [x] Idle/connecting/connected/ready/disconnected/reconnecting/closed/failed states.
- [x] Monotonic physical connection generation.
- [x] Old-generation frame and control callback fencing.
- [x] Single writer for Text, Ping, Pong, and Close.
- [x] Priority control queue so market/API traffic cannot starve heartbeats.
- [x] Same-payload Pong response to server Ping.
- [x] Unique active Ping payload and matching Pong requirement.
- [x] Default 5-second Ping, 3-second Pong timeout, 2-second write timeout.
- [x] Ping-write, Pong-timeout, read, write, and frame-overflow failure paths.
- [x] Automatic reconnect with exponential backoff, jitter, caps, and maximum attempts.
- [x] Stable-connection backoff reset.
- [x] Explicit Close and context cancellation permanently stop reconnects.
- [x] Typed errors, state events, heartbeat events, Ping RTT, and reconnect count.
- [x] Observer interface without a fixed logging dependency.
- [x] Gorilla WebSocket Dialer adapter.
- [x] Context-expired queued writes are discarded before socket transmission.

## Test evidence

The package includes deterministic fake-socket tests and real local Gorilla
WebSocket server tests covering:

- healthy active Ping/Pong;
- Pong timeout and reconnect;
- Ping write failure;
- read failure;
- connection generation changes;
- stale generation callback rejection;
- serialized concurrent writes;
- control-frame priority;
- server Ping / matching Pong;
- serialized Close frame;
- reconnect exhaustion;
- frame-buffer overflow;
- parent-context cancellation;
- no reconnect after Close;
- observer delivery;
- queued-write timeout without late transmission;
- exponential backoff and jitter bounds.

## Integration boundary

M2 should build `StreamSession` directly on `managed.Connection`. M3 should add
listen-key ownership above the same transport. M4 should replace the existing
WSAPI shared response channel with per-request futures while reusing this
connection kernel.
