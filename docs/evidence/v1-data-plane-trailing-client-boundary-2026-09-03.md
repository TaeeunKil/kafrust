# V1 client trailing-byte boundary — 2026-09-03

## Scope

At source commit `e1df007da77bd6ff6cc3031bc64de9c10b033da8`, an injected
broker-stream test sends an OffsetForLeaderEpoch v3 response with a valid empty
body followed by one sentinel byte. The client decodes the response header and
body, then returns `Error::Protocol(TrailingBytes { remaining: 1 })` rather
than accepting the frame. This confirms the protocol boundary is observable at
the high-level client path.

## Verification

```text
cargo test -p kafrust rejects_trailing_data_plane_response_bytes_over_injected_broker_stream -- --nocapture
1 passed; 0 failed
```

The source commit also passed the required workspace format, check, test,
Clippy, documentation, and diff checks. Its stable/Rust 1.81 CI result is
recorded at
[run 33745018716](https://github.com/TaeeunKil/kafrust/actions/runs/33745018716).

## Boundary

This is a deterministic injected-stream client regression. It does not claim
live broker malformed-response behavior, official Apache response oracles for
every shape, three-broker movement, long campaigns, service-canary readiness,
published compatibility completion, or release authorization.
