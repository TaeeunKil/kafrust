# Telemetry terminating push evidence (2026-09-04)

## Scope

This record covers the deterministic KIP-714 shutdown boundary where
`TelemetryClient::terminate` sends exactly one PushTelemetry v0 request with
the terminating flag set. The scripted broker validates the subscription ID,
terminating bit, negotiated compression, compact payload, tagged fields, and
the successful response before the client returns its summary.

Source commit: `75644e4b5a2ae85e8764eacc95f8bc95102bcdbc`.

The regression seeds subscription ID 4 and a three-byte payload, observes API
key 72, and verifies that the terminating request is encoded as expected. The
broker task is joined after the response, so the test also covers the bounded
shutdown path without leaving a detached fixture task.

## Verification

Focused command:

```text
cargo test -p kafrust --lib telemetry::tests::terminate_sends_one_terminating_push -- --exact --nocapture
```

Result: one test passed on Windows and one on company WSL2 `Ubuntu-T9` with
Rust 1.81.0. The WSL2 run also passed all 29 `fault_injection` tests. The
fixture used an in-memory duplex stream; no Docker container, network, or
volume was created or modified.

The required Windows workspace validation passed at this source commit:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` — 483 `kafrust` unit tests, 13
  broker-roundtrip tests, 29 fault-injection tests, 5 public-surface tests,
  285 protocol tests, 5 golden tests, 5 malformed-input tests, and 10
  doctests passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

## Boundary and non-claims

This is local deterministic request-encoding and shutdown evidence. It does
not qualify broker replacement, subscription mutation, throttling, codec
profiles, secure transport, published artifacts, long collection, service
canaries, or release authorization. The published 60-minute telemetry gates
and final secret/task/resource checks remain open.
