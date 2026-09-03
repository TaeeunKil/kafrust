# Telemetry push cancellation evidence (2026-09-04)

## Scope

This record covers the deterministic KIP-714 boundary where a caller drops a
`TelemetryClient::push_once` future after the PushTelemetry request frame has
been observed but before the broker response arrives. The persistent telemetry
connection is poisoned and a later push must fail rather than interpret a
response from the canceled request as a new operation.

Source commit: `528986f08c1fc8cee6ee37c57f2e1ae8e92608cb`.

The regression seeds a valid subscription, waits for the scripted broker to
observe API key 72, drops the push future, verifies the client is unusable, and
asserts that a subsequent push returns `NotConnected`. The broker task is
joined after the bounded wait.

## Verification

Focused command:

```text
cargo test -p kafrust --lib telemetry::tests::cancels_push_after_transmission_and_rejects_connection_reuse -- --exact --nocapture
```

Result: one test passed on Windows and one on company WSL2 `Ubuntu-T9` with
Rust 1.81.0. The WSL2 run also passed all 29 `fault_injection` tests. The
fixture used an in-memory duplex stream; no Docker container, network, or
volume was created or modified.

The required Windows workspace validation passed at this source commit:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` — 482 `kafrust` unit tests, 13
  broker-roundtrip tests, 29 fault-injection tests, 5 public-surface tests,
  285 protocol tests, 5 golden tests, 5 malformed-input tests, and 10
  doctests passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

## Boundary and non-claims

This is local deterministic connection-lifecycle evidence. It does not qualify
broker replacement, subscription mutation, throttling, compression profiles,
secure transport, published artifacts, long collection, or release
authorization. A canceled push has no broker outcome and requires a new
telemetry client/connection for further operation.
