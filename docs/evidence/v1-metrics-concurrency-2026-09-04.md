# V1 Metrics Concurrency Evidence — 2026-09-04

## Scope

Source commit `91c5592c6599eeb16df661616efa3fe0d5c7e0b4` adds a deterministic
concurrency regression for the shared `ClientMetrics` state. Four worker
threads synchronize on a barrier and each perform 100 updates. The test
asserts the exact request, byte, error, latency-bucket, and in-flight totals
after all workers join.

This evidence closes only the in-process atomic-update consistency slice of
V1-17. It does not claim broker collection, published-artifact behavior,
replacement identity, throttling, secure transport, or long-duration
qualification.

## Verification

Focused command:

```text
cargo test -p kafrust --lib metrics::tests::concurrent_metric_updates_keep_counters_consistent -- --nocapture
```

Result: one test passed on the company Windows x64 checkout.

The required workspace validation also passed in the Visual Studio x64
developer environment:

- `cargo fmt --all`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-features` (491 client unit tests, 13 broker
  roundtrip tests, 29 fault-injection tests, 5 public-surface tests, 285
  protocol tests, 5 golden tests, 5 malformed-input tests, and 10 doctests)
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo doc --workspace --all-features --no-deps`
- `git diff --check`

The exact pushed CI run is authoritative for the repository state:
`https://github.com/TaeeunKil/kafrust/actions/runs/33793340966`.

## Non-claims

This is one deterministic Windows run with no broker and no Docker resources.
It is not a published telemetry collection, secure multi-broker qualification,
long-duration campaign, service canary, or release authorization.
