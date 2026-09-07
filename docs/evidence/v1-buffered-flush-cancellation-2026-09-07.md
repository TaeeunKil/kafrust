# Buffered producer flush cancellation during Produce (2026-09-07)

## Scope

Source commit `b7c16eef44a33fb931844142b41ce69a5c7f4fd7` adds a deterministic
scripted-broker regression for a non-idempotent buffered producer whose
`flush()` future is dropped while the Produce response is held after the full
request is observed. Dropping the owner then aborts the worker, completes the
delivery with the typed cancellation outcome, rejects new sends through the
existing handle, and drains the buffered-record gauge without a second Produce
request.

## Verification

The focused test is
`dropping_buffered_flush_cancels_in_flight_delivery`. It passed on Windows with
the complete required Rust validation:

```text
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features  # 510 + 13 + 39 + 5 + 285 + 5 + 5 tests; all passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The test uses only an in-memory scripted TCP fixture. No Docker resource or
external Kafka broker was created or modified.

## Boundary

This is local deterministic buffered-cancellation evidence only. It does not
claim published mixed-outcome reconciliation, broker-side duplicate handling,
live qualification, long campaigns, service canaries, or release
authorization.
