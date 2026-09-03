# Buffered delivery sender cancellation (2026-09-03)

## Scope

At source commit `c1dc20943dd9ae7e7f9971a665c4ca15dfd3b8cc`, the caller drops a
`ProducerDelivery` immediately after enqueueing a buffered record. The worker
still sends the accepted record, releases the queue guard after completion,
and owner `flush()` and `close()` finish without a resource leak.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  buffered_delivery_sender_cancellation_releases_record_after_flush \
  -- --exact --nocapture
1 passed; 0 failed; 28 filtered out; finished in 0.01s

cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The scripted broker observed one Metadata, one ApiVersions, and one Produce
response at offset 44. Dropping the delivery receiver did not cancel the
accepted Produce; `flush()` returned successfully and
`buffered_records` reached zero before close joined the worker.

## Boundary

This closes deterministic cancellation of a delivery receiver before
transmission completion. It does not qualify cancellation of a future while
socket I/O is blocked, partial client request writes, published mixed-outcome
reconciliation, long campaigns, service canary qualification, or release
authorization.
