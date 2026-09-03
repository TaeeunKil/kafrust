# Buffered close edge cases (2026-09-03)

## Scope

At source commit `b88564748c56e1bcc8ae4c5944235b4cb1bb95e4`, two real buffered
producer close paths were exercised. A close with an in-flight Produce whose
response is held reports the ambiguous Produce-phase delivery deadline while
joining the worker and draining the gauge. A cloneable
`BufferedProducerHandle` can enqueue a record that the owning producer then
flushes and completes during `close()`.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  buffered_close_reports_in_flight_deadline_and_joins_worker \
  -- --exact --nocapture
1 passed; 0 failed; 27 filtered out; finished in 0.11s

cargo test -p kafrust --test fault_injection \
  buffered_close_flushes_handle_owned_record_before_worker_shutdown \
  -- --exact --nocapture
1 passed; 0 failed; 27 filtered out; finished in 0.00s

cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The in-flight close script observed Metadata, ApiVersions, and Produce, then
withheld the Produce response. `close()` returned
`DeliveryDeadlineExceeded { phase=Produce, possibly_transmitted=true }`, the
delivery returned the same terminal error, and the worker joined with zero
buffered records. The handle-owned success script observed one Metadata, one
ApiVersions, and one Produce response at offset 43; owner close resolved the
delivery and drained the gauge.

## Boundary

This closes deterministic close behavior for an in-flight deadline and a
handle-owned accepted record. It does not qualify cancellation during socket
I/O, delayed metadata/capability, published mixed-outcome reconciliation,
long campaigns, service canary qualification, or release authorization.
