# Buffered in-flight delivery deadline (2026-09-03)

## Scope

At source commit `ed71f6d3d1ac50aa0f27e3a89d3a626238a452bb`, the real
`BufferedProducer` worker accepts a record, flushes it through Metadata,
ApiVersions, and Produce, then receives no Produce response. The configured
100 ms total delivery budget expires in the Produce phase after the request
may have been transmitted. The delivery and flush operation both return the
typed ambiguous deadline error, the buffered-record gauge drains, and the
worker closes cleanly.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  buffered_delivery_deadline_expires_after_produce_without_response \
  -- --exact --nocapture
1 passed; 0 failed; 25 filtered out; finished in 0.13s

cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The scripted broker observed exactly one Metadata request, one ApiVersions
request, and one Produce request. No response was sent for Produce, so the
client reported `DeliveryDeadlineExceeded` with `phase=Produce` and
`possibly_transmitted=true` (the remaining budget can be 99 ms due to queue
and scheduling time). The delivery sender and buffered gauge were released,
and close joined the worker without creating any Docker resources.

## Boundary

This closes the deterministic buffered post-write deadline slice. It does not
qualify delayed metadata/capability, close while an in-flight request is still
blocked, cancellation during transmission, published mixed-outcome
reconciliation, long campaigns, service canary qualification, or release
authorization.
