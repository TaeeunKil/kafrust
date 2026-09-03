# Buffered close flushes accepted records (2026-09-03)

## Scope

At source commit `40905f137e5a1f13cb99e4923d95e35e4aa3f1c8`,
`buffered_close_flushes_accepted_record_before_worker_shutdown` enqueues one
record with a 10 second linger and then calls `BufferedProducer::close()`
without an explicit flush. Close sends the accepted record, resolves its
delivery, drains the buffered gauge, and joins the worker.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  buffered_close_flushes_accepted_record_before_worker_shutdown \
  -- --nocapture
1 passed; 0 failed

cargo check --workspace --all-targets
cargo test --workspace --all-features --quiet
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The scripted broker observed exactly one Metadata request, one ApiVersions
request, and one successful Produce request. The delivery returned offset 42,
`buffered_records` reached zero, and the worker closed cleanly.

## Boundary

This closes the deterministic buffered close/flush slice. It does not qualify
expired or in-flight close ambiguity, cancellation during transmission,
published mixed-outcome profiles, long campaigns, service canary
qualification, or release authorization.
