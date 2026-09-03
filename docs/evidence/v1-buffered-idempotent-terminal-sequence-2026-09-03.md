# Buffered idempotent terminal sequence errors (2026-09-03)

## Scope

At source commit `a46462f`, the scripted broker returns each Kafka identity
error (`OUT_OF_ORDER_SEQUENCE_NUMBER` 45, `INVALID_PRODUCER_EPOCH` 47, and
`PRODUCER_FENCED` 90) to a linger-buffered idempotent Produce. The buffered
worker completes the first delivery with the fatal broker error and rejects a
second queued delivery without transmitting another Produce request.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  buffered_idempotent_producer_fatal_sequence_errors_are_terminal \
  -- --nocapture
1 passed; 0 failed

cargo check --workspace --all-targets
cargo test --workspace --all-features --quiet
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

For each of the three fatal codes, metrics recorded one broker error and zero
retries. The scripted broker observed exactly
`ApiVersions`, `InitProducerId`, `Metadata`, `ApiVersions`, and one Produce
request; the second buffered delivery produced no additional frame and
completed with the same fatal code. The buffered worker then closed cleanly.

## Boundary

This closes deterministic buffered terminal-error behavior. It does not qualify
partial client request writes, cancellation/shutdown ambiguity, published
fault cycles, 100,000-record reconciliation, long campaigns, service canary
qualification, or release authorization.
