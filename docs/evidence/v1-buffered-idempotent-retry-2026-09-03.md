# Buffered idempotent response-loss retry (2026-09-03)

## Scope

At source commit `27a9f466bc87651123ba1cb9592de3f76574474a`, the scripted
`buffered_idempotent_producer_retries_dropped_response_with_same_batch_sequence`
regression exercises the opt-in `BufferedProducer` idempotent path. The broker
drops the first Produce response; the producer reconnects and replays the
same encoded batch. Kafka duplicate-sequence response code 46 resolves the
original delivery without allocating a new sequence.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  buffered_idempotent_producer_retries_dropped_response_with_same_batch_sequence \
  -- --nocapture
1 passed; 0 failed

cargo test --workspace --all-features --quiet
471 passed; 0 failed (main library target)
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The scripted broker observed the initialization, metadata/capability lookup,
first Produce, reconnect, and replay Produce frames. The first and replay
Produce request frames were byte-identical; metrics recorded a retry and one
broker error, and the buffered producer closed cleanly.

The exact pushed source commit passed the stable/Rust 1.81.0 CI matrix in
[run 33748516051](https://github.com/TaeeunKil/kafrust/actions/runs/33748516051).

## Boundary

This closes the deterministic buffered response-loss slice only. It does not
qualify the complete before/partial/after-write matrix, ten-cycle published
profiles, 100,000-record reconciliation, long campaigns, or release gates.
