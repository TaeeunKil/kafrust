# Idempotent partial-response retry (2026-09-03)

## Scope

At source commit `ed7d5eb60d1c11de3ad6f6f07c4850a2d2daccc5`, the test-only
scripted broker writes the Kafka response length and only an eight-byte prefix
of the response frame before closing the connection. The immediate and
linger-buffered idempotent producer paths both classify the truncated response
as ambiguous, reconnect, and replay the original Produce batch.

## Verification

```text
cargo test -p kafrust --test fault_injection \
  idempotent_producer_retries_partial_response_with_same_batch_sequence \
  -- --nocapture
1 passed; 0 failed

cargo test -p kafrust --test fault_injection \
  buffered_idempotent_producer_retries_partial_response_with_same_batch_sequence \
  -- --nocapture
1 passed; 0 failed

cargo check --workspace --all-targets
cargo test --workspace --all-features --quiet
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

Both tests observed a retry and one broker error. The replay Produce frame was
byte-identical to the original frame, retained the same producer identity and
batch sequence, and completed from the duplicate-sequence response without a
client-visible duplicate. The buffered delivery resolved and its worker closed
cleanly.

The pushed source commit passed the stable/Rust 1.81.0 matrix in
[CI run 33750844748](https://github.com/TaeeunKil/kafrust/actions/runs/33750844748).

## Boundary

This closes deterministic partial-response classification for immediate and
buffered sends. It does not prove a partial client request write, cancellation
or shutdown fault behavior, published ten-cycle profiles, 100,000-record
reconciliation, long campaigns, service canary qualification, or release
authorization.
