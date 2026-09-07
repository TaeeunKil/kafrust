# Direct consumer partition-queue cancellation (2026-09-07)

## Scope

Source commit `90e1ad3f968fd3c126accf641d382f63a50fda77` adds a deterministic
scripted-broker regression for a split direct-consumer partition queue whose
receiver is dropped before the next poll. The consumer detects the closed
bounded queue, removes only that queue route, returns the fetched record through
the normal `poll()` result, and advances the assignment position without
skipping the record.

## Verification

The focused test is
`consumer::tests::split_partition_queue_falls_back_to_poll_when_receiver_is_dropped`.
It passed on Windows with the complete required Rust validation:

```text
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features  # 510 + 13 + 38 + 5 + 285 + 5 + 5 tests; all passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The test uses only an in-memory scripted TCP fixture. No Docker resource or
external Kafka broker was created or modified.

## Boundary

This is local deterministic queue-cancellation evidence only. It does not claim
live broker retention or leader-movement recovery, published-artifact behavior,
100,000-record reconciliation, long campaigns, service canaries, or release
authorization.
