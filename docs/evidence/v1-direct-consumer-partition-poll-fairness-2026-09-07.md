# Direct consumer partition-poll fairness (2026-09-07)

## Scope

Source commit `3e526787f5905c000279c6d94df705c5d494cb3c` adds a deterministic
scripted-broker regression for a direct consumer with two assigned partitions
and `max_poll_records=1`. The consumer now carries a bounded round-robin start
cursor: after a poll reaches its record budget, the next poll starts at the
next assignment instead of repeatedly selecting the first partition. Assignment
replacement and explicit assignment keep the cursor within the current
assignment set.

## Verification

The focused test is
`consumer::tests::poll_rotates_partitions_when_max_poll_records_is_one`.
It passed together with the required Rust validation:

```text
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features  # 511 + 13 + 39 + 5 + 285 + 5 + 5 + 8 + 2 passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The fixture uses two in-memory scripted TCP brokers, one per partition. The
first poll returns partition 0 and the next poll returns partition 1 without
changing the configured poll budget. No Docker resource or external Kafka
broker was created or modified.

## Boundary

This is local deterministic fairness evidence only. It does not claim live
multi-partition throughput, retention or leader-movement recovery,
published-artifact behavior, 100,000-record reconciliation, long campaigns,
service canaries, or release authorization.
