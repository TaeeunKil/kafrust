# Transaction mutation cancellation after transmission (2026-09-03)

## Scope

Source commit `391a0562af676abbf838eaa6435c85965322f6fc` adds one bounded
regression for each remaining direct transactional mutation request:

- `AddPartitionsToTxn` (flexible v3)
- `AddOffsetsToTxn` (flexible v3)
- `TxnOffsetCommit` (legacy v0 fixture)

Each scripted coordinator observes the request frame and withholds its response.
The caller then drops the in-flight future. A transaction mutation guard marks
the producer `Defunct`, clears the registered partition set, and rejects a new
transaction start. Normal completed responses retain the existing retry and
state transitions.

## Windows verification

Required workspace validation from the Windows checkout passed:

```text
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
478 passed; 0 failed (unit, integration, and doc tests)
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The three focused regressions each passed on the Windows stable toolchain.

## Company WSL2 reproduction

The same source was run from the company Windows host `DESKTOP-OTP568E` in
Ubuntu-T9 WSL2 (`x86_64`, Rust 1.81.0) using in-memory scripted fixtures:

```text
cancels_add_partitions_after_transmission_marks_producer_defunct
1 passed; 0 failed; 474 filtered out; finished in 0.14s

cancels_add_offsets_after_transmission_marks_producer_defunct
1 passed; 0 failed; 474 filtered out; finished in 0.15s

cancels_transaction_offset_commit_after_transmission_marks_producer_defunct
1 passed; 0 failed; 474 filtered out; finished in 0.15s

cargo test -p kafrust --test fault_injection -- --nocapture
29 passed; 0 failed; finished in 0.60s
```

No Docker resources or external Kafka broker were created or modified.

## Boundary

This evidence closes only direct cancellation after possible transmission for
the three named mutation APIs. It does not claim cancellation coverage during
discovery, published-artifact behavior, read-committed reconciliation,
multi-broker qualification, security compatibility, long campaigns, service
canaries, or release authorization. The long V1-18, V1-21, V1-22, and V1-23
gates remain open.
