# Transaction EndTxn cancellation (2026-09-03)

## Scope

At source commit `34bbd443b835cd80056d330b21aa44ddc06ff6e0`, the producer marks
an active transaction as `Defunct` immediately before sending EndTxn. If the
caller cancels commit/abort while that mutation's response is pending, the
transaction cannot be reused with an unknown outcome. Known non-terminal broker
errors restore the active state before a permitted retry or return.

## Verification

```text
cargo test -p kafrust --lib producer::tests::cancels_end_transaction_after_transmission_marks_producer_defunct -- --exact --nocapture
1 passed; 0 failed; 471 filtered out; finished in 0.07s

cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
475 passed (kafrust unit tests); 29 passed (fault_injection); all workspace
integration tests and doctests passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The scripted coordinator observed ApiVersions v3 and an EndTxn v3 commit,
then withheld the response. The test canceled the commit future only after the
EndTxn frame was observed; the producer reported `TransactionStatus::Defunct`,
`in_transaction() == false`, and rejected a new begin with
`TransactionProducerDefunct`.

## Boundary

This closes direct producer EndTxn cancellation after transmission. AddOffsets,
AddPartitions, TxnOffsetCommit cancellation, published read-committed
reconciliation, long campaigns, multi-broker security profiles, service
canary qualification, and release authorization remain open.
