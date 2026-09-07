# Transactional Produce cancellation after transmission (2026-09-07)

## Scope

Source commit `22e110a6faf4a6d46b7577604e1f23195af4a0cc` closes the direct
transactional Produce cancellation boundary for both high-level send forms:

- `Producer::send`
- `Producer::send_batch`

Each deterministic scripted broker observes the Produce v3 request and then
withholds its response. Dropping the in-flight future now marks the
transaction `Defunct`, clears the active transaction state, and rejects a new
transaction start. A canceled transactional Produce is therefore not treated
as a known commit or as a reusable idempotent sequence.

The implementation reuses the existing transaction mutation guard around all
Produce request variants. The guard is armed only while the request future is
pending; completed retryable responses retain the existing retry path, while a
future drop performs the terminal transition. A fatal broker response keeps
its existing typed broker error precedence.

## Deterministic verification

```text
cargo test -p kafrust --lib cancels_transactional_ -- --nocapture
2 passed; 0 failed

cargo test --workspace --all-features --tests
507 library tests, 13 broker-roundtrip tests, 38 fault-injection tests,
5 public-surface tests, 285 protocol tests, 5 golden tests, and 5 malformed
tests passed; 0 failed
```

The focused tests assert the observed Produce API key/version, terminal
`TransactionStatus::Defunct`, `in_transaction() == false`, and
`TransactionProducerDefunct` on reuse. Existing fatal EndTxn behavior was
rerun and continues to return its broker error rather than being masked by the
new cancellation state check.

The required local checks `cargo fmt --all`,
`cargo check --workspace --all-targets`,
`cargo clippy --workspace --all-targets --all-features -- -D warnings`,
`cargo doc --workspace --all-features --no-deps`, and `git diff --check`
passed. The exact `cargo test --workspace --all-features` command was also
attempted; this Windows checkout could not link the unrelated
`consumer_epoch_failover` example because the installed Visual Studio toolset
does not provide `msvcrt.lib`. The library/integration/test-target command
above passed, and the pushed CI matrix remains the authoritative full-workspace
test gate.

## Boundary

This record closes only deterministic post-transmission cancellation for
transactional Produce. It does not claim read-committed reconciliation,
DescribeTransactions outcome resolution, published artifact behavior,
accepted-floor/current-broker qualification, security compatibility,
multi-broker movement, long campaigns, service canaries, or release
authorization. An unknown transaction outcome remains intentionally unknown;
the producer must be discarded.
