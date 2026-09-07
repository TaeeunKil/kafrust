# Idempotent Batch Partial Produce Write Retry (2026-09-07)

## Scope

Source commit `2fb3e23` adds a scripted regression for the idempotent
`Producer::send_batch` path when the first leader connection writes only the
first three bytes of a Produce request and then breaks. The retry broker
observes Produce v3 with the original `producer_id=42`, `producer_epoch=3`,
and `base_sequence=0`; the batch succeeds at offset `0` and the next base
sequence becomes `1`.

This covers batch request-write interruption and verifies that retry state does
not allocate a new sequence. It is deterministic in-memory transport evidence,
not live Kafka or published-artifact evidence.

## Verification

- Focused test: `cargo test -p kafrust retries_idempotent_batch_after_partial_produce_write_with_same_sequence --all-features -- --nocapture`
- Workspace validation: `cargo fmt --all`, `cargo check --workspace --all-targets`, `cargo test --workspace --all-features` (513 client tests, 13 broker-roundtrip, 39 fault-injection, 285 protocol, 5 golden, 5 malformed, and 10 doctests), `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo doc --workspace --all-features --no-deps`, and `git diff --check`
- Ledger validation: `python scripts/check_qualification_ledger.py`

## Boundary

The result does not close published ten-cycle or 100,000-record
reconciliation, live broker/security, long-campaign, canary, or release gates.
