# Controller Mutation Cancellation Boundary (2026-09-07)

## Scope

Source commit `8e0bc233d84170b14c680dc49e50fb46ded49713` adds the scripted
regression `cancels_create_topics_after_transmission_closes_controller_connection`.
The controller observes a complete CreateTopics v2 request, withholds its
response, and the caller drops the in-flight future. The controller-side
connection reaches EOF, proving the possibly-transmitted mutation connection is
not left available for reuse.

This is deterministic transport/lifecycle evidence for one controller-routed
mutation. It does not infer whether Kafka applied the mutation and does not
authorize replay.

## Verification

- Focused test: `cargo test -p kafrust cancels_create_topics_after_transmission_closes_controller_connection --all-features -- --nocapture`
- Workspace validation: `cargo fmt --all`, `cargo check --workspace --all-targets`, `cargo test --workspace --all-features` (514 client tests, 13 broker-roundtrip, 39 fault-injection, 285 protocol, 5 golden, 5 malformed, and 10 doctests), `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo doc --workspace --all-features --no-deps`, and `git diff --check`
- Ledger validation: `python scripts/check_qualification_ledger.py`
- CI: [run 34091602946](https://github.com/TaeeunKil/kafrust/actions/runs/34091602946), stable and Rust 1.81.0 jobs passed

## Boundary

The result does not close the complete controller operation cancellation
matrix, reconciliation, authorization, published failover, long-campaign, or
release gates.
