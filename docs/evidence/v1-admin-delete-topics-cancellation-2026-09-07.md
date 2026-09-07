# DeleteTopics Controller Cancellation Boundary (2026-09-07)

## Scope

Source commit `3702d796609eaddde4c44ed586fb910b6f984850` adds the scripted
regression `cancels_delete_topics_after_transmission_closes_controller_connection`.
The controller observes a complete DeleteTopics v3 request, withholds its
response, and the caller drops the in-flight future. The controller-side
connection reaches EOF, proving that this possibly-transmitted mutation
connection is not left available for reuse.

This is deterministic transport/lifecycle evidence for the DeleteTopics
controller mutation. It does not infer whether Kafka applied the deletion and
does not authorize replay.

## Verification

- Focused test: `cargo test -p kafrust admin::tests::cancels_delete_topics_after_transmission_closes_controller_connection -- --exact --nocapture`
- Workspace validation: `cargo fmt --all`, `cargo check --workspace --all-targets`, `cargo test --workspace --all-features` (515 client tests, 13 broker-roundtrip, 39 fault-injection, 285 protocol, 5 golden, 5 malformed, and 10 doctests), `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo doc --workspace --all-features --no-deps`, and `git diff --check`
- CI: [run 34094828470](https://github.com/TaeeunKil/kafrust/actions/runs/34094828470), stable and Rust 1.81.0 jobs passed

## Boundary

The result does not close the complete controller operation cancellation
matrix, reconciliation, authorization, published failover, long-campaign, or
release gates.
