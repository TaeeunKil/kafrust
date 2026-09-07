# Common Controller Mutation Cancellation Matrix (2026-09-07)

## Scope

Source commit `146f98d646cd527a413b081bc1da8d89f8565a69` adds a shared
scripted-controller cancellation fixture covering every common controller
mutation owned by V1-11:

| Operation | API version |
| --- | ---: |
| CreateTopics | v2 |
| DeleteTopics | v3 |
| CreatePartitions | v0 |
| ElectLeaders | v2 |
| AlterPartitionReassignments | v0 |
| UpdateFeatures | v1 |
| AddRaftVoter | v1 |
| RemoveRaftVoter | v0 |
| UnregisterBroker | v0 |

For each operation the scripted controller observes the complete request,
withholds its response, and the caller drops the in-flight future. The
controller-side connection reaches EOF, proving that a possibly-transmitted
mutation connection is not left available for reuse. API-version negotiation
is included for the operations that require it.

This is deterministic transport/lifecycle evidence. It does not infer whether
Kafka applied any mutation and does not authorize replay or automatic inverse
operations.

## Verification

- Focused tests: nine `admin::tests::cancels_*_after_transmission_closes_controller_connection` tests
- Workspace validation: `cargo fmt --all`, `cargo check --workspace --all-targets`, `cargo test --workspace --all-features` (522 client tests, 13 broker-roundtrip, 39 fault-injection, 285 protocol, 5 golden, 5 malformed, and 10 doctests), `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo doc --workspace --all-features --no-deps`, and `git diff --check`
- CI: [run 34096780922](https://github.com/TaeeunKil/kafrust/actions/runs/34096780922), stable and Rust 1.81.0 jobs passed

## Boundary

The common V1-11 deterministic cancellation matrix is covered. Published
authorization/failover profiles, per-operation reconciliation evidence,
long-campaign, service-canary, and release gates remain separate and open.
