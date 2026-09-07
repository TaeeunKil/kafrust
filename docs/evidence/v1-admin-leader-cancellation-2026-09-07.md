# Leader Mutation Cancellation Boundary (2026-09-07)

## Scope

Source commit `49da8a6c8f4641d878abae09715a0e8abf18fb76` adds a deterministic
`DeleteRecords` cancellation regression for the leader-routed V1-12 mutation
path. The scripted metadata/leader socket observes the complete DeleteRecords
v1 request, withholds the response, and the caller drops the in-flight future.
The leader connection reaches EOF rather than remaining available to the admin
idle cache.

DeleteRecords remains state-idempotent only for a fixed topic/partition/offset;
this test proves connection lifecycle safety on cancellation and does not infer
whether a broker applied the deletion or authorize a replay.

## Verification

- Focused test: `admin::tests::cancels_delete_records_after_transmission_closes_leader_connection`
  passed.
- Workspace validation: `cargo fmt --all`, `cargo check --workspace
  --all-targets`, `cargo test --workspace --all-features` (528 library tests,
  13 broker-roundtrip, 39 fault-injection, 5 public-surface, 285 protocol,
  5 golden, 5 malformed, and 10 doctests), `cargo clippy --workspace
  --all-targets --all-features -- -D warnings`, `cargo doc --workspace
  --all-features --no-deps`, and `git diff --check` all passed.
- CI: [run 34101093954](https://github.com/TaeeunKil/kafrust/actions/runs/34101093954),
  stable and Rust 1.81.0 jobs passed.

## Boundary

The local leader-routed cancellation/no-reuse boundary is covered for
DeleteRecords. Three-broker owner movement, published failover profiles,
long-campaign, service-canary, reconciliation, and release gates remain open.
