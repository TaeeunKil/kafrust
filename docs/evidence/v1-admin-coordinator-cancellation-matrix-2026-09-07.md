# Coordinator Mutation Cancellation Matrix (2026-09-07)

## Scope

Source commit `536b339afd2b6027570cd8f19f15238896f1c0d5` adds a shared
scripted-coordinator cancellation fixture for the remaining coordinator-routed
mutation wrappers in V1-12:

| Operation | API version |
| --- | ---: |
| OffsetCommit (classic) | v2 |
| OffsetCommit (member-aware fallback) | v9 |
| OffsetCommit (member-aware topic UUID) | v10 |
| OffsetDelete | v0 |
| DeleteGroups | v1 |

For each operation the scripted bootstrap socket answers FindCoordinator, the
coordinator observes the complete request, and the caller drops the in-flight
future while the response is withheld. The coordinator-side connection then
reaches EOF, proving that a possibly-transmitted mutation connection is not
left available for reuse. The member-aware cases also verify the negotiated
ApiVersions path and preserve the member/epoch or topic-UUID wire selection.

This is deterministic transport/lifecycle evidence. It does not infer whether
Kafka applied a mutation and does not authorize replay or automatic inverse
operations.

## Verification

- Focused tests: five new coordinator cancellation tests; fourteen controller
  and coordinator cancellation tests pass together.
- Workspace validation: `cargo fmt --all`, `cargo check --workspace
  --all-targets`, `cargo test --workspace --all-features`, `cargo clippy
  --workspace --all-targets --all-features -- -D warnings`, `cargo doc
  --workspace --all-features --no-deps`, and `git diff --check` all passed.
- CI: [run 34098931228](https://github.com/TaeeunKil/kafrust/actions/runs/34098931228),
  stable and Rust 1.81.0 jobs passed.

## Boundary

The deterministic coordinator cancellation/no-reuse slice is covered for
classic, member-aware v9, member-aware v10, OffsetDelete, and DeleteGroups.
Published authorization/owner-movement profiles, reconciliation evidence,
long-campaign, service-canary, and release gates remain separate and open.
