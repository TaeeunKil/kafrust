# V1 Data-Plane Response Golden Evidence — 2026-09-03

## Scope

This record captures the deterministic response-fixture increment for V1-03.
The source commit `fd3718484ce84cc37d4b8ebf1b3267a4e404e1b5` adds fixed,
non-empty response bytes to
[`crates/kafrust-protocol/tests/data_plane_golden.rs`](../../crates/kafrust-protocol/tests/data_plane_golden.rs)
and asserts decoded Kafka fields for the selected response versions:

- Produce v2, v7, v9, v11, v12, and v13;
- Fetch v4, v11, v12, and v13;
- Metadata v1 and v12;
- ListOffsets v1;
- OffsetForLeaderEpoch v3; and
- ApiVersions v0, v3, and v4.

The fixtures exercise non-empty topics, partitions, offsets, record errors,
aborted transactions, topic UUIDs, compact collections, and flexible tagged
fields. The test consumes each fixed body through the protocol decoder rather
than constructing response structs directly.

## Verification

Focused execution:

```text
cargo test -p kafrust-protocol --test data_plane_golden -- --nocapture
5 passed; 0 failed
cargo test -p kafrust-protocol --test data_plane_malformed -- --nocapture
3 passed; 0 failed
```

Required repository validation on the same source commit also passed:

```text
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
470 kafrust tests passed
284 kafrust-protocol tests passed
5 data-plane golden tests passed
3 data-plane malformed tests passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The pushed commit is covered by both CI jobs in
[CI run 33735496212](https://github.com/TaeeunKil/kafrust/actions/runs/33735496212).
This evidence entry is recorded after the run reaches a successful conclusion.

## Boundary

This closes the deterministic non-empty response-fixture slice only. It does
not claim official Apache compatibility for every broker response, complete
malformed length/trailing-byte coverage, accepted-floor or pinned-broker live
version qualification, three-broker leader movement, long campaigns, a service
canary, V1-03 completion, or release authorization.
