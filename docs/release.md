# Release Preparation

kafrust publishes two crates:

- `kafrust-protocol`: Kafka wire-format primitives and request/response types
- `kafrust`: the user-facing async Kafka client

Publish `kafrust-protocol` before `kafrust` because the client crate depends on the protocol crate by version.

## Versioning

The public alpha line starts at `0.1.0`. Until the protocol and runtime behavior stabilize, keep public API additions small and document alpha limits in the affected API direction document.

Before publishing:

1. Update both crate versions together.
2. Update the `kafrust-protocol` dependency version in `crates/kafrust/Cargo.toml`.
3. Update roadmap status and any user-facing API direction document affected by the release.
4. Keep `Cargo.lock` out of the commit unless the repository policy changes.

Never reuse a version that already exists on crates.io. The client and protocol
crates must use the same new patch version, and the protocol crate must be
published first. A local workspace build can pass while an isolated client
package still resolves an older published protocol crate, so the ordered
registry checks below are part of the release gate.

## Release Notes

Every GitHub release should use a consistent structure so downstream users can
evaluate alpha risk without reading the full diff. Use `None` explicitly when a
section does not apply.

```md
## Summary

- What changed for users.

## Breaking changes

- Renamed, removed, or behavior-changing public APIs.
- Changed defaults, feature flags, environment variables, or broker assumptions.

## Migration notes

- Old API or behavior.
- Replacement API or behavior.
- Required caller changes.

## Compatibility evidence

- Broker versions and security profiles verified for this release.
- Published crate or fresh-project checks completed after release.

## Verification

- Local, CI, packaging, docs, and broker smoke checks used for the tag.

## Known limits

- Alpha limitations users should consider before adoption.
```

Patch releases should usually have `None` under `Breaking changes` and
`Migration notes`. Any `0.x` minor release with public API changes should call
out the affected types, methods, variants, or defaults and link to the relevant
API direction document or roadmap entry.

## Required Checks

Run the same checks used by CI:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo check -p kafrust --examples
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc -p kafrust --no-deps
```

Run the protocol packaging check before publishing:

```sh
cargo package -p kafrust-protocol --allow-dirty
```

Use `--allow-dirty` only for local verification of intentional uncommitted version edits. Do not publish from a dirty worktree.

The `kafrust` package cannot be fully prepared until the matching `kafrust-protocol` version is available from crates.io. After `kafrust-protocol` is published, run:

```sh
cargo package -p kafrust
```

## Local Package Qualification

Before claiming a release candidate, verify both packages from their staged
package directories with all features enabled. This approximates the docs.rs
build without claiming that the external docs.rs service has completed:

```sh
cargo package -p kafrust-protocol --allow-dirty
cargo package -p kafrust --allow-dirty
cargo doc --manifest-path target/package/kafrust-protocol-<version>/Cargo.toml --all-features --no-deps
cargo doc --manifest-path target/package/kafrust-<version>/Cargo.toml --all-features --no-deps
```

The `0.2.18` packages passed Cargo publish verification and both staged
all-feature package-documentation builds on commit `373de00`. The matching
`kafrust-protocol` and `kafrust` packages were published in that order; both
docs.rs pages returned HTTP 200, and a fresh external project compiled the
published `kafrust 0.2.18` crate with `tls` enabled. The main CI run
[`31661719918`](https://github.com/TaeeunKil/kafrust/actions/runs/31661719918)
and complete 17-job Live Kafka Smoke run
[`31661883116`](https://github.com/TaeeunKil/kafrust/actions/runs/31661883116)
passed on the release preparation commit. Update this paragraph after each
release with the current package and external verification results. None of
these checks replace broader workload qualification.

The `0.2.19` packages passed Cargo publish verification on release commit
`6a0c34c`; `kafrust-protocol` was published before `kafrust`. Both docs.rs
pages returned HTTP 200, and a fresh external project compiled the published
`kafrust 0.2.19` crate with `tls` on Rust 1.81. The complete 17-job Live Kafka
Smoke matrix passed on commit `1e5d5c6` in
[`31663188419`](https://github.com/TaeeunKil/kafrust/actions/runs/31663188419).

The `0.2.20` packages passed Cargo publish verification on release preparation
commit `5d028f1`; `kafrust-protocol` was published before `kafrust`. Both
docs.rs pages returned HTTP 200, and a fresh external project compiled the
published `kafrust 0.2.20` crate with `tls` on Rust 1.81. The complete 17-job
Live Kafka Smoke matrix passed the Admin retry change on commit `ec293d1` in
[`31665016772`](https://github.com/TaeeunKil/kafrust/actions/runs/31665016772).

The `0.2.21` packages passed Cargo publish verification on release preparation
commit `e2859d7`; `kafrust-protocol` was published before `kafrust`. The
published package metadata resolved with HTTP 200 from crates.io, both docs.rs
pages returned HTTP 200, and a fresh external project compiled
`kafrust 0.2.21` with the `tls` feature. The complete 17-job Live Kafka Smoke
matrix, including the new Kafka 3.7.2 three-broker eager sticky group gate,
passed in [`31666975512`](https://github.com/TaeeunKil/kafrust/actions/runs/31666975512).
The external smoke project used the current stable toolchain; the repository's
Rust 1.81 compatibility remains covered by the required CI job.

The `0.2.22` packages passed Cargo publish verification on release preparation
commit `af52ab9`; `kafrust-protocol` was published before `kafrust`. The
release contains sticky duplicate-claim invalidation and Kafka-compatible
mixed-topic candidate ordering. The complete 17-job Live Kafka Smoke matrix,
including the Kafka 3.7.2 three-broker sticky group path, passed in
[`31668518895`](https://github.com/TaeeunKil/kafrust/actions/runs/31668518895).
Both crates.io package endpoints and both published docs.rs pages returned HTTP
200, and a fresh external project compiled `kafrust 0.2.22` with `tls`.

The `0.2.23` packages passed Cargo package and publish verification on release
preparation commit `ee49471`; `kafrust-protocol` was published before
`kafrust`. The release adds classic AlterConfigs v1 through a typed
`TopicConfigUpdate` API and updates the admin lifecycle example to exercise
classic replacement followed by incremental alteration. The complete 17-job
Live Kafka Smoke matrix passed on commit `1085880` in
[`31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872),
qualifying the plaintext broker profiles and Kafka 3.7.2 multi-broker path.
Both crates.io package endpoints and both docs.rs pages returned HTTP 200, and
a fresh external project compiled published `kafrust 0.2.23` with `tls`.

The `0.2.24` packages passed Cargo package and publish verification on release
preparation commit `f64df2e`; `kafrust-protocol` was published before
`kafrust`. The release adds broker-scoped fetch-session reuse for rack-aware
Fetch v11/v12 requests, with focused session and invalid-epoch retry coverage.
The complete 17-job Live Kafka Smoke matrix passed on code commit `8615833` in
[`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977),
including the Kafka 3.7.2 three-broker rack-aware follow-up request. Both
crates.io package endpoints and docs.rs pages returned HTTP 200, and a fresh
external project compiled published `kafrust 0.2.24` with `tls`.

The `0.2.25` packages passed Cargo package and publish verification on release
preparation commit `f222d05`; `kafrust-protocol` was published before
`kafrust`. The release broadens Fetch v11/v12 negotiation and broker-scoped
fetch-session reuse to direct and group consumers without `client_rack`, while
retaining Fetch v4 fallback for older broker capability ranges. The complete
17-job Live Kafka Smoke matrix passed on the release commit in
[`31673377685`](https://github.com/TaeeunKil/kafrust/actions/runs/31673377685).
Both crates.io package endpoints and docs.rs pages returned HTTP 200, and a
fresh external project compiled published `kafrust 0.2.25` with `tls`.

The `0.2.26` packages passed Cargo package and publish verification on release
preparation commit `3f917c6`; `kafrust-protocol` was published before
`kafrust`. The release adds automatic direct-consumer leader-epoch truncation
recovery after a fenced or unknown leader epoch, with focused injected-broker
coverage. The complete 17-job Live
Kafka Smoke matrix passed on code commit `1694889` in
[`31677617186`](https://github.com/TaeeunKil/kafrust/actions/runs/31677617186).
Both crates.io package endpoints and both docs.rs pages returned HTTP 200. A
fresh external project compiled published `kafrust 0.2.26` with `tls` on Rust
1.81 MSVC.

The workflow-only follow-up gate in
[`31679167875`](https://github.com/TaeeunKil/kafrust/actions/runs/31679167875)
also passed. Its Kafka 3.7.2 three-broker profile stopped the second leader
after the initial assigned-consumer poll and verified automatic recovery after
the leader epoch changed from 1 to 2. This qualifies the live direct-consumer
leader-epoch failover path; group rebalance and data-loss/log-retention
scenarios remain outside the release claim.

The `0.2.27` packages passed protocol-first Cargo publish verification on
release preparation commit `d549a96`; `kafrust-protocol` was published before
`kafrust`. `cargo search` resolved both crates at `0.2.27`, both docs.rs pages
returned HTTP 200, and a fresh external project compiled published
`kafrust 0.2.27` with `tls`. The annotated `v0.2.27` tag points to the same
release preparation commit. A follow-up current-main `Live Kafka Smoke` run
[`31716400583`](https://github.com/TaeeunKil/kafrust/actions/runs/31716400583)
passed all 17 jobs, including classic Kafka 3.7.2 and Kafka 4.3.1 KIP-848
leader-epoch recovery over plaintext, SASL/PLAIN, and SASL_SSL/SCRAM. The
follow-up workflow and example fixes are on `main`; they do not change the
already-published `0.2.27` library artifacts.

The next current-main qualification run
[`31717934296`](https://github.com/TaeeunKil/kafrust/actions/runs/31717934296)
also passed all 17 jobs after adding the direct assigned-consumer retention
example. Its controlled `DeleteRecords` scenario moved the low watermark past
the consumer position and verified `OffsetResetPolicy::Earliest` recovery on
Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1. This is a main-branch 1.0 gate and does
not modify the already-published `0.2.27` artifacts.

The published runtime smoke
[`31719041843`](https://github.com/TaeeunKil/kafrust/actions/runs/31719041843)
then created a fresh project outside the repository, resolved `kafrust 0.2.27`
and its matching protocol crate from crates.io, and executed a producer to
direct-consumer roundtrip against Kafka 3.7.2. This verifies published runtime
linkage in addition to the earlier compile-only external smoke.

The follow-up published runtime smoke
[`31721075666`](https://github.com/TaeeunKil/kafrust/actions/runs/31721075666)
expanded that external project to call `AdminClient::describe_cluster`, an
idempotent producer, a direct consumer, and a classic consumer group against
Kafka 3.7.2. It resolved both `0.2.27` crates from crates.io and passed without
a workspace path dependency. This is runtime coverage for representative
published APIs, not a replacement for the multi-broker and security matrices.

The following current-main live run
[`31719615947`](https://github.com/TaeeunKil/kafrust/actions/runs/31719615947)
also passed the controlled classic consumer-group combined-fault gate in the
Kafka 3.7.2 three-broker profile. The target broker was both coordinator and
partition leader; after it was stopped, the replacement leader accepted a new
record and the group rejoined to consume it. This remains a main-branch gate
and does not modify the published `0.2.27` artifacts.

The subsequent complete current-main matrix
[`31723663771`](https://github.com/TaeeunKil/kafrust/actions/runs/31723663771)
passed all 17 jobs after adding the protocol-selectable combined-fault path.
It qualified the Kafka 4.3.1 plaintext KIP-848 case where the stopped broker
was both group coordinator and target partition leader, and verified rejoin
plus post-failover record consumption. The Kafka 3.7.2 classic group path also
passed its observable post-failover record check. Secured combined faults and
broader workload matrices remain outside the release claim.

The next complete current-main matrix
[`31725607371`](https://github.com/TaeeunKil/kafrust/actions/runs/31725607371)
passed all 17 jobs. It added the Kafka 3.7.2 `SASL_PLAINTEXT` classic combined
fault gate, selecting a broker that was both group coordinator and partition
leader, then verifying authenticated replacement-leader production and group
consumption after rejoin. It also removed an execution-order assumption from
the Kafka 4.3.1 SASL_SSL/SCRAM KIP-848 leader-epoch gate. These are current-main
qualification results and do not modify the published `0.2.27` artifacts;
the secured KIP-848 combined gate was qualified in the subsequent matrix below.

The following complete current-main matrix
[`31726636088`](https://github.com/TaeeunKil/kafrust/actions/runs/31726636088)
passed all 17 jobs and qualified the Kafka 4.3.1 KIP-848 combined coordinator
and partition-leader fault over `SASL_SSL` with SCRAM-SHA-256. It stopped the
selected broker, produced through the authenticated replacement leader, and
verified group rejoin plus post-failover consumption. This current-main gate
does not modify the published `0.2.27` artifacts; broader fault and transaction
matrices remain outside the release claim.

## Optional Broker Checks

The default test suite does not require a Kafka broker. Before an alpha tag, run the opt-in examples or tests against a local broker when practical:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 cargo test -p kafrust --test broker_roundtrip -- --nocapture
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example producer_send
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example consumer_fetch
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=kafrust-smoke KAFRUST_TOPIC=kafrust-smoke cargo run -p kafrust --example consumer_group_poll
```

## Publish Order

Dry-run first:

```sh
cargo publish -p kafrust-protocol --dry-run
cargo publish -p kafrust --dry-run # only after protocol is visible on crates.io
```

Publish after dry-runs pass:

```sh
cargo publish -p kafrust-protocol
cargo publish -p kafrust
```

After publishing `kafrust-protocol`, wait for the crates.io index to expose the
new version and confirm it resolves before publishing `kafrust`:

```sh
cargo info kafrust-protocol@<version>
cargo publish -p kafrust --dry-run
```

After publishing, tag the release with a Conventional Commit history summary and include known alpha limits from the roadmap.

## Post-publish Verification

After both crates are published:

1. Confirm crates.io resolves both packages:

   ```sh
   cargo search kafrust --limit 5
   ```

2. Confirm a fresh project can compile against the published client crate. Replace `<version>` with the version being verified:

   ```sh
   cargo new --bin /tmp/kafrust-published-smoke
   cargo add kafrust@<version> --manifest-path /tmp/kafrust-published-smoke/Cargo.toml
   cargo check --manifest-path /tmp/kafrust-published-smoke/Cargo.toml
   ```

3. Run the manually dispatched `Published Crate Smoke` workflow with the same
   version. It creates an external project under `$RUNNER_TEMP`, resolves the
   dependency from crates.io, and executes representative Admin, idempotent
   producer, direct-consumer, and classic consumer-group paths against Kafka
   3.7.2. This is stronger than a workspace compile because the smoke project
   has no path dependency on the repository.
4. Confirm docs.rs builds the published documentation for both crates.
5. Push an annotated release tag and create a GitHub release.
6. Run the `Live Kafka Smoke` workflow from GitHub Actions against `main`.
