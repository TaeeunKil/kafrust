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

3. Confirm docs.rs builds the published documentation for both crates.
4. Push an annotated release tag and create a GitHub release.
5. Run the `Live Kafka Smoke` workflow from GitHub Actions against `main`.
