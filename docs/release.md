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

The previous `0.2.9` package contents passed both package verification and
both all-feature package-documentation builds on commit `e8b803e`. The matching
`kafrust-protocol` and `kafrust` packages were published in that order; both
docs.rs pages returned HTTP 200 and an external project compiled the published
`kafrust 0.2.9` crate with all features. Update this paragraph after each
release with the current package and external verification results. None of
these checks replace live broker qualification.

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
cargo publish -p kafrust --dry-run
```

Publish after dry-runs pass:

```sh
cargo publish -p kafrust-protocol
cargo publish -p kafrust
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
