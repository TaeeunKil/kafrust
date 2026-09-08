# kafrust

[English](README.md) | [한국어](README.ko.md)

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)

A pure Rust Kafka client for applications that need Kafka protocol access
without a required `librdkafka` or C client dependency.

The current published line is **`0.3.6`**. It is pre-`1.0` and its public API
may change between minor releases. The working tree can contain fixes that are
not part of the published package; check the release and evidence documents
before treating a source checkout as a release artifact.

Use kafrust for experiments, local broker checks, internal tools, and API
evaluation. If a mature, broad production client is required today,
[`rust-rdkafka`](https://github.com/fede1024/rust-rdkafka) remains the practical
Rust default.

## Contents

- [What it is](#what-it-is)
- [Install](#install)
- [Quick start](#quick-start)
- [Supported surface](#supported-surface)
- [Compatibility boundary](#compatibility-boundary)
- [Current limits](#current-limits)
- [Documentation](#documentation)
- [Development](#development)
- [License](#license)

## What it is

kafrust is a Kafka client, not a broker or Kafka-compatible server. Its public
APIs keep Kafka concepts visible: bootstrap servers, topics, partitions,
offsets, acknowledgements, metadata, consumer groups, heartbeats, and commits.

The project is deliberately pure Rust at its client boundary. The default
build does not use `librdkafka`, C bindings, or a required C toolchain. The
optional `tls` feature uses `rustls` and may require native tooling for its
cryptography provider on some platforms.

## Install

```toml
[dependencies]
kafrust = "0.3.6"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Or:

```sh
cargo add kafrust@0.3.6
```

Requirements:

- Rust `1.81` or newer.
- A Tokio runtime for async APIs.
- A Kafka broker for runtime calls.
- The optional `blocking` feature for the owned-runtime synchronous adapters.

## Quick start

Set a bootstrap address and topic, then run the included producer example:

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

A minimal producer looks like this:

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-producer")
        .acks(Acks::Leader)
        .build()
        .await?;

    let metadata = producer
        .send(ProducerRecord::to("kafrust-smoke").value("hello from kafrust"))
        .await?;
    println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());
    Ok(())
}
```

For consumers, groups, transactions, buffering, Share, Streams, and Admin,
start with the [API documentation](docs/README.md#api-and-usage).

## Supported surface

The published client includes these main areas:

- low-level Kafka request roundtrips through `Client`;
- immediate, batch, buffered, idempotent, and transactional producers;
- direct consumers and classic/KIP-848 consumer groups;
- Share and Streams-related surfaces with their documented stability limits;
- typed Admin operations for topics, groups, configs, ACLs, quotas, SCRAM,
  reassignment, offsets, transactions, and controller paths;
- plaintext, TLS, SASL/PLAIN, SCRAM, and OAUTHBEARER connection paths;
- compression, bounded response/decode limits, metrics, and client telemetry.

Feature details and examples live in the topic documents. The README does not
repeat the complete API or every protocol version claim.

## Compatibility boundary

The documented qualification target is KRaft-based Apache Kafka with `3.7.2`
as the floor and newer pinned profiles including `3.8.1`, `3.9.1`, `4.0.0`,
and `4.3.1`. The exact broker, security, feature, and workload claims are
maintained in [Compatibility](docs/compatibility.md).

The current support boundary is intentionally narrower than universal
`rust-rdkafka` parity:

- KRaft is in scope; ZooKeeper and managed-service equivalence are unclaimed.
- Tokio async is the primary runtime; alternate-runtime support is not a goal.
- A passing workflow or source implementation is not published-artifact proof
  until the corresponding evidence row says so.
- Detailed live results and historical runs belong in
  [docs/evidence](docs/evidence/), not in this README.

## Current limits

- The public API is pre-`1.0` and may change between minor releases.
- Some advanced Admin, Share, Streams, security, and broker-internal paths are
  implemented but remain separately qualified or unstable.
- Long fault/soak and performance gates are distinct from short smoke tests.
- A named service canary, forward cutover, credential rotation, and rollback
  are required for the `1.0` program and are not implied by local tests.
- No universal production-readiness, managed-service, or drop-in
  `rust-rdkafka` claim is made.

See [the v1.0 milestone program](docs/milestones/v1.0/README.md) for the
remaining broad qualification gates. The practical 0.x sequence is in the
[pre-1.0 release track](docs/milestones/pre-1.0/README.md), and
[release preparation](docs/release.md) defines version policy.

## Documentation

Use [the documentation index](docs/README.md) to find the current guide by
topic.

Generated API documentation:

- [`kafrust`](https://docs.rs/kafrust/0.3.6/kafrust/)
- [`kafrust-protocol`](https://docs.rs/kafrust-protocol/0.3.6/kafrust_protocol/)

## Development

```sh
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

Read [Contributing](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) before making
changes. Keep the client pure Rust, preserve Kafka concepts in public APIs,
use Conventional Commits, and add focused tests for observable behavior.

## License

MIT OR Apache-2.0. See [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).
