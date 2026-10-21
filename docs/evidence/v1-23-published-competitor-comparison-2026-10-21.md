# Competitor comparison for the `0.5.0` security release

- date: 2026-10-21 Asia/Seoul
- candidate: coordinated `kafrust 0.5.0` and `kafrust-protocol 0.5.0`
- current published baseline before publication: `0.4.0`
- registry versions: queried from the crates.io API on the date above
- decision: proceed with the user-requested, scoped security release after the
  protocol-first registry checks; preserve the existing production and feature
  breadth non-claims

| Project | Current published version | Relevance to this release |
| --- | --- | --- |
| [`krafka`](https://crates.io/crates/krafka) | `0.27.1` | Broader pure-Rust async Kafka client. Its feature breadth remains ahead; this security-only release does not claim parity. |
| [`kacrab`](https://crates.io/crates/kacrab) | `0.4.0` | Broad client/Admin comparison. Its broker matrix and dependencies are separate from kafrust's pure-Rust dependency boundary. |
| [`kafkit-client`](https://crates.io/crates/kafkit-client) | `0.1.9` | Modern Kafka 4.0+ native async client with a narrower broker floor. |
| [`kafka_client`](https://crates.io/crates/kafka_client) | `0.8.0` | Pure-Rust Tokio client with producer, consumer, group, Admin, TLS, and SASL APIs. Registry metadata does not establish kafrust's tested broker matrix. |
| [`rdkafka`](https://crates.io/crates/rdkafka) | `0.39.0` | Mature librdkafka wrapper and practical production baseline, outside kafrust's no-required-C-toolchain boundary. |

## Decision and non-claims

The release addresses a user-visible security defect in kafrust's own
Zstandard record decoder and packages the fix for downstream users. The
comparison shows no reason to widen the release claim: it does not close
feature breadth, production workload, managed-service, fault campaign, or
performance gaps against the listed projects. Keep the existing alpha status
and compatibility evidence limits unchanged.

The Rust 1.87 minimum is a source compatibility change from `0.4.0`; the
coordinated `0.5.0` minor version signals that change in this pre-1.0 project.
This release must publish `kafrust-protocol` first, verify it from a fresh
external project, then publish and verify `kafrust`. The registry baseline and
published workflow defaults stay at `0.4.0` until both artifacts are visible
and fresh external resolution passes.
