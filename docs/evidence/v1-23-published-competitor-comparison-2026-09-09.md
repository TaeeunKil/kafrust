# Competitor comparison for the `0.4.0` scoped release

- date_utc: 2026-09-09
- candidate: `kafrust 0.4.0` and `kafrust-protocol 0.4.0`
- current_published_baseline: `0.3.6`
- decision: proceed with the scoped `0.4.0` pre-1.0 boundary after its
  protocol-first registry checks; do not make a broad production or `1.0.0`
  claim
- source: `628d130` candidate preparation; the bounded workload evidence keeps
  its own run source identities in
  [`v1-local-bounded-followup-2026-09-08.md`](v1-local-bounded-followup-2026-09-08.md)

## Registry snapshot

The versions below were read from the crates.io sparse index with `cargo search`
on the date above. Feature descriptions are adoption context, not independent
qualification of those projects.

| Project | Published line | Relative position for this release |
| --- | --- | --- |
| [`krafka`](https://crates.io/crates/krafka) | `0.22.0` | Broader pure-Rust async client and test infrastructure remain ahead in breadth. Its current public source/docs describe newer protocol and operational coverage that this release does not claim to match. |
| [`kacrab`](https://crates.io/crates/kacrab) | `0.4.0` | Broad producer, consumer, and Admin surface remains a feature-breadth comparison. Its dependency choices and broker matrix are separate from kafrust's no-required-C-toolchain boundary. |
| [`kafkit-client`](https://crates.io/crates/kafkit-client) | `0.1.9` | Modern Kafka 4.0+ native async client with Share and transaction paths; it intentionally has a narrower broker floor and does not replace kafrust's classic/older-broker target. |
| [`kafka_client`](https://crates.io/crates/kafka_client) | `0.8.0` | Pure-Rust Tokio client with producer, consumer, group, Admin, TLS, and SASL APIs; its public registry page does not establish kafrust's tested broker matrix. |
| [`rdkafka`](https://crates.io/crates/rdkafka) | `0.39.0` | Mature librdkafka wrapper and the practical production baseline; it is outside kafrust's pure-Rust replacement boundary. |

## Decision and non-claims

The `0.4.0` candidate closes an independently consumable boundary in kafrust:
the post-`0.3.6` protocol/runtime hardening, complete decoder checks, and the
declared six-hour secured plus twelve-hour plaintext workstation soak are
packaged together with coordinated protocol/client versions. The retained
soak has zero loss, duplicates, and unknown outcomes, but it is a low-rate
single-workstation diagnostic.

The comparison still shows material breadth and workload-specific throughput
gaps against the mature competitors. We preserve those as explicit non-claims
and re-plan them under the later `0.6` recovery and `0.7` baseline milestones:

- this release does not claim universal feature parity with `krafka`, `kacrab`,
  or `rdkafka`;
- the bounded workload is not V1-21 high-load fault evidence or V1-22 SLO
  evidence;
- no managed-service, service-canary, API-freeze, or `1.0.0` claim follows;
- published smoke defaults remain on `0.3.6` until the new pair is visible and
  fresh external resolution passes.

This record is the dated competitor refresh required by the release policy. It
supports a scoped pre-1.0 publication; it does not promote the project above
the documented competitor gaps.
