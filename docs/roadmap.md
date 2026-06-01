# Roadmap

kafrust milestones are ordered by implementation risk and user-visible value. The project should keep Kafka concepts familiar to existing Kafka users while building a native Rust implementation underneath.

Status legend:

- Done: implemented and covered by CI.
- Implemented: code and examples exist, but live-broker verification is opt-in/manual.
- In progress: useful slices exist, but exit criteria are not fully met.
- Planned: not started.

## M0 Foundation

Status: Done.

Goal: make the repository ready for steady development.

Scope:

- Cargo workspace
- initial crate or module layout
- license
- Rust toolchain or MSRV policy
- CI for `cargo fmt`, `cargo clippy`, and `cargo test`

Exit criteria:

- the workspace builds
- formatting, linting, and tests can run locally and in CI
- future work has a clear crate/module home

Evidence:

- Cargo workspace with `kafrust` and `kafrust-protocol` crates.
- CI runs format, build, clippy, and tests on Rust 1.75.0 and stable.
- Main has stayed buildable through short-lived PRs.

## M1 Protocol Core

Status: Done for the currently implemented APIs; ongoing as new Kafka APIs are added.

Goal: encode and decode Kafka wire-format messages without needing a broker.

Scope:

- primitive wire types
- strings, nullable strings, bytes, and nullable bytes
- compact strings, compact bytes, compact arrays, and tagged fields
- request and response headers
- ApiVersions messages
- Metadata messages

Exit criteria:

- byte-level protocol tests cover the implemented primitives
- known request/response fixtures are checked where practical
- protocol code is separated from high-level client ergonomics

Evidence:

- Primitive codec, frame, request header, response header, ApiVersions, Metadata, Produce v2, and Fetch v2 live in `kafrust-protocol`.
- Protocol-focused unit tests cover byte-level encode/decode behavior.
- High-level client APIs depend on protocol types instead of mixing protocol parsing into user-facing builders.

## M2 Broker Roundtrip

Status: Implemented; live-broker verification is opt-in/manual.

Goal: prove kafrust can talk to a real Kafka broker.

Scope:

- TCP connection
- request and response framing
- correlation IDs
- client ID handling
- ApiVersions request/response
- Metadata request/response
- basic error decoding

Exit criteria:

- kafrust can connect to a local Kafka broker
- ApiVersions roundtrip succeeds
- Metadata roundtrip succeeds for at least one topic

Evidence:

- `Client` can connect over Tokio TCP, frame requests, increment correlation IDs, and decode response headers.
- `api_versions` and `metadata` roundtrip methods exist.
- `broker_roundtrip` example and opt-in integration test use `KAFRUST_BOOTSTRAP_SERVERS`.

Remaining verification:

- Run the opt-in broker test against a real Kafka broker in a local or CI service environment.

## M3 Producer MVP

Status: Implemented; live produce verification is opt-in/manual.

Goal: provide a familiar minimal producer for Kafka users.

Scope:

- `Producer::builder()`
- `bootstrap_servers`
- `client_id`
- topic, key, and value records
- Produce request
- `acks=1`
- metadata-based leader routing
- basic retry behavior

Exit criteria:

- an example can produce a record to a real topic
- producer API exposes Kafka concepts directly
- basic metadata refresh and retry behavior are documented

Evidence:

- `ProducerConfig`, `ProducerRecord`, `Acks`, and `RecordMetadata` are public.
- `Producer::send` does metadata lookup, leader routing, Produce v2 encoding, ProduceResponse v2 decoding, and broker error surfacing.
- Producer retries stale-metadata-style produce errors once after refreshing metadata.
- `producer_send` example and `docs/producer-api.md` document the current path.

Known limits:

- Current wire path uses legacy MessageSet v1, so record headers are rejected until RecordBatch encoding lands.
- `acks=0` is rejected because the current request loop expects a broker response.
- Produce-to-real-topic validation still requires running the example against Kafka.

## M4 Consumer MVP

Status: In progress.

Goal: provide a minimal consumer path before implementing full consumer groups.

Scope:

- Fetch request
- direct topic/partition assignment
- offset selection
- record batch decoding
- stream-like record consumption API

Exit criteria:

- an example can fetch records from a real topic partition
- offsets and partitions are visible to users
- record decoding is covered by focused tests

Evidence:

- Fetch v2 protocol request/response types exist.
- Legacy MessageSet records are decoded and covered by focused tests.
- `ConsumerConfig`, `Consumer`, and `ConsumerRecord` expose direct topic/partition/offset fetch.
- `Consumer::assign` and `Consumer::poll` provide a stream-like path with in-memory offset advancement.
- `consumer_fetch` example and `docs/consumer-api.md` document the current path.

Next work:

- Add RecordBatch decoding so the consumer can read modern Kafka record batches, not only legacy MessageSet records.
- Run producer and consumer examples against a real broker.

## M5 Consumer Group Alpha

Status: Planned.

Scope:

- FindCoordinator
- JoinGroup
- SyncGroup
- Heartbeat
- OffsetFetch
- OffsetCommit
- rebalance handling

## M6 Production Behavior

Status: Planned.

Scope:

- timeouts
- retry policy
- metadata refresh
- reconnects
- failover
- error classification
- tracing
- backpressure

## M7 Public Alpha

Status: Planned.

Scope:

- examples
- API docs
- integration tests
- crates.io release preparation
