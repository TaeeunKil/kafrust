# Roadmap

kafrust milestones are ordered by implementation risk and user-visible value. The project should keep Kafka concepts familiar to existing Kafka users while building a native Rust implementation underneath.

Status legend:

- Done: implemented and covered by CI.
- Implemented: code and examples exist, with live-broker verification outside default PR CI.
- Published: released on crates.io with release artifacts and post-release checks.
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

Status: Implemented; live-broker verification is opt-in and scheduled.

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
- The `Live Kafka Smoke` workflow has passed the broker roundtrip test against Kafka 3.7.2.

Ongoing verification:

- Keep the scheduled/manual `Live Kafka Smoke` workflow passing before release tags.

## M3 Producer MVP

Status: Implemented; live produce verification is opt-in and scheduled.

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
- The `Live Kafka Smoke` workflow has produced records to Kafka 3.7.2.

Known limits:

- Current high-level producer path negotiates Produce API support, uses v3 RecordBatch for headers, and falls back to v2 MessageSet only for records without headers.
- `acks=0` is rejected because the current request loop expects a broker response.
- Live produce validation runs through the scheduled/manual `Live Kafka Smoke` workflow.

## M4 Consumer MVP

Status: Implemented; live fetch verification is opt-in and scheduled.

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
- Legacy MessageSet and RecordBatch v2 records are decoded and covered by focused tests.
- `ConsumerConfig`, `Consumer`, and `ConsumerRecord` expose direct topic/partition/offset fetch.
- `Consumer::assign` and `Consumer::poll` provide a stream-like path with in-memory offset advancement.
- `consumer_fetch` example and `docs/consumer-api.md` document the current path.
- The `Live Kafka Smoke` workflow has fetched records from Kafka 3.7.2.

Next work:

- Extend live fetch checks across more record shapes and broker versions.

## M5 Consumer Group Alpha

Status: Implemented; live group verification is opt-in and scheduled.

Scope:

- FindCoordinator (implemented as protocol + client roundtrip)
- JoinGroup (implemented as protocol + client roundtrip)
- SyncGroup (implemented as protocol + client roundtrip)
- Heartbeat (implemented as protocol + client roundtrip)
- classic consumer protocol subscription/assignment v0 payloads
- internal range assignment for classic rebalance leaders
- OffsetFetch (implemented as protocol + client roundtrip)
- OffsetCommit (implemented as protocol + client roundtrip)
- ConsumerGroup alpha API with join, sync, heartbeat, background heartbeat, poll, rejoin, and commit
- rebalance handling (poll-triggered rejoin for coordinator, generation, member, and rebalance heartbeat errors)

Known limits:

- Rebalance handling is poll-triggered, not background-driven.
- Background heartbeats are opt-in and surface group errors through `ConsumerGroupHeartbeat::try_wait` or `ConsumerGroupHeartbeat::stop`; they can trigger poll-time rejoin through `ConsumerGroup::poll_with_heartbeat`, and stale same-group heartbeat handles are stopped before polling, but background tasks are not restarted automatically yet.
- Live group validation runs through the scheduled/manual `Live Kafka Smoke` workflow.

## M6 Production Behavior

Status: Implemented; deeper resilience behavior remains iterative.

Scope:

- request timeouts (implemented through `ClientConfig::request_timeout_ms`)
- producer retry policy (implemented through `ProducerConfig::max_retries`)
- producer metadata cache and refresh on retriable send failures
- producer reconnect on retriable send failures
- consumer fetch retry and reconnect on transient failures
- bootstrap failover (implemented by trying configured bootstrap servers in order)
- error classification (initial `BrokerErrorKind` mapping implemented)
- request and operation tracing (implemented with `tracing` events for request/response, producer, direct consumer, and group metadata)
- poll backpressure (implemented through `ConsumerConfig::max_poll_records`)

Known limits:

- Reconnects happen through operation retries, not long-lived connection recovery.
- Metadata caching currently exists on the producer and direct consumer paths.
- Tracing emits request lifecycle and high-level operation metadata, but does not yet use structured spans across complete workflows.
- Backpressure is limited to per-poll record count, not socket or memory pressure.

## M7 Public Alpha

Status: Published.

Scope:

- examples (implemented for broker roundtrip, producer send, direct consumer fetch, coordinator discovery, and group poll)
- API docs (implemented for the public `kafrust` API and enforced with `missing_docs`)
- integration tests (implemented as opt-in broker roundtrip tests)
- crates.io release preparation and publish flow

Evidence:

- `kafrust-protocol v0.1.0` and `kafrust v0.1.0` are published on crates.io.
- GitHub release `v0.1.0` is tagged and published.
- A fresh external project can add `kafrust = "0.1.0"` and compile.
- docs.rs pages for both crates build successfully.
- The `Live Kafka Smoke` workflow runs the broker roundtrip, producer, direct consumer, and consumer group examples against Kafka 3.7.2.

Known limits:

- Live broker checks are opt-in and scheduled, not part of default pull request CI.
- Published `0.1.x` APIs remain alpha APIs and may change while Kafka protocol coverage and runtime behavior stabilize.

## M8 Alpha Operations

Status: Done.

Goal: make the alpha reliable to operate during development and small experiments.

Scope:

- scheduled live Kafka smoke checks
- docs.rs and published-crate install smoke
- release checklist updates after each publish
- issue templates or labels for protocol bugs, runtime bugs, and API design questions
- documented compatibility notes for tested Kafka broker versions

Exit criteria:

- live smoke runs on a schedule and can be run manually before release tags
- release docs include post-publish verification, not only pre-publish commands
- known Kafka broker compatibility is visible in docs
- reported failures can be triaged into protocol, client runtime, or API surface areas

Evidence:

- `Live Kafka Smoke` exists and has passed manually against Kafka 3.7.2.
- `docs/broker-roundtrip.md` records the latest manual live smoke and the scheduled workflow.
- v0.1.0 was verified from a fresh external project with `kafrust = "0.1.0"`.
- `docs/release.md` includes post-publish crates.io, docs.rs, release tag, and live smoke verification.
- `docs/compatibility.md` documents the current Kafka 3.7.2 compatibility claim and known non-claims.
- GitHub issue forms route reports into protocol bugs, client runtime bugs, or API design questions.

Known limits:

- Compatibility has been verified against Kafka 3.7.2 only.
- Issue forms provide triage structure, but repository labels are not required yet.

## M9 Consumer Group Resilience

Status: Done.

Goal: make the consumer group alpha behavior safer under normal Kafka rebalances and coordinator changes.

Scope:

- background heartbeat error observation and recovery strategy
- automatic rejoin coordination between foreground poll and background heartbeat
- clearer member generation state transitions
- offset commit behavior during rejoin and stale generations
- focused tests for coordinator, generation, member, and rebalance error paths

Exit criteria:

- background heartbeat failures can trigger a controlled rejoin path or a clearly documented terminal state
- foreground `poll` and background heartbeat do not race over stale generation or member IDs
- offset commits fail predictably or recover after rejoin, with visible Kafka context
- docs describe when users should spawn background heartbeats and how failures are surfaced

Evidence:

- `ConsumerGroup::poll_with_heartbeat` observes background heartbeat task completion before polling and uses the existing rejoin path for rejoinable group errors.
- Focused unit tests cover running tasks, rejoinable background heartbeat errors, and non-rejoinable background heartbeat errors.
- `ConsumerGroupHeartbeat` records the group ID, member ID, and generation ID it was spawned for, and stale same-group handles are stopped before polling to avoid sending heartbeats for an older generation.
- `ConsumerGroup::commit_offsets` rejoins after rejoinable offset commit errors and returns the original commit error instead of retrying stale assignment offsets under a new generation.
- `docs/consumer-groups.md` describes when to spawn background heartbeats, how heartbeat failures are surfaced, and how offset commit rejoin behavior works.

Known limits:

- Background heartbeats can trigger a rejoin when users call `ConsumerGroup::poll_with_heartbeat` with the heartbeat task handle, but the client does not restart a new background heartbeat task automatically after rejoin.
- Range assignment is the only high-level group assignment strategy.

## M10 Producer Throughput

Status: In progress.

Goal: move from single-record send ergonomics toward practical producer throughput while keeping Kafka concepts visible.

Scope:

- multi-record produce requests
- per-topic and per-partition batching
- configurable linger and batch size
- retry behavior for partial partition failures
- clearer delivery metadata for batched sends

Exit criteria:

- users can send batches without manually building protocol structures
- batching preserves topic, partition, key, value, headers, acks, and offset metadata
- partial failures are surfaced per topic partition
- live smoke covers at least one multi-record produce and fetch roundtrip

Known limits:

- The high-level producer can send multiple records with `Producer::send_batch`, grouping records by topic, partition, and leader. Linger-based buffering can enqueue records and flush or close through the existing batch path, but automatic linger, record-count, and byte-count flush triggers are not implemented yet.
- `acks=0` remains unsupported because the request loop expects a broker response.

Evidence:

- `Producer::send_batch` accepts multiple records, batches same topic-partition groups into one Produce request, and returns metadata in input order.
- `Producer::send_batch_report` surfaces per-record success and failure outcomes in input order, including broker Produce response errors for failed topic partitions.
- Batch retry recovery keeps successful records fixed and retries only input records whose topic partition returned a retryable Produce response error.
- `ProducerConfig::max_records_per_batch` splits large topic-partition groups across multiple Produce requests without changing input-order outcomes.
- `ProducerConfig::max_batch_bytes` splits large topic-partition groups by encoded Kafka record-set bytes without preventing an oversized single record from being sent.
- Focused unit tests cover batch Produce API version selection and batch metadata cache invalidation.
- The `Live Kafka Smoke` workflow runs the `producer_send_batch` example before direct fetch and group poll checks.
- Manual `Live Kafka Smoke` run `26989271377` passed on 2026-06-05 after the batch outcome, partial retry, and record-limit changes.
- `docs/producer-buffering.md` defines the planned opt-in buffered producer path, linger flush triggers, delivery semantics, and implementation slices.
- `ProducerConfig::linger_ms` and `ProducerConfig::build_buffered` provide the first buffered producer lifecycle skeleton with `flush`, `close`, and `is_closed`.
- `BufferedProducer::send` queues records through a bounded channel and returns per-record `ProducerDelivery` handles; `flush` and `close` send pending records through `send_batch_report` and complete delivery handles from per-record outcomes.
- Focused unit tests cover buffered enqueue, delivery cancellation, pending delivery failure, per-record delivery completion, and defensive handling for missing batch outcomes.

## M11 Security And Connectivity

Status: Planned.

Goal: support common secured Kafka deployments without adding librdkafka or C bindings.

Scope:

- TLS transport using a Rust TLS stack
- SASL PLAIN and SCRAM evaluation
- client configuration for security protocol and authentication material
- secure error messages that do not leak secrets
- docs for local plaintext, TLS, and SASL broker profiles

Exit criteria:

- plaintext behavior remains the default and stays simple
- TLS connections can complete ApiVersions and Metadata roundtrips
- at least one SASL mechanism can authenticate against a broker in live smoke or documented manual checks
- credentials are kept out of tracing events and error displays

Known limits:

- Current networking is plaintext TCP only.
- No SASL mechanisms are implemented yet.

## M12 API Stabilization

Status: Planned.

Goal: prepare a stable pre-1.0 API shape with clear compatibility rules for downstream users.

Scope:

- audit public types for Kafka terminology, naming, and minimality
- decide which protocol types remain public re-exports
- builder validation and explicit error variants for common configuration failures
- docs examples that compile from published crates
- semver policy for `0.x` releases and migration notes

Exit criteria:

- public APIs have documented intended stability levels
- examples cover producer, direct consumer, and consumer group happy paths from published crates
- release notes call out breaking changes and migration steps
- downstream users can evaluate whether kafrust is suitable for experiments, staging, or production-like tests

Known limits:

- The project is still pre-1.0 and can make breaking changes between minor versions.
- Protocol coverage is intentionally incomplete and grows API by API.
