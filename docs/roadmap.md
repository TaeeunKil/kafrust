# Roadmap

kafrust milestones are ordered by implementation risk and user-visible value. The project should keep Kafka concepts familiar to existing Kafka users while building a native Rust implementation underneath.

See [Project Strategy](project-strategy.md) for the replacement target, non-goals, existing alternatives, completion tiers, and the rationale for building a pure Rust client instead of wrapping librdkafka.

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
- CI runs format, build, clippy, and tests on Rust 1.81.0 and stable.
- The MSRV moved from Rust 1.75 to 1.81 when bidirectional pure-Rust Zstd
  support required language features stabilized in Rust 1.81.
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
- Background heartbeats are opt-in and surface group errors through
  `ConsumerGroupHeartbeat::try_wait` or `ConsumerGroupHeartbeat::stop`;
  `poll_with_heartbeat` triggers poll-time rejoin and replaces completed or
  stale same-group heartbeat tasks for the current generation.
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

- `kafrust-protocol v0.1.0`, `kafrust v0.1.0`, `kafrust-protocol v0.2.0`, `kafrust v0.2.0`, `kafrust-protocol v0.2.1`, and `kafrust v0.2.1` are published on crates.io.
- GitHub releases `v0.1.0`, `v0.2.0`, and `v0.2.1` are tagged and published.
- A fresh external project can add `kafrust = "0.2.1"` and compile.
- docs.rs pages for both crates build successfully.
- The `Live Kafka Smoke` workflow runs the broker roundtrip, producer, direct consumer, and consumer group examples against Kafka 3.7.2.

Known limits:

- Live broker checks are opt-in and scheduled, not part of default pull request CI.
- Published `0.x` APIs remain alpha APIs and may change while Kafka protocol coverage and runtime behavior stabilize.

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
- v0.1.0, v0.2.0, and v0.2.1 were verified from fresh external projects.
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
- `poll_with_heartbeat` replaces completed and stale same-group heartbeat
  handles after background or foreground rejoin while preserving the configured
  heartbeat interval.
- Manual `Live Kafka Smoke` run `30067372344` passed a real two-member
  rebalance, automatic rejoin, and heartbeat handle replacement on Kafka
  3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext brokers.
- Focused unit tests cover running tasks, rejoinable background heartbeat errors, and non-rejoinable background heartbeat errors.
- `ConsumerGroupHeartbeat` records the group ID, member ID, and generation ID it was spawned for, and stale same-group handles are stopped before polling to avoid sending heartbeats for an older generation.
- `ConsumerGroup::commit_offsets` rejoins after rejoinable offset commit errors and returns the original commit error instead of retrying stale assignment offsets under a new generation.
- `docs/consumer-groups.md` describes when to spawn background heartbeats, how heartbeat failures are surfaced, and how offset commit rejoin behavior works.

Known limits:

- Background heartbeats can trigger a rejoin when users call
  `ConsumerGroup::poll_with_heartbeat`; the mutable handle is automatically
  replaced after background or foreground rejoin.
- Range assignment is the only high-level group assignment strategy.

## M10 Producer Throughput

Status: Done.

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

- `acks=0` remains unsupported because the request loop expects a broker response.

Evidence:

- `Producer::send_batch` accepts multiple records, batches same topic-partition groups into one Produce request, and returns metadata in input order.
- `Producer::send_batch_report` surfaces per-record success and failure outcomes in input order, including broker Produce response errors for failed topic partitions.
- Batch retry recovery keeps successful records fixed and retries only input records whose topic partition returned a retryable Produce response error.
- `ProducerConfig::max_records_per_batch` splits large topic-partition groups across multiple Produce requests without changing input-order outcomes.
- `ProducerConfig::max_batch_bytes` splits large topic-partition groups by encoded Kafka record-set bytes without preventing an oversized single record from being sent.
- Focused unit tests cover batch Produce API version selection and batch metadata cache invalidation.
- The `Live Kafka Smoke` workflow runs the `producer_send_batch` and `producer_buffered` examples before direct fetch and group poll checks.
- Manual `Live Kafka Smoke` run `26989271377` passed on 2026-06-05 after the batch outcome, partial retry, and record-limit changes.
- Manual `Live Kafka Smoke` run `26999258762` passed on 2026-06-05 after the buffered producer flush trigger and smoke example changes.
- `docs/producer-buffering.md` defines the planned opt-in buffered producer path, linger flush triggers, delivery semantics, and implementation slices.
- `ProducerConfig::linger_ms` and `ProducerConfig::build_buffered` provide the first buffered producer lifecycle skeleton with `flush`, `close`, and `is_closed`.
- `BufferedProducer::send` queues records through a bounded channel and returns per-record `ProducerDelivery` handles; `flush` and `close` send pending records through `send_batch_report` and complete delivery handles from per-record outcomes.
- Automatic buffered flush triggers cover `linger_ms`, `max_records_per_batch`, and `max_batch_bytes`, with `linger_ms(0)` meaning no intentional wait before background flush.
- Focused unit tests cover buffered enqueue, delivery cancellation, pending delivery failure, per-record delivery completion, defensive handling for missing batch outcomes, and flush trigger selection.

## M11 Security And Connectivity

Status: Complete.

Goal: support common secured Kafka deployments without adding librdkafka or C bindings.

Scope:

- TLS transport using a Rust TLS stack
- SASL PLAIN secured client path
- client configuration for security protocol and authentication material
- secure error messages that do not leak secrets
- docs for local plaintext, TLS, and SASL broker profiles

Exit criteria:

- plaintext behavior remains the default and stays simple
- TLS connections can complete ApiVersions and Metadata roundtrips
- at least one SASL mechanism can authenticate against a broker in live smoke or documented manual checks
- credentials are kept out of tracing events and error displays

Known limits:

- Security protocol configuration exists and defaults to plaintext.
- TLS transport exists behind the non-default `tls` crate feature and has completed recorded broker roundtrip, producer, direct consumer, and consumer group smoke paths against a TLS broker.
- TLS workflows beyond the listed TLS smoke examples are not claimed yet.
- The current `tls` feature uses the `rustls` ring crypto provider, which can require native build tooling in some environments; the default kafrust build still has no required C toolchain.
- `SecurityProtocol::Tls` returns `Unsupported` when kafrust is built without the `tls` feature.
- SASL/PLAIN authentication is implemented and has completed recorded broker
  roundtrip, producer, direct consumer, and consumer group smoke paths against a
  SASL_PLAINTEXT broker.
- SASL_SSL and SASL workflows beyond the listed SASL_PLAINTEXT smoke examples
  are not claimed yet.
- SCRAM live smoke and SASL_SSL are owned by M13 Secured Enterprise Connectivity.

Evidence:

- `SecurityProtocol` models Kafka `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` connection modes.
- `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig` expose `security_protocol` builders.
- `SaslMechanism::Plain` and `SaslCredentials::plain` model SASL/PLAIN authentication material separately from `SecurityProtocol`, and config debug output redacts passwords.
- `ClientConfig` performs `SaslHandshake v1` followed by `SaslAuthenticate v0` for configured SASL/PLAIN connections; mock broker tests verify request ordering, PLAIN auth bytes, missing-credential errors, and authentication error redaction.
- All current internal broker connection paths go through `ClientConfig`, so future TLS/SASL transport work has one configuration source.
- `Client` owns an internal broker stream abstraction instead of storing `TcpStream` directly, so the TLS stream reuses the same Kafka request framing, timeout, and tracing path.
- The non-default `tls` crate feature wires `SecurityProtocol::Tls` through `tokio-rustls`, `rustls`, and `rustls-platform-verifier` without pulling `aws-lc-rs`; plaintext remains the default build.
- Focused tests cover TLS bootstrap server-name extraction, invalid TLS server names, SASL missing-credential behavior, SASL/PLAIN handshake behavior, and TLS unsupported behavior when the feature is disabled.
- CI runs `check`, `clippy`, and `test` for both the default workspace path and the `kafrust --features tls` path.
- The broker roundtrip test and example accept `KAFRUST_SECURITY_PROTOCOL`, `KAFRUST_SASL_USERNAME`, and `KAFRUST_SASL_PASSWORD`, so plaintext, TLS, and SASL broker profiles can use the same smoke entry point.
- `kafrust-protocol` includes `SaslHandshake v1` and `SaslAuthenticate v0` request/response wire types with byte-level tests.
- Manual `Live Kafka Smoke` run `27326596181` passed on 2026-06-11 from `main`; the TLS job completed broker roundtrip test and example checks against Kafka 3.7.2 with `SecurityProtocol::Tls`.
- Manual `Live Kafka Smoke` run `27397850803` passed on 2026-06-12 from `main`; the SASL_PLAINTEXT job completed broker roundtrip test and example checks against Kafka 3.7.2 with `SecurityProtocol::SaslPlaintext`.
- Manual `Live Kafka Smoke` run `27399057735` passed on 2026-06-12 from `main`; the SASL_PLAINTEXT job completed broker roundtrip, producer, direct consumer, and consumer group checks against Kafka 3.7.2 with `SecurityProtocol::SaslPlaintext`.
- Manual `Live Kafka Smoke` run `27399394544` passed on 2026-06-12 from `main`; the TLS and SASL_PLAINTEXT jobs completed broker roundtrip, producer, direct consumer, and consumer group checks against Kafka 3.7.2.

Strategic role:

- This milestone established the baseline secured client path. TLS and SASL_PLAINTEXT producer, direct consumer, and consumer group smoke paths are now covered; M13 owns SASL_SSL, SCRAM, multi-broker secured profiles, and broader enterprise compatibility.

## M12 API Stabilization

Status: Complete.

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

Evidence:

- `docs/api-stability.md` documents the current pre-1.0 versioning policy,
  stability levels, change rules, and migration note expectations.
- `docs/public-api-audit.md` records the current root re-export surface,
  module visibility decision points, and the `kafrust::protocol` re-export
  policy.
- `cargo test -p kafrust --doc` compiles the crate README examples for
  producer, batch producer, buffered producer, direct consumer, and consumer
  group usage; CI runs this explicitly.
- `docs/release.md` defines a release note template with required breaking
  change, migration note, compatibility evidence, verification, and known-limit
  sections.

Strategic role:

- This milestone made the current alpha public surface explicit before adding
  more Kafka feature coverage. Future milestones can still change APIs before
  `1.0`, but those changes now have a documented stability policy, root export
  audit, compiled rustdoc examples, and release note migration format.

## M13 Secured Enterprise Connectivity

Status: Complete.

Goal: make kafrust usable against common secured Kafka deployments.

Scope:

- TLS transport with a pure Rust TLS stack
- configurable root certificates and server name validation
- SASL PLAIN
- SASL SCRAM-SHA-256 and SCRAM-SHA-512
- credential redaction in errors, debug output, logs, and tracing
- secured broker examples and manual smoke instructions

Exit criteria:

- `SecurityProtocol::Tls` can complete ApiVersions and Metadata roundtrips against a TLS broker
- `SecurityProtocol::SaslPlaintext` authenticates with at least SASL PLAIN
- `SecurityProtocol::SaslTls` authenticates with at least one SCRAM mechanism
- failed authentication errors do not expose passwords, tokens, salts, nonce material, or raw credentials
- compatibility docs list plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL broker profiles with verification dates

Known limits:

- SASL/SCRAM-SHA-512 is implemented and covered by focused tests, but the live
  broker profile is not claimed yet.
- SASL mechanisms beyond PLAIN and SCRAM-SHA-256/512 are not implemented.

Evidence:

- `ClientConfig::tls_server_name` and the matching producer, consumer, and
  consumer group builders allow TLS certificate validation to use an explicit
  server name when the bootstrap host differs from the broker certificate
  subject alternative name. Broker smoke examples accept
  `KAFRUST_TLS_SERVER_NAME`.
- `ClientConfig::tls_root_certificate_der` and the matching producer, consumer,
  and consumer group builders add DER-encoded root certificates while keeping
  platform roots enabled. Broker smoke examples accept
  `KAFRUST_TLS_ROOT_CERT_DER_PATH`.
- `SaslMechanism` models Kafka `PLAIN`, `SCRAM-SHA-256`, and
  `SCRAM-SHA-512`; `SaslCredentials` has matching constructors and the shared
  client, producer, consumer, and consumer group configs expose SCRAM builder
  methods without changing `SecurityProtocol`.
- `ClientConfig` performs SCRAM client-first and client-final
  `SaslAuthenticate v0` exchanges after `SaslHandshake v1`, verifies the
  server-final signature, and reports invalid SCRAM responses without exposing
  passwords or raw credentials.
- Focused tests cover SCRAM-SHA-256 and SCRAM-SHA-512 proof generation,
  username escaping, nonce mismatch handling, server-final verification, mock
  broker SCRAM authentication ordering, and secret-safe invalid server-final
  errors.
- The broker roundtrip test and smoke examples accept
  `KAFRUST_SASL_MECHANISM` with `plain`, `scram-sha-256`, and
  `scram-sha-512`, so live broker profiles can exercise the same entry points
  once SCRAM users are configured.
- The `Live Kafka Smoke` workflow includes a SASL_SSL SCRAM profile that
  creates a Kafka SCRAM-SHA-256 user, configures kafrust with
  `KAFRUST_SECURITY_PROTOCOL=sasl_tls`,
  `KAFRUST_SASL_MECHANISM=scram-sha-256`, and a DER root certificate, then runs
  the broker roundtrip, producer, direct consumer, and consumer group smoke
  paths.
- Manual `Live Kafka Smoke` run `27531812308` passed on 2026-06-15 from
  `main`; the plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL SCRAM jobs completed
  broker roundtrip, producer, direct consumer, and consumer group checks against
  Kafka 3.7.2.

Strategic role:

- This is the first milestone where kafrust can plausibly be tested in typical company Kafka environments.

## M14 Multi-Broker And Failover Compatibility

Status: In progress.

Goal: handle normal multi-broker cluster behavior instead of only single-node broker checks.

Scope:

- metadata refresh across multiple brokers
- leader movement and partition leader failover
- bootstrap server failover beyond initial connect
- coordinator movement for consumer groups
- partition expansion handling
- broker disconnect and reconnect behavior under load
- live smoke workflows for at least one multi-broker Kafka profile

Exit criteria:

- producer sends recover after leader movement without user-visible duplicate success reports
- direct consumers recover after partition leader movement
- consumer groups recover after coordinator movement or a controlled rebalance
- compatibility docs distinguish single-node, multi-broker plaintext, and multi-broker secured claims
- tests cover stale metadata, unknown leader, coordinator movement, and reconnect paths

Strategic role:

- This milestone moves kafrust from local/simple broker evaluation toward production-like cluster evaluation.

Evidence:

- Producer and direct consumer retry classification treats missing partition
  leaders and missing broker metadata as stale metadata, invalidates the topic
  metadata cache, and refreshes metadata before retrying within the configured
  retry budget.
- Producer and direct consumer retry classification also treats unknown
  topic-partition entries from cached metadata as refreshable, which gives
  partition expansion and just-created topic metadata one retry budget to
  converge before surfacing the original Kafka concept to callers.
- Smoke examples and opt-in broker roundtrip tests accept comma-separated
  `KAFRUST_BOOTSTRAP_SERVERS` values, so multi-broker live checks can use the
  same environment format as Kafka's standard client configuration.
- The `Live Kafka Smoke` workflow includes a plaintext three-broker Kafka 3.7.2
  profile that creates a replicated topic and runs broker roundtrip, producer,
  direct consumer, and consumer group smoke paths against comma-separated
  bootstrap servers.
- Manual `Live Kafka Smoke` run `28009105074` passed on 2026-06-23; the
  multi-broker job completed broker roundtrip, producer, direct consumer, and
  consumer group checks against a three-broker Kafka 3.7.2 KRaft cluster,
  verified long-lived producer and direct consumer operations across a stopped
  partition leader, then reran batch producer, direct consumer, and consumer
  group checks through the remaining brokers.
- The batch producer smoke example accepts explicit partition lists so the
  multi-broker workflow can route one batch call across multiple partition
  leaders.
- The single-record producer smoke example accepts an explicit partition so the
  multi-broker workflow can cover both single-record and batch leader routing.
- The multi-broker smoke workflow stops the first configured bootstrap broker
  and reruns batch producer, direct consumer, and consumer group checks through
  the remaining brokers.
- The `producer_failover` smoke example sends twice through one producer
  instance, and the multi-broker workflow selects a partition led by the first
  broker, stops that broker during the configured pause, and then requires the
  second send to complete through refreshed metadata.
- The `consumer_failover` smoke example fetches twice through one direct
  consumer instance in the same broker-stop window, so stale direct-consumer
  metadata refresh is covered by the multi-broker workflow.
- Consumer group coordinator connection I/O failures and coordinator request
  timeouts are classified as rejoinable in group contexts, so poll,
  background-heartbeat observation, stale-heartbeat shutdown, and offset commit
  paths can rediscover the coordinator instead of treating only broker error
  codes as rejoin signals.

## M15 Compression Compatibility

Status: Complete.

Goal: support common compressed Kafka record batches while preserving the no-required-C-toolchain policy.

Scope:

- gzip
- snappy
- lz4
- zstd evaluation under the project rule against required C toolchains
- compressed Produce request encoding
- compressed Fetch response decoding
- size and decompression safety limits

Exit criteria:

- producer can send compressed record batches with supported pure Rust codecs
- consumer can decode compressed batches for supported codecs
- unsupported or disabled codecs fail with typed, documented errors
- decompression limits prevent unbounded allocation or decompression bomb behavior
- live smoke or focused broker checks cover gzip, snappy, lz4, and zstd

Strategic role:

- Compression is required for realistic Kafka throughput and for compatibility with existing topics.

Evidence:

- Gzip compression is implemented with a Rust backend and no required C
  toolchain.
- Produce v3 RecordBatch encoding can write gzip-compressed record payloads.
- Fetch v4 RecordBatch decoding can read gzip-compressed record payloads.
- `ProducerConfig::compression(Compression::Gzip)` enables gzip for immediate,
  batch, and buffered producer paths when Produce API v3 is available.
- Manual `Live Kafka Smoke` run `28009105074` passed on 2026-06-23; the
  single-node and multi-broker plaintext jobs completed gzip batch producer
  checks against Kafka 3.7.2.
- Unsupported codecs currently return typed protocol errors instead of being
  decoded as uncompressed data.
- Gzip decompression is bounded to prevent unbounded decoded record payload
  growth.
- Snappy compression uses the pure-Rust `snap` backend with
  Kafka-compatible Xerial framing and no C toolchain dependency.
- Produce v3 RecordBatch encoding writes chunked Snappy frames, while Fetch v4
  RecordBatch decoding accepts both Xerial-framed and raw Snappy payloads.
- Snappy decoding validates each block's declared output length before
  allocation and enforces the record batch decompression limit.
- Focused tests cover multi-block Snappy roundtrips, raw-block compatibility,
  oversized declared output, malformed framing, and Produce-to-Fetch RecordBatch
  roundtrips.
- Manual `Live Kafka Smoke` run `29984929590` passed on 2026-07-23; the
  single-node and multi-broker plaintext jobs completed Snappy batch producer
  checks against Kafka 3.7.2.
- LZ4 compression uses the pure-Rust `lz-fear` backend with independent blocks
  and no C toolchain dependency.
- Produce v3 RecordBatch encoding writes standard LZ4 frames, and Fetch v4
  RecordBatch decoding reads those frames with a bounded output size.
- Focused tests cover the Kafka LZ4 frame magic, multi-block roundtrips,
  malformed frames, decompression limits, and Produce-to-Fetch RecordBatch
  roundtrips.
- Manual `Live Kafka Smoke` run `29986018854` passed on 2026-07-23; the
  single-node and multi-broker plaintext jobs completed LZ4 batch producer
  checks against Kafka 3.7.2.
- Zstd compression uses the pure-Rust `ruzstd` 0.8.1 backend with its optional
  checksum dependency disabled and no C toolchain dependency.
- Produce v7 RecordBatch encoding writes standard Zstd frames, while Fetch v4
  RecordBatch decoding validates declared content and window sizes before
  decoder allocation and bounds decoded output to 64 MiB.
- Focused tests cover the Zstd frame magic, multi-block roundtrips, malformed
  frames, declared window limits, decoded output limits, and Produce-to-Fetch
  RecordBatch roundtrips.
- Manual `Live Kafka Smoke` run
  [`29988390924`](https://github.com/TaeeunKil/kafrust/actions/runs/29988390924)
  passed on 2026-07-23; the
  single-node and multi-broker plaintext jobs completed Zstd Produce v7 batch
  checks against Kafka 3.7.2.

- All four decoders enforce the configurable
  `max_decompressed_record_bytes` limit inherited from `ClientConfig`,
  `ProducerConfig`, `ConsumerConfig`, and `ConsumerGroupConfig`. Oversized
  output returns a typed `protocol::Error::LimitExceeded` failure.

## M16 Admin API MVP

Status: Complete.

Goal: provide the admin operations needed by common applications and test harnesses.

Scope:

- list topics and describe cluster metadata
- create topics
- delete topics
- describe topic configs
- alter basic topic configs
- describe consumer groups
- list and delete groups
- delete consumer group offsets evaluation
- admin examples and typed request errors

Exit criteria:

- users can provision and inspect test topics without external Kafka CLI tools
- admin APIs expose Kafka concepts directly instead of generic resource abstractions
- live smoke covers create, describe, produce/fetch, and cleanup for a topic
- unsupported admin APIs are explicit and documented

Strategic role:

- Admin support reduces friction for integration tests, smoke workflows, and service bootstrap code.

Implemented evidence:

- `AdminClient::describe_cluster` exposes typed broker IDs, advertised
  endpoints, rack IDs, and the active controller. `AdminClient::list_topics`
  exposes names, internal-topic flags, partition counts, and topic-level Kafka
  error classifications.
- Injected broker tests distinguish Metadata v1's empty topic array for
  cluster-only inspection from its null array for all-topic listing and verify
  broker error metrics for partial metadata failures.
- DescribeConfigs v1 supports all or selected topic keys, optional synonyms,
  nullable and sensitive values, raw resource errors, typed config sources,
  broker throttle time, tracing, and shared broker-error metrics.
- IncrementalAlterConfigs v0 exposes Set, Delete, Append, and Subtract
  operations, validate-only mode, resource-level atomicity and partial
  outcomes, broker throttle time, tracing, and broker-error metrics.
- DescribeGroups v1 discovers each requested group's coordinator independently
  and preserves state, protocol, member identity, raw protocol metadata and
  assignments, per-group errors, throttle time, tracing, and metrics.
- ListGroups v1 queries every advertised broker and returns sorted,
  deduplicated listings with protocol type, coordinator ID, and throttle time.
- DeleteGroups v1 routes each group to its coordinator and preserves
  per-group results, including a typed `NonEmptyGroup` classification.
- OffsetDelete v0 routes to the group's coordinator and preserves its
  top-level group error plus every per-partition result. Typed classifications
  cover missing groups and active topic subscriptions.
- The `admin_describe_group` example runs after the consumer-group smoke path
  across plaintext, multi-broker, TLS, SASL_PLAINTEXT, and SASL_SSL profiles.
- The admin lifecycle example waits for asynchronous metadata propagation in
  multi-broker clusters and verifies `cleanup.policy` through
  `describe_topic_configs` before deleting the topic.
- CreateTopics v2 request encoding and response decoding preserve automatic
  and manual replica assignment, nullable topic configs, validate-only mode,
  broker timeout, throttle time, and topic-level partial failures.
- `AdminClient::create_topics` discovers the current controller through
  Metadata v1 and routes the request using the security, timeout, decode-limit,
  and metrics settings from `ClientConfig`.
- `NewTopic`, `CreateTopicsOptions`, `CreateTopicsResult`, and
  `CreateTopicResult` expose Kafka topic creation concepts without flattening
  partial responses into a single generic error.
- DeleteTopics v3 request encoding and response decoding preserve topic-level
  partial failures and broker throttle time. `AdminClient::delete_topics`
  shares the controller routing, security configuration, tracing, and metrics
  behavior of topic creation.
- Focused byte-level tests and an injected two-connection test cover protocol
  encoding, decoding, controller routing, topic error preservation, and broker
  error metrics.
- The `admin_create_topic` example creates a topic, verifies it through a
  subsequent metadata lookup, and deletes it. The live Kafka workflow runs it
  against the Kafka 3.7.2 and current stable single-node profiles and the
  Kafka 3.7.2 three-broker profile.
- Manual `Live Kafka Smoke` run `30059517473` passed CreateTopics v2 and its
  follow-up Metadata v1 description on 2026-07-24 against Kafka 3.7.2 and
  4.3.1 single-node brokers and the Kafka 3.7.2 three-broker cluster.
- Manual run `30060723690` passed cluster/topic inspection, bounded metadata
  propagation, CreateTopics v2, DescribeConfigs v1, and DeleteTopics v3 on
  Kafka 3.7.2 and 4.3.1 single-node brokers and the Kafka 3.7.2 three-broker
  cluster. The same three-broker job passed the subsequent broker-stop
  producer/consumer failover checks.
- Manual run `30061073263` passed IncrementalAlterConfigs v0 update and
  DescribeConfigs v1 readback on Kafka 3.7.2 and 4.3.1 single-node brokers and
  the Kafka 3.7.2 three-broker cluster, followed by the full existing smoke and
  failover sequence.
- Manual run `30061497355` passed DescribeGroups v1 on Kafka 3.7.2 and 4.3.1
  plaintext brokers plus TLS, SASL_PLAINTEXT, and SASL_SSL profiles. The
  three-broker job passed DescribeGroups and broker-stop failover before the
  run result was recorded.
- Manual run `30062203069` passed OffsetDelete v0 after broker-side group
  session expiry on all six live profiles, including Kafka 3.7.2 and 4.3.1,
  TLS, SASL_PLAINTEXT, SASL_SSL, and three brokers. The three-broker job also
  passed its subsequent broker-stop producer, consumer, and group checks.
- Manual run `30065771327` passed broker-wide ListGroups v1 and
  coordinator-routed DeleteGroups v1 across Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 plaintext brokers, TLS, SASL_PLAINTEXT, SASL_SSL, and the three-broker
  profile. The cleanup path accepted Kafka's expected `GroupIdNotFound` after
  OffsetDelete removed the empty group's final committed offset.

## M17 Idempotent Producer

Status: Complete.

Goal: support Kafka idempotent producer semantics for duplicate-safe retries within a producer session.

Scope:

- InitProducerId
- producer ID and epoch tracking
- per-topic-partition sequence numbers
- max in-flight request limits compatible with idempotence
- retry behavior that preserves Kafka ordering and sequence rules
- broker error handling for producer fencing, out-of-order sequence, and duplicate sequence cases

Exit criteria:

- idempotence can be enabled explicitly through producer configuration
- retries do not produce duplicate acknowledged records within the supported broker profile
- sequence state is scoped per topic partition and reset only under documented conditions
- focused tests cover sequence assignment, retry, fencing, and fatal idempotence errors
- live smoke verifies an idempotent send path against a real broker

Strategic role:

- This is a major requirement before kafrust can replace mature clients for many write-heavy services.

Evidence:

- `InitProducerId v0` request/response protocol types and the low-level client
  roundtrip are implemented with byte-level and injected-broker tests.
- RecordBatch v2 encoding accepts producer ID, producer epoch, and base
  sequence metadata while preserving the non-idempotent sentinel values.
- `ProducerConfig::enable_idempotence(true)` initializes a non-transactional
  producer ID, enforces `acks=all` with retries, and keeps acknowledged
  sequences scoped per topic partition for single-record, batch, and buffered
  sends.
- Batch sequence reservations are retained by input record across request and
  partial-record retries. Acknowledged state advances only after broker
  success, and later chunks are held back after a failed idempotent chunk to
  preserve partition ordering.
- `DUPLICATE_SEQUENCE_NUMBER` is accepted as an already delivered retry with
  unknown offset and timestamp metadata. `OUT_OF_ORDER_SEQUENCE_NUMBER`,
  `INVALID_PRODUCER_EPOCH`, and `PRODUCER_FENCED` are classified as fatal and
  leave the producer instance defunct for subsequent sends.
- A deterministic injected-broker test drops the connection after receiving
  the first Produce request, verifies that the retry frame is byte-for-byte
  identical, returns `DUPLICATE_SEQUENCE_NUMBER`, and verifies one sequence
  advancement with unknown delivery metadata.
- Manual `Live Kafka Smoke` run `29991254722` passed the idempotent
  single-record, batch, and buffered producer paths against Kafka 3.7.2 and
  Kafka 4.3.1; all six plaintext, multi-broker, TLS, SASL_PLAINTEXT, and
  SASL_SSL jobs passed.

## M18 Transactions And Read-Committed Consumers

Status: Complete.

Goal: support Kafka exactly-once workflows where applications need transactional produce and read-committed consumption.

Scope:

- transactional producer API
- begin, commit, and abort transaction flows
- AddPartitionsToTxn
- AddOffsetsToTxn
- TxnOffsetCommit
- EndTxn
- transactional error classification and producer fencing
- read-committed consumer behavior

Exit criteria:

- users can produce to multiple partitions in one transaction
- users can commit consumed offsets as part of a transaction where supported
- aborted transaction records are hidden from read-committed consumers
- transaction state transitions are explicit and documented
- live smoke verifies commit and abort paths against a real broker

Strategic role:

- This is required for broad replacement of clients used in exactly-once and stream-processing-style services.

Evidence:

- `EndTxn v0` request and response protocol types encode commit and abort
  results using Kafka API key 26 and decode coordinator throttle/error fields.
- `Client::end_txn_v0` provides the low-level framed roundtrip, covered by
  byte-level commit/abort tests and an injected-broker response test.
- `FindCoordinator v1` now exposes transaction coordinator discovery using
  coordinator type 1, with protocol and injected-broker client coverage.
- `AddPartitionsToTxn v0` request/response types preserve topic-partition
  registration and partition-scoped broker errors. The low-level client
  roundtrip is covered by byte-level and injected-broker tests.
- `AddOffsetsToTxn v0` encodes the transactional producer identity and target
  consumer group, with low-level client and injected-broker coverage for
  coordinator errors.
- `TxnOffsetCommit v0` encodes transactional topic-partition offsets and
  metadata, and preserves partition-scoped group errors through the low-level
  client roundtrip.
- `ProducerConfig::transactional_id` initializes a transactional producer ID
  and enforces idempotent producer settings. `Producer::begin_transaction`,
  `commit_transaction`, and `abort_transaction` expose explicit state
  transitions; sends outside an active transaction are rejected.
- Transactional sends register each topic partition through
  `AddPartitionsToTxn v0`, pass the transactional ID to Produce v3/v7, and
  complete through `EndTxn v0`. Transactional Produce requests set the
  RecordBatch transactional attribute as well as the request transactional ID.
- Transactional initialization discovers the transaction coordinator before
  `InitProducerId`. Partition registration rediscovers and retries transient
  coordinator errors, including `CONCURRENT_TRANSACTIONS`, using the configured
  retry limit.
- `IsolationLevel::ReadCommitted` is available on direct and group consumer
  configurations. Fetch v4 preserves producer and transactional/control batch
  metadata, hides control records, and filters aborted producer ranges while
  advancing poll offsets past hidden records.
- `Producer::send_group_offsets_to_transaction` binds current
  `ConsumerGroup::metadata` and assignments through `AddOffsetsToTxn v0` and
  commits offsets through generation-fenced `TxnOffsetCommit v3` before
  EndTxn. Transaction
  initialization, partition registration, offset integration, and completion
  rediscover coordinators and retry transient coordinator errors within the
  configured retry limit.
- Manual `Live Kafka Smoke` run `29995762812` passed commit, abort,
  read-uncommitted versus read-committed isolation, and a consume-transform-
  produce transaction that committed group offsets against Kafka 3.7.2 and
  Kafka 4.3.1. All six plaintext, multi-broker, TLS, SASL_PLAINTEXT, and
  SASL_SSL jobs passed.
- Manual run `30063099869` passed the generation-fenced `TxnOffsetCommit v3`
  path on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext brokers plus the
  Kafka 3.7.2 TLS, SASL_PLAINTEXT, SASL_SSL, and three-broker profiles.

Known limits:

- Transactional buffered production is not implemented.
- Multi-broker transaction coordinator failover and live transaction
  failure-injection profiles are not yet claimed.

## M19 Observability, Limits, And Performance

Status: Complete.

Goal: make kafrust measurable, tunable, and safe under sustained load.

Scope:

- metrics for requests, retries, errors, bytes, records, batches, queue depth, and latency
- structured tracing spans across complete producer, consumer, and group operations
- memory limits for producer buffers, fetch responses, decompression, and decode arrays
- producer and consumer throughput benchmarks
- latency benchmarks for common record sizes
- load, soak, and failure-injection test profiles

Exit criteria:

- users can observe throughput, latency, retries, and broker errors without inspecting payloads
- benchmark baselines are published for selected broker profiles
- configured memory limits produce typed errors instead of unbounded growth
- soak tests run long enough to catch connection, timer, and background task leaks
- docs explain operational tuning knobs and tradeoffs

Strategic role:

- Without observability and limits, kafrust cannot be responsibly adopted as a production client dependency.

Implemented evidence:

- `ClientMetrics` provides shared lock-free counters for started, successful,
  failed, timed-out, cancelled, and in-flight request roundtrips, request and
  response payload bytes, and total and maximum latency.
- `ClientConfig`, `ProducerConfig`, `ConsumerConfig`, and
  `ConsumerGroupConfig` accept a shared metrics handle. Every bootstrap,
  leader, coordinator, TLS, and SASL connection created from that
  configuration retains the same handle.
- Request start, response, and failure events now execute inside a
  `kafka.request` tracing span with API key, API version, correlation ID, and
  request byte count. Payload contents remain excluded.
- Focused tests cover shared success/failure accounting, timeout
  classification, byte counters, latency, cancellation cleanup, and in-flight
  gauge cleanup.
- The shared metrics snapshot counts actual additional attempts for producer
  sends, partial batch retries, consumer fetches, metadata reconnects,
  idempotent initialization, transactional coordinator operations, and
  automatic consumer-group rejoins.
- Broker response frame allocation is bounded to 100 MiB by default and is
  configurable through all four client configuration builders. Oversized frame
  declarations return typed `Error::ResponseTooLarge { size, max }` failures
  before response payload allocation.
- Buffered producer command capacity is bounded to 1024 records by default and
  configurable through `ProducerConfig::buffer_capacity`. Full queues apply
  async backpressure, while shared metrics report current and maximum
  outstanding accepted records through lifecycle-safe gauges.
- Shared metrics count acknowledged produced records, successful
  topic-partition Produce chunks, and records returned after consumer
  isolation filtering and poll limits.
- Kafka response arrays, nested record counts, and record headers are checked
  before vector allocation. The default maximum is 1,000,000 elements and is
  configurable through all four client configuration builders.
- Fetched record batches are bounded to 64 MiB after decompression by default.
  The configurable limit is inherited by nested Fetch decoders and enforced by
  gzip, Snappy, LZ4, and Zstd, with typed
  `protocol::Error::LimitExceeded { kind, actual, max }` failures.
- Debug-level spans cover immediate and buffered producer operations,
  transaction completion and offset attachment, direct-consumer poll/fetch,
  and consumer-group join, poll, background/explicit heartbeat, and offset
  commit. Existing `kafka.request` spans nest under these operation spans, and
  all fields exclude record and protocol payload contents.
- The `throughput_benchmark` live example measures end-to-end batch Produce and
  offset-based Fetch throughput, Produce batch p50/p95/p99 latency, request
  counts, and retries. The manual `Kafka Benchmark` workflow runs selected
  payload and compression profiles against Kafka 4.3.1 and uploads JSONL
  results for comparison.
- Manual benchmark run `30057817575` published the first selected-profile
  baseline on 2026-07-24. The 1-KiB profiles reached 47,883 records/s
  uncompressed and 50,555 records/s with Zstd on a GitHub-hosted runner.
  Standard-check-vector table CRC and logarithmic exact-size batch selection
  improved those profiles by 37.6x and 29.1x over run `30057137300`.
- The `soak` live example continuously pairs acknowledged Produce batches with
  offset-based Fetch reads, verifies final record counts and zero in-flight and
  buffered gauges, and can require an observed error followed by recovery.
- The weekly `Kafka Soak` workflow runs the profile against Kafka 4.3.1,
  restarts the broker during active load, and uploads the final JSON result.
- Manual soak run `30058270907` passed on 2026-07-24: 1,038,200 records
  completed in 60 seconds across a ten-second broker outage, 145 high-level
  operation errors and 1,011 internal retries were observed, recovery
  completed, and both final resource gauges were zero.
- Shared metrics count non-zero Kafka error codes handled by authentication,
  producer, transaction, consumer, and consumer-group operations, including
  retry attempts and partial batch failures. This separates protocol-level
  broker failures from transport request failures without inspecting payload
  contents.

## M20 Compatibility Matrix And Migration Guide

Status: Complete.

Goal: make replacement decisions concrete for teams comparing kafrust with existing Kafka clients.

Scope:

- broker version matrix across Kafka 3.7, 3.8, 3.9, and current stable Kafka
- plaintext, TLS, SASL_PLAINTEXT, and SASL_SSL profiles
- single-node and multi-broker profiles
- producer, consumer, group, admin, compression, idempotence, and transaction checklists
- migration guide from `rust-rdkafka`
- comparison notes for pure Rust alternatives
- release qualification checklist

Exit criteria:

- compatibility claims are backed by dated workflow runs or documented manual checks
- migration docs show how to map common producer, consumer, group, and admin usage
- unsupported features are listed with alternatives or planned milestones
- release qualification requires docs.rs success, fresh published-crate compile, CI, and relevant live smoke profiles

Strategic role:

- This milestone turns kafrust from a project into an evaluable replacement candidate.

Evidence:

- Manual `Live Kafka Smoke` run `29989550933` passed the single-node plaintext
  producer, all-codec compression, direct consumer, and consumer group paths
  against Kafka 3.7.2 and current stable Kafka 4.3.1.
- The Kafka 4.3.1 run exposed the removal of Fetch v2 support; the high-level
  consumer path now uses Fetch v4, which is supported by both verified broker
  versions.
- `docs/migration-from-rust-rdkafka.md` maps typed configuration, producer,
  direct consumer, classic consumer group, transactions, and admin workflows;
  it also identifies blocking feature gaps and requires staged dual-client,
  failure-injection, performance, and canary qualification.
- Manual run `30062587935` passed the complete single-node plaintext path on
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1. The same run passed the secured
  Kafka 3.7.2 profiles and the three-broker broker-stop failover profile.
- `docs/project-strategy.md` records a dated comparison with krafka, rskafka,
  and kafka-rust while distinguishing self-reported feature claims from
  kafrust's own verified evidence.
- `docs/release.md` requires CI-equivalent checks, package dry runs,
  docs.rs verification, a fresh published-crate compile, a GitHub release,
  and the relevant live broker workflow.

## M21 Broad Kafka Client Replacement

Status: In progress.

Goal: make kafrust a credible pure Rust replacement for Kafka client dependencies in a broad set of Rust services.

Scope:

- stable 1.0 candidate API surface
- producer, consumer, group, admin, security, compression, idempotence, and transaction workflows
- compatibility matrix maintained across supported Kafka versions
- documented operational limits and performance baselines
- migration guide and release notes with semver discipline
- security review of credential handling and unsafe-free dependency posture
- deprecation and compatibility policy for future Kafka protocol growth

Exit criteria:

- kafrust can replace an existing Kafka client dependency for representative producer-only, consumer-only, consumer group, admin, secured, compressed, idempotent, and transactional workloads
- default docs direct production users to supported broker profiles instead of broad unsupported claims
- `docs.rs` builds are green for every release candidate
- fresh external projects compile and run documented examples from published crates
- live compatibility workflows pass for every supported broker/security profile before release
- public APIs have clear stability guarantees and migration notes

Non-goal:

- This milestone does not replace Apache Kafka brokers, controllers, storage, replication, or server-side group coordination.

Strategic role:

- This is the "complete replacement" target for Kafka client dependencies in Rust applications.

Implemented evidence:

- Producer records without an explicit partition use Kafka-compatible Murmur2
  routing when a key is present, preserving standard-client key affinity.
- Keyless producer records use per-topic batch-sticky round-robin routing.
  Single sends rotate after completion, records in the same batch or buffered
  flush stay together, and retries keep the original sticky partition.
- Manual `Live Kafka Smoke` run `30066831820` passed the exact
  `0,1,2,3,4,5,0` keyless rotation sequence against a six-partition,
  three-broker Kafka 3.7.2 topic while all seven regression profiles remained
  green.
- Manual `Live Kafka Smoke` run `30066328105` passed key-derived producer
  routing and buffered fetch-back across every selected partition on the
  three-broker Kafka 3.7.2 profile. The same run passed Kafka 3.7.2, 3.8.1,
  3.9.1, and 4.3.1 single-node plaintext plus TLS, SASL_PLAINTEXT, and
  SASL_SSL/SCRAM-SHA-256 profiles.
- Static classic-group membership carries a configured stable instance ID
  through JoinGroup v5, SyncGroup v3, Heartbeat v3, generation-fenced
  TxnOffsetCommit v3, and OffsetCommit v7. Duplicate instance fencing is
  classified separately from rejoinable group errors.
- Classic groups can advertise and execute either Kafka's `range` or
  `roundrobin` assignor, including mixed topic subscriptions.
- Dynamic and static members can explicitly leave through LeaveGroup v3,
  avoiding session-timeout cleanup after graceful shutdown.
- Manual `Live Kafka Smoke` run `30065025169` passed graceful LeaveGroup v3 on
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext brokers plus TLS,
  SASL_PLAINTEXT, SASL_SSL, and the three-broker regression profile.
- Consumer group assignments without committed offsets support typed
  `Earliest`, `Latest`, and explicit absolute offset reset policies.
  Leader-routed `ListOffsets v1` resolution and the earliest/latest behavioral
  example passed Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 in manual `Live Kafka
  Smoke` run `30229718813`; all multi-broker, TLS, SASL_PLAINTEXT, and
  SASL_SSL regression profiles also passed.
- `AdminClient::create_partitions` routes CreatePartitions v0 to the active
  controller, supports automatic or explicit replica assignment, and preserves
  per-topic errors. Manual `Live Kafka Smoke` run `30230301762` expanded a
  topic and verified its exact Metadata v1 partition count on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1 plus the three-broker Kafka 3.7.2 profile; every
  secured regression profile also passed.
- Manual `Live Kafka Smoke` run `30064594451` passed the round-robin
  static-member path on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext
  brokers; all secured and multi-broker regression jobs also passed.
- Manual `Live Kafka Smoke` run `30064182907` passed static join, poll,
  heartbeat, and OffsetCommit v7 on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1
  plaintext brokers while every existing secured and multi-broker regression
  job remained green.
