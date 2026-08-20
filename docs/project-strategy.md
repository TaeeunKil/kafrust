# Project Strategy

kafrust is a pure Rust Kafka client project. It is not a Kafka broker or Kafka-compatible server project.

The strategic goal is to become a practical Rust-native client for applications that need Kafka protocol compatibility without librdkafka, C bindings, or a required C toolchain.

## Replacement Target

kafrust should replace a Kafka client dependency in selected Rust applications before it tries to replace every feature of mature Kafka client stacks.

In scope:

- pure Rust Kafka wire protocol implementation
- producer, consumer, and classic consumer group client APIs
- Kafka concepts kept visible in public APIs
- Tokio-based async runtime behavior
- plaintext, TLS, and SASL client connectivity
- compatibility claims backed by live broker smoke tests
- small, auditable feature slices with focused protocol and runtime tests

Out of scope:

- replacing Apache Kafka brokers
- implementing a Kafka-compatible broker, proxy, controller, or storage engine
- wrapping librdkafka, C client bindings, or a required C toolchain
- hiding Kafka topics, partitions, offsets, acknowledgements, groups, commits, or broker errors behind generic queue abstractions
- claiming broad broker compatibility before the broker version and deployment profile has been tested

## Existing Alternatives

For production Kafka workloads that need broad feature coverage today, [`rust-rdkafka`](https://docs.rs/rdkafka/latest/rdkafka/) remains the practical default in Rust. It is mature and feature-rich because it wraps librdkafka, including support for areas such as mature producer and consumer behavior, admin operations, metrics, and exactly-once workflows.

That does not make kafrust redundant. kafrust's reason to exist is different:

- no librdkafka dependency
- no required C toolchain
- protocol code that can be audited and tested in Rust
- a smaller API surface while behavior is still stabilizing
- Kafka terminology preserved instead of abstracted away

Pure Rust alternatives also exist. This comparison is a source-documented
snapshot from 2026-08-20, not an independent benchmark:

The reproducible source inspection behind this comparison is recorded in the
[competitor source audit](competitor-source-audit-2026-08-20.md). It keeps
published artifacts, current source-tree claims, and local kafrust evidence
separate.

- [`krafka`](https://github.com/hupe1980/krafka) is the closest broad
  pure-Rust competitor. Its current `main` README claims Kafka 4.3 protocol
  parity, while the current published [`docs.rs page`](https://docs.rs/krafka/latest/krafka/)
  resolves to `0.19.0`. Treat claims that exist only on `main` beyond the
  published artifact as unreleased until a published crate and its build
  confirm them.
  It documents a Kafka 3.9+ broker floor, classic and KIP-848
  groups, ShareConsumer/KIP-932, transactions, OAUTHBEARER, AWS MSK IAM,
  broad Admin coverage, telemetry, Prometheus metrics, a fault-injecting fake
  broker, 2,350+ tests, and six fuzz targets. It is currently ahead of kafrust
  in pure-Rust feature breadth and test infrastructure. Its optional Zstd path
  requires a C toolchain, while kafrust's supported codecs remain pure Rust.
  Kafrust also verifies an older Kafka 3.7-to-current broker window.
- [`rskafka`](https://github.com/influxdata/rskafka) explicitly targets simple
  distributed write-ahead-log workloads. Its README excludes offset tracking,
  consumer groups, and transactions, so it is not pursuing the same broad
  replacement target.
- [`kafkit-client`](https://docs.rs/kafkit-client/latest/kafkit_client/) is a
  newer native async Rust client aimed at Kafka 4.0+ and KRaft. Its published
  `0.1.9` documentation lists KIP-848 groups, ShareConsumer, transactions,
  Admin, TLS/SASL, compression, and metrics. It deliberately drops classic
  group and older-broker compatibility, so it is a modern-broker competitor,
  not a drop-in replacement for every Kafka deployment.
- [`kafka_client`](https://docs.rs/kafka_client/latest/kafka_client/) is another
  published Tokio-native pure-Rust candidate. Its current `0.5.2` documentation lists
  producer, consumer, group coordination, Admin, TLS, and PLAIN/SCRAM/GSSAPI
  authentication. The public documentation does not yet establish a Kafka
  4.x protocol-parity claim or a broad real-broker compatibility matrix, so it
  is tracked as a secondary adoption competitor rather than a parity leader.
- [`kafka-rust`](https://github.com/kafka-rust/kafka-rust) provides established
  producer and consumer APIs and is being maintained again, but its documented
  tested broker range currently ends at Kafka 3.1 and it does not claim all
  newer Kafka features.
- [`kacrab`](https://github.com/pirumu/kacrab) is another broad, Rust-native
  competitor. Its currently published [`0.4.0` release](https://docs.rs/kacrab/latest/kacrab/)
  claims Kafka 4.3.0 producer,
  consumer, share-consumer, and 62-operation Admin surfaces, classic and KIP-848 groups,
  transactions, four codecs, TLS/SASL, generated protocol code, broker-matrix
  CI, and fuzzing. Its repository may contain newer unreleased work, so the
  published docs are the adoption baseline. Its documented verified matrix is
  centered on 3.3.2, 3.6.2, 3.9.0, 4.0.0, and 4.3.0. It also offers feature-gated TLS and codec choices whose dependency
  posture must be checked separately when a strict no-native-toolchain build is
  required. Its queue semantics and Java-shaped configuration make it a direct
  comparison for the replacement goal, not merely a protocol library.

The broad competitors above should not be read as Kafka Streams application
engines. Their documented client surfaces cover Kafka's producer, consumer,
share-group, and Admin protocols; Kafka Streams DSL execution, state stores,
and task processing remain a separate application/runtime problem. Kafrust's
current `StreamsGroupSession` is likewise only a source-level Streams group
membership and heartbeat layer. It is a useful protocol lead, not a claim that
kafrust can run a Kafka Streams topology.

Mutual TLS is now represented consistently in the shared configuration,
high-level builders, broker-roundtrip test, and repository examples. The
dedicated workflow still needs a passing Kafka run before this becomes a live
compatibility claim; configuration coverage alone does not close the security
evidence gap.

These projects are references and competitors, not sources to copy blindly.
Feature claims must be validated in kafrust's own protocol tests, failure
injection, and live broker matrix.

## Current Competitive Gap

The target is not to win a README feature checklist. A competitor counts as
surpassed only when kafrust has the behavior, an adoptable public API, and
repeatable evidence from real brokers or deterministic failure injection.

| Comparison target | Current position | Remaining gate to surpass it |
| --- | --- | --- |
| `krafka` current source tree and published `0.19.0` line | Behind modern protocol breadth and test infrastructure. Its README claims Kafka 4.3 parity, 2,350+ tests, six fuzz targets, a fault-injecting fake broker, and a 3.9-4.3 integration matrix. | Finish the current API/version surface, then match protocol-parity, fuzz, fake-broker, and broker-matrix evidence. Treat source-tree claims beyond the published artifact as unreleased until the published build confirms them. |
| Published `kacrab 0.4.0` | Behind its claimed 4.3.0 breadth, 62 Admin operations, share consumer, and five-version broker matrix. | Close remaining Admin and modern group/share gaps, then reproduce the matrix and failure/security evidence in kafrust's pure-Rust/no-librdkafka build. |
| Published `kafkit-client 0.1.9` | Modern Kafka 4.0+ surface is broad, but it intentionally excludes classic groups and older brokers. | Match its modern producer/consumer/share/transaction/security/admin surface only after Kafka 3.7-through-current compatibility is preserved; do not copy its narrower broker floor. |
| Published `kafka_client 0.5.2` | Broad-looking Tokio/TLS/SASL/Admin API, but public evidence is thinner than the parity leaders. | Treat it as an adoption and API-design comparison; require reproducible 4.x broker, failure, and security evidence before counting it as operationally surpassed. |
| `rust-rdkafka 0.39.0` | Not a pure-Rust competitor; it remains the mature adoption baseline because it wraps librdkafka and exposes a wide operational surface. | A broad replacement needs migration coverage, semantic compatibility, long soaks, security matrix, performance evidence, and a stable 1.0 API. Protocol feature count alone is insufficient. |
| `rskafka` | Not the same target. Its README explicitly excludes consumer groups, offset tracking, and transactions. | No need to copy its scope; kafrust already targets the broader client role. |

Current planning estimate, based on the repository's implemented surface and
the published competitor claims above:

- Current re-baseline against the latest published broad pure-Rust clients:
  protocol/API breadth is roughly 75-85%, high-level runtime semantics are
  roughly 45-55%, and production evidence/release maturity is roughly 35-45%.
  The weighted replacement-readiness estimate is therefore about 50-60%, not
  75-85%. The higher protocol number must not be presented as drop-in client
  readiness.
- The current workspace executes 738 local test and doctest cases and
  has ten fuzz targets. It now also has a reusable in-process scripted broker
  harness with Admin metadata-reconnect, idempotent-producer response-loss,
  transactional EndTxn response-loss, and direct-consumer Fetch response-loss
  regression gates, plus classic consumer-group coordinator-lookup retry and
  coordinator-connection-loss rejoin gates, including assignment restoration
  and post-rejoin Fetch, and a public ShareConsumer
  ShareAcknowledge response-loss gate, plus a member-aware Admin
  `OffsetCommit v9` response-loss gate. The Share gate verifies the actual
  `build() -> poll() -> acknowledge() -> commit()` path classifies a dropped
  response as `ShareAcknowledgementOutcomeUnknown` without replay, retains one
  record for reconciliation, then accepts the broker-redelivered record after
  session reset.
  The producer gate verifies that a retry preserves the
  request frame and treats Kafka's duplicate sequence response as successful;
  it also verifies that `OUT_OF_ORDER_SEQUENCE_NUMBER`,
  `INVALID_PRODUCER_EPOCH`, and `PRODUCER_FENCED` are terminal, are not retried,
  and leave subsequent sends rejected before transmission;
  the transaction gate preserves the unknown-outcome and defunct-producer
  safety boundary; the consumer gate verifies metadata/fetch re-discovery and
  one-record recovery. The group gate verifies a transient
  `COORDINATOR_NOT_AVAILABLE` response, the retry, and the subsequent
  JoinGroup/SyncGroup/OffsetFetch assignment, while the rejoin gate verifies a
  replacement coordinator and new generation through `poll()`. A separate
  KIP-848 gate now drives repeated `REBALANCE_IN_PROGRESS` responses through
  coordinator rediscovery and full modern-protocol rejoin, negotiates
  OffsetFetch v10 and OffsetCommit v10 on the replacement coordinator, and
  completes a real Fetch at generation 2. Mixed-capability fixtures also
  verify the v9 fallback for both offset operations.
  This is
  a deterministic transport baseline, not yet the
  full fault matrix or field-level schema parity gate documented by the leading
  competitors. The current CI now has an offline Apache Kafka 4.3.1 metadata
  gate for twelve high-risk request/response schemas, while the scheduled/manual
  Apache Schema Audit checks API identity and version bounds for all 76 local
  request/response pairs against the pinned tag. ConsumerGroupHeartbeat v1
  low-level coverage and high-level regex transport selection, plus high-level
  topic-UUID Fetch v13, now reach the current runtime maxima.
  The deterministic fault-injection suite also covers a regex KIP-848
  assignment that introduces an unknown topic UUID: the client refreshes
  Metadata v1/v12, remaps the UUID-based offset response, and fetches the new
  topic without requiring an explicit rejoin.
  Fetch v16-v18 now have low-level request/response coverage for node
  endpoints, directory IDs, and replica high-watermark tags; expanding the
  gate to every implemented API with byte-level fixtures remains open. Existing
  live workflows remain strong evidence for named paths,
  not a substitute for recurring failure, matrix, and long-soak gates. The
  Share acknowledgement gate is deterministic transport evidence; live
  ambiguous-outcome reconciliation and sustained soak evidence remain open.
  Member-aware Admin offset methods now also have a local v10/v9 capability
  gate with automatic Metadata v12 topic-ID resolution; complete UUIDs remain
  an opt-in way to skip that lookup. It is source-level evidence only until a
  live Kafka 4.x Admin run and published-artifact smoke qualify the path.
- Pure-Rust feature-breadth match: roughly 60-70% complete; about 12-18
  focused months remain for the highest-impact protocol/API gaps and their
  compatibility evidence. The range is lower than a raw protocol count
  because the latest competitors now claim full 4.3 Admin, Share, security,
  and multi-broker behavior.
- Operationally surpassing the published pure-Rust competitors: roughly
  40-50% complete; about 18-30 focused months remain for matrix, failure,
  security, fuzz, performance, and release evidence. This is the earliest
  meaningful “ahead of krafka/kacrab” target.
- Credible broad `rust-rdkafka` replacement: roughly 25-35% complete; about
  24-48 focused months remain, or about 3-5 years for one person working
  part-time. This includes the compatibility long tail, migration surface,
  and production evidence that feature checklists do not measure.

These are effort ranges, not calendar promises. The immediate core exit
criteria are: finish the public ShareConsumer and consumer-group workflows,
close the remaining modern public Admin/protocol gaps, establish a reproducible
3.3/3.6/3.7/3.9/4.0/4.3 matrix, extend the new deterministic transport harness
into a broader fault-injection matrix, and publish a fresh crate/docs.rs smoke
before calling kafrust ahead. Share Group State APIs 83-87 and dynamic KRaft
quorum mutation remain valuable advanced qualifications, but they are
unstable/internal-adjacent protocols and are not substitutes for public client
compatibility evidence.

Use this decision rule:

- If the user needs mature production Kafka features immediately, recommend `rust-rdkafka`.
- If the user needs a pure Rust client, no C dependency, and can accept alpha limits, kafrust is the project to grow.
- If the user needs a Kafka-compatible broker, kafrust is the wrong repository unless the project scope is deliberately changed.

## Completion Tiers

These tiers define what "usable" and "complete" mean for this project. Dates are planning estimates for a small project, not commitments.

### Alpha Client

Current state after the published v0.3.1 alpha release.

Expected use:

- experiments
- local broker checks
- simple internal tools
- protocol and API evaluation

Required evidence:

- crates.io release
- docs.rs build
- fresh published-crate compile smoke
- scheduled or manual Kafka live smoke

Known limits:

- plaintext remains the default networking path; TLS, SASL/PLAIN, and SASL_SSL/SCRAM-SHA-256 are live-verified for broker roundtrip, producer, direct consumer, and consumer group smoke paths on single-node Kafka 3.7.2 profiles
- the default build has no C toolchain dependency, but the optional TLS feature currently uses the rustls ring provider and may require native build tooling
- narrow broker compatibility matrix
- incomplete protocol coverage
- pre-1.0 public API

### Constrained Internal Client

Estimated effort: 2-4 months.

Target users can use kafrust for constrained internal workloads where Kafka deployment assumptions are known and narrow.

Required work:

- stabilize common producer and direct consumer workflows
- improve consumer group lifecycle behavior
- add focused failure tests for disconnects, stale metadata, and coordinator movement
- document exact supported broker profiles
- keep live smoke passing on every release candidate

Non-goal:

- broad replacement for `rust-rdkafka`

### Production-Like Plaintext Client

Estimated effort: 4-8 months.

Target users can test kafrust against production-like plaintext Kafka deployments.

Required work:

- multi-broker compatibility and leader failover verification
- broker version matrix beyond Kafka 3.7.2
- stronger reconnect behavior
- memory and backpressure limits
- producer and consumer performance benchmarks
- compatibility tests for record shapes, headers, partition expansion, and rebalances

Non-goal:

- secured enterprise Kafka deployments

### Common Enterprise Client

Estimated effort: 6-12 months.

Target users can connect kafrust to common company Kafka deployments.

Required work:

- TLS
- SASL PLAIN
- SASL SCRAM
- credential-safe errors and tracing
- documented plaintext, TLS, and SASL broker profiles
- live smoke or manual verification for secured broker profiles

This is the earliest tier where "real use" becomes plausible for many organizations.

### Broad Rust Kafka Client Replacement

Estimated effort: 18-36 months for a small team or sustained focused effort.

Target users can consider kafrust as a serious alternative to mature Kafka clients for a broad set of Rust services.

This is the complete replacement target for this repository: kafrust should be able to replace a Kafka client dependency in Rust applications. It does not mean replacing Apache Kafka brokers or implementing server-side Kafka storage, replication, controllers, or group coordination.

Required work:

- idempotent producer
- transactions and read-committed consumer behavior
- compression support
- admin APIs
- mature consumer group rebalancing
- metrics and structured tracing spans
- extensive broker version and deployment matrix
- load, soak, and failure-injection testing
- migration notes and semver discipline

This tier is where comparisons with `rust-rdkafka` and pure Rust alternatives become meaningful. It should not be promised from the current alpha line. The execution path for this target is tracked in roadmap milestones M13 through M21.

### Current Competitive Gap (2026-08-20)

The published `0.3.0` line has already closed the basic connectivity,
compression, security, multi-broker recovery, idempotence, transactions,
Admin MVP, observability, and migration-documentation gates recorded in the
roadmap. The published group smoke now also qualifies bounded normal and
abrupt member departure recovery plus committed-offset restoration across
Kafka 3.7.2 Classic and Kafka 4.3.1 KIP-848. The current development branch
has also added the first high-level ShareConsumer runtime on top of the stable
KIP-932 v1 wire path. It is still behind the broad pure-Rust competitors in
three areas:

- modern protocol breadth: the high-level Share Consumer/KIP-932 runtime and
  live qualification (the runtime exists, now negotiates KIP-1206 ShareFetch
  v2 for strict record limits, and has an opt-in cancellable heartbeat task and
  foreground coordinator rediscovery, and implements KIP-1222 renewal with
  broker lock-timeout tracking and Renew redelivery replacement; the Kafka
  4.3.1 single-node poll/Renew/expiry-redelivery/Accept path passed in
  [run 32213499877](https://github.com/TaeeunKil/kafrust/actions/runs/32213499877),
  and the three-broker fresh-consumer failover path passed in
  [run 32214201983](https://github.com/TaeeunKil/kafrust/actions/runs/32214201983),
  while an active-heartbeat coordinator-movement path passed in
  [run 32215845737](https://github.com/TaeeunKil/kafrust/actions/runs/32215845737)
  and three independent matrix attempts passed in
  [run 32216383214](https://github.com/TaeeunKil/kafrust/actions/runs/32216383214),
  plus three consecutive in-process coordinator churn cycles in
  [run 32219147942](https://github.com/TaeeunKil/kafrust/actions/runs/32219147942);
  expiry/reconciliation soak and live ambiguous-acknowledgement fault injection
  remain open; the runtime now exposes a safe
  `reconcile_acknowledgement_outcomes` session-reset path that never replays an
  unknown acknowledgement; ShareGroupDescribe plus Share Group offset mutation,
  and deletion APIs 77, 90, 91, and 92 are now implemented with focused wire,
  coordinator-routing, and Kafka 4.3.1 lifecycle tests in
  [run 32225957928](https://github.com/TaeeunKil/kafrust/actions/runs/32225957928);
  KIP-714 beyond the new low-level wire path (the high-level provider,
  scheduler, broker-negotiated pure-Rust compression, optional built-in OTLP
  generation, and the Kafka 3.7.2 KRaft broker delivery gate passed in
  [run 32229640441](https://github.com/TaeeunKil/kafrust/actions/runs/32229640441));
  active subscription mutation and unknown-subscription recovery now also pass
  in [run 32236749392](https://github.com/TaeeunKil/kafrust/actions/runs/32236749392),
  including Kafka 3.7.2 quota cooldown handling on the same connection;
  the advertised broker payload-limit gate now also passes in
  [run 32237664774](https://github.com/TaeeunKil/kafrust/actions/runs/32237664774),
  with a typed pre-send rejection at the broker's 128-byte ceiling. Longer
  telemetry collection remains open, alongside remaining modern Admin paths
  and protocol-parity automation;
- production hardening: long-duration group churn, ambiguous transaction and
  mutation outcomes, resource ceilings, recurring fuzz campaign depth, fault-injected
  broker coverage, longer secured multi-broker soaks, and
  assignment/rebalance fault matrices;
- adoption surface: stable 1.0 APIs, wider migration compatibility, published
  examples, and repeatable performance/SLO evidence across representative
  workloads.

`DescribeQuorum` is now live-qualified through the explicit controller-listener
path on Kafka 3.7.2 and 4.3.1. The remaining gap is broader modern Admin and
KRaft controller coverage, not this API's basic request/response path.
ConsumerGroupDescribe API key 69 is now available through the typed protocol,
low-level client, and high-level Admin path; its live KIP-848 qualification is
now runs through the existing Kafka 4.3.1 member-aware Admin workflow, with the
workflow result retained as the qualification evidence.

The current development branch now also has typed KRaft `AddRaftVoter` API 80
and `RemoveRaftVoter` API 81, including v1 committed-acknowledgement gating,
controller routing, and ambiguous-mutation handling. This closes one modern
Admin implementation gap against broad pure-Rust competitors, but not the
operational gap: live dynamic-quorum mutation, controller replacement, and
post-change quorum-health evidence are still required before counting it as a
compatibility or replacement win.

The branch now makes high-level `AdminClient::list_groups` negotiate ListGroups
v4/v5 per broker and exposes `list_groups_with_options` for state/type filters;
the low-level v1 method remains available for exact legacy compatibility. This
closes a concrete modern Admin API gap at the wire and injected-broker levels;
the live smoke gate now exercises v4 on Kafka 3.7.2 and v5 filters on Kafka
4.3.1, but the workflow still needs a successful run. Authorization coverage
and long coordinator-churn validation remain open.

### Estimate To Surpass

These are effort ranges, not calendar promises. They are deliberately more
conservative than a feature-count estimate: the competing projects publish
large broker matrices, fuzz targets, in-process fault injection, and failure
semantics that are expensive to reproduce and maintain.

- **Stay ahead of narrow pure-Rust clients:** already achieved for the broad
  producer, consumer-group, transaction, security, and Admin surface. This is
  not a production replacement claim; `rskafka` explicitly excludes groups,
  transactions, and offset tracking, while older `kafka-rust` releases target
  a much smaller broker/feature window.
- **Match one broad pure-Rust feature checklist:** roughly 12-18 focused
  full-time engineering months, or about 18-30 calendar months at the current
  solo-project pace. The remaining work is not one missing API: it includes
  modern protocol parity, Share and telemetry completion, remaining Admin
  coverage, security breadth, bounded resources, configuration-surface
  consistency, and live evidence.
- **Surpass `krafka` and `kacrab` operationally:** roughly 18-30 focused
  engineering months, or about 24-42 calendar months solo. The exit bar is
  repeatable Kafka 3.7-through-current matrices, secured and multi-broker
  fault injection, recurring fuzz campaigns, long soaks, resource/SLO data,
  modern group/share/Streams protocol qualification, and a migration path that
  works from fresh published crates. Feature names alone do not meet this bar.
- **Credibly replace `rust-rdkafka` for broad Rust services:** roughly 24-48
  focused engineering months, or about 3-5 years solo. This includes the
  compatibility long tail, callback/configuration migration surface,
  old-broker behavior, production SLOs, ecosystem adoption, and release
  discipline. Pure Rust should be the advantage; absolute throughput against
  a mature C client is not a realistic universal success criterion.

The practical near-term target is therefore not “beat every README claim.” It
is to become the strongest strict-pure-Rust option for Kafka 3.7-through-current
deployments, with every advertised feature backed by a reproducible live test.
That target is narrower than replacing `rust-rdkafka` everywhere, but it is
measurable and gives kafrust a defensible reason to exist.

The practical "surpassed" gate is therefore: a representative service can
remove `rust-rdkafka`, compile against kafrust, pass its producer/consumer/
group/admin/security/transaction tests, survive the documented fault matrix,
and retain a rollback path. Feature count alone does not satisfy that gate.

### Kafka Broker Replacement

This is not a kafrust completion tier.

Replacing Apache Kafka as a broker means implementing a distributed log server, controller behavior, storage, replication, leader election, group coordination, transactions, admin surfaces, operational tooling, and compatibility behavior. That is a separate multi-year server project and would require a new repository strategy.

## Strategic Priorities

The next work should be ordered by user unlock, not by protocol completeness alone:

1. Security and connectivity: TLS and SASL are required for common company Kafka deployments.
2. Multi-broker behavior: metadata refresh, leader changes, and failover need live verification.
3. Compression: common record batches need snappy, gzip, lz4, and zstd compatibility.
4. Admin APIs: topic, config, and group administration are needed for integration tests and service bootstrap.
5. Idempotent producer: duplicate-safe retries are required for serious producer replacement.
6. Transactions and read-committed consumers: exactly-once workflows are required for broad replacement.
7. Observability and limits: metrics, structured spans, memory limits, and benchmark baselines are required for production operation.
8. Compatibility matrix and migration guide: replacement decisions need dated broker evidence and clear migration paths.

Do not expand public APIs just to look complete. Add public surface only when the protocol behavior and runtime behavior are tested enough to document.

## Success Criteria

kafrust is successful if it becomes the best choice for Rust users who value pure Rust implementation, auditability, and simple Kafka-native APIs enough to accept a narrower feature set than librdkafka-backed clients.

kafrust is not successful if it merely becomes a smaller, less compatible clone of `rust-rdkafka` without a clear pure-Rust advantage.
