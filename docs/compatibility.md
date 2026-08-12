# Compatibility

kafrust compatibility claims are scoped to behavior that has been verified against a real broker. Protocol types can exist before the high-level client path has been validated against every broker version or deployment mode.

The KIP-848 `ConsumerGroupHeartbeat v0` wire types, Metadata v12 topic UUID
mapping, and high-level foreground group path are implemented and covered by
focused tests. The classic and KIP-848 paths are separate selections through
`ConsumerGroupProtocol`. The dedicated Kafka 4.3.1 KIP-848 live profile also
passes join, assignment, foreground/background heartbeat, v9 offset commit,
v9 offset fetch, rejoin, and graceful leave behavior. The Kafka 4.3.1
three-broker profile additionally verifies coordinator broker-stop recovery for
the foreground group poll path.

## Current Compatibility Claim

An EndTxn transport failure where the broker outcome cannot be observed is
reported as `Error::TransactionOutcomeUnknown`, and the producer becomes
`TransactionStatus::Defunct`. This is an explicit safety boundary; kafrust
does not claim that the old transaction committed or aborted in that case.

The complete `Live Kafka Smoke` matrix on `main` passed in
[`31576212276`](https://github.com/TaeeunKil/kafrust/actions/runs/31576212276)
after this transaction-safety change. The 17 jobs covered the supported
single-node broker versions, TLS, SASL/PLAIN, SASL/SCRAM, the test-only
SASL/OAUTHBEARER validator, ACL administration, three-broker failover, and
Kafka 4.3.1 KIP-848 coordinator recovery.

The `0.2.x` alpha line is verified against Apache Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 KRaft brokers over plaintext TCP in the single-node profile. Kafka 3.7.2 is also verified in a three-broker plaintext profile. TLS, SASL/PLAIN over SASL_PLAINTEXT, and SASL/SCRAM-SHA-256 and SCRAM-SHA-512 over SASL_SSL are verified against Kafka 3.7.2 for the documented single-node smoke paths. The Kafka 3.7.2 three-broker SASL_PLAINTEXT profile verifies authenticated transaction coordinator, consumer-group coordinator, producer, and direct-consumer recovery after broker stops in [`31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594). Kafka 4.3.1 KIP-848 coordinator recovery over SASL_PLAINTEXT is verified in a three-broker profile in [`31569709189`](https://github.com/TaeeunKil/kafrust/actions/runs/31569709189), and the SASL_SSL SCRAM-SHA-256 profile is verified in [`31570924845`](https://github.com/TaeeunKil/kafrust/actions/runs/31570924845). A Kafka 3.7.2 SASL_SSL OAUTHBEARER path is also live-verified against Kafka's built-in unsecured test validator; this does not claim production OAuth/OIDC provider integration. The SHA-512 profile covers broker roundtrip, producer, batch producer, buffered producer, direct consumer, and consumer group poll paths. ACL create, describe, and delete plus client quota set, describe, and remove are live-verified against a Kafka 3.7.2 KRaft broker with StandardAuthorizer enabled. SCRAM credential upsert, describe, and delete are live-verified over the SASL_SSL profile. Controller-routed partition reassignment submission and completion polling are live-verified in the Kafka 3.7.2 three-broker profile. The cooperative-sticky consumer protocol path, multi-member ownership transfer, transient-member rollback, and member-loss recovery are live-verified in the three-broker profile by main run `31474626799`. Acks=0 immediate and batch Produce dispatch is live-verified against Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 single-node plaintext profiles; this verifies request completion, not broker acceptance. The high-level producer uses flexible `ApiVersions v3` capability negotiation across all supported plaintext, TLS, SASL, and multi-broker profiles in live run `31494820868`. Idempotent producer recovery through the three-broker broker-stop window is live-verified in run `31495298593`. The Kafka 3.7.2 three-broker SASL_SSL SCRAM profile also qualifies safe transactional producer recovery after coordinator failure in [`31572745537`](https://github.com/TaeeunKil/kafrust/actions/runs/31572745537): the old producer terminates on `INVALID_PRODUCER_EPOCH`, and a new producer with the same transactional ID reinitializes, completes recovery, and commits a new transaction verified with `read_committed`.

The signed local OIDC/JWKS fixture also passes Kafka's validator, the Java Kafka
client, and kafrust static and provider-backed paths in the [`31584760474` OIDC
job](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94075906934).

The complete `Live Kafka Smoke` matrix in [`31589394777`](https://github.com/TaeeunKil/kafrust/actions/runs/31589394777)
passed after adding the DescribeProducers and DescribeTransactions examples.
The run exercised DescribeProducers v0 against the Kafka 3.7.2, 3.8.1, 3.9.1,
and 4.3.1 single-node plaintext profiles plus the Kafka 3.7.2 three-broker
profile. DescribeTransactions v0 also passed on those profiles and through the
Kafka 3.7.2 three-broker SASL_SSL SCRAM failover profile. This verifies the
documented leader/coordinator routing and successful authorization paths; target
permissions, destructive operational policy, and broader fault-injection
behavior still require workload-specific qualification.

The latest complete `Live Kafka Smoke` matrix in
[`31601732149`](https://github.com/TaeeunKil/kafrust/actions/runs/31601732149)
passed all 17 jobs at commit `65b607e`. It reran the supported Kafka 3.7.2,
3.8.1, 3.9.1, and 4.3.1 plaintext profiles, TLS, SASL_PLAINTEXT,
SASL_SSL/SCRAM, OAUTHBEARER, ACL administration, multi-broker failover, and
KIP-848 paths. The matrix also passed the current `DescribeProducers` and
`DescribeTransactions` examples on single-node and multi-broker profiles,
including the secured failover profiles. This qualifies the new client-side
retry implementation against the supported live matrix; an injected broker
stop during the Admin request itself remains a separate qualification item.

The follow-up matrix in
[`31603195530`](https://github.com/TaeeunKil/kafrust/actions/runs/31603195530)
passed all 17 jobs at commit `c58d755`. Its Kafka 4.3.1 single-node and
three-broker KIP-848 jobs executed the member-aware Admin offset example,
covering OffsetFetch v9 and OffsetCommit v9 with a live member ID and member
epoch. This is live qualification for the PLAINTEXT KIP-848 profiles; secured
KIP-848 Admin offset qualification and broker-stop injection during the Admin
request remain open.

The complete `Live Kafka Smoke` matrix in
[`31593984640`](https://github.com/TaeeunKil/kafrust/actions/runs/31593984640)
also passed the opt-in automatic consumer-group commit example across the
classic protocol on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1, plus the KIP-848
consumer protocol on Kafka 4.3.1. The qualification covers assignment-position
queueing, bounded interval flush, leave, rejoin, and restored committed
positions; it does not claim exactly-once processing semantics.

The complete `Live Kafka Smoke` matrix in
[`31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915)
also passed the administrative consumer-group offset example on Kafka 3.7.2,
3.8.1, 3.9.1, and 4.3.1. Each run listed the existing committed offset,
altered it through OffsetCommit v2, and verified the new value and metadata
through OffsetFetch v2.

The follow-up matrix in
[`31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667)
also passed the same operation on the Kafka 3.7.2 three-broker profile, TLS,
SASL_PLAINTEXT, and SASL_SSL/SCRAM profiles. These checks cover coordinator
routing through the configured security transports; they do not yet inject a
coordinator stop during the admin request itself.

Admin coordinator discovery now retries transient `CoordinatorLoadInProgress`,
`CoordinatorNotAvailable`, and `NotCoordinator` responses, as well as discovery
transport timeouts and I/O failures, with a bounded exponential retry budget
from 50ms through 800ms. The focused
mock-broker test `retries_group_coordinator_discovery_after_transient_error`
verifies a failed `FindCoordinator v1`, bootstrap reconnect, successful
rediscovery, and the subsequent `OffsetFetch v2` request. This is request
discovery coverage only; a coordinator stop after the admin connection has been
established still requires live failure-injection evidence.
The default five-attempt budget can be changed with
`AdminClient::max_retries`, including `0` to disable these admin retries.

The read-only `OffsetFetch v2` admin path additionally reconnects and retries
after a coordinator connection drops, a request timeout, or a transient
coordinator broker error. Its focused injected-broker tests verify both a lost
response and a `CoordinatorLoadInProgress` response before the coordinator is
rediscovered and the same offset query succeeds. Administrative `OffsetCommit
v2` retries use the same bounded reconnect path because repeating the exact
committed offsets is state-idempotent; focused tests cover both a lost response
and a transient coordinator partition error. Other administrative writes remain
conservative because a timeout can leave their broker-side outcome ambiguous.

The read-only `DescribeGroups v1` admin path also reconnects and retries after
an established coordinator connection drops. Its focused mock-broker test
verifies the failed request, coordinator rediscovery, and successful group
description on the replacement connection.

The read-only `DescribeProducers v0` path retries leader movement, metadata
convergence failures, transport disconnects, and request timeouts through a
fresh Metadata v1 lookup. A transient per-partition leader error causes the
whole read to be re-routed so the final typed response is assembled from the
current leaders. `DescribeTransactions v0` applies the same bounded retry
policy to transaction-coordinator discovery, coordinator transport failures,
and transient per-ID coordinator errors. Focused mock-broker tests cover a
dropped leader/coordinator request and transient leader/coordinator responses.
These tests prove client-side recovery; live broker-stop injection during
these two requests remains a separate qualification item.

| Broker | Mode | Security | Verification | Status |
| --- | --- | --- | --- | --- |
| Apache Kafka 3.7.2 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | PLAINTEXT | `Live Kafka Smoke` multi-broker job, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | TLS | `Live Kafka Smoke` TLS job, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_PLAINTEXT with SASL/PLAIN | `Live Kafka Smoke` SASL_PLAINTEXT job, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_SSL with SCRAM-SHA-256 | `Live Kafka Smoke` SASL_SSL SCRAM job, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_SSL with SCRAM-SHA-512 | `Live Kafka Smoke` SASL_SSL SCRAM-SHA-512 subpath, manual run `31452872400` on 2026-08-11 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_SSL with OAUTHBEARER and Kafka's built-in unsecured validator | `Live Kafka Smoke` OAuth-only job, manual run `31478375106` on 2026-08-11 | Passing; test-only validator |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_SSL with signed OAUTHBEARER and local OIDC/JWKS validator | [`Live Kafka Smoke` OIDC job, run `31584760474`](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94075906934) on 2026-08-12 | Passing; local fixture |
| Apache Kafka 3.8.1 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 3.9.1 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 4.3.1 | single-node KRaft | PLAINTEXT | `Live Kafka Smoke`, manual run `30067372344` on 2026-07-24 | Passing |
| Apache Kafka 4.3.1 | single-node KRaft | KIP-848 consumer protocol over PLAINTEXT | [`Live Kafka Smoke`, run `31492612082`](https://github.com/TaeeunKil/kafrust/actions/runs/31492612082) on 2026-08-11 | Passing |
| Apache Kafka 4.3.1 | three-broker KRaft | KIP-848 consumer protocol; coordinator broker-stop recovery | [`Live Kafka Smoke`, run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371) on 2026-08-12 | Passing |
| Apache Kafka 4.3.1 | three-broker KRaft | KIP-848 consumer protocol over SASL_PLAINTEXT; coordinator broker-stop recovery | [`Live Kafka Smoke`, run `31569709189`](https://github.com/TaeeunKil/kafrust/actions/runs/31569709189) on 2026-08-12 | Passing |
| Apache Kafka 4.3.1 | three-broker KRaft | KIP-848 consumer protocol over SASL_SSL with SCRAM-SHA-256; coordinator broker-stop recovery | [`Live Kafka Smoke`, run `31570924845`](https://github.com/TaeeunKil/kafrust/actions/runs/31570924845) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft with StandardAuthorizer | PLAINTEXT ACL and client-quota admin | `Live Kafka Smoke` ACL authorizer job, manual run `31459874329` on 2026-08-11 | Passing |
| Apache Kafka 3.7.2 | single-node KRaft | SASL_SSL SCRAM credential administration | `Live Kafka Smoke` SASL_SSL SCRAM job, manual run `31461980967` on 2026-08-11 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | controller-routed partition reassignment | `Live Kafka Smoke` multi-broker job, manual run `31462962605` on 2026-08-11 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | cooperative-sticky consumer protocol, multi-member transfer, transient-member rollback, member-loss recovery, and rebalance listener lifecycle | [`Live Kafka Smoke`, run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | SASL_PLAINTEXT with SASL/PLAIN; transaction/group coordinator, producer, and direct-consumer broker-stop recovery | [`Live Kafka Smoke`, run `31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | SASL_SSL with SCRAM-SHA-256; group-coordinator and partition-leader broker-stop recovery | [`Live Kafka Smoke`, run `31568412595`](https://github.com/TaeeunKil/kafrust/actions/runs/31568412595) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | SASL_SSL with SCRAM-SHA-256; safe transactional producer reinitialization after transaction-coordinator broker stop | [`Live Kafka Smoke`, run `31572745537`](https://github.com/TaeeunKil/kafrust/actions/runs/31572745537) on 2026-08-12 | Passing; old producer outcome remains explicitly unknown |
| Apache Kafka 3.7.2 | three-broker KRaft | PLAINTEXT; repeated partition-leader broker-stop recovery for producer and direct consumer | [`Live Kafka Smoke`, run `31573662135`](https://github.com/TaeeunKil/kafrust/actions/runs/31573662135) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | single-node KRaft | DescribeProducers v0 leader routing and DescribeTransactions v0 coordinator routing | [`Live Kafka Smoke`, run `31589394777`](https://github.com/TaeeunKil/kafrust/actions/runs/31589394777) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft; PLAINTEXT and SASL_SSL SCRAM failover profiles | DescribeProducers v0 leader routing; DescribeTransactions v0 coordinator routing | [`Live Kafka Smoke`, run `31589394777`](https://github.com/TaeeunKil/kafrust/actions/runs/31589394777) on 2026-08-12 | Passing |
| Apache Kafka 4.3.1 | single-node and three-broker KRaft | KIP-848 member-aware Admin OffsetFetch v9 and OffsetCommit v9 | [`Live Kafka Smoke`, run `31603195530`](https://github.com/TaeeunKil/kafrust/actions/runs/31603195530) on 2026-08-12 | Passing; PLAINTEXT |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | single-node KRaft | Produce `acks=0` immediate and batch dispatch | `Live Kafka Smoke`, manual run `31464933145` on 2026-08-11 | Passing; broker acceptance is intentionally unconfirmed |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1; Kafka 4.3.1 KIP-848 | single-node KRaft | opt-in automatic consumer-group commit and restored positions | [`Live Kafka Smoke`, run `31593984640`](https://github.com/TaeeunKil/kafrust/actions/runs/31593984640) on 2026-08-12 | Passing; at-least-once tradeoff |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | single-node KRaft | classic consumer-group offset listing and administrative alteration | [`Live Kafka Smoke`, run `31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft; TLS; SASL_PLAINTEXT; SASL_SSL with SCRAM-SHA-256 | classic consumer-group offset listing and administrative alteration | [`Live Kafka Smoke`, run `31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667) on 2026-08-12 | Passing |

## Verified Paths

The Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext smoke paths cover:

- `ApiVersions v0` and `Metadata v1` roundtrips, plus flexible `ApiVersions
  v3` capability negotiation used by the high-level producer.
- `FindCoordinator v1` for consumer group coordinator discovery.
- `ListOffsets v1` for earliest/latest consumer group offset reset, routed to
  each assigned partition leader.
- Controller-routed CreateTopics v2 followed by Metadata v1 description.
  Manual run `30059517473` passed this path against Kafka 3.7.2 and Kafka
  4.3.1.
- Controller-routed CreatePartitions v0 followed by exact Metadata v1
  partition-count verification. Manual run `30230301762` expanded topics on
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1.
- Admin cluster/topic inspection, CreateTopics v2, bounded metadata propagation,
  DescribeConfigs v1, and DeleteTopics v3. Manual run `30060723690` passed this
  lifecycle against Kafka 3.7.2 and Kafka 4.3.1.
- ACL create, describe, and delete through `AdminClient` with typed bindings,
  filters, and partial outcomes. The focused ACL authorizer job in manual run
  `31457478358` passed create -> describe -> delete against Kafka 3.7.2
  StandardAuthorizer using the explicitly configured `User:ANONYMOUS`
  superuser.
- Client quota set, exact-filter describe, and remove through `AdminClient`
  with typed entities, `FLOAT64` values, and per-entity outcomes. The focused
  ACL authorizer job in manual run `31459874329` passed the roundtrip against
  Kafka 3.7.2 StandardAuthorizer using `User:ANONYMOUS`; the example uses
  bounded polling because KRaft quota metadata becomes visible asynchronously.
- SCRAM credential upsert, describe, and delete through `AdminClient` using
  flexible API v0 request/response encoding. The SASL_SSL SCRAM profile passed
  this roundtrip against Kafka 3.7.2 in manual run `31461980967`.
- Controller-routed `AlterPartitionReassignments v0` accepts replica targets or
  cancellation requests, while `ListPartitionReassignments v0` exposes the
  current, adding, and removing replica sets. The three-broker Kafka 3.7.2
  profile passed submission followed by bounded completion polling in manual
  run `31462962605`.
- The `cooperative-sticky` consumer strategy encodes Kafka consumer protocol
  Subscription v1 owned partitions, preserves valid ownership, and stages
  transfers across rejoin cycles. Focused tests cover ownership preservation,
  new-member balancing, and empty-assignment encoding. The Kafka 3.7.2
  three-broker profile passed the cooperative group example in manual run
  `31464021305`. Manual run
  [`31474626799`](https://github.com/TaeeunKil/kafrust/actions/runs/31474626799)
  additionally passed multi-member ownership transfer, transient-member
  rollback, and member-loss recovery.
- Produce `acks=0` encodes the requested Produce API version, writes and flushes
  the request, and returns unknown-offset metadata without attempting to read a
  response. Immediate and batch examples passed against Kafka 3.7.2, 3.8.1,
  3.9.1, and 4.3.1 in manual run `31464933145`. This mode intentionally cannot
  report broker or partition-level failures after the write.
- IncrementalAlterConfigs v0 followed by DescribeConfigs v1 verification.
  Manual run `30061073263` passed this update-and-readback path against Kafka
  3.7.2 and Kafka 4.3.1.
- Coordinator-routed DescribeGroups v1. Manual run `30061497355` passed this
  path against Kafka 3.7.2 and Kafka 4.3.1 plaintext brokers plus the Kafka
  3.7.2 TLS, SASL_PLAINTEXT, and SASL_SSL profiles.
- Coordinator-routed OffsetDelete v0 with separate group-level and
  partition-level outcomes is covered by byte-level and injected-broker tests.
  Manual run `30062203069` passed offset deletion after group session expiry
  on Kafka 3.7.2 and 4.3.1 plaintext brokers, TLS, SASL_PLAINTEXT, SASL_SSL,
  and the three-broker profile. The three-broker job then passed its existing
  broker-stop failover sequence.
- Coordinator-routed OffsetFetch v2 listing and OffsetCommit v2 administrative
  alteration preserve group, topic, and partition outcomes. Byte-level,
  injected-coordinator, and Kafka 3.7.2/3.8.1/3.9.1/4.3.1 live smoke tests
  pass in [`31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915).
  The multi-broker and secured transport qualification passes in
  [`31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667).
- Broker-wide ListGroups v1 and coordinator-routed DeleteGroups v1 are covered
  by manual run `30065771327` on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1
  plaintext brokers plus TLS, SASL_PLAINTEXT, SASL_SSL, and the three-broker
  profile. The cleanup path also verifies Kafka's `GroupIdNotFound` result
  after deleting an empty group's final committed offset.
- Manual run `30062587935` passed the complete plaintext path on Kafka 3.8.1
  and 3.9.1, including all four compression codecs, idempotent and
  transactional production, direct and group consumption, topic/config admin,
  group description, and offset deletion.
- High-level producer metadata lookup, leader routing, flexible `ApiVersions v3`
  capability negotiation, negotiated Produce API selection, single-record send,
  batch send, gzip-, Snappy-, LZ4-, and Zstd-compressed batch send, and buffered
  send with `acks=1`. Against Kafka 3.7.2, the current path selects Produce v3
  RecordBatch for Gzip, Snappy, and LZ4, and Produce v7 for Zstd. Live run
  `31494820868` passed this producer path across the supported plaintext,
  secured, and three-broker profiles.
- Sequential producer sends to the same leader reuse one authenticated broker
  connection and its negotiated capability response; a focused injected-broker
  test verifies one ApiVersions exchange followed by two Produce requests on
  one socket. Transport failure eviction remains covered by the ambiguous
  idempotent retry test.
- Full live smoke rerun after the leader connection reuse and transient
  classic-group JoinGroup retry changes passed all 11 broker, security, ACL,
  KIP-848, and multi-broker failover jobs in
  [`31500606310`](https://github.com/TaeeunKil/kafrust/actions/runs/31500606310)
  on the merged `main` branch.
- Direct consumer sequential fetches reuse one partition-leader connection;
  the focused injected-broker test covers two Fetch requests on one socket.
- Release `v0.2.4` published both crates after protocol-first dry-run and
  upload verification. The exact docs.rs pages for `kafrust` and
  `kafrust-protocol` return HTTP 200, and a fresh external project compiled
  the published client with default and `tls` features on Rust 1.81.
- Release `v0.2.5` published `kafrust-protocol` before `kafrust`; both package
  dry-runs and uploads passed. The exact docs.rs pages for both crates return
  HTTP 200, and fresh external projects resolved the published client with
  default and `tls` features on Rust 1.81. The release's live qualification is
  [`31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371),
  including the cooperative multi-member rebalance listener lifecycle check.
- Release `v0.2.6` published `kafrust-protocol` before `kafrust`; both crates
  resolve at `0.2.6`, fresh external projects compiled the published client
  with default and dependency-level `tls` features, and the exact docs.rs
  pages return HTTP 200 for [`kafrust 0.2.6`](https://docs.rs/kafrust/0.2.6/kafrust/)
  and [`kafrust-protocol 0.2.6`](https://docs.rs/kafrust-protocol/0.2.6/kafrust_protocol/).
  Release CI passed on stable and Rust 1.81 in
  [`31566231208`](https://github.com/TaeeunKil/kafrust/actions/runs/31566231208),
  and the post-release live matrix passed in
  [`31565059236`](https://github.com/TaeeunKil/kafrust/actions/runs/31565059236).
- The follow-up live matrix also exercised the bounded partition queue from a
  KIP-848 consumer-group poll on Kafka 4.3.1; the direct, classic group, and
  KIP-848 queue examples all passed in
  [`31566898432`](https://github.com/TaeeunKil/kafrust/actions/runs/31566898432).
- Partition queue handles also survived the documented multi-broker
  coordinator and broker-stop workflows, including Kafka 3.7.2
  `SASL_PLAINTEXT` and Kafka 4.3.1 KIP-848 failover, in
  [`31567226615`](https://github.com/TaeeunKil/kafrust/actions/runs/31567226615).
- Current `main` performance and recovery evidence includes benchmark run
  [`31574062876`](https://github.com/TaeeunKil/kafrust/actions/runs/31574062876)
  and 120-second broker-restart soak run
  [`31574065286`](https://github.com/TaeeunKil/kafrust/actions/runs/31574065286).
  The benchmark covered 100-byte, 1-KiB, 10-KiB, and 1-KiB Zstd profiles. The
  soak processed 6,223,500 records across a ten-second outage and ended with
  zero in-flight and buffered records after recovery.
- The scheduled five-minute broker-restart soak run
  [`31568595989`](https://github.com/TaeeunKil/kafrust/actions/runs/31568595989)
  processed 17,019,900 1-KiB records, observed 190 operation errors, 1,118
  failed requests, and 1,329 retries, then recovered with zero in-flight and
  buffered records. The artifact reports approximately 56.7k records/s over
  the full window.
- Latest benchmark run
  [`31569180500`](https://github.com/TaeeunKil/kafrust/actions/runs/31569180500)
  measured 109,368 records/s for 100-byte payloads, 58,135 records/s for
  1-KiB payloads, 3,295 records/s for 10-KiB payloads, and 55,226 records/s
  for 1-KiB Zstd payloads against Kafka 4.3.1, with zero retries in every
  profile.
- Kafka-compatible Murmur2 routing for keyed records without an explicit
  partition. Manual run `30066328105` verified key-derived routing and
  fetch-back by partition and offset across the three-broker Kafka 3.7.2
  profile while all other broker and security profiles remained green.
- Per-topic batch-sticky round-robin routing for keyless records. Manual run
  `30066831820` verified the exact `0,1,2,3,4,5,0` sequence through one
  producer against the six-partition, three-broker Kafka 3.7.2 profile.
- Opt-in idempotent single-record, batch, and buffered produce using
  `InitProducerId v0`, `acks=all`, and partition-scoped RecordBatch producer
  identity and sequence metadata. Manual run `29991254722` passed these paths
  against Kafka 3.7.2 and Kafka 4.3.1.
- Opt-in alpha transactional produce using transaction coordinator discovery,
  transactional `InitProducerId v0`, `AddPartitionsToTxn v0`, Produce v3/v7,
  and `EndTxn v0`. Manual run `29994041530` passed a committed transaction
  followed by an aborted transaction against Kafka 3.7.2 and Kafka 4.3.1.
- Direct and group consumer `ReadCommitted` isolation through Fetch v4.
  Transactional/control RecordBatch metadata is preserved for filtering,
  control records are hidden, and aborted transaction records are excluded.
  Manual run `29995122439` compared `ReadUncommitted` and `ReadCommitted`
  results after real commit and abort flows on Kafka 3.7.2 and Kafka 4.3.1.
- Transactional consumer group offset integration through
  `Producer::send_group_offsets_to_transaction`, `AddOffsetsToTxn v0`, and
  generation-fenced `TxnOffsetCommit v3`. Manual run `30063099869` passed a
  read-committed group poll followed by atomic output production and the
  generation-fenced group offset commit on Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 plaintext brokers plus the Kafka 3.7.2 TLS, SASL_PLAINTEXT, SASL_SSL,
  and three-broker profiles.
- Transactional buffered production serializes begin, queued deliveries,
  group-offset attachment, commit, and abort through one worker. Manual run
  `30334327631` passed buffered commit and abort visibility, read-committed
  filtering, and generation-fenced group offset attachment on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1 plaintext brokers. The Kafka 3.7.2 three-broker,
  TLS, SASL_PLAINTEXT, and SASL_SSL profiles remained green as regression
  coverage.
- Transaction coordinator transport failures reconnect through the bootstrap
  set and rediscover the coordinator before retrying. Manual run
  `30335739033` stopped the active transaction coordinator in the Kafka 3.7.2
  three-broker profile, then passed `EndTxn` commit and read-committed
  fetch-back before restoring the broker. The existing broker-stop failover
  sequence and all seven other broker/security profiles also passed.
- Gzip Produce v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused tests and the plaintext live smoke profile.
- Snappy Produce v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused tests using Kafka-compatible Xerial framing and by the
  plaintext single-node and multi-broker live smoke profiles.
- LZ4 Produce v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused standard-frame and decompression-limit tests and by the
  plaintext single-node and multi-broker live smoke profiles.
- Zstd Produce v7 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused standard-frame, declared-window, and decompression-limit
  tests and by the plaintext single-node and multi-broker live smoke profiles.
- Direct consumer fetch from an assigned topic partition using Fetch v4 response decoding. The v4 path is required because Kafka 4.x no longer accepts Fetch v2.
- Consumer group join, sync, heartbeat, poll, and offset commit through the alpha classic consumer group path with range assignment.
- Client-side regex topic subscription resolves Metadata v1 topic names before
  classic or KIP-848 joins and is covered by focused ordering, filtering, and
  no-match tests. The two-topic initial assignment path passed on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1, including explicit rejoin and the corrected Kafka
  4.3.1 KIP-848 path, in [`Live Kafka Smoke`, run
  `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247).
  Secured-broker topic-discovery permission behavior still requires targeted
  qualification.
- The regex consumer smoke also fetched a produced record, queued its next
  offset with `commit_record`, and flushed the queue through
  `commit_queued_offsets`. The classic path passed across Kafka 3.7.2, 3.8.1,
  3.9.1, and 4.3.1, and the KIP-848 path passed on Kafka 4.3.1 in
  [`Live Kafka Smoke`, run `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247).
- The same regex smoke now starts the bounded interval commit worker. Classic
  worker flush, explicit worker flush, rejoin state synchronization, and
  graceful worker shutdown passed across the Kafka 3.7.2, 3.8.1, 3.9.1, and
  4.3.1 matrix; the KIP-848 worker path passed on Kafka 4.3.1 in
  [`Live Kafka Smoke`, run `31563953123`](https://github.com/TaeeunKil/kafrust/actions/runs/31563953123).
  Direct and group consumers now also expose bounded partition queues through
  `split_partition_queue`; focused tests cover routing, queue backpressure,
  and position preservation. The direct and group queue examples passed across
  Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 in
  [`Live Kafka Smoke`, run `31566523106`](https://github.com/TaeeunKil/kafrust/actions/runs/31566523106).
  Secured topic-discovery permission behavior remains open compatibility work.
- KIP-848 consumer groups through `ConsumerGroupHeartbeat v0`, Metadata v12
  topic UUID assignment, member-epoch foreground/background heartbeats,
  OffsetFetch v9, OffsetCommit v9, rejoin after concurrent membership, and
  explicit leave. The same group is run twice to exercise committed-offset
  recovery.
  The dedicated Kafka 4.3.1 profile passed this path in
  [`Live Kafka Smoke` run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371).
- Kafka 4.3.1 three-broker KIP-848 coordinator recovery: the active
  coordinator is stopped after the first poll, and the foreground group
  process completes through the remaining brokers before the stopped broker is
  restarted. This path passed in
  [`Live Kafka Smoke` run `31557534371`](https://github.com/TaeeunKil/kafrust/actions/runs/31557534371).
- Kafka 4.3.1 three-broker KIP-848 coordinator recovery over
  `SASL_PLAINTEXT`: the same broker-stop sequence uses SASL/PLAIN credentials,
  the group process completes through the remaining authenticated brokers, and
  the stopped broker is restarted afterward. This path passed in
  [`Live Kafka Smoke` run `31569709189`](https://github.com/TaeeunKil/kafrust/actions/runs/31569709189).
- Kafka 4.3.1 three-broker KIP-848 coordinator recovery over `SASL_SSL` with
  SCRAM-SHA-256: all three external TLS listeners are certificate-verified,
  the group process completes through the remaining authenticated brokers, and
  the stopped broker is restarted afterward. This path passed in
  [`Live Kafka Smoke` run `31570924845`](https://github.com/TaeeunKil/kafrust/actions/runs/31570924845).
- Consumer group assignments without committed offsets resolve
  `OffsetResetPolicy::Earliest` or `Latest` from the partition leader. Manual
  `Live Kafka Smoke` run `30229718813` passed both policies on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1.
- Direct and group consumers expose in-memory `position`, `seek`, `pause`, and
  `resume` controls for assigned topic partitions. Manual `Live Kafka Smoke`
  run `30230885629` verified that paused consumers return no records, resumed
  consumers read again from an explicit seek position, and their positions
  advance on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1. The same run kept the
  three-broker, TLS, SASL_PLAINTEXT, and SASL_SSL regression profiles green.
- Leader-routed direct consumer `fetch_watermarks` queries passed Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1 plus the Kafka 3.7.2 three-broker, TLS,
  SASL_PLAINTEXT, and SASL_SSL profiles in manual `Live Kafka Smoke` run
  `30333202216`. The assignment-independent consumer group delegate passed the
  four single-node plaintext broker versions in the same run.
- Background heartbeat recovery through a real two-member classic-group
  rebalance. Manual run `30067372344` verified automatic rejoin and heartbeat
  handle replacement on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 plaintext
  brokers.
- Static classic-group membership through JoinGroup v5, SyncGroup v3,
  Heartbeat v3, and OffsetCommit v7. Manual run `30064182907` passed join,
  poll, heartbeat, and static-member-fenced offset commit on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1 plaintext brokers.
- Range and round-robin classic assignment are implemented. Manual run
  `30064594451` passed the round-robin static-member path on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1 plaintext brokers.
- Dynamic and static members explicitly leave with LeaveGroup v3. Manual run
  `30065025169` passed the graceful-leave group example on Kafka 3.7.2, 3.8.1,
  3.9.1, and 4.3.1 plaintext brokers and all secured profiles.

The Kafka 3.7.2 multi-broker plaintext smoke path covers:

- A three-broker KRaft cluster with comma-separated bootstrap servers and a replicated smoke topic.
- Metadata roundtrip with at least three brokers visible to kafrust.
- Controller discovery, CreateTopics v2, and follow-up Metadata v1 description
  through three externally advertised broker addresses. Manual run
  `30059517473` passed this path.
- CreatePartitions v0 expansion with automatic replica placement followed by
  exact Metadata v1 count verification. Manual run `30230301762` passed this
  path on the three-broker Kafka 3.7.2 profile.
- The complete admin lifecycle, including all-topic listing, DescribeConfigs
  v1, bounded metadata propagation, and DeleteTopics v3. Manual run
  `30060723690` passed before the existing broker-stop failover checks.
- IncrementalAlterConfigs v0 update and readback also passed in manual run
  `30061073263` before the three-broker failover sequence.
- The `broker_roundtrip` example against multi-broker advertised listener metadata.
- High-level producer single-record send with explicit partition routing,
  buffered send, batch send with explicit partition routing, and gzip-,
  Snappy-, LZ4-, and Zstd-compressed batch send with explicit partition routing
  across the replicated smoke topic.
- Long-lived producer metadata refresh after stopping the broker that leads the
  selected partition between two sends from the same producer instance.
- Direct consumer fetch from an assigned topic partition.
- Long-lived direct consumer metadata refresh after stopping the broker that
  leads the selected partition between two fetches from the same consumer
  instance.
- Consumer group join, sync, heartbeat, poll, and offset commit through the alpha classic consumer group path.
- Graceful LeaveGroup v3 cleanup before the subsequent group-admin checks.
- First configured bootstrap broker stop followed by batch producer, direct consumer, and consumer group checks through the remaining brokers.

The Kafka 3.7.2 secured multi-broker failover path covers:

- A three-broker KRaft cluster with an external `SASL_PLAINTEXT` listener and
  SASL/PLAIN credentials.
- A replicated six-partition topic and a selected partition led by the broker
  intentionally stopped during the test.
- Producer send and direct-consumer fetch before and after the broker stop,
  with both operations completing through the remaining brokers.
- Transaction coordinator failover during a transactional commit, followed by
  `read_committed` verification through the remaining brokers.
- Consumer-group coordinator failover during a classic group poll, followed by
  successful group recovery through the remaining brokers.
- The dedicated job in [`Live Kafka Smoke` run `31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594).

The Kafka 3.7.2 TLS smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through `SecurityProtocol::Tls`.
- `FindCoordinator v1` for consumer group coordinator discovery through `SecurityProtocol::Tls`.
- The `broker_roundtrip` example through `SecurityProtocol::Tls`.
- High-level producer metadata lookup, leader routing, single-record send, batch send, and buffered send through `SecurityProtocol::Tls`.
- Direct consumer fetch from an assigned topic partition through `SecurityProtocol::Tls`.
- Consumer group join, sync, heartbeat, poll, and offset commit through `SecurityProtocol::Tls`.

The Kafka 3.7.2 SASL_PLAINTEXT smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through `SecurityProtocol::SaslPlaintext` using SASL/PLAIN.
- `FindCoordinator v1` for consumer group coordinator discovery through `SecurityProtocol::SaslPlaintext`.
- The `broker_roundtrip` example through `SecurityProtocol::SaslPlaintext`.
- High-level producer metadata lookup, leader routing, single-record send, batch send, and buffered send through `SecurityProtocol::SaslPlaintext`.
- Direct consumer fetch from an assigned topic partition through `SecurityProtocol::SaslPlaintext`.
- Consumer group join, sync, heartbeat, poll, and offset commit through `SecurityProtocol::SaslPlaintext`.

The Kafka 3.7.2 SASL_SSL SCRAM smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through `SecurityProtocol::SaslTls` using SASL/SCRAM-SHA-256.
- `FindCoordinator v1` for consumer group coordinator discovery through `SecurityProtocol::SaslTls`.
- The `broker_roundtrip` example through `SecurityProtocol::SaslTls`.
- High-level producer metadata lookup, leader routing, single-record send, batch send, and buffered send through `SecurityProtocol::SaslTls`.
- Direct consumer fetch from an assigned topic partition through `SecurityProtocol::SaslTls`.
- Consumer group join, sync, heartbeat, poll, and offset commit through `SecurityProtocol::SaslTls`.
- TLS certificate validation with an extra DER root certificate configured through `tls_root_certificate_der`.

The Kafka 3.7.2 secured SCRAM failover path covers:

- A three-broker KRaft cluster with a shared `SASL_SSL` listener and
  SCRAM-SHA-256 credentials.
- TLS preflight validation against all three externally advertised broker
  listeners with the generated test CA.
- Consumer-group coordinator broker-stop recovery while the client keeps its
  configured SCRAM/TLS settings and partition queue handle.
- Partition-leader broker-stop recovery for a producer and direct consumer
  using the same authenticated bootstrap set.
- [`Live Kafka Smoke`, run `31568412595`](https://github.com/TaeeunKil/kafrust/actions/runs/31568412595).

The same profile also covers safe transaction recovery after a transaction
coordinator broker stop:

- The original producer returns `INVALID_PRODUCER_EPOCH` and is treated as
  defunct, matching Kafka's transactional producer safety model.
- After the coordinator is restarted, a new producer with the same
  `transactional_id` initializes successfully and lets Kafka finish the
  incomplete transaction before beginning a new one.
- The recovery transaction is committed and returned by a `read_committed`
  consumer, while the old transaction is not claimed as committed.
- [`Live Kafka Smoke`, run `31572745537`](https://github.com/TaeeunKil/kafrust/actions/runs/31572745537).

The same Kafka 3.7.2 SASL_SSL profile's SCRAM-SHA-512 subpath covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through `SecurityProtocol::SaslTls`.
- The `broker_roundtrip` example, producer, batch producer, buffered producer,
  direct consumer, and consumer group poll paths through `SecurityProtocol::SaslTls`.
- SCRAM-SHA-512 authentication against the live Kafka 3.7.2 broker using the
  same DER root certificate validation path.

The Kafka 3.7.2 SASL_SSL OAUTHBEARER smoke path covers:

- `ApiVersions v0` and `Metadata v1` roundtrips through
  `SecurityProtocol::SaslTls` using SASL/OAUTHBEARER.
- The broker roundtrip path with a token supplied through
  `KAFRUST_SASL_TOKEN` and optional authorization identity.
- TLS certificate validation with the configured DER root certificate.
- The dedicated OAuth-only broker job in `Live Kafka Smoke` run
  `31478375106`.

The profile also passes a signed local OIDC/JWKS fixture that exercises
signature, issuer, audience, and provider-backed token loading in the
[`31584760474` OIDC job](https://github.com/TaeeunKil/kafrust/actions/runs/31584760474/job/94075906934).
The public API also supports an async token provider for new broker
authentications. External provider-specific behavior remains unclaimed.
- The low-level `Client` records the broker-advertised SASL session lifetime;
  provider-backed OAUTHBEARER re-authentication is covered by focused injected
  tests and the signed OIDC live job above. OAUTHBEARER uses flexible
  `SaslAuthenticate v2`, while PLAIN and SCRAM remain on `v1`; detached refresh
  workers and provider-specific production OAuth/OIDC qualification remain
  unclaimed.

## Not Yet Claimed

The current compatibility claim does not cover:

- TLS workflows beyond the listed TLS smoke examples.
- SASL workflows beyond the listed SASL_PLAINTEXT and SASL_SSL smoke examples.
- Production SASL/OAUTHBEARER provider compatibility beyond the local signed
  OIDC/JWKS fixture, including discovery/token endpoints, key rotation,
  provider-specific failure behavior, and operational outage semantics. The
  async token-provider callback is implemented and bounded by
  `ClientConfig::request_timeout_ms`.
- Production OAuth/OIDC provider compatibility or rack-aware client routing.
  The SCRAM multi-broker group-coordinator, partition-leader, and safe
  transactional producer reinitialization paths are claimed above. This does
  not claim transparent continuation of the old producer or the outcome of a
  transaction whose commit returned `INVALID_PRODUCER_EPOCH`.
- Broader transaction and KIP-848 failure-injection profiles beyond the
  verified coordinator broker-stop paths, including repeated transaction or
  KIP-848 coordinator faults and broader partition-leader fault matrices.
- Idempotent failure-injection profiles beyond the verified three-broker
  leader-stop recovery path. The ambiguous-response duplicate path is covered
  by a deterministic injected broker test.
- Kafka APIs that are not listed in the verified paths.

## Updating Compatibility

When a new broker version or deployment mode is verified:

1. Run the `Live Kafka Smoke` workflow manually against the target branch, or add a focused workflow job for that broker profile.
2. Record the broker version, deployment mode, security mode, verification command or workflow, date, and result in this document.
3. Update `docs/roadmap.md` if the result changes a milestone status or known limit.
4. If a failure is found, open the issue with the closest template:
   - protocol bug for encoding, decoding, API keys, versions, and Kafka error-code handling
   - client runtime bug for connection, timeout, retry, metadata, producer, consumer, or group behavior
   - API design question for public API naming, builders, defaults, and Kafka concept exposure
