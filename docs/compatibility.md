# Compatibility

kafrust compatibility claims are scoped to behavior that has been verified against a real broker. Protocol types can exist before the high-level client path has been validated against every broker version or deployment mode.

The KIP-848 `ConsumerGroupHeartbeat v0` wire types, Metadata v12 topic UUID
mapping, and high-level foreground group path are implemented and covered by
focused tests. The classic and KIP-848 paths are separate selections through
`ConsumerGroupProtocol`. The dedicated Kafka 4.3.1 KIP-848 live profile also
passes join, assignment, foreground/background heartbeat, v9 offset commit,
v9 offset fetch, rejoin, and graceful leave behavior. The Kafka 4.3.1
three-broker profile additionally verifies coordinator broker-stop recovery for
the foreground group poll path. The secured three-broker profile additionally
passes a repeated coordinator broker-stop sequence, with separate groups
recovering through different coordinators, in
[`31695433295`](https://github.com/TaeeunKil/kafrust/actions/runs/31695433295).

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

The latest complete 17-job matrix on current `main`,
[`31727573855`](https://github.com/TaeeunKil/kafrust/actions/runs/31727573855),
passed all supported broker, security, ACL, multi-broker, and KIP-848 jobs.
Its direct assigned-consumer leader-epoch gate and secured/KIP-848 leader-epoch
gates kept the heartbeat alive, isolated the partition leader from the group
coordinator, and verified automatic `OffsetForLeaderEpoch` recovery where the
broker path returned a leader-epoch transition. The classic group combined
gate separately verifies rejoin and post-failover record consumption. The same
matrix also passed the secured Kafka 3.7.2 `SASL_PLAINTEXT` classic combined
gate, where the stopped broker was both coordinator and partition leader. The
same matrix also passed the secured Kafka 4.3.1 KIP-848 combined gate over
SASL_SSL/SCRAM.

The dedicated ambiguous-EndTxn gate passed in the Kafka 3.7.2 three-broker job
[`94476744970`](https://github.com/TaeeunKil/kafrust/actions/runs/31708995196/job/94476744970).
The proxy dropped the first EndTxn response, kafrust reported
`TransactionOutcomeUnknown` without replaying EndTxn, and a fresh producer with
the same transactional ID committed a recovery transaction that was observed
alongside the original transaction through `read_committed`. The surrounding
run later failed in an existing consumer-group leader-epoch smoke gate, so this
is recorded as a gate-level result rather than a complete-matrix pass.

The complete 17-job matrix for classic AlterConfigs v1 passed at commit
`1085880` in [`31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872).
The admin lifecycle example exercised complete-map topic configuration
replacement followed by incremental alteration on Kafka 3.7.2, 3.8.1, 3.9.1,
and 4.3.1 single-node profiles plus the Kafka 3.7.2 three-broker profile.
A follow-up complete matrix on commit `0ec1512` in
[`31674680581`](https://github.com/TaeeunKil/kafrust/actions/runs/31674680581)
ran the same lifecycle over Kafka 3.7.2 TLS, SASL/PLAIN, and SASL_SSL
SCRAM-SHA-256 profiles. The secured logs confirm CreateTopics,
DescribeConfigs, classic AlterConfigs, IncrementalAlterConfigs, and
DeleteTopics over authenticated connections. This qualifies secured Admin
AlterConfigs for those profiles; post-transmission mutation recovery remains
unclaimed.

The latest complete matrix in
[`31624278107`](https://github.com/TaeeunKil/kafrust/actions/runs/31624278107)
passed all 17 jobs at commit `256847f` after adding bounded pre-transmission
controller discovery retries for controller-routed Admin writes. This confirms
the existing controller-routed topic, partition, SCRAM, and reassignment
workflows remain green across the supported broker, security, failover, ACL,
and KIP-848 profiles; write transport failures after transmission remain
intentionally single-attempt.

The complete 17-job matrix also passed at commit `ec293d1` in
[`31665016772`](https://github.com/TaeeunKil/kafrust/actions/runs/31665016772)
after adding bounded retry handling for retryable DeleteGroups and OffsetDelete
coordinator responses. The matrix covers their ordinary plaintext, TLS, SASL,
SCRAM, and KIP-848-adjacent workflows; it does not claim transparent replay
after a mutation transport failure.

The follow-up complete 17-job matrix in
[`31627790408`](https://github.com/TaeeunKil/kafrust/actions/runs/31627790408)
also passed at commit `25d614a` after the ACL authorizer example added bounded
polling for asynchronous post-create visibility. This confirms the documented
ACL create -> describe -> delete smoke path without weakening the client's
typed error handling.

The preceding complete 17-job matrix in
[`31629022740`](https://github.com/TaeeunKil/kafrust/actions/runs/31629022740)
passed at commit `562dccd` after controller Metadata discovery also retried
transient responses before any controller-routed mutation was transmitted.

The latest complete 17-job matrix in
[`31630339333`](https://github.com/TaeeunKil/kafrust/actions/runs/31630339333)
passed at commit `43969e0`. It reran the supported Kafka 3.7.2, 3.8.1, 3.9.1,
and 4.3.1 plaintext profiles, TLS, SASL_PLAINTEXT, SASL_SSL/SCRAM,
OAUTHBEARER, ACL administration, multi-broker failover, and KIP-848. The
Kafka 3.7.2 multi-broker job also passed the in-flight DeleteRecords and
DescribeProducers leader-stop recovery gates.

The latest complete matrix, ListTransactions, rack-aware, and topic-ID Produce
qualification in
[`31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947)
passed at commit `1a844d8`. Its Kafka 3.7.2 three-broker profile configured
broker racks and Kafka's `RackAwareReplicaSelector`; the direct consumer sent
Fetch v12 with `client_rack`, fetched records, and observed a preferred replica
route. The single-node profiles also passed the Produce negotiation gate: Kafka
4.3.1 selected topic-ID Produce v13, Kafka 3.8.1 and 3.9.1 selected Produce
v11, and Kafka 3.7.2 selected Produce v9. This qualifies the v13 topic-ID path
and the v12/v11/v9 name-based fallback matrix on the documented single-node
profiles.
ListTransactions returned broker-sharded
transaction listings on the same single-node profiles and the Kafka 3.7.2
three-broker profile.
The same consumer
retains Fetch v11 and Fetch v4 fallback paths.
This qualifies the documented rack-aware replica-selection path, not every
possible rack topology or security combination.

The current rack-aware Fetch session slice passed the complete 17-job matrix in
[`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977) at
commit `8615833`. The Kafka 3.7.2 three-broker job issued the initial Fetch v12,
follow-up Fetch v12 requests, and preferred-replica route successfully. This
qualifies session reuse on the documented rack-aware direct-consumer path; it
does not claim session reuse for the Fetch v4 fallback or every security and
topology combination.

Release `v0.2.25` broadens this negotiated Fetch v11/v12 and session path to
direct and group consumers without `client_rack`; the rack field is empty when
no rack is configured. The complete 17-job matrix passed at commit `f222d05`
in [`31673377685`](https://github.com/TaeeunKil/kafrust/actions/runs/31673377685),
including the supported plaintext, TLS, SASL, KIP-848, and multi-broker paths.
The v4 fallback remains covered by a focused broker-capability regression test.

Release `v0.2.26` adds automatic direct-consumer truncation recovery after a
fenced or unknown leader epoch. The complete 17-job matrix passed on code
commit `1694889` in
[`31677617186`](https://github.com/TaeeunKil/kafrust/actions/runs/31677617186),
including plaintext, TLS, SASL, SCRAM, OAUTHBEARER, multi-broker, and KIP-848
profiles. The follow-up matrix passed on the workflow-only qualification
commit `0d4f7b7` in
[`31679167875`](https://github.com/TaeeunKil/kafrust/actions/runs/31679167875):
the Kafka 3.7.2 three-broker job stopped the second leader after the initial
direct-consumer poll, observed the leader epoch move from 1 to 2, and verified
automatic recovery through the bounded OffsetForLeaderEpoch path. This proves
live direct-consumer leader-epoch failover recovery; group rebalance recovery
and data-loss/log-retention fault scenarios remain separate claims.

The complete matrix in
[`31702236760`](https://github.com/TaeeunKil/kafrust/actions/runs/31702236760)
also qualified assigned consumer-group leader-epoch recovery. The Kafka 3.7.2
three-broker classic profile preserved the group session across the broker
stop, waited for the selected partition's new leader, and verified automatic
OffsetForLeaderEpoch recovery from the assigned consumer in job
[`94453938654`](https://github.com/TaeeunKil/kafrust/actions/runs/31702236760/job/94453938654),
including a live partition epoch transition from 3 to 4. The Kafka 4.3.1
three-broker KIP-848 profile passed the corresponding consumer-protocol gate
in job
[`94453938633`](https://github.com/TaeeunKil/kafrust/actions/runs/31702236760/job/94453938633),
including a live partition epoch transition from 0 to 1. This closes the
tested classic and plaintext KIP-848 group leader-epoch recovery claims. The
same consumer-protocol leader-stop gate also passed over Kafka 4.3.1
`SASL_PLAINTEXT` in job
[`94459402338`](https://github.com/TaeeunKil/kafrust/actions/runs/31703868759/job/94459402338)
and over Kafka 4.3.1 `SASL_SSL` with SCRAM-SHA-256 in job
[`94459402266`](https://github.com/TaeeunKil/kafrust/actions/runs/31703868759/job/94459402266).
These secured KIP-848 runs observed live epoch transitions from 2 to 3 and
from 1 to 2 respectively. Data-loss/log-retention scenarios remain separate
claims.

The follow-up complete 17-job matrix in
[`31703868759`](https://github.com/TaeeunKil/kafrust/actions/runs/31703868759)
passed at commit `9e53941` after adding the secured KIP-848 leader-epoch gates
and a bounded retry around the existing secured group bootstrap step. The
retry is limited to five attempts and does not change client retry semantics.

The current development line also implements controller-routed ElectLeaders
v0-v2 negotiation and typed preferred/unclean election results. Focused wire
and injected-controller tests pass, and the multi-broker smoke workflow now
executes the preferred-election example after replica reassignment. The Kafka
3.7.2 three-broker path returned a successful preferred election in run
[`31681439569`](https://github.com/TaeeunKil/kafrust/actions/runs/31681439569),
qualifying plaintext controller routing and per-partition success decoding.
The same preferred/no-op path over three-broker SASL_SSL with SCRAM-SHA-256
passed in the complete matrix
[`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180).
Unclean election remains an explicit, data-loss-sensitive operation and is not
part of the default smoke gate.

The current development line also implements broker-local DescribeLogDirs v1-v5
negotiation with typed replica size, offset lag, future-log, volume-capacity,
and cordoned-state fields. Focused wire and injected-broker tests pass. The
Kafka 3.7.2 three-broker filtered query returned successful responses from all
three brokers, including `/tmp/kafka-logs` partition size and volume capacity,
in run [`31682889124`](https://github.com/TaeeunKil/kafrust/actions/runs/31682889124).
The same filtered broker-1/2/3 query passed over three-broker SASL_SSL with
SCRAM-SHA-256 in the complete matrix
[`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180),
qualifying authenticated broker routing and response decoding for this profile.

The current development line also implements broker-local
AlterReplicaLogDirs v1-v2 negotiation. `AdminClient::alter_replica_log_dirs`
requires an explicit broker ID and destination path, preserves per-partition
error codes, and does not replay a request after an ambiguous send. Focused
wire and injected-broker tests pass. The Kafka 3.7.2 three-broker profile in
the complete matrix moved a disposable replica to `/tmp/kafka-logs-2` and
observed `future=false` completion in
[`31688516207`](https://github.com/TaeeunKil/kafrust/actions/runs/31688516207).
The same configured movement over three-broker SASL_SSL with SCRAM-SHA-256
passed in the complete matrix
[`31691204180`](https://github.com/TaeeunKil/kafrust/actions/runs/31691204180).
This qualifies the tested authenticated multi-broker path; it is not a claim
that a destination directory is portable across arbitrary clusters, nor that
an ambiguous mutation send is safe to replay.

Release `v0.2.19` additionally qualifies Fetch v12 forwarding of the last
fetched partition leader epoch and consumer-group `Earliest`/`Latest` recovery
when a committed offset is no longer retained. The dedicated offset-reset
topic and the complete 17-job matrix passed for Kafka 3.7.2, 3.8.1, 3.9.1,
and 4.3.1 in [`31663188419`](https://github.com/TaeeunKil/kafrust/actions/runs/31663188419).

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

An earlier complete `Live Kafka Smoke` matrix in
[`31601732149`](https://github.com/TaeeunKil/kafrust/actions/runs/31601732149)
passed all 17 jobs at commit `65b607e`. It reran the supported Kafka 3.7.2,
3.8.1, 3.9.1, and 4.3.1 plaintext profiles, TLS, SASL_PLAINTEXT,
SASL_SSL/SCRAM, OAUTHBEARER, ACL administration, multi-broker failover, and
KIP-848 paths. The matrix also passed the current `DescribeProducers` and
`DescribeTransactions` examples on single-node and multi-broker profiles,
including the secured failover profiles. This qualifies the new client-side
retry implementation against the supported live matrix. The follow-up run
below adds the first in-flight Admin broker-stop qualification for
DeleteRecords.

The follow-up matrix in
[`31607006237`](https://github.com/TaeeunKil/kafrust/actions/runs/31607006237)
passed all jobs at commit `fc0cf7b`. Its Kafka 4.3.1 single-node and
three-broker KIP-848 jobs executed the member-aware Admin offset example,
covering OffsetFetch v9 and OffsetCommit v9 with a live member ID and member
epoch. The same path also passed on three-broker SASL_PLAINTEXT and
SASL_SSL/SCRAM profiles.

The latest matrix in
[`31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960)
passed all jobs at commit `17cae6e`. Its Kafka 3.7.2 three-broker job held
leader-routed DeleteRecords v1 and DescribeProducers v0, coordinator-routed
DescribeTransactions v0, DescribeGroups v1, OffsetFetch v2, and
state-idempotent OffsetCommit v2, plus broker-routed DescribeConfigs v1 and
ListGroups v1, before TCP transmission. It stopped the relevant broker, leader,
or coordinator, released the requests, and observed retry recovery for all
eight operations; the ListGroups gate recorded `retries=7` while broker 1
restarted. This proves deterministic in-flight recovery for the documented
read paths and exact-offset administrative commits; other coordinator-routed
writes remain separate live fault-injection gates.

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
routing through the configured security transports. The secured three-broker
SASL_SSL/SCRAM profile now holds Admin DescribeGroups, OffsetFetch, and
exact-offset OffsetCommit before transmission, stops the group coordinator,
and completes all three operations with `retries=1` after rediscovery in
[`31698102459`](https://github.com/TaeeunKil/kafrust/actions/runs/31698102459)
([job](https://github.com/TaeeunKil/kafrust/actions/runs/31698102459/job/94440433930)).
This qualifies the secured coordinator read paths and state-idempotent
OffsetCommit failure-injection path for that three-broker profile; other
coordinator-routed mutations remain separate workload-specific gates.

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

Controller-routed administrative writes now retry only the pre-transmission
controller discovery after bootstrap or Metadata transport or retryable
response failure. The controller request itself remains single-attempt, so a
timeout or connection drop after a mutation was sent is surfaced instead of
blindly duplicating the operation. Focused CreateTopics coverage verifies both
discovery recovery cases and the existing live write paths retain their
partial-result semantics.

The read-only `DescribeGroups v1` admin path also reconnects and retries after
an established coordinator connection drops. Its focused mock-broker test
verifies the failed request, coordinator rediscovery, and successful group
description on the replacement connection.

The read-only `DescribeAcls v1` admin path now retries broker transport
failures, request timeouts, and retryable top-level broker responses within the
same bounded budget. The focused mock-broker regression test verifies a dropped
request and a successful typed ACL response on the replacement connection;
authorizer-specific broker-stop qualification remains separate.

The read-only `DescribeClientQuotas v0` path applies the same bounded retry
policy to transport failures, request timeouts, and retryable top-level broker
responses. Its focused mock-broker regression test verifies that the typed
filter is resent after a dropped request and the quota result is preserved;
StandardAuthorizer permission and broker-stop qualification remain separate.

The read-only `DescribeProducers v0` path retries retryable Metadata topic or
partition responses, leader movement, metadata convergence failures, transport
disconnects, and request timeouts through a fresh Metadata v1 lookup. A
transient per-partition leader error causes the whole read to be re-routed so
the final typed response is assembled from the current leaders.
`DescribeTransactions v0` applies the same bounded retry
policy to transaction-coordinator discovery, coordinator transport failures,
and transient per-ID coordinator errors. Focused mock-broker tests cover a
dropped leader/coordinator request and transient leader/coordinator responses.
These tests prove client-side recovery. DescribeCluster and ListTopics also
have live broker-stop injection coverage with `retries=1` each in
[`31620595346`](https://github.com/TaeeunKil/kafrust/actions/runs/31620595346).
DeleteRecords, DescribeProducers,
DescribeTransactions, DescribeGroups, OffsetFetch, exact-offset OffsetCommit,
DescribeConfigs, and ListGroups also have live broker-stop injection coverage
in the three-broker profile in
[`31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960);
the initial ListGroups Metadata discovery additionally retries a dropped
bootstrap response before broker enumeration.
other coordinator-routed writes remain separate qualification items.

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
| Apache Kafka 3.7.2 | three-broker KRaft | classic eager sticky consumer assignor; mixed-topic subscriptions, multi-member transfer, transient-member rollback, and member-loss recovery | [`Live Kafka Smoke`, run `31668518895`](https://github.com/TaeeunKil/kafrust/actions/runs/31668518895) on 2026-08-13 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | SASL_PLAINTEXT with SASL/PLAIN; transaction/group coordinator, producer, and direct-consumer broker-stop recovery | [`Live Kafka Smoke`, run `31554396594`](https://github.com/TaeeunKil/kafrust/actions/runs/31554396594) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | SASL_SSL with SCRAM-SHA-256; group-coordinator and partition-leader broker-stop recovery | [`Live Kafka Smoke`, run `31568412595`](https://github.com/TaeeunKil/kafrust/actions/runs/31568412595) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft | SASL_SSL with SCRAM-SHA-256; safe transactional producer reinitialization after transaction-coordinator broker stop | [`Live Kafka Smoke`, run `31572745537`](https://github.com/TaeeunKil/kafrust/actions/runs/31572745537) on 2026-08-12 | Passing; old producer outcome remains explicitly unknown |
| Apache Kafka 3.7.2 | three-broker KRaft | PLAINTEXT; repeated partition-leader broker-stop recovery for producer and direct consumer | [`Live Kafka Smoke`, run `31573662135`](https://github.com/TaeeunKil/kafrust/actions/runs/31573662135) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | single-node KRaft | DescribeProducers v0 leader routing and DescribeTransactions v0 coordinator routing | [`Live Kafka Smoke`, run `31589394777`](https://github.com/TaeeunKil/kafrust/actions/runs/31589394777) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft; PLAINTEXT and SASL_SSL SCRAM failover profiles | DescribeProducers v0 leader routing; DescribeTransactions v0 coordinator routing | [`Live Kafka Smoke`, run `31589394777`](https://github.com/TaeeunKil/kafrust/actions/runs/31589394777) on 2026-08-12 | Passing |
| Apache Kafka 4.3.1 | single-node and three-broker KRaft; three-broker SASL_PLAINTEXT and SASL_SSL/SCRAM | KIP-848 member-aware Admin OffsetFetch v9 and OffsetCommit v9 | [`Live Kafka Smoke`, run `31607006237`](https://github.com/TaeeunKil/kafrust/actions/runs/31607006237) on 2026-08-12 | Passing; plaintext and secured profiles |
| Apache Kafka 3.7.2 | three-broker KRaft | In-flight leader-routed DeleteRecords v1 and DescribeProducers v0, coordinator-routed DescribeTransactions v0, DescribeGroups v1, OffsetFetch v2, and exact-offset OffsetCommit v2, plus broker-routed DescribeConfigs v1 and ListGroups v1, pre-transmission gates, broker stops, fresh discovery/retry | [`Live Kafka Smoke`, run `31616181960`](https://github.com/TaeeunKil/kafrust/actions/runs/31616181960) on 2026-08-12 | Passing; ListGroups recorded `retries=7` |
| Apache Kafka 3.7.2 | three-broker KRaft with SASL_SSL/SCRAM-SHA-256 | In-flight coordinator-routed Admin DescribeGroups v1, OffsetFetch v2, and exact-offset OffsetCommit v2, coordinator stop, secured rediscovery/retry | [`Live Kafka Smoke`, run `31698102459`](https://github.com/TaeeunKil/kafrust/actions/runs/31698102459) on 2026-08-13 | Passing; all recorded `retries=1` |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | single-node KRaft; Kafka 3.7.2 also three-broker KRaft; Kafka 3.7.2 TLS, SASL_PLAINTEXT, and SASL_SSL SCRAM single-node profiles | classic AlterConfigs v1 complete-map topic replacement followed by IncrementalAlterConfigs v0 | [Plaintext run `31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872); [secured run `31674680581`](https://github.com/TaeeunKil/kafrust/actions/runs/31674680581) on 2026-08-13 | Passing; plaintext and secured admin lifecycle profiles |
| Apache Kafka 3.7.2 | three-broker KRaft | Metadata v1 `DescribeCluster` and `ListTopics` bootstrap failover after broker 1 stop | [`Live Kafka Smoke`, run `31620595346`](https://github.com/TaeeunKil/kafrust/actions/runs/31620595346) on 2026-08-12 | Passing; `retries=1` for each path |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | single-node KRaft | Produce `acks=0` immediate and batch dispatch | `Live Kafka Smoke`, manual run `31464933145` on 2026-08-11 | Passing; broker acceptance is intentionally unconfirmed |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1; Kafka 4.3.1 KIP-848 | single-node KRaft | opt-in automatic consumer-group commit and restored positions | [`Live Kafka Smoke`, run `31593984640`](https://github.com/TaeeunKil/kafrust/actions/runs/31593984640) on 2026-08-12 | Passing; at-least-once tradeoff |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | single-node KRaft | classic consumer-group offset listing and administrative alteration | [`Live Kafka Smoke`, run `31595485915`](https://github.com/TaeeunKil/kafrust/actions/runs/31595485915) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2 | three-broker KRaft; TLS; SASL_PLAINTEXT; SASL_SSL with SCRAM-SHA-256 | classic consumer-group offset listing and administrative alteration | [`Live Kafka Smoke`, run `31597505667`](https://github.com/TaeeunKil/kafrust/actions/runs/31597505667) on 2026-08-12 | Passing |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | complete 17-job KRaft matrix; plaintext, TLS, SASL, OAUTHBEARER, ACL, multi-broker, and KIP-848 profiles | Full smoke plus Kafka 3.7.2 multi-broker DeleteRecords and DescribeProducers leader-stop recovery | [`Live Kafka Smoke`, run `31630339333`](https://github.com/TaeeunKil/kafrust/actions/runs/31630339333) on 2026-08-13 | Passing |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | complete 17-job KRaft matrix; plaintext, TLS, SASL, OAUTHBEARER, ACL, multi-broker, and KIP-848 profiles | ListTransactions broker-shard aggregation, topic-ID Produce v13 with name-based v12/v11/v9 fallback, rack-aware Fetch v12, and existing failover gates | [`Live Kafka Smoke`, run `31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947) on 2026-08-13 | Passing; Produce selected v13 on Kafka 4.3.1, v11 on 3.8.1/3.9.1, and v9 on 3.7.2; ListTransactions examples returned records on single-node and 3.7.2 multi-broker profiles |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | complete 17-job KRaft matrix; dedicated offset-reset topic | Fetch v12 last-fetched leader epoch; group `Earliest`/`Latest` reset with committed out-of-range recovery | [`Live Kafka Smoke`, run `31663188419`](https://github.com/TaeeunKil/kafrust/actions/runs/31663188419) on 2026-08-13 | Passing |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | complete 17-job KRaft matrix; single-node retention topic | direct assigned-consumer `Earliest` recovery after Admin `DeleteRecords` moves the low watermark past the current position | [`Live Kafka Smoke`, run `31717934296`](https://github.com/TaeeunKil/kafrust/actions/runs/31717934296) on 2026-08-13 | Passing; controlled retained-log boundary only, not arbitrary retention timing or unclean-election data loss |
| Apache Kafka 3.7.2 | three-broker KRaft; replicated retention topic | classic consumer-group committed offset recovery after Admin `DeleteRecords` moves the low watermark past the committed position | [`Live Kafka Smoke`, run `31727573855`](https://github.com/TaeeunKil/kafrust/actions/runs/31727573855); job `Kafka 3.7.2 multi-broker`, step `Run multi-broker consumer-group retained-log recovery` | Passing; controlled group retained-log boundary; arbitrary retention timing and unclean-election data loss remain separate |
| Apache Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 | complete 17-job KRaft matrix; plaintext, TLS, SASL, SCRAM, OAUTHBEARER, multi-broker, and KIP-848 profiles | v0.2.26 direct-consumer leader-epoch recovery regression matrix | [`Live Kafka Smoke`, run `31677617186`](https://github.com/TaeeunKil/kafrust/actions/runs/31677617186) on 2026-08-13 | Passing; leader-epoch log-truncation injection and arbitrary retention timing are not claimed |
| Apache Kafka 3.7.2 | three-broker KRaft; repeated leader failover | v0.2.26 direct assigned-consumer automatic leader-epoch recovery after the second leader broker stop | [`Live Kafka Smoke`, run `31679167875`](https://github.com/TaeeunKil/kafrust/actions/runs/31679167875) on 2026-08-13 | Passing; group rebalance and unclean-election data-loss scenarios remain separate |
| Apache Kafka 3.7.2 | three-broker KRaft | classic `ConsumerGroup` automatic leader-epoch recovery after the assigned partition's leader broker stop | [`Live Kafka Smoke`, run `31700020132`](https://github.com/TaeeunKil/kafrust/actions/runs/31700020132) on 2026-08-13; job [`94446655280`](https://github.com/TaeeunKil/kafrust/actions/runs/31700020132/job/94446655280) | Passing; membership preserved through the tested failover; KIP-848 and unclean-election data-loss scenarios remain separate |
| Apache Kafka 3.7.2 | three-broker KRaft; classic consumer group; coordinator and partition leader intentionally colocated | group rejoin and post-failover record consumption when one stopped broker is both the group coordinator and target partition leader | [`Live Kafka Smoke`, run `31723663771`](https://github.com/TaeeunKil/kafrust/actions/runs/31723663771); job steps `Run consumer-group leader-epoch failover`, `Prepare combined coordinator and partition leader failover`, and `Run combined coordinator and partition leader failover` | Passing; plaintext classic path; secured and broader combined fault matrices remain separate |
| Apache Kafka 3.7.2 | three-broker KRaft; classic consumer group; `SASL_PLAINTEXT`; coordinator and partition leader intentionally colocated | authenticated group rejoin and post-failover record consumption when one stopped broker is both the group coordinator and target partition leader | [`Live Kafka Smoke`, run `31725607371`](https://github.com/TaeeunKil/kafrust/actions/runs/31725607371); job `Kafka 3.7.2 multi-broker SASL_PLAINTEXT failover`, step `Run SASL combined coordinator and partition leader failover` | Passing; SASL/PLAIN classic path; SASL_SSL/SCRAM KIP-848 and broader combined fault matrices remain separate |
| Apache Kafka 4.3.1 | three-broker KRaft; KIP-848 consumer protocol; PLAINTEXT | group rejoin and post-failover record consumption when one stopped broker is both the group coordinator and target partition leader | [`Live Kafka Smoke`, run `31723663771`](https://github.com/TaeeunKil/kafrust/actions/runs/31723663771); job steps `Prepare KIP-848 combined coordinator and partition leader failover` and `Run KIP-848 combined coordinator and partition leader failover` | Passing; plaintext KIP-848 path only; broader combined fault matrices remain separate |
| Apache Kafka 4.3.1 | three-broker KRaft; KIP-848 consumer protocol; `SASL_SSL` with SCRAM-SHA-256 | authenticated group rejoin and post-failover record consumption when one stopped broker is both the group coordinator and target partition leader | [`Live Kafka Smoke`, run `31727573855`](https://github.com/TaeeunKil/kafrust/actions/runs/31727573855); job `Kafka 4.3.1 multi-broker SASL_SSL SCRAM KIP-848 failover`, step `Run KIP-848 SCRAM combined coordinator and partition leader failover` | Passing; this secured KIP-848 path only; broader transaction and fault matrices remain separate |
| Published `kafrust 0.2.27` and `kafrust-protocol 0.2.27` | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 and 4.3.1 single-node profiles | published Admin `describe_cluster`, idempotent producer, direct consumer, and classic/KIP-848 consumer-group poll and leave | [`Published Crate Smoke`, run `31729003352`](https://github.com/TaeeunKil/kafrust/actions/runs/31729003352) on 2026-08-13 | Passing; validates published runtime linkage for these representative profiles, not the full replacement or multi-broker claim |
| Published `kafrust 0.2.27` with `tls` and matching protocol crate | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node `SASL_SSL` with SCRAM-SHA-256 | published TLS/SCRAM Admin, idempotent producer, direct consumer, and classic consumer-group poll and leave | [`Published Crate Smoke`, run `31729868783`](https://github.com/TaeeunKil/kafrust/actions/runs/31729868783) on 2026-08-13 | Passing; validates the tested published security profile, not every security provider, topology, or failure mode |
| Published `kafrust 0.2.27` transaction path | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, and Kafka 3.7.2 `SASL_SSL`/SCRAM | aborted transaction followed by committed transaction; `ReadCommitted` hides the aborted record and returns the committed record | [`Published Crate Smoke`, run `31730411006`](https://github.com/TaeeunKil/kafrust/actions/runs/31730411006) on 2026-08-13 | Passing; representative published transaction semantics only, not every failure or throughput workload |
| Published `kafrust 0.2.27` compression paths | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 single-node | Gzip, Snappy, LZ4, and Zstd producer compression with direct fetch, transaction commit/abort, and `ReadCommitted` verification | [`Published Crate Smoke`, run `31731421599`](https://github.com/TaeeunKil/kafrust/actions/runs/31731421599) on 2026-08-13 | Passing; published codec roundtrips only, not codec-specific throughput or failure qualification |
| Published `kafrust 0.2.27` Admin lifecycle | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 `SASL_SSL`/SCRAM, and four compression profiles | public `AdminClient` topic create, metadata list, topic config describe, and topic delete | [`Published Crate Smoke`, run `31731934027`](https://github.com/TaeeunKil/kafrust/actions/runs/31731934027) on 2026-08-13 | Passing; representative Admin runtime only, not every Admin API or authorization policy |
| Published `kafrust 0.2.28` and `kafrust-protocol 0.2.28` | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 `SASL_SSL`/SCRAM, and Gzip/Snappy/LZ4/Zstd profiles | published Admin lifecycle, idempotent producer, transactions and `ReadCommitted`, direct consumer, group read, per-record offset commit, same-group leave/rejoin, and post-commit resume without replay | [`Published Crate Smoke`, run `31734198869`](https://github.com/TaeeunKil/kafrust/actions/runs/31734198869) on 2026-08-13 | Passing; representative published runtime and offset-restore evidence, not the full replacement, multi-broker, authorization, or workload claim |
| Published `kafrust 0.2.28` multi-broker failover | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 three-broker KRaft, replication factor 3, classic group | observed three brokers, committed a replicated-topic record, stopped its partition leader, verified replica leader movement, then produced and consumed a post-failover record after same-group rejoin | [`Published Multi-Broker Smoke`, run `31735177161`](https://github.com/TaeeunKil/kafrust/actions/runs/31735177161) on 2026-08-13 | Passing; one published classic leader-failover workload only, not every multi-broker topology, security profile, or failure mode |
| Published `kafrust 0.2.28` KIP-848 multi-broker failover | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft, replication factor 3, KIP-848 `consumer` group | observed three brokers, committed a replicated-topic record, stopped its partition leader, verified replica leader movement, then produced and consumed a post-failover record after KIP-848 same-group rejoin | [`Published Multi-Broker Smoke`, run `31735762087`](https://github.com/TaeeunKil/kafrust/actions/runs/31735762087) on 2026-08-14 | Passing; one published KIP-848 leader-failover workload only, not every multi-member topology, security profile, or failure mode |
| Apache Kafka 3.7.2 | three-broker KRaft | EndTxn response loss, typed unknown outcome, same-transactional-ID recovery, and `read_committed` reconciliation | [`Live Kafka Smoke`, run `31708995196`](https://github.com/TaeeunKil/kafrust/actions/runs/31708995196); job [`94476744970`](https://github.com/TaeeunKil/kafrust/actions/runs/31708995196/job/94476744970) on 2026-08-13 | Passing at gate level; the surrounding run later failed in an existing consumer-group leader-epoch gate |
| Apache Kafka 3.7.2 and 4.3.1 | three-broker KRaft; classic and KIP-848; PLAINTEXT, SASL_PLAINTEXT, and SASL_SSL/SCRAM | consumer-group leader-epoch recovery with heartbeat-preserved membership and coordinator-isolated partition failover | [`Live Kafka Smoke`, run `31716400583`](https://github.com/TaeeunKil/kafrust/actions/runs/31716400583) on 2026-08-13 | Passing; broader data-loss/log-retention and combined-fault matrices remain separate |

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
  lifecycle against Kafka 3.7.2 and Kafka 4.3.1. The read-only
  `describe_cluster` and `list_topics` paths retry metadata transport and
  timeout failures within the bounded `AdminClient` budget. `list_topics` also
  retries transient topic/partition metadata errors while preserving final
  topic-level partial errors; focused mock-broker coverage verifies both paths.
- ACL create, describe, and delete through `AdminClient` with typed bindings,
  filters, and partial outcomes. The focused ACL authorizer job in manual run
  `31457478358` passed create -> describe -> delete against Kafka 3.7.2
  StandardAuthorizer using the explicitly configured `User:ANONYMOUS`
  superuser. The example polls successful DescribeAcls responses for bounded
  authorizer propagation after CreateAcls before asserting visibility.
- Client quota set, exact-filter describe, and remove through `AdminClient`
  with typed entities, `FLOAT64` values, and per-entity outcomes. The focused
  ACL authorizer job in manual run `31459874329` passed the roundtrip against
  Kafka 3.7.2 StandardAuthorizer using `User:ANONYMOUS`; the example uses
  bounded polling because KRaft quota metadata becomes visible asynchronously.
- SCRAM credential upsert, describe, and delete through `AdminClient` using
  flexible API v0 request/response encoding. The SASL_SSL SCRAM profile passed
  this roundtrip against Kafka 3.7.2 in manual run `31461980967`.
- Delegation-token create, describe, renew, and expire through `AdminClient`
  use negotiated Kafka API versions, controller routing, and redacted HMAC
  handling. Focused protocol and injected-controller tests pass. The opt-in
  Kafka 4.3.1 SASL_PLAINTEXT lifecycle gate passed in the complete 17-job
  matrix at commit `9d3916f` in
  [`31688516207`](https://github.com/TaeeunKil/kafrust/actions/runs/31688516207)
  (job [`94410181682`](https://github.com/TaeeunKil/kafrust/actions/runs/31688516207/job/94410181682)).
  The job exercised create, describe, renew, and immediate expire against a
  three-broker KRaft cluster; secured coverage beyond this SASL_PLAINTEXT
  profile and post-transmission mutation recovery remain separate claims.
  The same lifecycle also passed over TLS with SCRAM-SHA-256 against Kafka
  3.7.2 in the complete matrix
  [`31689260396`](https://github.com/TaeeunKil/kafrust/actions/runs/31689260396).
  This qualifies the documented single-broker SASL_SSL/SCRAM path. The same
  create, describe, renew, and immediate-expire lifecycle also passed over
  three-broker SASL_SSL with SCRAM-SHA-256 in the complete matrix
  [`31691911558`](https://github.com/TaeeunKil/kafrust/actions/runs/31691911558)
  (job [`94420894174`](https://github.com/TaeeunKil/kafrust/actions/runs/31691911558/job/94420894174)).
  Token-authenticated data-plane failover and post-transmission mutation
  recovery remain separate claims.
- The read-only `DescribeUserScramCredentials v0` path retries transport,
  timeout, and retryable top-level broker failures within the bounded
  `AdminClient` budget; focused mock-broker coverage verifies a dropped request
  is re-sent with the typed user filter. Live credential-policy and
  broker-stop qualification remains separate.
- Controller-routed `AlterPartitionReassignments v0` accepts replica targets or
  cancellation requests, while `ListPartitionReassignments v0` exposes the
  current, adding, and removing replica sets. The read-only listing path
  re-discovers the controller after transport, timeout, or retryable broker
  failures, with focused mock-broker coverage for a dropped request. The
  three-broker Kafka 3.7.2 profile passed submission followed by bounded
  completion polling in manual run `31462962605`; live listing broker-stop
  recovery remains a separate release gate.
- The `cooperative-sticky` consumer strategy encodes Kafka consumer protocol
  Subscription v1 owned partitions, preserves valid ownership, and stages
  transfers across rejoin cycles. Focused tests cover ownership preservation,
  new-member balancing, and empty-assignment encoding. The Kafka 3.7.2
  three-broker profile passed the cooperative group example in manual run
  `31464021305`. Manual run
  [`31474626799`](https://github.com/TaeeunKil/kafrust/actions/runs/31474626799)
  additionally passed multi-member ownership transfer, transient-member
  rollback, and member-loss recovery.
- The classic eager `sticky` strategy now encodes Kafka's previous-assignment
  Subscription v0 `user_data` schema, accepts both the legacy v0 and generation-
  carrying v1 user-data forms, preserves valid ownership, and applies transfers
  in the current SyncGroup assignment. Leader-side classic subscription parsing
  accepts the append-only v0-v3 envelope used by Kafka 3.7 through current
  clients. Focused tests cover wire bytes, generation metadata, balancing, and
  immediate transfer, duplicate-claim invalidation, and mixed-topic candidate
  ordering. The Kafka 3.7.2 three-broker live matrix also passed
  multi-member transfer, transient-member rollback, and member-loss recovery in
  [`31668518895`](https://github.com/TaeeunKil/kafrust/actions/runs/31668518895).
  This verifies the documented eager sticky workflow, not exact parity with
  every Kafka assignor edge case or arbitrary mixed-subscription workload.
- Produce `acks=0` encodes the requested Produce API version, writes and flushes
  the request, and returns unknown-offset metadata without attempting to read a
  response. Immediate and batch examples passed against Kafka 3.7.2, 3.8.1,
  3.9.1, and 4.3.1 in manual run `31464933145`. This mode intentionally cannot
  report broker or partition-level failures after the write.
- IncrementalAlterConfigs v0 followed by DescribeConfigs v1 verification.
  Manual run `30061073263` passed this update-and-readback path against Kafka
  3.7.2 and Kafka 4.3.1.
- Classic AlterConfigs v1 has a typed `TopicConfigUpdate` API, protocol
  fixtures, injected-broker partial-result coverage, and live complete-map
  replacement followed by incremental readback on the supported plaintext
  profiles in run [`31669906872`](https://github.com/TaeeunKil/kafrust/actions/runs/31669906872).
  The same lifecycle passed over Kafka 3.7.2 TLS, SASL/PLAIN, and SASL_SSL
  SCRAM-SHA-256 in [`31674680581`](https://github.com/TaeeunKil/kafrust/actions/runs/31674680581).
  Post-transmission mutation recovery remains a separate qualification gate.
- Coordinator-routed DescribeGroups v1. Manual run `30061497355` passed this
  path against Kafka 3.7.2 and Kafka 4.3.1 plaintext brokers plus the Kafka
  3.7.2 TLS, SASL_PLAINTEXT, and SASL_SSL profiles.
- Coordinator-routed OffsetDelete v0 with separate group-level and
  partition-level outcomes is covered by byte-level and injected-broker tests.
  Manual run `30062203069` passed offset deletion after group session expiry
  on Kafka 3.7.2 and 4.3.1 plaintext brokers, TLS, SASL_PLAINTEXT, SASL_SSL,
  and the three-broker profile. The three-broker job then passed its existing
  broker-stop failover sequence. Focused retry tests now cover
  `CoordinatorLoadInProgress`, `CoordinatorNotAvailable`, and `NotCoordinator`
  responses with fresh coordinator discovery; post-transmission transport
  failures remain single-attempt because the deletion outcome can be ambiguous.
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
  after deleting an empty group's final committed offset. Focused retry tests
  cover a transient `NotCoordinator` result with fresh discovery; an
  in-flight DeleteGroups broker-stop gate remains open.
- Manual run `30062587935` passed the complete plaintext path on Kafka 3.8.1
  and 3.9.1, including all four compression codecs, idempotent and
  transactional production, direct and group consumption, topic/config admin,
  group description, and offset deletion.
- High-level producer metadata lookup, leader routing, flexible `ApiVersions v3`
  capability negotiation, negotiated Produce API selection, single-record send,
  batch send, gzip-, Snappy-, LZ4-, and Zstd-compressed batch send, and buffered
  send with `acks=1`. When Produce v13 is advertised, the producer requests the
  topic UUID through Metadata v12 and uses topic-ID Produce v13; if UUID lookup
  is unavailable it falls back to name-based flexible Produce v12, then v11,
  then v9, RecordBatch on brokers that advertise it, and finally Produce v7,
  v3, or v2. The focused v9/v11/v12/v13 request fixtures and selection tests
  pass locally. The [Kafka 4.3 protocol](https://kafka.apache.org/43/design/protocol/)
  defines the topic-ID Produce v13 schema.
- `AdminClient::list_transactions` queries every metadata broker, negotiates
  ListTransactions v1 when available, falls back to v0, and aggregates the
  broker-local transaction-state shards. The complete 17-job matrix passed the
  unfiltered listing example in
  [`31648660947`](https://github.com/TaeeunKil/kafrust/actions/runs/31648660947).
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
- The newer merged-main benchmark run
[`31621648602`](https://github.com/TaeeunKil/kafrust/actions/runs/31621648602)
completed all four profiles with zero retries: 142,018 records/s for 100-byte
payloads, 68,037 for 1-KiB, 3,773 for 10-KiB, and 68,922 for 1-KiB Zstd.
  The newer five-minute soak run
  [`31621654970`](https://github.com/TaeeunKil/kafrust/actions/runs/31621654970)
processed 16,773,500 records across a ten-second outage and ended recovered
with zero in-flight requests and buffered records.
- The latest five-minute broker-restart soak run
  [`31631358207`](https://github.com/TaeeunKil/kafrust/actions/runs/31631358207)
  processed 16,847,700 1-KiB records across a ten-second outage, observed 148
  operation errors, 782 failed requests, and 1,035 retries, then recovered with
  zero in-flight requests and buffered records. The latest 20,000-record
  benchmark run [`31631563194`](https://github.com/TaeeunKil/kafrust/actions/runs/31631563194)
  measured 118,556 records/s for 100-byte payloads, 54,006 for 1-KiB, 3,030
  for 10-KiB, and 60,486 for 1-KiB Zstd, with zero retries in every profile.
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
  transactional `InitProducerId v0`, `AddPartitionsToTxn v0`, Produce v12/v11/v9/v7/v3,
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
- Gzip Produce v13/v12/v11/v9/v3 RecordBatch encoding and Fetch v4 RecordBatch
  decoding are covered by focused tests and the plaintext live smoke profile;
  topic-ID and name-based flexible Produce negotiation are qualified by the
  live matrix gate.
- Snappy Produce v12/v11/v9/v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused tests using Kafka-compatible Xerial framing and by the
  plaintext single-node and multi-broker live smoke profiles.
- LZ4 Produce v12/v11/v9/v3 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused standard-frame and decompression-limit tests and by the
  plaintext single-node and multi-broker live smoke profiles.
- Zstd Produce v12/v11/v9/v7 RecordBatch encoding and Fetch v4 RecordBatch decoding are
  covered by focused standard-frame, declared-window, and decompression-limit
  tests and by the plaintext single-node and multi-broker live smoke profiles.
- Direct consumer fetch from an assigned topic partition using Fetch v4 response
  decoding. When `client_rack` is configured, the consumer prefers flexible
  Fetch v12, falls back to Fetch v11 or Fetch v4, and follows
  `preferred_read_replica` when available;
  focused wire and injected multi-broker routing tests cover this path. The
  Kafka 3.7.2 three-broker rack-aware profile is also live-qualified in
  [`31640494509`](https://github.com/TaeeunKil/kafrust/actions/runs/31640494509).
  Fetch v11/v12 requests reuse a broker-scoped session across sequential polls,
  with session reset on assignment or position changes, reconnects, and fetch
  errors; the rack-aware path passed in
  [`31671783977`](https://github.com/TaeeunKil/kafrust/actions/runs/31671783977).
- Consumer group join, sync, heartbeat, poll, and offset commit through the alpha classic consumer group path with range assignment.
- Client-side regex topic subscription resolves Metadata v1 topic names before
  classic or KIP-848 joins and is covered by focused ordering, filtering, and
  no-match tests. The two-topic initial assignment path passed on Kafka 3.7.2,
  3.8.1, 3.9.1, and 4.3.1, including explicit rejoin and the corrected Kafka
  4.3.1 KIP-848 path, in [`Live Kafka Smoke`, run
  `31561944247`](https://github.com/TaeeunKil/kafrust/actions/runs/31561944247).
  A Kafka 3.7.2 StandardAuthorizer job then ran the same regex subscription
  over SASL_PLAINTEXT as a restricted user with one allowed and one denied
  topic. The initial assignment and explicit rejoin exposed only the allowed
  topic and fetched its record in
  [`31694784179`](https://github.com/TaeeunKil/kafrust/actions/runs/31694784179).
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
- The same Kafka 4.3.1 three-broker SASL_SSL/SCRAM KIP-848 profile then ran a
  second group through another coordinator broker-stop after the first broker
  had recovered. Both groups completed their poll and leave paths in
  [`Live Kafka Smoke` run `31695433295`](https://github.com/TaeeunKil/kafrust/actions/runs/31695433295),
  extending the secured evidence beyond a single coordinator failure.
- Consumer group assignments without committed offsets resolve
  `OffsetResetPolicy::Earliest` or `Latest` from the partition leader. The same
  policies recover a committed assignment whose offset is below the retained
  low watermark, while explicit absolute offsets preserve the broker error.
  The complete 17-job `Live Kafka Smoke` run
  [`31663188419`](https://github.com/TaeeunKil/kafrust/actions/runs/31663188419)
  passed this path on Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1.
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
- Production OAuth/OIDC provider compatibility beyond the local signed
  OIDC/JWKS fixture.
- The SCRAM multi-broker group-coordinator, partition-leader, and safe
  transactional producer reinitialization paths are claimed above. This does
  not claim transparent continuation of the old producer or the outcome of a
  transaction whose commit returned `INVALID_PRODUCER_EPOCH`.
- Broader transaction and KIP-848 failure-injection profiles beyond the
  verified coordinator broker-stop paths, including repeated transaction or
  KIP-848 coordinator faults and broader partition-leader fault matrices.
- Idempotent failure-injection profiles beyond the verified three-broker
  leader-stop recovery path. The ambiguous-response duplicate path is covered
  by a deterministic injected broker test.
- Transparent retry of an Admin mutation after its request has been
  transmitted. Bootstrap connection failures before transmission are retried
  within the bounded AdminClient budget; ambiguous post-transmission outcomes
  remain returned to the caller for explicit reconciliation.
- Automatic direct-consumer leader-epoch recovery is live-qualified for the
  repeated Kafka 3.7.2 three-broker leader-stop path in
  [`31679167875`](https://github.com/TaeeunKil/kafrust/actions/runs/31679167875).
  A controlled direct-consumer retained-log boundary is separately qualified
  through Admin `DeleteRecords` in [`31717934296`](https://github.com/TaeeunKil/kafrust/actions/runs/31717934296).
  This does not claim group rebalance recovery, arbitrary retention timing,
  unclean-election/data-loss scenarios, or transparent recovery for every
  consumer topology.
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
