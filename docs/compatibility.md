# Compatibility

## Artifact Qualification Boundary

The published compatibility record currently ends at the crates.io `0.3.5`
artifacts. The coordinated `0.3.6` package candidate is being qualified by
V1-00, but it is not published and must not be treated as published-artifact
or broker-compatibility evidence. Package-only checks use unpacked tarballs
outside the workspace; live and published claims remain tied to their named
source commits and workflow runs below.

## Protocol Surface Gate

The repository runs `scripts/check_protocol_api_surface.py` in CI before the
Rust build. This dependency-free check verifies that every protocol source
module is registered from `api/mod.rs`, that API keys are unique, and that the
reviewed Kafka API-key manifest has not drifted. It is a surface guard, not a
claim of byte-for-byte parity with Apache Kafka schemas; schema snapshots and
live broker qualification remain separate compatibility gates.

The repository also runs `scripts/check_apache_schema_versions.py`. Its
offline snapshot records the official Kafka 4.3.1 metadata for the high-risk
Produce, Fetch, OffsetCommit, OffsetFetch, and ConsumerGroupHeartbeat request
and response schemas. The gate checks API identity, local version bounds, and
the flexible-version boundary. It currently reports intentional local lag
  against newer stable versions (for example Fetch v18) without claiming
  those versions are implemented.
The scheduled/manual [`Apache Schema Audit`](../.github/workflows/apache-schema-audit.yml)
workflow runs the same checker with `--online-all`. That mode discovers every
top-level local request type, fetches its matching Apache 4.3.1 request and
response definitions, and checks message identity, API key, and local version
bounds. It is an API identity/version guard only; it does not establish
field-level or byte-for-byte parity, which still requires generated or golden
schema fixtures and targeted live qualification.
The latest online audit passed in
[`32384257319`](https://github.com/TaeeunKil/kafrust/actions/runs/32384257319),
checking 152 request/response schemas from the 76 local request types. The
audit accepts Apache's singleton ranges such as `validVersions: "0"` as a
closed range; its regression test covers that metadata shape.
  OffsetCommit and OffsetFetch v10 now have UUID-based protocol types,
  low-level `Client` methods, and high-level group/Admin negotiation with a v9
  fallback. The snapshot is an identity and version guard, not field-level or
  byte-for-byte parity; expanding it to all implemented APIs and adding
  generated/golden byte comparisons remain open.

kafrust compatibility claims are scoped to behavior that has been verified against a real broker. Protocol types can exist before the high-level client path has been validated against every broker version or deployment mode.

The KIP-848 `ConsumerGroupHeartbeat v0` and `v1` wire types, Metadata v12 topic UUID
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

The published `kafrust 0.3.1` artifact also passed ten independent abrupt-member
churn cycles on a three-broker Kafka 4.3.1 cluster for both classic group
protocol in [`32369216807`](https://github.com/TaeeunKil/kafrust/actions/runs/32369216807)
and KIP-848 consumer protocol in
[`32369216929`](https://github.com/TaeeunKil/kafrust/actions/runs/32369216929).
This is a bounded published rejoin/assignment gate. It does not claim
unbounded group churn, retention/restart combinations, every assignor, or
service-canary behavior.

High-level regex subscriptions now select `ConsumerGroupHeartbeat v1` and send
the regex as the broker subscription field while omitting the resolved topic
name list; explicit-name subscriptions continue to use v0. A duplex wire
regression test covers the v1 header and nullable regex field. Because Kafka
4.3.1 v1 requires a client-generated member ID, regex joins also generate and
retain a UUID-shaped member ID for the lifetime of the consumer. The source
refreshes Metadata v1/v12 and retries assignment mapping when a regex
assignment contains an unknown topic UUID.
The `Live Kafka Smoke` workflow now has a Kafka 4.3.1-only dynamic regex job
that creates and produces a matching topic after the group joins, then waits
for the assignment and record. The complete 17-job matrix, including the
Kafka 4.3.1 regex v1 initial join and post-join dynamic topic assignment,
passed in [`32382586220`](https://github.com/TaeeunKil/kafrust/actions/runs/32382586220).
The same initial-assignment, dynamic-topic, commit, and rejoin path passed from
published `kafrust 0.3.1` in a fresh external project in
[`32341967051`](https://github.com/TaeeunKil/kafrust/actions/runs/32341967051),
including a Kafka CLI group-offset check.

Direct and group consumers now select flexible topic-UUID Fetch v13 when the
broker advertises it and Metadata v12 resolves a non-zero topic ID. They retain
the negotiated name-based Fetch v12/v11/v4 fallback for older or incomplete
capability responses. Low-level `Client` also exposes the wire-equivalent
Fetch v14 path with tiered-storage error semantics and Fetch v15's tagged
replica-state request field. Fetch v16-v18 are also exposed at the low level:
v16 preserves KIP-951 node endpoints, v17 encodes follower directory IDs, and
v18 encodes follower high-watermarks. High-level consumers intentionally remain
on the capability-qualified v13 path until the newer follower-only fields have
live broker qualification.

Produce response versions 9 through 13 now preserve Kafka 4.3.1's tagged
`NodeEndpoints` field instead of discarding it. The typed
`ProduceNodeEndpointV10` result keeps broker ID, host, port, and nullable rack
data available to callers; a focused v13 fixture covers tag 0 decoding. This
closes the response-level endpoint and current-leader preservation slice. The
high-level producer now consumes a complete hint only for a retryable broker
error and only for the next attempt, avoiding an unconditional metadata
refresh when Kafka already supplied a usable replacement endpoint. This is
still covered by deterministic unit tests rather than a live leader-movement
qualification, so it does not expand the current broker compatibility claim.

## Current Compatibility Claim

The current development branch has a newer evidence set than the historical
release entries below. The latest recorded live matrix covered Kafka 3.7.2,
3.8.1, 3.9.1, and 4.3.1 with plaintext, TLS, SASL/PLAIN, SASL_SSL/SCRAM,
OAUTHBEARER, ACL authorization, multi-broker failover, transaction
reconciliation, and KIP-848 paths in
[`32382586220`](https://github.com/TaeeunKil/kafrust/actions/runs/32382586220).
The published `0.3.0` artifact also passed a 600-second Kafka 4.3.1
three-broker simultaneous-loss soak in
[`32230130048`](https://github.com/TaeeunKil/kafrust/actions/runs/32230130048).
These are live results for the named paths, not a claim of complete Kafka or
`rust-rdkafka` parity.

The published `0.3.1` artifact also passed the secured simultaneous-loss soak
against Kafka 4.3.1 in
[`32345082487`](https://github.com/TaeeunKil/kafrust/actions/runs/32345082487),
using SASL_SSL/SCRAM-SHA-256 and simultaneous ten-second outages of brokers 1
and 2. The 600-second run processed 19,667,500 records, recorded 263 expected
operation errors, 6 failed requests, and 9 retries, then recovered with zero
final in-flight or buffered records. This is the named published secured
availability/durability profile, not production SLO, unclean-election data-loss,
or service-canary evidence.

Mutual TLS client certificate authentication is implemented in the shared
configuration and high-level builders. The manual
[`live-mtls.yml`](../.github/workflows/live-mtls.yml) workflow passed its
current-source qualification on Kafka 3.7.2 in
[`32343983601`](https://github.com/TaeeunKil/kafrust/actions/runs/32343983601)
and Kafka 4.3.1 in
[`32343983397`](https://github.com/TaeeunKil/kafrust/actions/runs/32343983397).
Those runs covered the required-client-certificate handshake, Admin, producer,
direct consumer, consumer group, transactional/read-committed, low-level, and
coordinator roundtrips. Certificate rotation behavior remains a separate gate.

The published `kafrust 0.3.4` artifact also passed the dedicated external
OAUTHBEARER gate on Kafka 3.7.2 in
[`32411655133`](https://github.com/TaeeunKil/kafrust/actions/runs/32411655133).
The fresh external Cargo project resolved the client from crates.io and
verified SASL_SSL with Kafka's built-in unsecured OAUTHBEARER validator, an
async token provider, `AdminClient::describe_cluster`, an `acks=all` produce,
and direct-consumer readback. This qualifies the published basic
OAUTHBEARER/TLS path; signed OIDC/JWKS, provider discovery, key rotation, and
provider-specific outage behavior remain separate gates.

The published `kafrust 0.3.4` artifact also passed the signed OAUTHBEARER
variant on Kafka 3.7.2 in
[`32412721829`](https://github.com/TaeeunKil/kafrust/actions/runs/32412721829).
The external project used a local OIDC/JWKS HTTP fixture with an RS256 token;
Kafka verified the signature, issuer, audience, and subject before the
published async-provider client completed `describe_cluster`, `acks=all`
produce, and direct-consumer readback. This qualifies the published signed
local OIDC/JWKS path, not external provider discovery, token endpoint,
rotation, or outage semantics.

The published `kafrust 0.3.5` artifact then passed the signed OAUTHBEARER
re-authentication variant on Kafka 3.7.2 in
[`32420723537`](https://github.com/TaeeunKil/kafrust/actions/runs/32420723537).
The fresh external project resolved both `0.3.5` crates from crates.io, Kafka
validated the RS256 issuer/audience/subject claims, and the client completed
re-authentication on the same connection after the configured session
lifetime threshold; the client trace also confirmed the expected re-authentication
wire versions. This qualifies the local signed re-authentication path;
provider discovery, key rotation, and outage behavior remain separate gates.
If a provider fails after the re-authentication handshake has started, the
client marks that connection unusable so a higher-level pool can replace it;
this deterministic callback-failure behavior does not qualify external
provider outage semantics.

`CachedOAuthBearerTokenProvider` now single-flights refreshes shared by cloned
client configurations: concurrent broker connections re-check the cache after
the first refresh completes instead of invoking the token source independently.
The behavior is covered by a deterministic concurrent-refresh regression test;
the provider also rejects and does not cache a source result that is already
expired. These safeguards reduce refresh pressure and prevent stale source
results from reaching SASL, but they do not qualify external provider
discovery, key rotation, or provider-specific outage policy.

The current-source `UpdateFeatures` gate now sends a non-empty v1
`validate_only` request for the broker's finalized `metadata.version` level
and checks the returned top-level and feature-level success. Kafka 3.7.2
passed in [`32360936437`](https://github.com/TaeeunKil/kafrust/actions/runs/32360936437)
and Kafka 4.3.1 passed in
[`32361035007`](https://github.com/TaeeunKil/kafrust/actions/runs/32361035007).
This qualifies the populated request and response mapping only; it does not
claim a finalized upgrade/downgrade or state-changing mutation during
controller failover. A
separate SASL/PLAIN StandardAuthorizer matrix qualified UpdateFeatures
authorization on both Kafka versions in
[`32362301496`](https://github.com/TaeeunKil/kafrust/actions/runs/32362301496):
the restricted principal was rejected with error 31 and the administrator was
accepted. The separate three-broker controller-failover matrix
[`32363072430`](https://github.com/TaeeunKil/kafrust/actions/runs/32363072430)
stopped the active controller, waited for a new leader, and passed the same
v1 validation through the surviving broker quorum on both versions. The Kafka
4.3.1 lifecycle run [`32363428806`](https://github.com/TaeeunKil/kafrust/actions/runs/32363428806)
then performed `transaction.version` `2 -> 1 -> 2`, verifying the finalized
level after each state change. Metadata-version transitions across the declared
broker matrix and state-changing mutation during controller failover remain
open.

The current-source
`.github/workflows/live-delete-topics-authorization.yml` matrix passed on Kafka
3.7.2 and 4.3.1 in
[`32365120994`](https://github.com/TaeeunKil/kafrust/actions/runs/32365120994).
A restricted SASL/PLAIN principal with cluster and target-topic `Describe`, but
without delete permission, received `TopicAuthorizationFailed` (29) and the
topic remained present; the administrator then deleted it. This closes the
DeleteTopics authorization sub-gate only and does not establish universal ACL
or Admin mutation parity.

The current-source
`.github/workflows/live-create-partitions-authorization.yml` matrix passed on
Kafka 3.7.2 and 4.3.1 in
[`32366048755`](https://github.com/TaeeunKil/kafrust/actions/runs/32366048755).
A restricted SASL/PLAIN principal with cluster/topic discovery, but without
the partition-change permission, received `TopicAuthorizationFailed` (29) and
the one-partition topic remained unchanged; the administrator then expanded it
to two partitions and cleaned it up. This closes the CreatePartitions
authorization sub-gate only.

The current-source
`.github/workflows/live-alter-configs-authorization.yml` matrix passed on Kafka
3.7.2 and 4.3.1 in
[`32365666970`](https://github.com/TaeeunKil/kafrust/actions/runs/32365666970).
A restricted SASL/PLAIN principal with cluster/topic discovery and
`DescribeConfigs`, but without `AlterConfigs`, received
`TopicAuthorizationFailed` (29) and the existing `retention.ms` value remained
unchanged; the administrator then applied the replacement value and cleaned up
the topic. This closes the classic AlterConfigs authorization sub-gate only.

The current-source
`.github/workflows/live-incremental-alter-configs-authorization.yml` matrix
passed on Kafka 3.7.2 and 4.3.1 in
[`32366418605`](https://github.com/TaeeunKil/kafrust/actions/runs/32366418605).
The restricted SASL/PLAIN principal received `TopicAuthorizationFailed` (29)
and the existing `retention.ms` value remained unchanged, while the
administrator applied the incremental alteration. This closes the
IncrementalAlterConfigs authorization sub-gate only.

The current-source
`.github/workflows/live-alter-client-quotas-authorization.yml` matrix passed on
Kafka 3.7.2 and 4.3.1 in
[`32367537887`](https://github.com/TaeeunKil/kafrust/actions/runs/32367537887).
A restricted SASL/PLAIN principal with cluster discovery but without the quota
mutation permission received `ClusterAuthorizationFailed` (31); a separate
administrator readback confirmed no quota was applied before the administrator
applied and removed it. This closes the AlterClientQuotas authorization
sub-gate only.

The low-level `Client::api_versions_cached` helper now prefers flexible
`ApiVersions` v4 and falls back to v3 when a broker returns
`UNSUPPORTED_VERSION`. Existing higher-level paths retain their established
v3 negotiation until a live broker matrix qualifies making v4 the default.
The low-level client also exposes v5 request encoding with optional cluster and
node identity checks; those checks remain opt-in until a broker profile
explicitly qualifies them. The v4 response decoder preserves the v3 body shape
while retaining feature minimum version zero, matching Apache Kafka's current
protocol schema.

The high-level KIP-848 consumer assignment path now negotiates OffsetFetch
v10 on coordinators that advertise the Kafka 4.x UUID-based schema and when
Metadata v12 supplies every assigned topic UUID. It maps the response back to
the public topic-name assignment model and falls back to OffsetFetch v9 for
older coordinators or incomplete UUID metadata. Focused fault-injection
coverage exercises both the v9 fallback and v10 path, including a regex
assignment whose topic UUID changes during rejoin.

The same high-level KIP-848 commit path prefers OffsetCommit v10 when the
coordinator advertises it and uses the stored topic UUIDs to encode and
validate the response. OffsetCommit v9 remains the compatibility fallback for
older coordinators or incomplete topic-ID state. Foreground commits and the
bounded background commit worker share this negotiation policy; the local
fault-injection gate covers v10 after coordinator replacement and v9 fallback.

The member-aware Admin offset methods now expose the same negotiated v10/v9
behavior. Complete topic UUIDs supplied through
`ConsumerGroupOffsetQuery::topic_id` and `ConsumerGroupOffset::topic_id` avoid
metadata discovery; name-only calls resolve UUIDs through Metadata v12 before
using v10 and fall back to v9 when resolution or broker capability is absent.
Admin response UUIDs are mapped back to the caller's topic names. The current
source live matrix passed the Kafka 4.3.1 v10 path in
[`32339508792`](https://github.com/TaeeunKil/kafrust/actions/runs/32339508792),
and published `kafrust 0.3.1` passed a fresh external-project v10 OffsetFetch /
OffsetCommit smoke with Kafka CLI offset verification in
[`32341534974`](https://github.com/TaeeunKil/kafrust/actions/runs/32341534974).

The modern `ConsumerGroupDescribe` API 69 path is published-qualified on Kafka
4.3.1 in [`32408765709`](https://github.com/TaeeunKil/kafrust/actions/runs/32408765709).
A fresh external `kafrust 0.3.4` project joined a KIP-848 group and verified
`state=Stable`, group and assignment epochs of `2`, the joined member's
`member_type=1` and `member_epoch=2`, plus both current and target assignment
of the generated topic's partition 0. Kafka 3.7.2 is outside this gate because
ConsumerGroupDescribe is advertised by Kafka 3.8+ brokers.

The stable `ShareGroupDescribe` API 77 path is also published-qualified on
Kafka 4.3.1 in [`32410690294`](https://github.com/TaeeunKil/kafrust/actions/runs/32410690294).
A fresh external `kafrust 0.3.4` project joined a real ShareConsumer member and
verified `state=Stable`, group and assignment epochs of `3`, the joined member's
epoch of `3`, subscription metadata, assignment of topic partition 0, and
`authorized_operations=3400`. This qualifies the published ShareGroupDescribe
v1 read path; Kafka 4.0's removed early-access v0, security variants, and
multi-member Admin reads remain separate gates.

The same published `ShareGroupDescribe` path was rerun with `kafrust 0.3.5`
from a fresh external project in
[`32422303910`](https://github.com/TaeeunKil/kafrust/actions/runs/32422303910).
Kafka 4.3.1 again reported stable state, group/assignment epochs, member
epoch, the subscribed topic and partition assignment, and authorization bits.
This confirms the current published artifact; security variants and
multi-member Admin reads remain separate gates.

The current source also implements API key 74 across Kafka's version split:
v0 `ListClientMetricsResources` compatibility for Kafka 3.9-era brokers and
v1 `ListConfigResources` for Kafka 4.1+. The typed protocol, low-level
`Client`, and `AdminClient::list_config_resources` preserve the negotiated
version. v0 is selected only for an exact client-metrics filter; v1 supports
the full typed resource filter. Both paths have injected-broker coverage. The
manual [`live-list-config-resources.yml`](../.github/workflows/live-list-config-resources.yml)
workflow passed the v1 path together with the opt-in DescribeConfigs v4
metadata path and DescribeCluster v1 on Kafka 4.3.1 in
[`32342304005`](https://github.com/TaeeunKil/kafrust/actions/runs/32342304005).
The Kafka 3.9.1 v0 path was then separately verified with the client-metrics
resource filter in
[`32342680037`](https://github.com/TaeeunKil/kafrust/actions/runs/32342680037);
the published `kafrust 0.3.1` v0 path passed the same filter in
[`32343145837`](https://github.com/TaeeunKil/kafrust/actions/runs/32343145837).
The published `kafrust 0.3.1` v1 path and documentation-aware configuration
inspection also passed on Kafka 4.3.1 in
[`32343030081`](https://github.com/TaeeunKil/kafrust/actions/runs/32343030081).

The current source also implements Kafka 4.x `StreamsGroupDescribe` v0 (API key
89) through the typed protocol, low-level `Client`, and coordinator-routed
`AdminClient::describe_streams_groups` path. Focused wire and injected-broker
tests cover the flexible request and nested topology/member response. This is
source-level protocol coverage only: a live Kafka Streams application must
still qualify topology initialization, member metadata, task offsets, and
assignment state before this API is added to the published compatibility claim.
`StreamsGroupHeartbeat` v0 (API key 88) is now also exposed through the
low-level client and the alpha `StreamsGroupSession`, which manages member
epochs, nullable topology/task payloads, bounded reconnect/rejoin, and graceful
leave. The session is a manual heartbeat API, not a Kafka Streams DSL or task
processor. Background lifecycle management, assignment reconciliation, and
single/multi-broker membership qualification are covered by the live gates
below; complete topology processing, state stores, and Kafka Streams DSL
compatibility remain open.

The low-level `Client::streams_group_heartbeat_v0` path now covers the Kafka
4.x API 88 request and response schemas, including nullable topology metadata,
task assignments and offsets, member status, and Interactive Queries endpoint
partitions. The high-level session now covers the source-level member epoch and
shutdown path, and `StreamsGroupSessionHandle` owns bounded background
heartbeat scheduling, task-state commands, assignment snapshots, and graceful
close. The current Kafka 4.3.1 live gate passed in
[`32373425539`](https://github.com/TaeeunKil/kafrust/actions/runs/32373425539)
with join, assignment notification, background heartbeat, nullable task-offset
omission, two-member membership, member departure convergence, and clean leave.
The separate three-broker Kafka 4.3.1 gate passed in
[`32374858753`](https://github.com/TaeeunKil/kafrust/actions/runs/32374858753)
after stopping the elected Streams coordinator and observing a successful
post-stop heartbeat and clean leave. The published `kafrust 0.3.3` public
surface also compiles from a fresh external Cargo project with no workspace
path dependency on stable Rust and Rust 1.81, including
`StreamsGroupSessionHandle`, assignment-watch, and `StreamsTaskRuntime` APIs.
The separate published broker-runtime gate is recorded below and passes the
joined-session lifecycle; topology integration and a live Kafka Streams
application remain open.

Topic configuration inspection remains DescribeConfigs v1 by default. An
explicit `DescribeConfigsOptions::include_documentation(true)` request uses
DescribeConfigs v4 after capability negotiation and preserves Kafka's raw
configuration type and documentation fields. The v4 protocol and Admin
capability tests pass locally, and the Kafka 4.3.1 live check passed in
[`32342304005`](https://github.com/TaeeunKil/kafrust/actions/runs/32342304005).
The published `kafrust 0.3.1` external check also preserved configuration
type and documentation fields in
[`32343030081`](https://github.com/TaeeunKil/kafrust/actions/runs/32343030081).

The current source also exposes an opt-in `AdminClient::describe_cluster_with_options`
path using Kafka `DescribeCluster` API 60 v1, preserving cluster ID, endpoint
type, broker rack, and cluster authorized operations. The published `kafrust
0.3.3` path now passes from fresh external projects against Kafka 3.7.2 and
4.3.1 in [`32400851719`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851719)
and [`32400851830`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851830),
with crates.io resolution and lockfile verification. The gate checks API 60
cluster identity, authorized operations, broker endpoint metadata, and the
existing Metadata fallback. This qualifies the broker-bootstrap path only:
requesting `Controllers` through a broker listener returns Kafka's
`MISMATCHED_ENDPOINT_TYPE`, so controller bootstrap configuration and a
controller-endpoint qualification remain open.

The published `kafrust 0.3.4` path now passes from fresh external projects on
Kafka 3.7.2 and 4.3.1 in [`32403253526`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253526)
and [`32403253688`](https://github.com/TaeeunKil/kafrust/actions/runs/32403253688).
The gate resolves both crates from crates.io, verifies the generated lockfile,
checks API 60 broker and controller endpoint sets, cluster identity, authorized
operations, broker metadata, and Metadata fallback. This qualifies the
explicit controller-bootstrap path; broader Admin version, security, and
failure matrices remain open.

The same published gate also calls `AdminClient::describe_features` through
ApiVersions v3. Kafka 3.7.2 returned one supported and one finalized feature
with finalized epoch 68; Kafka 4.3.1 returned one supported and six finalized
features with finalized epoch 80. Both result artifacts recorded
`feature_metadata=true`.

The current-source `DescribeTopicPartitions` qualification passed on commit
`d833f9f`: Kafka 3.7.2 correctly returned the explicit unsupported capability
result, while Kafka 4.3.1 returned and decoded topic UUID, partition leader/ISR,
nullable replica-state fields, and the next paging cursor. The runs were
[`31778114684`](https://github.com/TaeeunKil/kafrust/actions/runs/31778114684)
and [`31778116310`](https://github.com/TaeeunKil/kafrust/actions/runs/31778116310).
This qualifies that API path only; it does not expand the complete Kafka API
or `rust-rdkafka` parity claim.

The current-source `DescribeQuorum` qualification passed on commit `1ffa9c8`
against Kafka 3.7.2 and 4.3.1 in
[`31781263986`](https://github.com/TaeeunKil/kafrust/actions/runs/31781263986)
and [`31781264035`](https://github.com/TaeeunKil/kafrust/actions/runs/31781264035).
The example connected through the explicitly configured controller listener,
negotiated v0 on Kafka 3.7.2 and v2 on Kafka 4.3.1, and decoded the metadata
quorum leader, high watermark, voters, observers, and v2 node listener data.
The same jobs also checked the Kafka quorum CLI through the broker listener;
this is a `DescribeQuorum` qualification, not a claim of complete KRaft admin
or controller protocol coverage.

The current-source `UpdateFeatures` qualification uses the highest version each
broker advertises. Kafka 3.7.2 and 4.3.1 both advertise API 57 v1 and pass an
empty `validate_only` request through the controller-routed Admin path. The
Kafka 3.7.2 and 4.3.1 runs passed in
[`32346412517`](https://github.com/TaeeunKil/kafrust/actions/runs/32346412517)
and
[`32346412771`](https://github.com/TaeeunKil/kafrust/actions/runs/32346412771).
An earlier Kafka 3.7.2 run exposed an incorrect v0 workflow expectation in
[`32346210023`](https://github.com/TaeeunKil/kafrust/actions/runs/32346210023);
the workflow now expects the version actually advertised by both brokers.
v0 fallback remains covered by typed and injected-client tests rather than
this live broker matrix.

The current source also implements KRaft `AddRaftVoter` v0/v1 (API key 80) and
`RemoveRaftVoter` v0 (API key 81) through typed protocol, low-level Client, and
controller-routed Admin paths. Flexible request/response encoding,
`ack_when_committed` version gating, and mutation outcome classification have
focused and injected-controller coverage. The new
`.github/workflows/live-dynamic-quorum.yml` workflow formats a Kafka 4.3.1
dynamic quorum from a standalone controller, provisions a second controller
with `--no-initial-controllers`, and runs the public
`admin_dynamic_quorum` example through Add/RemoveRaftVoter plus
DescribeQuorum convergence checks. The workflow passed in
[`32383742320`](https://github.com/TaeeunKil/kafrust/actions/runs/32383742320)
on 2026-08-20, revalidating voter/observer membership before the mutation, after
AddRaftVoter, and after RemoveRaftVoter. This qualifies the tested Kafka 4.3.1
dynamic membership path. The follow-up
`.github/workflows/live-dynamic-quorum-authorization.yml` gate passed in
[`32364161150`](https://github.com/TaeeunKil/kafrust/actions/runs/32364161150),
using a SASL/PLAIN controller listener to reject a restricted principal with
only cluster `Describe` (`ClusterAuthorizationFailed`, error 31) without
changing membership, then to complete the Add/Remove lifecycle as `User:admin`.
Broader controller failure workloads remain separate.

The high-level `ShareConsumer` path now has recorded Kafka 4.3.1 single-node,
three-broker leader-movement, active-heartbeat coordinator-loss, and repeated
in-process churn results in the Share workflows. Kafka 4.3.1 also passed the
response-loss ambiguity workflow, including unknown-outcome classification,
redelivery, and replacement acknowledgement, in
[`32347035522`](https://github.com/TaeeunKil/kafrust/actions/runs/32347035522).
The published `kafrust 0.3.3` artifact also passed a fresh external single-node
runtime in [`32384767744`](https://github.com/TaeeunKil/kafrust/actions/runs/32384767744)
and a 64-record acknowledgement/commit soak in
[`32385522647`](https://github.com/TaeeunKil/kafrust/actions/runs/32385522647).
Its fresh external three-broker leader-failover path also passed: the client
accepted a pre-failover record, survived broker 1 leader loss, and accepted a
post-failover record through surviving bootstrap servers in
[`32386637555`](https://github.com/TaeeunKil/kafrust/actions/runs/32386637555).
The published active-heartbeat workflow then passed three consecutive
coordinator-loss cycles, stopping coordinators 1, 3, and 1 while the external
heartbeat task remained alive and recovered through surviving bootstrap servers,
in [`32387564503`](https://github.com/TaeeunKil/kafrust/actions/runs/32387564503).
The remaining Share claim is narrower: long-running ambiguous reconciliation,
higher-cycle member-loss/rebalance beyond the four-cycle profile, long-running
multi-broker ownership, and broad published-artifact coverage remain open.
Bounded two-member published
ownership/assignment and one forced member-loss gate passed in
[`32388813780`](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780).
The forced member-loss result is recorded in
[`32390219711`](https://github.com/TaeeunKil/kafrust/actions/runs/32390219711).
The current evidence is listed in `docs/share-consumer.md` and the
corresponding workflow history.

The current-source acknowledgement soak additionally passed in
[`32369562416`](https://github.com/TaeeunKil/kafrust/actions/runs/32369562416),
processing 64 independently seeded records with one-at-a-time acknowledgement
and commit checks plus unique value/offset reconciliation. The published
`0.3.3` soak repeats that flow from a fresh external project and additionally
checks heartbeat shutdown and lockfile resolution. This remains bounded
single-node evidence; it does not establish long-running multi-broker
ownership, dynamic assignment/rebalance behavior, or production readiness.

Share Group State APIs 83-87 are tracked separately from this public
ShareConsumer claim. Kafka currently marks those wire APIs unstable, and broad
clients such as krafka intentionally omit the broker-internal state-persister
surface. Kafrust's typed implementation now uses the ordinary Group
coordinator for membership/admin operations and KIP-932 FindCoordinator v6
with a per-partition `group:topic-id:partition` key for durable state. The
topic-id segment follows Kafka's URL-safe Base64-without-padding
`Uuid::toString()` representation.
Multi-topic and multi-partition Admin requests are split by the per-resource
FindCoordinator v6 result and their partition-level responses are merged;
injected coverage exercises two different coordinators for Initialize, Read,
Write, Delete, and Summary. The current Kafka 4.3.1 replicated-state gate
passed in [`32398034582`](https://github.com/TaeeunKil/kafrust/actions/runs/32398034582):
the workflow verified replicated internal state, moved the Share coordinator
after broker loss, and completed post-failover read, summary, and delete. This
is still an unstable broker-internal API qualification, not general
ShareConsumer or `rust-rdkafka` replacement evidence.

The published `kafrust 0.3.3` package passed the same replicated-state
qualification from a fresh external Cargo project in
[`32399284180`](https://github.com/TaeeunKil/kafrust/actions/runs/32399284180):
Kafka 4.3.1 replicated `__share_group_state`, the Share coordinator moved after
its broker was stopped, and post-failover read, summary, and delete completed.
The workflow also verified `kafrust 0.3.3` and `kafrust-protocol 0.3.3` in the
generated `Cargo.lock`. This is published-artifact evidence for the tested
Share Group State path, not general ShareConsumer or `rust-rdkafka` replacement
evidence.

The complete 17-job `Live Kafka Smoke` matrix also passed on the current
connection-lifecycle hardening commit `e0e7e03` in
[`31765585666`](https://github.com/TaeeunKil/kafrust/actions/runs/31765585666).
The run covered Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1, TLS, SASL/PLAIN,
SASL/SCRAM, OAUTHBEARER validation, ACL administration, classic and KIP-848
consumer groups, transaction ambiguity handling, and multi-broker failover.
This is evidence that retiring a low-level connection after transport or
framing failure does not regress the tested reconnect and recovery paths.

The follow-up complete 17-job matrix passed on commit `9f96bf1` in
[`31766439591`](https://github.com/TaeeunKil/kafrust/actions/runs/31766439591)
after making classic and KIP-848 background heartbeat shutdown cancel
in-flight requests. The matrix covered the same supported broker, security,
consumer-group, transaction, and multi-broker failover profiles; the focused
shutdown tests additionally verify completion without waiting for a 30-second
broker request timeout.

The next complete 17-job matrix passed on commit `b96f369` in
[`31767641781`](https://github.com/TaeeunKil/kafrust/actions/runs/31767641781)
after exposing delayed KIP-848 assignment expiry as the typed
`Error::ConsumerGroupAssignmentTimeout` variant. The matrix again passed the
supported broker, security, ACL, consumer-group, transaction, and multi-broker
failover profiles, with no regression in the existing live paths.

The current-source direct comparison workflow passed on commit `1528862` in
[`31767095380`](https://github.com/TaeeunKil/kafrust/actions/runs/31767095380).
Against Kafka 4.3.1, fresh one-partition topics, 20,000 1-KiB records, and
batch size 200, kafrust measured 49,161.76 producer and 226,166.96 consumer
records/s. `rust-rdkafka 0.39.0` measured 84,235.49 producer and 220,147.27
consumer records/s. The kafrust side used the repository source while the
comparison-only `librdkafka` build remained isolated from kafrust dependencies;
this is a repeatable engineering baseline, not API/feature parity, a
production SLO, or a universal performance ranking.

The same comparison using the published `kafrust 0.2.30` artifact passed in
[`31768138519`](https://github.com/TaeeunKil/kafrust/actions/runs/31768138519).
The published artifact measured 51,834.49 producer and 233,242 consumer
records/s, while `rust-rdkafka 0.39.0` measured 87,752.37 producer and
176,675.91 consumer records/s. This confirms the comparison from a fresh
crates.io project; it remains a single workload baseline, not API/feature
parity, a production SLO, or a universal performance ranking.

The same comparison now passes from a fresh external project resolving the
published `kafrust 0.3.1` artifact in
[`32355261735`](https://github.com/TaeeunKil/kafrust/actions/runs/32355261735).
Against Kafka 4.3.1 with 20,000 1-KiB records and batch size 200, kafrust
measured 54,613.98 producer and 256,404.94 consumer records/s, while
`rust-rdkafka 0.39.0` measured 91,577.21 producer and 323,988.79 consumer
records/s. Two earlier `0.3.0` attempts failed because the published Fetch
v13 encoder placed flexible tagged fields before the request body; Kafka
closed the malformed request and kafrust surfaced `UnexpectedEof`. The
schema-order fix is in current source and the published `0.3.1` run passes.
This remains a single workload baseline, not API/feature parity, a production
SLO, or a universal performance ranking.

The `Published rust-rdkafka Comparison` workflow is manually dispatched and
keeps every historical run visible in GitHub Actions. A red run therefore
describes that specific historical input and commit, not the current latest
artifact. The workflow now defaults to three independent repetitions per
implementation, uses a fresh topic for every repetition, records the
repetition number in `comparison-results.jsonl`, and verifies that both
implementations produced the complete repetition set. This improves
measurement repeatability but still covers only the documented produce/fetch
profile; it does not establish feature parity, failure compatibility, or a
production SLO.

The repeated published `kafrust 0.3.1` profile passed in
[`32368443357`](https://github.com/TaeeunKil/kafrust/actions/runs/32368443357).
Across three repetitions, kafrust ranged from 45,767.55 to 61,927.71
producer records/s (median 56,239.28) and 217,064.20 to 292,880.32 consumer
records/s (median 283,033.26). `rust-rdkafka 0.39.0` ranged from 86,085.93 to
165,229.42 producer records/s (median 153,708.92) and 207,251.77 to
795,296.43 consumer records/s (median 786,673.72). The broad spread reinforces
that this is a reproducible profile, not a universal performance ranking.

The repeated published `kafrust 0.3.3` profile passed in
[`32381987301`](https://github.com/TaeeunKil/kafrust/actions/runs/32381987301)
after the comparison workflow default moved to the current release. Across
three repetitions, kafrust ranged from 63,555.46 to 74,929.50 producer
records/s (median 70,279.61) and 299,478.68 to 392,140.42 consumer records/s
(median 388,288.51). `rust-rdkafka 0.39.0` ranged from 73,808.39 to
173,215.35 producer records/s (median 161,271.11) and 256,167.65 to
837,349.05 consumer records/s (median 795,363.67). This is current published
artifact evidence for one produce/fetch workload, not feature parity, failure
compatibility, a production SLO, or a universal performance ranking.

The published `kafrust 0.3.4` comparison then passed in
[`32407748417`](https://github.com/TaeeunKil/kafrust/actions/runs/32407748417).
Across three repetitions of the same Kafka 4.3.1 produce/fetch profile, kafrust
measured 61,769.53 to 75,025.68 producer records/s (median 62,392.59) and
287,191.16 to 353,270.47 consumer records/s (median 330,812.61). `rust-rdkafka
0.39.0` measured 101,218.85 to 185,629.68 producer records/s (median
149,516.77) and 491,081.45 to 811,890.00 consumer records/s (median
580,226.56). This is the current published performance baseline: feature
parity, failure compatibility, and production SLOs remain separate gates.

The current published `kafrust 0.3.5` comparison passed in
[`32432679837`](https://github.com/TaeeunKil/kafrust/actions/runs/32432679837).
Across the same three-repetition Kafka 4.3.1 produce/fetch profile, kafrust
ranged from 54,556.11 to 66,631.10 producer records/s (median 65,451.79) and
272,020.73 to 359,731.73 consumer records/s (median 351,376.54).
`rust-rdkafka 0.39.0` ranged from 89,056.54 to 164,571.09 producer records/s
(median 162,827.84) and 273,994.67 to 733,472.79 consumer records/s (median
615,755.30). This is the current published workload baseline; it does not
claim API/feature parity, failure compatibility, or production SLOs.

The two red comparison runs shown in the workflow history,
[`32335003103`](https://github.com/TaeeunKil/kafrust/actions/runs/32335003103)
and [`32335827325`](https://github.com/TaeeunKil/kafrust/actions/runs/32335827325),
were manually run with the historical published `kafrust 0.3.0` input. They
failed before producing a result because that artifact sent malformed Fetch
v13 field ordering and surfaced `UnexpectedEof`; they are not failures of the
current `0.3.5` comparison. The workflow now preserves per-implementation
stderr logs as artifacts so future failures identify the exact repetition and
implementation.

The published `0.2.30` single-node broker-restart soak passed for 300 seconds
in [`31768319413`](https://github.com/TaeeunKil/kafrust/actions/runs/31768319413).
The fresh external project processed 21,597,600 1-KiB records through a
10-second Kafka 4.3.1 outage, recovered successfully, and ended with zero
in-flight or buffered records. The run observed 180 operation errors, 954
failed requests, and 1,243 retries.

The published `0.2.30` three-broker broker-restart soak also passed for 120
seconds in
[`31768320764`](https://github.com/TaeeunKil/kafrust/actions/runs/31768320764).
The fresh external project processed 4,404,900 1-KiB records across three
replicated partitions through the same outage, recovered successfully, and
ended with zero in-flight or buffered records. It observed 1 operation error,
21 failed requests, and 1,021 retries. These are published plaintext recovery
profiles, not secured soak or production SLO evidence.

The published `0.2.30` artifacts passed the seven-profile external
`Published Crate Smoke` run
[`31762679537`](https://github.com/TaeeunKil/kafrust/actions/runs/31762679537),
including Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, SASL_SSL/SCRAM, and all
four compression codecs. This confirms representative published-artifact
behavior; it does not expand the broker or workload compatibility claim.

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

The follow-up matrix on commit `0368f68`,
[`31756119753`](https://github.com/TaeeunKil/kafrust/actions/runs/31756119753),
passed all 17 jobs after correcting KIP-848 join completion for a member that
legitimately receives an empty assignment when another member owns the
available partition. The Kafka 4.3.1 single-node background-heartbeat rejoin
path now accepts `Some(empty)` as a delivered assignment while still waiting
for a response when the assignment is `None`. The same run also passed the
Kafka 4.3.1 multi-broker KIP-848, security, ACL, and failover jobs.

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
unclaimed for the published artifact. The current-source `9e390cf` response-
drop gate separately qualifies typed CreateTopics ambiguity and reconciliation
on Kafka 3.7.2 and 4.3.1 in [`31770443512`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443512)
and [`31770443484`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443484).
The follow-up `d73511c` gate applies the same response-drop proxy to
DeleteTopics and confirms deletion through list_topics on Kafka 3.7.2 and
4.3.1 in [`31771419625`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419625)
and [`31771419124`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419124).
The current-source
`.github/workflows/live-create-topics-authorization.yml` matrix also passed
on Kafka 3.7.2 and 4.3.1 in
[`32364633106`](https://github.com/TaeeunKil/kafrust/actions/runs/32364633106):
the restricted SASL/PLAIN principal received per-topic
`TopicAuthorizationFailed` (29) and no topic was created, while the
administrator completed create and cleanup. This is operation-specific
authorization evidence.
The current-source gate also qualifies API 64 `UnregisterBroker`: Kafka 3.7.2
and 4.3.1 both drop the first response, return a typed unknown outcome without
replay, and reconcile broker 1 as absent through `DescribeCluster` in
[`32357381909`](https://github.com/TaeeunKil/kafrust/actions/runs/32357381909)
and [`32357381879`](https://github.com/TaeeunKil/kafrust/actions/runs/32357381879).
These are operation-specific proofs, not a universal claim for every Admin
mutation. The four-job matrix then stopped broker 1, unregistered it through
the surviving controller quorum, restarted the same node, and verified broker
re-registration plus quorum health for Kafka 3.7.2, 3.8.1, 3.9.1, and 4.3.1 in
[`32359316032`](https://github.com/TaeeunKil/kafrust/actions/runs/32359316032).
This qualifies the multi-controller operational path for those versions; it
does not replace authorization- or workload-specific gates. The same
current-source gate also qualifies CreatePartitions by
expanding a topic from one to two partitions before dropping the response, on
Kafka 3.7.2 in [`31771635710`](https://github.com/TaeeunKil/kafrust/actions/runs/31771635710)
and Kafka 4.3.1 in [`31771636082`](https://github.com/TaeeunKil/kafrust/actions/runs/31771636082).
The current-source gate also qualifies IncrementalAlterConfigs by setting
`retention.ms`, dropping the response, and confirming the value with
DescribeConfigs on Kafka 3.7.2 in
[`31771864914`](https://github.com/TaeeunKil/kafrust/actions/runs/31771864914)
and Kafka 4.3.1 in
[`31771865024`](https://github.com/TaeeunKil/kafrust/actions/runs/31771865024).
Classic AlterConfigs is likewise qualified by replacing `retention.ms`,
dropping the response, and confirming the setting with DescribeConfigs on
Kafka 3.7.2 in [`31772009182`](https://github.com/TaeeunKil/kafrust/actions/runs/31772009182)
and Kafka 4.3.1 in [`31772008771`](https://github.com/TaeeunKil/kafrust/actions/runs/31772008771).
The same current-source gate qualifies ACL response loss with Kafka's
`StandardAuthorizer` and an explicitly configured `User:ANONYMOUS` test
superuser. CreateAcls response loss was reconciled through DescribeAcls on
Kafka 3.7.2 in [`31772403290`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403290)
and Kafka 4.3.1 in [`31772403077`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403077);
DeleteAcls response loss was reconciled by confirming the binding was absent
on Kafka 3.7.2 in [`31772470761`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470761)
and Kafka 4.3.1 in [`31772470590`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470590).
These are operation-specific proofs, not a universal authorization or
post-transmission guarantee for every Admin mutation.
The dedicated `UnregisterBroker` authorization gate then used
`StandardAuthorizer` with SASL/PLAIN on Kafka 3.7.2 and 4.3.1. A restricted
principal with only cluster discovery permission received
`ClusterAuthorizationFailed` (error code 31), while the configured
administrator principal was allowed to unregister the broker in
[`32360499520`](https://github.com/TaeeunKil/kafrust/actions/runs/32360499520).
This is an operation-specific permission result, not a claim about a
production ACL policy or every Admin API.
The current-source gate also qualifies AlterClientQuotas by setting
`producer_byte_rate`, dropping the response, and confirming the value through
DescribeClientQuotas on Kafka 3.7.2 in
[`31772731756`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731756)
and Kafka 4.3.1 in
[`31772731963`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731963).
This remains an operation-specific proof and does not establish target quota
policy or authorization parity.
The current-source gate also qualifies AlterUserScramCredentials by creating
an SCRAM-SHA-256 credential, dropping the response, and confirming the
credential through DescribeUserScramCredentials on Kafka 3.7.2 in
[`31772992221`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992221)
and Kafka 4.3.1 in
[`31772992381`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992381).
This is an operation-specific mutation proof, not a complete credential
policy or authenticated-channel parity claim.
The current-source gate also qualifies CreateDelegationToken over authenticated
SASL/PLAIN. After the response was dropped, DescribeDelegationTokens found a
new token owned by `User:admin` on Kafka 3.7.2 in
[`31773884142`](https://github.com/TaeeunKil/kafrust/actions/runs/31773884142)
and Kafka 4.3.1 in
[`31773883953`](https://github.com/TaeeunKil/kafrust/actions/runs/31773883953).
The gate redacts the token HMAC and proves only this mutation's
reconciliation boundary, not every delegation-token policy or lifecycle path.
The current-source gate also qualifies administrative OffsetCommit v2 after
waiting for coordinator readiness. It drops the response, surfaces
`AdminMutationOutcomeUnknown` without replaying the transmitted request, and
reconciles committed offset `42` through OffsetFetch on Kafka 3.7.2 in
[`31774729128`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729128)
and Kafka 4.3.1 in
[`31774729263`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729263).
This covers one classic offset mutation path only; DeleteGroups, member-aware
failures, and target authorization remain separate qualification.
The current-source gate also qualifies OffsetDelete v0 after a committed offset
is established. The response is dropped, the transmitted delete is classified
as `AdminMutationOutcomeUnknown` without replay, and OffsetFetch confirms the
partition offset is gone on Kafka 3.7.2 in
[`31774990676`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990676)
and Kafka 4.3.1 in
[`31774990554`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990554).
The current-source gate also qualifies DeleteGroups v1 after making the group
visible through ListGroups. The response is dropped, the transmitted delete
is classified as `AdminMutationOutcomeUnknown` without replay, and ListGroups
confirms the group is absent on Kafka 3.7.2 in
[`31775333815`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333815)
and Kafka 4.3.1 in
[`31775333736`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333736).
Active-member behavior, member-aware failures, and target authorization remain
separate.
The current-source gate also qualifies `AlterPartitionReassignments` v0. It
drops the real response after transmission, returns
`AdminMutationOutcomeUnknown` without replay, and reconciles the completed
replica movement through `ListPartitionReassignments` and final topic
metadata. Kafka 3.7.2 passed in
[`31776694068`](https://github.com/TaeeunKil/kafrust/actions/runs/31776694068)
and Kafka 4.3.1 passed in
[`31776695970`](https://github.com/TaeeunKil/kafrust/actions/runs/31776695970).
The final metadata check requires the requested `Replicas` order and the same
broker set in `Isr`; ISR order is not treated as significant. Authorization,
cancellation, broker-loss, and data-movement qualification remain separate.
The current-source KIP-848 member-aware gate also qualifies `OffsetCommit` v9
on Kafka 4.3.1 in
[`31777089953`](https://github.com/TaeeunKil/kafrust/actions/runs/31777089953)
(job [`94694703630`](https://github.com/TaeeunKil/kafrust/actions/runs/31777089953/job/94694703630)).
It drops the commit response after transmission, returns
`AdminMutationOutcomeUnknown` without replay, and reconciles offset `42`
through member-aware OffsetFetch and the Kafka consumer-groups CLI. Active
member deletion, member-aware offset deletion, and target authorization remain
separate.

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
The live smoke workflow now also asserts ListGroups v4 negotiation on Kafka
3.7.2 and ListGroups v5 state/type-filter negotiation on Kafka 4.3.1. Both
assertions passed in the complete current-source matrix
[`32382586220`](https://github.com/TaeeunKil/kafrust/actions/runs/32382586220),
so this modern path now has released-source evidence; authorization and
long-duration coordinator-churn behavior remain separate gates.
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
| Apache Kafka 3.7.2 and 4.3.1 | single-node KRaft with StandardAuthorizer; SASL/PLAIN | `UnregisterBroker` authorization: restricted cluster-discovery principal denied with error code 31 and administrator allowed | [`Live Unregister Broker Authorization`, run `32360499520`](https://github.com/TaeeunKil/kafrust/actions/runs/32360499520) on 2026-08-20 | Passing; operation-specific permission evidence only |
| Apache Kafka 3.7.2 and 4.3.1 | single-node KRaft with StandardAuthorizer; SASL/PLAIN | `DeleteTopics` authorization: restricted principal with cluster and target-topic `Describe` denied with error code 29, administrator allowed, and target topic retained after denial | [`Live DeleteTopics Authorization`, run `32365120994`](https://github.com/TaeeunKil/kafrust/actions/runs/32365120994) on 2026-08-20 | Passing; operation-specific permission evidence only |
| Apache Kafka 3.7.2 and 4.3.1 | single-node KRaft with StandardAuthorizer; SASL/PLAIN | classic `AlterConfigs` authorization: restricted principal with discovery and `DescribeConfigs` denied with error code 29, existing config retained, and administrator allowed | [`Live AlterConfigs Authorization`, run `32365666970`](https://github.com/TaeeunKil/kafrust/actions/runs/32365666970) on 2026-08-20 | Passing; operation-specific permission evidence only |
| Apache Kafka 3.7.2 and 4.3.1 | single-node KRaft with StandardAuthorizer; SASL/PLAIN | `IncrementalAlterConfigs` authorization: restricted principal denied with error code 29, existing config retained, and administrator allowed | [`Live IncrementalAlterConfigs Authorization`, run `32366418605`](https://github.com/TaeeunKil/kafrust/actions/runs/32366418605) on 2026-08-20 | Passing; operation-specific permission evidence only |
| Apache Kafka 3.7.2 and 4.3.1 | single-node KRaft with StandardAuthorizer; SASL/PLAIN | `AlterClientQuotas` authorization: restricted cluster-discovery principal denied with error code 31, administrator readback confirmed no quota mutation, administrator then allowed and removed quota | [`Live AlterClientQuotas Authorization`, run `32367537887`](https://github.com/TaeeunKil/kafrust/actions/runs/32367537887) on 2026-08-20 | Passing; operation-specific permission evidence only |
| Apache Kafka 3.7.2 and 4.3.1 | single-node KRaft with StandardAuthorizer; SASL/PLAIN | `CreatePartitions` authorization: restricted principal with cluster/topic discovery denied with error code 29 and one-partition topic retained, administrator allowed | [`Live CreatePartitions Authorization`, run `32366048755`](https://github.com/TaeeunKil/kafrust/actions/runs/32366048755) on 2026-08-20 | Passing; operation-specific permission evidence only |
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
| Apache Kafka 4.3.1 | single-node KRaft | API 74 `ListConfigResources` v1, documentation-aware `DescribeConfigs` v4, and `DescribeCluster` v1 | [`Live ListConfigResources Compatibility`, run `32342304005`](https://github.com/TaeeunKil/kafrust/actions/runs/32342304005) on 2026-08-20 | Passing; current-source MSRV gate |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node KRaft; stable Rust and Rust 1.81 | `DescribeCluster` API 60 v1 broker endpoint, cluster ID, authorized operations, broker metadata, and Metadata fallback | [`Published DescribeCluster`, run `32400851719`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851719) on 2026-08-21 | Passing; published broker-bootstrap path and lockfile verification; controller endpoint remains unqualified |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft; stable Rust and Rust 1.81 | `DescribeCluster` API 60 v1 broker endpoint, cluster ID, authorized operations, broker metadata, and Metadata fallback | [`Published DescribeCluster`, run `32400851830`](https://github.com/TaeeunKil/kafrust/actions/runs/32400851830) on 2026-08-21 | Passing; published broker-bootstrap path and lockfile verification; controller endpoint remains unqualified |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node KRaft; stable Rust and Rust 1.81; dedicated controller bootstrap | `DescribeCluster` API 60 v1 broker and controller endpoint sets plus `AdminClient::describe_features` through ApiVersions v3; cluster ID, authorized operations, broker metadata, and Metadata fallback | [`Published DescribeCluster`, run `32406914244`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914244) on 2026-08-21 | Passing; feature metadata `supported=1`, `finalized=1`, `epoch=68`, artifact `feature_metadata=true`, crates.io resolution, generated lockfile, and [`kafrust 0.3.4` docs.rs](https://docs.rs/kafrust/0.3.4/kafrust/) verified |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft; stable Rust and Rust 1.81; dedicated controller bootstrap | `DescribeCluster` API 60 v1 broker and controller endpoint sets plus `AdminClient::describe_features` through ApiVersions v3; cluster ID, authorized operations, broker metadata, and Metadata fallback | [`Published DescribeCluster`, run `32406914237`](https://github.com/TaeeunKil/kafrust/actions/runs/32406914237) on 2026-08-21 | Passing; feature metadata `supported=1`, `finalized=6`, `epoch=80`, artifact `feature_metadata=true`, crates.io resolution, generated lockfile, and [`kafrust 0.3.4` docs.rs](https://docs.rs/kafrust/0.3.4/kafrust/) verified |
| Apache Kafka 3.9.1 | single-node KRaft | API 74 `ListClientMetricsResources` v0 with the client-metrics resource filter | [`Live ListConfigResources Compatibility`, run `32342680037`](https://github.com/TaeeunKil/kafrust/actions/runs/32342680037) on 2026-08-20 | Passing; current-source MSRV gate |
| Apache Kafka 3.7.2 | single-node KRaft with required client certificates | current-source mTLS handshake, Admin, producer, direct consumer, classic consumer group, transactional/read-committed, low-level, and coordinator roundtrips | [`Live Mutual TLS`, run `32343983601`](https://github.com/TaeeunKil/kafrust/actions/runs/32343983601) on 2026-08-20 | Passing; short-lived generated certificates; rotation remains separate |
| Apache Kafka 4.3.1 | single-node KRaft with required client certificates | current-source mTLS handshake, Admin, producer, direct consumer, KIP-848 consumer group, transactional/read-committed, low-level, and coordinator roundtrips | [`Live Mutual TLS`, run `32343983397`](https://github.com/TaeeunKil/kafrust/actions/runs/32343983397) on 2026-08-20 | Passing; short-lived generated certificates; rotation remains separate |
| Published `kafrust 0.3.1` | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node KRaft with required client certificates | published mTLS Admin, producer, direct consumer, classic consumer group, and transactional/read-committed roundtrips | [`Published Mutual TLS`, run `32344673371`](https://github.com/TaeeunKil/kafrust/actions/runs/32344673371) on 2026-08-20 | Passing; crates.io resolution verified; certificate rotation remains separate |
| Published `kafrust 0.3.1` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft with required client certificates | published mTLS Admin, producer, direct consumer, KIP-848 consumer group, and transactional/read-committed roundtrips | [`Published Mutual TLS`, run `32344673373`](https://github.com/TaeeunKil/kafrust/actions/runs/32344673373) on 2026-08-20 | Passing; crates.io resolution verified; certificate rotation remains separate |
| Published `kafrust 0.3.1` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft | API 74 `ListConfigResources` v1 and documentation-aware `DescribeConfigs` v4 | [`Published API 74 Configuration Smoke`, run `32343030081`](https://github.com/TaeeunKil/kafrust/actions/runs/32343030081) on 2026-08-20 | Passing; crates.io resolution and v4 metadata verified |
| Published `kafrust 0.3.1` | fresh external Cargo project with no workspace path dependency; Kafka 3.9.1 single-node KRaft | API 74 `ListClientMetricsResources` v0 with the client-metrics resource filter | [`Published API 74 Configuration Smoke`, run `32343145837`](https://github.com/TaeeunKil/kafrust/actions/runs/32343145837) on 2026-08-20 | Passing; crates.io resolution verified |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft | API 74 `ListConfigResources` v1 and documentation-aware `DescribeConfigs` v4 | [`Published API 74 Configuration Smoke`, run `32382623298`](https://github.com/TaeeunKil/kafrust/actions/runs/32382623298) on 2026-08-20 | Passing; crates.io resolution and `0.3.3` lockfile verification completed |
| Published `kafrust 0.3.1` and `kafrust-protocol 0.3.1` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft | published member-aware Admin OffsetFetch v10 and OffsetCommit v10; committed offset verified by Kafka CLI | [`Published Member Offset Smoke`, run `32341534974`](https://github.com/TaeeunKil/kafrust/actions/runs/32341534974) on 2026-08-20 | Passing; crates.io resolution and API version marker verified |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft; KIP-848 consumer member joined by the fixture | `ConsumerGroupDescribe` API 69 through `AdminClient::describe_consumer_groups_modern`; stable state, group/assignment epochs, member type/epoch, and current/target topic-partition assignments | [`Published ConsumerGroupDescribe Smoke`, run `32408765709`](https://github.com/TaeeunKil/kafrust/actions/runs/32408765709) on 2026-08-21 | Passing; `state=Stable`, epochs `2/2`, `member_type=1`, `member_epoch=2`, and partition 0 assignment verified from crates.io artifact |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft with share groups enabled; active ShareConsumer member joined by the fixture | stable `ShareGroupDescribe` API 77 through `AdminClient::describe_share_groups`; state/epochs, member identity/epoch, subscription metadata, assignment, and authorization bits | [`Published ShareGroupDescribe Smoke`, run `32410690294`](https://github.com/TaeeunKil/kafrust/actions/runs/32410690294) on 2026-08-21 | Passing; `state=Stable`, epochs `3/3`, `member_epoch=3`, `assignment_partition=0`, `subscribed_topic=true`, and `authorized_operations=3400` verified from crates.io artifact |
| Published `kafrust 0.3.5` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft with share groups enabled; active ShareConsumer member joined by the fixture | stable `ShareGroupDescribe` API 77 through `AdminClient::describe_share_groups`; state/epochs, member identity/epoch, subscription metadata, assignment, and authorization bits | [`Published ShareGroupDescribe Smoke`, run `32422303910`](https://github.com/TaeeunKil/kafrust/actions/runs/32422303910) on 2026-08-21 | Passing; current published artifact reproduced the stable state, epochs, member epoch, topic/partition assignment, and authorization bits |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node KRaft over SASL_SSL; built-in unsecured OAUTHBEARER validator; stable Rust | published async OAuth token provider, `AdminClient::describe_cluster`, `acks=all` produce, and direct-consumer readback | [`Published OAUTHBEARER Smoke`, run `32411655133`](https://github.com/TaeeunKil/kafrust/actions/runs/32411655133) on 2026-08-21 | Passing; crates.io resolution and generated lockfile verified; signed OIDC/JWKS and provider-specific behavior remain separate |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node KRaft over SASL_SSL; local OIDC/JWKS validator; stable Rust | RS256-signed token validation for issuer/audience/subject, published async OAuth token provider, `AdminClient::describe_cluster`, `acks=all` produce, and direct-consumer readback | [`Published Signed OAUTHBEARER Smoke`, run `32412721829`](https://github.com/TaeeunKil/kafrust/actions/runs/32412721829) on 2026-08-21 | Passing; crates.io resolution and generated lockfile verified; external provider discovery, rotation, and outage behavior remain separate |
| Published `kafrust 0.3.5` | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node KRaft over SASL_SSL; local OIDC/JWKS validator; stable Rust | RS256-signed token validation plus provider-backed SASL re-authentication on the same connection after the broker session lifetime threshold | [`Published Signed OAUTHBEARER Smoke`, run `32420723537`](https://github.com/TaeeunKil/kafrust/actions/runs/32420723537) on 2026-08-21 | Passing; both `0.3.5` crates resolved from crates.io, initial signed authentication and produce/readback passed, same-connection re-authentication passed, and trace confirmed the expected wire versions; provider discovery, rotation, and outage behavior remain separate |
| Published `kafrust 0.3.5` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | published ShareConsumer produced and consumed a record before broker 1 leader loss, waited for a replacement leader, then produced and consumed a post-failover record through surviving bootstrap servers | [`Published ShareConsumer Multi-Broker Failover`, run `32423091397`](https://github.com/TaeeunKil/kafrust/actions/runs/32423091397) on 2026-08-21 | Passing in 1m43s; published dependency verification and both pre/post-failover paths passed; this is a leader-failover slice, not long-running ownership, multi-member churn, secured Share, or production SLO evidence |
| Published `kafrust 0.3.5` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft with share groups enabled; Rust 1.81 | 64 unique ShareConsumer records acquired, acknowledged, and committed, with unique offset accounting and published dependency verification | [`Published ShareConsumer Acknowledgement Soak`, run `32423629077`](https://github.com/TaeeunKil/kafrust/actions/runs/32423629077) on 2026-08-21 | Passing in 1m13s; `cycles=64`, `unique_offsets=64`; bounded acknowledgement/commit evidence only, not multi-member churn, broker failover, secured Share, or production SLO evidence |
| Apache Kafka 3.7.2 | single-node KRaft with the test-only broker telemetry plugin | KIP-714 client telemetry subscription, mutation recovery, ordinary payload delivery, and terminating push | [`Live Client Telemetry`, run `32422305042`](https://github.com/TaeeunKil/kafrust/actions/runs/32422305042) on 2026-08-21 | Passing; current-source single-broker gate only; payload-limit handling is qualified separately; secured, multi-broker, and long-running telemetry remain separate |
| Published `kafrust 0.3.1` and `kafrust-protocol 0.3.1` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft | KIP-848 regex v1 member assignment, dynamic topic discovery/record, offset commit, and explicit rejoin | [`Published KIP-848 Regex Smoke`, run `32341967051`](https://github.com/TaeeunKil/kafrust/actions/runs/32341967051) on 2026-08-20 | Passing; API key 68 v1 and UUID-shaped member ID observed in logs |
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
| Published `kafrust 0.3.2` and `kafrust-protocol 0.3.2` | fresh external Cargo project with no workspace path dependency; stable Rust and Rust 1.81 | published `StreamsGroupSession`, `StreamsGroupSessionHandle`, assignment snapshot, and assignment-watch API compile | crates.io [`kafrust 0.3.2`](https://crates.io/crates/kafrust/0.3.2), [`kafrust-protocol 0.3.2`](https://crates.io/crates/kafrust-protocol/0.3.2); [`Published Streams Surface`, run `32377502460`](https://github.com/TaeeunKil/kafrust/actions/runs/32377502460) on 2026-08-20 | Passing; package/API surface only, not broker runtime or complete Kafka Streams application compatibility |
| Published `kafrust 0.3.3` and `kafrust-protocol 0.3.3` | fresh external Cargo project with no workspace path dependency; stable Rust and Rust 1.81 | published `StreamsGroupSession`, `StreamsGroupSessionHandle`, `StreamsTaskRuntime`, canonical task IDs, and task transition API compile | crates.io [`kafrust 0.3.3`](https://crates.io/crates/kafrust/0.3.3), [`kafrust-protocol 0.3.3`](https://crates.io/crates/kafrust-protocol/0.3.3); [`Published Streams Surface`, run `32380345199`](https://github.com/TaeeunKil/kafrust/actions/runs/32380345199) on 2026-08-20 | Passing; package/API surface only, not broker runtime or complete Kafka Streams application compatibility |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft with Streams groups enabled; Rust 1.81 | published Streams group join, background heartbeat, `StreamsTaskRuntime` reconciliation call, dependency verification, and graceful leave | [`Published Streams Group Runtime`, run `32381356444`](https://github.com/TaeeunKil/kafrust/actions/runs/32381356444) on 2026-08-20 | Passing; published membership/runtime gate, not a complete Kafka Streams processor, state-store, or DSL compatibility claim |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | published ShareConsumer pre/post leader failover plus three consecutive active-heartbeat coordinator-loss recoveries | [`Published Share Multi-Broker Failover`, run `32386637555`](https://github.com/TaeeunKil/kafrust/actions/runs/32386637555); [`Published Share Heartbeat Failover`, run `32387564503`](https://github.com/TaeeunKil/kafrust/actions/runs/32387564503) on 2026-08-20 | Passing; published Share failover evidence, not long-running ownership, multi-member assignment/rebalance, or production SLO |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | two published ShareConsumer members joined one group, each accepted three records, and all six seeded partitions were observed exactly once across the members | [`Published ShareConsumer Multi-Member Ownership`, run `32388813780`](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780) on 2026-08-20 | Passing; bounded two-member ownership/assignment evidence only; dynamic member-loss/rebalance, long-running soak, and production SLO remain open |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | two published ShareConsumer members ran for 60 seconds, each accepted and observed 192 of 384 seeded records, reported `in_flight=0` before close, and all partition/offset pairs were unique with exact per-partition counts | [`Published ShareConsumer Multi-Member Ownership`, run `32391918666`](https://github.com/TaeeunKil/kafrust/actions/runs/32391918666) on 2026-08-20 | Passing; bounded 384-record multi-member soak and metrics-drain evidence only; member-loss/rebalance, backpressure SLO, and production readiness remain open |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81; six replicated partitions | two published ShareConsumer members remained active for 300 seconds, each retained three partitions, accepted and consumed 30 of 60 seeded records, and closed with `in_flight=0` and zero failed requests | [`Published ShareConsumer Multi-Member Ownership`, run `32404294014`](https://github.com/TaeeunKil/kafrust/actions/runs/32404294014) on 2026-08-20 | Passing; exact per-partition counts and unique partition/offset pairs verified from crates.io `0.3.4`; bounded long-running ownership evidence, not member-loss, backpressure SLO, or production readiness |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | after both members started heartbeats, member 2 was force-terminated; surviving member 1 rebalanced to all six partitions and accepted one record from each while member 2 produced no output | [`Published ShareConsumer Member Loss`, run `32390219711`](https://github.com/TaeeunKil/kafrust/actions/runs/32390219711) on 2026-08-20 | Passing; one published member-loss/reassignment profile, not repeated churn, long-running backpressure, or production SLO evidence |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | two forced member-loss cycles in one group: member 1 took over all six partitions, member 2 rejoined, then member 2 took over all six after member 1 stopped; 12 records had unique offsets | [`Published ShareConsumer Repeated Member Loss`, run `32391027028`](https://github.com/TaeeunKil/kafrust/actions/runs/32391027028) on 2026-08-20 | Passing; bounded two-cycle churn evidence, not higher-cycle, long-running backpressure, or production SLO evidence |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | three forced member-loss cycles in one group: ownership moved to member 1, then member 2, then a rejoined member 1; 18 records had three per partition and unique offsets, and the final survivor reported `consumed=6`, `in_flight=0`, and no failed requests | [`Published ShareConsumer Repeated Member Loss`, run `32392994232`](https://github.com/TaeeunKil/kafrust/actions/runs/32392994232) on 2026-08-20 | Passing; bounded three-cycle churn and final metrics-drain evidence, not higher-cycle, long-running backpressure, or production SLO evidence |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81 | four forced member-loss cycles in one group: ownership moved member 1 → member 2 → member 1 → member 2; 24 records had four per partition and unique offsets, and the final survivor reported `consumed=6`, `in_flight=0`, and no failed requests | [`Published ShareConsumer Repeated Member Loss`, run `32394453120`](https://github.com/TaeeunKil/kafrust/actions/runs/32394453120) on 2026-08-20 | Passing; bounded four-cycle churn and final metrics-drain evidence, not higher-cycle, long-running backpressure, or production SLO evidence |
| Published `kafrust 0.3.4` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with share groups enabled; Rust 1.81; four forced member-loss cycles and 180-second member runtime | ownership moved member 1 → member 2 → member 1 → member 2; 24 records had four per partition and unique offsets, and the final survivor owned all six partitions with `accepted=6`, `consumed=6`, `in_flight=0`, and zero failed requests | [`Published ShareConsumer Repeated Member Loss`, run `32405501232`](https://github.com/TaeeunKil/kafrust/actions/runs/32405501232) on 2026-08-20 | Passing; exact unique partition/offset verification and crates.io `0.3.4` lockfile verification; bounded repeated churn evidence, not higher-cycle, backpressure SLO, or production readiness |
| Published `kafrust 0.3.3` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with replicated Share Group State; Rust 1.81 | replicated `__share_group_state`, Share coordinator failover after broker stop, post-failover read/summary/delete, and `Cargo.lock` verification for `kafrust` plus `kafrust-protocol` | [`Published Share Group State Failover`, run `32399284180`](https://github.com/TaeeunKil/kafrust/actions/runs/32399284180) on 2026-08-21 | Passing; published unstable Share Group State qualification only; general ShareConsumer replacement, long-running SLO, and broader security/version matrices remain open |
| Published `kafrust 0.3.3` versus `rust-rdkafka 0.39.0` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft; comparison-only `librdkafka` build isolated from kafrust release dependencies | three independent repetitions of identical 20,000-record, 1-KiB payload, batch-size-200 produce/fetch profiles on fresh one-partition topics | [`Published rust-rdkafka Comparison`, run `32381987301`](https://github.com/TaeeunKil/kafrust/actions/runs/32381987301) on 2026-08-20 | Passing; kafrust median 70,279.61 producer and 388,288.51 consumer records/s; rust-rdkafka median 161,271.11 producer and 795,363.67 consumer records/s; one workload baseline only |
| Published `kafrust 0.3.4` versus `rust-rdkafka 0.39.0` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft; comparison-only `librdkafka` build isolated from kafrust release dependencies | three independent repetitions of identical 20,000-record, 1-KiB payload, batch-size-200 produce/fetch profiles on fresh one-partition topics | [`Published rust-rdkafka Comparison`, run `32407748417`](https://github.com/TaeeunKil/kafrust/actions/runs/32407748417) on 2026-08-21 | Passing; kafrust median 62,392.59 producer and 330,812.61 consumer records/s; rust-rdkafka median 149,516.77 producer and 580,226.56 consumer records/s; current workload baseline only |
| Published `kafrust 0.2.27` and `kafrust-protocol 0.2.27` | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 and 4.3.1 single-node profiles | published Admin `describe_cluster`, idempotent producer, direct consumer, and classic/KIP-848 consumer-group poll and leave | [`Published Crate Smoke`, run `31729003352`](https://github.com/TaeeunKil/kafrust/actions/runs/31729003352) on 2026-08-13 | Passing; validates published runtime linkage for these representative profiles, not the full replacement or multi-broker claim |
| Published `kafrust 0.2.27` with `tls` and matching protocol crate | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 single-node `SASL_SSL` with SCRAM-SHA-256 | published TLS/SCRAM Admin, idempotent producer, direct consumer, and classic consumer-group poll and leave | [`Published Crate Smoke`, run `31729868783`](https://github.com/TaeeunKil/kafrust/actions/runs/31729868783) on 2026-08-13 | Passing; validates the tested published security profile, not every security provider, topology, or failure mode |
| Published `kafrust 0.2.27` transaction path | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, and Kafka 3.7.2 `SASL_SSL`/SCRAM | aborted transaction followed by committed transaction; `ReadCommitted` hides the aborted record and returns the committed record | [`Published Crate Smoke`, run `31730411006`](https://github.com/TaeeunKil/kafrust/actions/runs/31730411006) on 2026-08-13 | Passing; representative published transaction semantics only, not every failure or throughput workload |
| Published `kafrust 0.2.27` compression paths | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 single-node | Gzip, Snappy, LZ4, and Zstd producer compression with direct fetch, transaction commit/abort, and `ReadCommitted` verification | [`Published Crate Smoke`, run `31731421599`](https://github.com/TaeeunKil/kafrust/actions/runs/31731421599) on 2026-08-13 | Passing; published codec roundtrips only, not codec-specific throughput or failure qualification |
| Published `kafrust 0.2.27` Admin lifecycle | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 `SASL_SSL`/SCRAM, and four compression profiles | public `AdminClient` topic create, metadata list, topic config describe, and topic delete | [`Published Crate Smoke`, run `31731934027`](https://github.com/TaeeunKil/kafrust/actions/runs/31731934027) on 2026-08-13 | Passing; representative Admin runtime only, not every Admin API or authorization policy |
| Published `kafrust 0.2.28` and `kafrust-protocol 0.2.28` | fresh external Cargo projects with no workspace path dependency; Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 `SASL_SSL`/SCRAM, and Gzip/Snappy/LZ4/Zstd profiles | published Admin lifecycle, active-group list/describe, classic or KIP-848 group offset reads, idempotent producer, transactions and `ReadCommitted`, direct consumer, group read, per-record offset commit, same-group leave/rejoin, and post-commit resume without replay | [`Published Crate Smoke`, run `31737581786`](https://github.com/TaeeunKil/kafrust/actions/runs/31737581786) on 2026-08-14 | Passing; representative published runtime and Admin/offset evidence, not the full replacement, multi-broker, authorization, or workload claim |
| Published `kafrust 0.3.5` | fresh external projects with no workspace path dependency; Kafka 3.7.2 classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 `SASL_SSL`/SCRAM, and Gzip/Snappy/LZ4/Zstd profiles | published producer, direct consumer, group, transaction/read-committed, TLS/SCRAM, and compression roundtrips | [`Published Crate Smoke`, run `32420987547`](https://github.com/TaeeunKil/kafrust/actions/runs/32420987547) on 2026-08-21 | Passing; all seven profiles resolved `0.3.5` from crates.io and verified generated lockfiles; representative published-artifact evidence, not the full replacement, multi-broker, authorization, or workload claim |
| Published `kafrust 0.3.5` | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft with `SASL_SSL`/SCRAM-SHA-256; Rust 1.81 | default 600-second published soak with a simultaneous ten-second stop of brokers 1 and 2, restart, recovery verification, and final `in_flight_requests=0` / `buffered_records=0` resource checks | [`Published Secure Multi-Broker Soak Smoke`, run `32440677496`](https://github.com/TaeeunKil/kafrust/actions/runs/32440677496) on 2026-08-21 | Passing; the published artifact and result artifact were verified; this closes one secured simultaneous-loss soak slice, not production SLO, unclean election, or the complete 1.0 fault matrix |
| Apache Kafka 4.3.1 | current-source single-node KRaft with Share groups enabled and an injected response-loss proxy | deliberately dropped `ShareAcknowledge` response followed by acknowledgement reconciliation, duplicate-safe delivery accounting, and clean shutdown verification | [`Live Kafka Share Acknowledgement Ambiguity`, run `32449038941`](https://github.com/TaeeunKil/kafrust/actions/runs/32449038941) on 2026-08-21 | Passing; response-loss semantics are qualified for this injected path, not every Share failure mode or long-running production SLO |
| Published `kafrust 0.2.28` multi-broker failover | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 three-broker KRaft, replication factor 3, classic group | observed three brokers, committed a replicated-topic record, stopped its partition leader, verified replica leader movement, then produced and consumed a post-failover record after same-group rejoin | [`Published Multi-Broker Smoke`, run `31735177161`](https://github.com/TaeeunKil/kafrust/actions/runs/31735177161) on 2026-08-13 | Passing; one published classic leader-failover workload only, not every multi-broker topology, security profile, or failure mode |
| Published `kafrust 0.2.28` KIP-848 multi-broker failover | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft, replication factor 3, KIP-848 `consumer` group | observed three brokers, committed a replicated-topic record, stopped its partition leader, verified replica leader movement, then produced and consumed a post-failover record after KIP-848 same-group rejoin | [`Published Multi-Broker Smoke`, run `31735762087`](https://github.com/TaeeunKil/kafrust/actions/runs/31735762087) on 2026-08-14 | Passing; one published KIP-848 leader-failover workload only, not every multi-member topology, security profile, or failure mode |
| Published `kafrust 0.2.28` classic multi-member rebalance | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 three-broker KRaft, replication factor 3, classic range group | started two members in one group, verified disjoint ownership of all six partitions, and consumed one published record from each partition | [`Published Group Rebalance Smoke`, run `31736939236`](https://github.com/TaeeunKil/kafrust/actions/runs/31736939236) on 2026-08-14 | Passing; representative two-member classic workload only, not every assignor, security profile, or failure mode |
| Published `kafrust 0.2.28` KIP-848 multi-member rebalance | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft, replication factor 3, KIP-848 `consumer` group | started two members in one group, verified disjoint ownership of all six partitions, and consumed one published record from each partition | [`Published Group Rebalance Smoke`, run `31736362411`](https://github.com/TaeeunKil/kafrust/actions/runs/31736362411) on 2026-08-14 | Passing; representative two-member KIP-848 workload only, not every assignor, security profile, or failure mode |
| Published `kafrust 0.2.30` classic multi-member rejoin position | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 three-broker KRaft, replication factor 3, classic range group | started two members, verified disjoint ownership and one record per partition, then preserved an explicit `seek(0)` position across same-member rejoin | [`Published Group Rebalance Smoke`, run `31763950353`](https://github.com/TaeeunKil/kafrust/actions/runs/31763950353) on 2026-08-14 | Passing; representative position contract only, not every assignor, security profile, or failure mode |
| Published `kafrust 0.2.30` KIP-848 multi-member rejoin position | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft, replication factor 3, KIP-848 `consumer` group | started two members, verified disjoint ownership and one record per partition, then preserved an explicit `seek(0)` position across same-member rejoin | [`Published Group Rebalance Smoke`, run `31763952591`](https://github.com/TaeeunKil/kafrust/actions/runs/31763952591) on 2026-08-14 | Passing; representative position contract only, not every assignor, security profile, or failure mode |
| Published `kafrust 0.2.28` secured multi-member rebalance | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 classic and Kafka 4.3.1 KIP-848 three-broker KRaft with `SASL_SSL` and SCRAM-SHA-256 | authenticated two members in one group, verified disjoint ownership of all six partitions, and consumed one published record from each partition | [Kafka 3.7.2 classic, run `31740436499`](https://github.com/TaeeunKil/kafrust/actions/runs/31740436499); [Kafka 4.3.1 KIP-848, run `31740567979`](https://github.com/TaeeunKil/kafrust/actions/runs/31740567979) on 2026-08-14 | Passing; representative published secured two-member workload only, not every assignor, security mechanism, or failure mode |
| Published `kafrust 0.2.28` transaction coordinator failover | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 three-broker KRaft, transaction state replication factor 3 | identified the transaction coordinator, stopped it while a transaction was open, committed through the replacement coordinator, and verified the result with `ReadCommitted` | [`Published Transaction Failover Smoke`, run `31738090052`](https://github.com/TaeeunKil/kafrust/actions/runs/31738090052) on 2026-08-14 | Passing; coordinator-stop recovery only, not every ambiguous outcome, fencing, security profile, or throughput workload |
| Published `kafrust 0.2.28` secured multi-broker failover | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 classic and Kafka 4.3.1 KIP-848 three-broker KRaft with `SASL_SSL` and SCRAM-SHA-256 | validated all three external TLS listeners, authenticated Admin and producer/group connections, committed before stopping broker 1's partition leader, then produced and consumed through the replacement leader after same-group rejoin | [Kafka 3.7.2 classic, run `31738997447`](https://github.com/TaeeunKil/kafrust/actions/runs/31738997447); [Kafka 4.3.1 KIP-848, run `31739154764`](https://github.com/TaeeunKil/kafrust/actions/runs/31739154764) on 2026-08-14 | Passing; representative published secured leader-failover workloads only, not coordinator-plus-leader colocation, every security mechanism, or the full 1.0 fault matrix |
| Published `kafrust 0.2.28` secured coordinator-plus-leader failover | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 classic and Kafka 4.3.1 KIP-848 three-broker KRaft with `SASL_SSL` and SCRAM-SHA-256 | listed the active group's coordinator, selected a replicated partition led by that same broker, stopped the combined coordinator/partition leader, then produced and consumed through the replacement leader after group rejoin | [Kafka 3.7.2 classic, run `31739763944`](https://github.com/TaeeunKil/kafrust/actions/runs/31739763944); [Kafka 4.3.1 KIP-848, run `31739927915`](https://github.com/TaeeunKil/kafrust/actions/runs/31739927915) on 2026-08-14 | Passing; representative published secured combined-fault workloads only, not repeated faults, every security mechanism, or the full 1.0 fault matrix |
| Published `kafrust 0.2.28` secured repeated leader failover | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 classic and Kafka 4.3.1 KIP-848 three-broker KRaft with `SASL_SSL` and SCRAM-SHA-256 | stopped broker 1's partition leader, verified published producer/group recovery, restarted it, stopped a different partition leader, and verified a second producer/group recovery | [Kafka 3.7.2 classic, run `31743322062`](https://github.com/TaeeunKil/kafrust/actions/runs/31743322062); [Kafka 4.3.1 KIP-848, run `31743497415`](https://github.com/TaeeunKil/kafrust/actions/runs/31743497415) on 2026-08-14 | Passing; repeated leader-failover workload only, not unclean election, simultaneous multi-broker loss, every security mechanism, or the full 1.0 fault matrix |
| Published `kafrust 0.2.28` secured transaction coordinator failover | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 and 4.3.1 three-broker KRaft with `SASL_SSL` and SCRAM-SHA-256, transaction state replication factor 3 | opened a transaction, stopped its transaction coordinator, committed through the replacement coordinator, and verified the committed record with an authenticated `ReadCommitted` consumer | [Kafka 3.7.2, run `31741012713`](https://github.com/TaeeunKil/kafrust/actions/runs/31741012713); [Kafka 4.3.1, run `31741137784`](https://github.com/TaeeunKil/kafrust/actions/runs/31741137784) on 2026-08-14 | Passing; coordinator-stop recovery only, not every ambiguous outcome, fencing, repeated fault, or throughput workload |
| Published `kafrust 0.2.28` restricted Admin authorization | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 and 4.3.1 single-node KRaft with `StandardAuthorizer`, `SASL_SSL`, and SCRAM-SHA-256 | authenticated as a non-superuser with cluster describe/idempotent-write, allowed topic config/read/write, and group permissions; verified denied topic config, topic create, and topic delete results | [Kafka 3.7.2, run `31741997691`](https://github.com/TaeeunKil/kafrust/actions/runs/31741997691); [Kafka 4.3.1, run `31742115305`](https://github.com/TaeeunKil/kafrust/actions/runs/31742115305) on 2026-08-14 | Passing; representative published authorization policy only, not every ACL pattern, Admin API, security provider, or mutation-failure workload |
| Published `kafrust 0.2.28` restricted Admin mutation and offset management | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 and 4.3.1 single-node KRaft with `StandardAuthorizer`, `SASL_SSL`, and SCRAM-SHA-256 | authenticated allowed `IncrementalAlterConfigs`, preserved denied topic config alteration, committed through a group, listed the committed offset, reset it through Admin OffsetCommit v2, and consumed from the reset position | [Kafka 3.7.2, run `31742788549`](https://github.com/TaeeunKil/kafrust/actions/runs/31742788549); [Kafka 4.3.1, run `31742924984`](https://github.com/TaeeunKil/kafrust/actions/runs/31742924984) on 2026-08-14 | Passing; representative published mutation and offset policy only, not every Admin mutation, ACL pattern, security provider, or ambiguous failure mode |
| Published `kafrust 0.2.28` performance baseline | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 and 4.3.1 single-node KRaft; no compression and Zstd | 10,000 records of 1-KiB payloads in batches of 200; measured producer/consumer throughput, batch p50/p95/p99 latency, retries, and final queue gauges | [`Published Performance Smoke`, run `31744206188`](https://github.com/TaeeunKil/kafrust/actions/runs/31744206188) on 2026-08-14 | Passing; producer 43.7k-48.9k records/s, consumer 210.6k-268.3k records/s, zero retries, and zero final in-flight/buffered records across all four profiles; baseline only, not a direct rust-rdkafka comparison or production SLO |
| Published `kafrust 0.2.28` versus `rust-rdkafka 0.39.0` direct comparison | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft; comparison-only `librdkafka` build isolated from kafrust release dependencies | identical 2,000-record, 1-KiB payload, batch-size-100 profiles on fresh one-partition topics; `Acks::Leader` versus `acks=1`; measured produce and consume throughput | [`Published rust-rdkafka Comparison`, run `31753172293`](https://github.com/TaeeunKil/kafrust/actions/runs/31753172293) on 2026-08-14 | Passing; kafrust produced 51,834/s and consumed 129,875/s, while rust-rdkafka produced 48,452/s and consumed 252,306/s; single profile baseline only, not API/feature parity, production SLO, or a universal performance ranking |
| Current-source kafrust commit `1528862` versus `rust-rdkafka 0.39.0` direct comparison | fresh external Cargo project using the repository source for kafrust; Kafka 4.3.1 single-node KRaft; comparison-only `librdkafka` build isolated from kafrust dependencies | identical 20,000-record, 1-KiB payload, batch-size-200 profiles on fresh one-partition topics; measured produce and consume throughput | [`Published rust-rdkafka Comparison`, run `31767095380`](https://github.com/TaeeunKil/kafrust/actions/runs/31767095380) on 2026-08-14 | Passing; kafrust produced 49,161.76/s and consumed 226,166.96/s, while rust-rdkafka produced 84,235.49/s and consumed 220,147.27/s; current-source baseline only, not API/feature parity, production SLO, or a universal performance ranking |
| Published `kafrust 0.2.30` versus `rust-rdkafka 0.39.0` direct comparison | fresh external Cargo project resolving `kafrust 0.2.30` from crates.io; Kafka 4.3.1 single-node KRaft; comparison-only `librdkafka` build isolated from kafrust dependencies | identical 20,000-record, 1-KiB payload, batch-size-200 profiles on fresh one-partition topics; measured produce and consume throughput | [`Published rust-rdkafka Comparison`, run `31768138519`](https://github.com/TaeeunKil/kafrust/actions/runs/31768138519) on 2026-08-14 | Passing; kafrust produced 51,834.49/s and consumed 233,242/s, while rust-rdkafka produced 87,752.37/s and consumed 176,675.91/s; published-artifact baseline only, not API/feature parity, production SLO, or a universal performance ranking |
| Published `kafrust 0.2.28` long-running broker-restart soak | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 single-node KRaft | 120-second, 1-KiB, batch-size-100 workload; stopped the broker after one third of the run, restarted it after 10 seconds, required observed error/recovery, record-count reconciliation, and zero final queue gauges | [`Published Soak Smoke`, run `31744827441`](https://github.com/TaeeunKil/kafrust/actions/runs/31744827441) on 2026-08-14 | Passing; 7,229,000 records, 173 operation errors, 982 failed requests, 1,210 retries, `recovered=true`, and zero final in-flight/buffered records; one published single-node recovery profile, not a multi-broker soak or production SLO |
| Published `kafrust 0.2.30` long-running broker-restart soak | fresh external Cargo project resolving `kafrust 0.2.30` from crates.io; Kafka 4.3.1 single-node KRaft | 300-second, 1-KiB, batch-size-100 workload; stopped the broker after one third of the run, restarted it after 10 seconds, required observed error/recovery, record-count reconciliation, and zero final queue gauges | [`Published Soak Smoke`, run `31768319413`](https://github.com/TaeeunKil/kafrust/actions/runs/31768319413) on 2026-08-14 | Passing; 21,597,600 records, 180 operation errors, 954 failed requests, 1,243 retries, `recovered=true`, and zero final in-flight/buffered records; published single-node plaintext evidence, not secured soak or production SLO |
| Published `kafrust 0.2.28` multi-broker broker-restart soak | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft, three replicated partitions | 120-second, 1-KiB, batch-size-100 workload; stopped broker 1 after one third of the run, restarted it after 10 seconds, required observed error/recovery, per-partition record reconciliation, and zero final queue gauges | [`Published Multi-Broker Soak Smoke`, run `31746182158`](https://github.com/TaeeunKil/kafrust/actions/runs/31746182158) on 2026-08-14 | Passing; 4,918,800 records, 1 operation error, 7 failed requests, 1,006 retries, `recovered=true`, and zero final in-flight/buffered records; one published plaintext multi-broker recovery profile, not secured, simultaneous-loss, or production SLO evidence |
| Published `kafrust 0.2.30` multi-broker broker-restart soak | fresh external Cargo project resolving `kafrust 0.2.30` from crates.io; Kafka 4.3.1 three-broker KRaft, three replicated partitions | 120-second, 1-KiB, batch-size-100 workload; stopped broker 1 after one third of the run, restarted it after 10 seconds, required observed error/recovery, per-partition record reconciliation, and zero final queue gauges | [`Published Multi-Broker Soak Smoke`, run `31768320764`](https://github.com/TaeeunKil/kafrust/actions/runs/31768320764) on 2026-08-14 | Passing; 4,404,900 records, 1 operation error, 21 failed requests, 1,021 retries, `recovered=true`, and zero final in-flight/buffered records; published plaintext multi-broker evidence, not secured soak or production SLO |
| Published `kafrust 0.2.28` secured multi-broker broker-restart soak | fresh external Cargo project with no workspace path dependency; Kafka 4.3.1 three-broker KRaft, SASL_SSL/SCRAM-SHA-256, three replicated partitions | 120-second, 1-KiB, batch-size-100 workload; stopped broker 1 after one third of the run, restarted it after 10 seconds, required TLS/SCRAM connectivity, retry-based recovery, per-partition record reconciliation, and zero final queue gauges | [`Published Secure Multi-Broker Soak Smoke`, run `31747389166`](https://github.com/TaeeunKil/kafrust/actions/runs/31747389166) on 2026-08-14 | Passing; 2,288,700 records, 0 operation errors, 1 failed request, 1,001 retries, `recovered=true`, and zero final in-flight/buffered records; one published secured recovery profile, not simultaneous-loss or production SLO evidence |
| Published `kafrust 0.2.28` simultaneous broker-loss soak | fresh external Cargo project with no workspace path dependency; Kafka 3.7.2 and 4.3.1 three-broker KRaft, three replicated partitions | 120-second, 1-KiB, batch-size-100 workload; stopped brokers 1 and 2 simultaneously after one third of the run, restarted both after 10 seconds, required retry-based recovery, per-partition record reconciliation, and zero final queue gauges | [Kafka 4.3.1, run `31748293446`](https://github.com/TaeeunKil/kafrust/actions/runs/31748293446); [Kafka 3.7.2, run `31748860976`](https://github.com/TaeeunKil/kafrust/actions/runs/31748860976) on 2026-08-14 | Passing; Kafka 4.3.1 processed 4,423,200 records with 999 retries and Kafka 3.7.2 processed 4,620,200 records with 1,008 retries; both had `recovered=true`, zero final in-flight/buffered records, and no sustained high-level failure; plaintext simultaneous-loss profiles, not secured or production SLO evidence |
| Published `kafrust 0.2.28` secured simultaneous broker-loss soak | fresh external Cargo project with no workspace path dependency and the `tls` feature; Kafka 3.7.2 and 4.3.1 three-broker KRaft with SASL_SSL/SCRAM-SHA-256, three replicated partitions, `Acks::All`, and `min.insync.replicas=2` | 60-second Kafka 3.7.2 and 120-second Kafka 4.3.1 workloads; stopped brokers 1 and 2 simultaneously after one third of each run, restarted both after 10 seconds, required replicated-ack record reconciliation, TLS/SCRAM retry recovery, and zero final queue gauges | [Kafka 3.7.2, run `31751812178`](https://github.com/TaeeunKil/kafrust/actions/runs/31751812178); [Kafka 4.3.1, run `31750274774`](https://github.com/TaeeunKil/kafrust/actions/runs/31750274774) on 2026-08-14 | Passing; Kafka 3.7.2 processed 686,700 records with 330 expected operation errors, 2 failed requests, and 3 retries; Kafka 4.3.1 processed 2,704,200 records with 282 expected operation errors, 2 failed requests, and 3 retries; both reported `recovered=true` with zero final in-flight/buffered records; secured simultaneous-loss durability/availability profiles, not unclean-election data-loss, production SLO, or service-canary evidence |
 | Published `kafrust 0.3.1` secured simultaneous broker-loss soak | fresh external Cargo project resolving `kafrust 0.3.1` from crates.io; Kafka 4.3.1 three-broker KRaft with SASL_SSL/SCRAM-SHA-256, three replicated partitions, simultaneous brokers 1 and 2 outage | 600-second, 1-KiB, batch-size-100 workload; stopped brokers 1 and 2 simultaneously after one third of the run, restarted both after 10 seconds, required published dependency verification, TLS/SCRAM retry recovery, per-partition record reconciliation, and zero final queue gauges | [`Published Secure Multi-Broker Soak Smoke`, run `32345082487`](https://github.com/TaeeunKil/kafrust/actions/runs/32345082487) on 2026-08-20 | Passing; 19,667,500 records, 263 operation errors, 6 failed requests, 9 retries, `recovered=true`, and zero final in-flight/buffered records; named secured simultaneous-loss profile, not production SLO, unclean-election data-loss, or service-canary evidence |
| Published `kafrust 0.3.3` plaintext simultaneous broker-loss soak | fresh external Cargo project resolving `kafrust 0.3.3` from crates.io; Kafka 4.3.1 three-broker KRaft, three replicated partitions, simultaneous brokers 1 and 2 outage | 120-second, 1-KiB, batch-size-100 workload; stopped brokers 1 and 2 simultaneously after one third of the run, restarted both after 10 seconds, required published dependency verification, retry recovery, per-partition record reconciliation, and zero final queue gauges | [`Published Multi-Broker Soak Smoke`, run `32395288682`](https://github.com/TaeeunKil/kafrust/actions/runs/32395288682) on 2026-08-20 | Passing; 4,099,200 records, 1 operation error, 11 failed requests, 1,018 retries, `recovered=true`, and zero final in-flight/buffered records; current published plaintext simultaneous-loss profile, not secured loss, unclean-election data-loss, or production SLO evidence |
| Published `kafrust 0.3.3` secured simultaneous broker-loss soak | fresh external Cargo project resolving `kafrust 0.3.3` from crates.io with `tls`; Kafka 4.3.1 three-broker KRaft with SASL_SSL/SCRAM-SHA-256 and three replicated partitions | 600-second, 1-KiB, batch-size-100 workload; stopped brokers 1 and 2 simultaneously after one third of the run, restarted both after 10 seconds, required published dependency verification, TLS/SCRAM retry recovery, per-partition record reconciliation, and zero final queue gauges | [`Published Secure Multi-Broker Soak Smoke`, run `32396241090`](https://github.com/TaeeunKil/kafrust/actions/runs/32396241090) on 2026-08-20 | Passing; 19,188,800 records, 283 operation errors, 6 failed requests, 9 retries, `recovered=true`, and zero final in-flight/buffered records; current published secured simultaneous-loss profile, not repeated campaigns, unclean-election data-loss, production SLO, or service-canary evidence |
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
  [`31757363941`](https://github.com/TaeeunKil/kafrust/actions/runs/31757363941)
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
- The published performance workflow
  [`31744206188`](https://github.com/TaeeunKil/kafrust/actions/runs/31744206188)
  passed four fresh external `kafrust 0.2.28` projects: Kafka 3.7.2 and 4.3.1
  with no compression and Zstd. Each project produced and consumed 10,000
  1-KiB records in batches of 200, recorded batch p50/p95/p99 latency, and
  finished with zero retries and zero in-flight or buffered records. Producer
  throughput ranged from 43.7k to 48.9k records/s; consumer throughput ranged
  from 210.6k to 268.3k records/s. This is a reproducible published-crate
  baseline, not a production SLO.
- The direct published comparison workflow
  [`31753172293`](https://github.com/TaeeunKil/kafrust/actions/runs/31753172293)
  passed a fresh external `kafrust 0.2.28` versus `rust-rdkafka 0.39.0` project
  against Kafka 4.3.1. Both used fresh one-partition topics, 2,000 1-KiB
  records, and batches of 100. Kafrust measured 51,834 producer and 129,875
  consumer records/s; rust-rdkafka measured 48,452 producer and 252,306
  consumer records/s. This is one direct benchmark profile, not API/feature
  parity, a production SLO, or a universal performance ranking.
- The published soak workflow
  [`31744827441`](https://github.com/TaeeunKil/kafrust/actions/runs/31744827441)
  passed a fresh external `kafrust 0.2.28` project against Kafka 4.3.1. The
  120-second, 1-KiB workload stopped the broker after one third of the run,
  restarted it after ten seconds, and processed 7,229,000 records. It observed
  173 operation errors, 982 failed requests, and 1,210 retries, then recovered
  with `recovered=true` and zero final in-flight or buffered records. This is
  one published single-node recovery profile, not a multi-broker soak or
  production SLO.
- The published multi-broker soak workflow
  [`31746182158`](https://github.com/TaeeunKil/kafrust/actions/runs/31746182158)
  passed a fresh external `kafrust 0.2.28` project against Kafka 4.3.1 with
  three replicated partitions. The 120-second, 1-KiB workload stopped broker
  1 after one third of the run, restarted it after ten seconds, and processed
  4,918,800 records. It observed one operation error, seven failed requests,
  and 1,006 retries, then recovered with `recovered=true` and zero final
  in-flight or buffered records. This is a published plaintext multi-broker
  recovery profile, not secured, simultaneous-loss, or production SLO evidence.
- The published secured multi-broker soak workflow
  [`31747389166`](https://github.com/TaeeunKil/kafrust/actions/runs/31747389166)
  passed a fresh external `kafrust 0.2.28` project with the `tls` feature
  against Kafka 4.3.1 SASL_SSL/SCRAM-SHA-256. The three-broker, three-replicated-
  partition, 120-second workload stopped broker 1 after one third of the run,
  restarted it after ten seconds, and processed 2,288,700 records. It observed
  one failed request and 1,001 retries with no high-level operation errors,
  then reported `recovered=true` and zero final in-flight or buffered records.
  This is one published secured recovery profile, not simultaneous-loss or
  production SLO evidence.
- The published simultaneous broker-loss workflow
  [`31748293446`](https://github.com/TaeeunKil/kafrust/actions/runs/31748293446)
  passed a fresh external `kafrust 0.2.28` project against Kafka 4.3.1. The
  three-broker, three-replicated-partition, 120-second workload stopped brokers
  1 and 2 simultaneously after one third of the run, restarted both after ten
  seconds, and processed 4,423,200 records. It observed one failed request and
  999 retries with no high-level operation errors, then reported `recovered=true`
  and zero final in-flight or buffered records. This is one published plaintext
  simultaneous-loss profile, not secured or production SLO evidence.
- The published simultaneous-loss workflow also passed Kafka 3.7.2 in
  [`31748860976`](https://github.com/TaeeunKil/kafrust/actions/runs/31748860976).
  The fresh external `0.2.28` project processed 4,620,200 records across the
  three replicated partitions, observed one failed request and 1,008 retries,
  and ended with `recovered=true` plus zero final in-flight or buffered records.
  Together these runs qualify the tested plaintext simultaneous-loss behavior
  on Kafka 3.7.2 and 4.3.1, not secured simultaneous loss or production SLOs.
- Kafka-compatible Murmur2 routing for keyed records without an explicit
  partition. Manual run `30066328105` verified key-derived routing and
  fetch-back by partition and offset across the three-broker Kafka 3.7.2
  profile while all other broker and security profiles remained green.
- Per-topic batch-sticky round-robin routing for keyless records. Manual run
  `30066831820` verified the exact `0,1,2,3,4,5,0` sequence through one
  producer against the six-partition, three-broker Kafka 3.7.2 profile.
- Opt-in idempotent single-record, batch, and buffered produce using negotiated
  `InitProducerId v2` with a v0 fallback, `acks=all`, and partition-scoped
  RecordBatch producer identity and sequence metadata. Manual run `29991254722`
  passed these paths against Kafka 3.7.2 and Kafka 4.3.1.
- Opt-in alpha transactional produce using transaction coordinator discovery,
  negotiated transactional `InitProducerId v2`, `AddPartitionsToTxn v3`,
  Produce v12/v11/v9/v7/v3, and `EndTxn v3`, with v0 fallbacks for older
  brokers. Manual run `29994041530` passed a committed transaction followed by
  an aborted transaction against Kafka 3.7.2 and Kafka 4.3.1.
- Direct and group consumer `ReadCommitted` isolation through Fetch v4.
  Transactional/control RecordBatch metadata is preserved for filtering,
  control records are hidden, and aborted transaction records are excluded.
  Manual run `29995122439` compared `ReadUncommitted` and `ReadCommitted`
  results after real commit and abort flows on Kafka 3.7.2 and Kafka 4.3.1.
- Transactional consumer group offset integration through
  `Producer::send_group_offsets_to_transaction`, negotiated `AddOffsetsToTxn v3`
  with a v0 fallback, and generation-fenced `TxnOffsetCommit v3`. Manual run
  `30063099869` passed a
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
authentications. `CachedOAuthBearerTokenProvider` adds source-provided expiry,
refresh-window rotation, and valid-token fallback during a temporary source
outage; this deterministic policy is covered by unit tests. External
provider-specific behavior remains unclaimed.
- The low-level `Client` records the broker-advertised SASL session lifetime;
  provider-backed OAUTHBEARER re-authentication is covered by focused injected
  tests and the published signed OIDC live job above. OAUTHBEARER uses
  flexible `SaslAuthenticate v2` for initial authentication and `v1` for
  same-connection re-authentication, while PLAIN and SCRAM remain on `v1`;
  detached refresh workers and provider-specific production OAuth/OIDC
  qualification remain unclaimed.

## Not Yet Claimed

The current compatibility claim does not cover:

- TLS workflows beyond the listed TLS smoke examples.
- Certificate rotation behavior beyond the current-source and published-artifact
  runs listed above.
- SASL workflows beyond the listed SASL_PLAINTEXT and SASL_SSL smoke examples.
- Production SASL/OAUTHBEARER provider compatibility beyond the local signed
  OIDC/JWKS fixture, including discovery/token endpoints, key rotation, and
  provider-specific failure behavior. The async token-provider callback and
  the opt-in cached expiry/rotation wrapper are implemented and bounded by
  `ClientConfig::request_timeout_ms`, but HTTP discovery, JWKS retrieval, and
  external outage qualification remain open.
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
