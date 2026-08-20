# Admin API

`AdminClient` uses the same `ClientConfig` as low-level broker connections, so
TLS, SASL, request timeouts, decode limits, and shared metrics apply to admin
operations. Controller-scoped operations discover the current controller from
cluster metadata before opening the request connection.

Call `AdminClient::validate()` during startup to check the connection settings
without opening a broker connection. This includes bootstrap entries, request
and decode limits, required SASL credentials, and an explicitly configured TLS
server name.

## Describe Features

`AdminClient::describe_features` reads Kafka's broker-supported and
cluster-finalized feature metadata from the tagged fields of `ApiVersions` v3
or newer. The result includes supported version ranges, finalized feature
ranges, the finalized metadata epoch, and Kafka's ZooKeeper-migration-ready
flag. This is a capability read, so it does not issue a controller mutation
and is safe to repeat after reconnecting to another broker.

The protocol layer keeps unknown ApiVersions tags intact for forward
compatibility. Brokers that only support ApiVersions versions below v3 cannot
provide this metadata and return the normal negotiated-version error instead
of an incomplete feature result.

## List Configuration Resources

`AdminClient::list_config_resources` exposes Kafka API key 74 across its two
wire meanings. It uses flexible `ListConfigResources` v1 on Kafka 4.1+ to list
configuration-bearing topic, broker, broker-logger, client-metrics, and group
resources. On Kafka 3.9-era brokers, an exact `ClientMetrics` filter selects
the compatible v0 `ListClientMetricsResources` shape and maps each result to
`ConfigResourceType::ClientMetrics`. Empty or broader filters return
`Error::Unsupported` when only v0 is advertised because v0 cannot represent
those resource types. Unknown v1 resource codes are preserved as
`ConfigResourceType::Other` for forward compatibility.

This is a discovery operation only. Use `describe_topic_configs` or the
corresponding future resource-specific configuration API to read values. The
protocol, low-level client, and Admin routing are covered by injected-broker
tests. The manual
[`live-list-config-resources.yml`](../.github/workflows/live-list-config-resources.yml)
workflow qualifies discovery and the opt-in DescribeConfigs v4 metadata path
against Kafka 4.1.0, 4.2.0, or 4.3.1. The current-source Kafka 4.3.1 gate
passed in [`32342304005`](https://github.com/TaeeunKil/kafrust/actions/runs/32342304005),
and published `kafrust 0.3.1` checks passed for Kafka 4.3.1 v1 in
[`32343030081`](https://github.com/TaeeunKil/kafrust/actions/runs/32343030081)
and Kafka 3.9.1 v0 in
[`32343145837`](https://github.com/TaeeunKil/kafrust/actions/runs/32343145837).

```rust,no_run
use kafrust::{
    AdminClient, ClientConfig, ConfigResourceType, ListConfigResourcesOptions,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let resources = admin
    .list_config_resources(
        ListConfigResourcesOptions::new()
            .resource_type(ConfigResourceType::Topic)
            .resource_type(ConfigResourceType::Group),
    )
    .await?;

for resource in resources.resources() {
    println!("{}: {:?}", resource.name(), resource.resource_type());
}
# Ok(())
# }
```

## Update Features

`AdminClient::update_features` routes Kafka UpdateFeatures v1 to the active
KRaft controller when the broker advertises it, and falls back to v0 for
older brokers. `FeatureUpdate::new` sets a finalized feature's maximum
version level; call `allow_downgrade(true)` for a safe downgrade or use
`upgrade_type(FeatureUpgradeType::UnsafeDowngrade)` when Kafka's v1 unsafe
downgrade operation is explicitly intended. The result retains the top-level
and per-feature error codes and messages.

`UpdateFeaturesOptions::validate_only(true)` performs broker-side validation
without finalizing changes and requires v1. Unsafe downgrades and validation-
only requests return `Error::Unsupported` rather than being silently weakened
when the broker only advertises v0.

This is a controller mutation, so kafrust does not replay it after a request
may have been transmitted. A transport failure in that window returns
`Error::AdminMutationOutcomeUnknown`; inspect the controller's feature metadata
before deciding whether another update is required. The v0 path remains the
compatibility path for Kafka 3.7-era brokers and newer brokers that advertise
API key 57 without v1.

The current-source live gate sends a non-empty v1 `validate_only` request for
the broker's finalized `metadata.version` level and checks the returned
feature-level result. Kafka 3.7.2 passed in
[`32360936437`](https://github.com/TaeeunKil/kafrust/actions/runs/32360936437)
and Kafka 4.3.1 passed in
[`32361035007`](https://github.com/TaeeunKil/kafrust/actions/runs/32361035007).
These runs qualify request population and result mapping without changing the
cluster's finalized feature state. A separate SASL/PLAIN authorizer gate
qualified the same non-empty `metadata.version` request on Kafka 3.7.2 and
4.3.1: a restricted principal with only cluster `Describe` was rejected with
`ClusterAuthorizationFailed` (31), while an administrator with `Alter Cluster`
was accepted in [`32362301496`](https://github.com/TaeeunKil/kafrust/actions/runs/32362301496).
The controller-routed validation path also survived an active-controller
failure on both versions in [`32363072430`](https://github.com/TaeeunKil/kafrust/actions/runs/32363072430).
The Kafka 4.3.1 lifecycle gate [`32363428806`](https://github.com/TaeeunKil/kafrust/actions/runs/32363428806)
also performed and verified `transaction.version` `2 -> 1` safe downgrade and
`1 -> 2` upgrade. Metadata-version transitions across the declared broker
matrix and state-changing mutation during controller failover remain open.

## DescribeTopicPartitions

`AdminClient::describe_topic_partitions` exposes Kafka's flexible
`DescribeTopicPartitions` v0 API (API key 75). It returns topic UUIDs,
partition leaders and epochs, replica/ISR state, eligible leader replica
fields, offline replicas, authorized-operations bits, and Kafka's paging
cursor. Use `DescribeTopicPartitionsOptions::with_cursor` to continue from a
previous `next_cursor` and `with_response_partition_limit` to bound one page.

This API is capability-negotiated. A broker that does not advertise API 75
returns `Error::Unsupported`; callers supporting Kafka 3.7-era brokers should
fall back to `list_topics` or the Metadata API rather than assuming the newer
response shape. The current-source gate passed the expected unsupported path
on Kafka 3.7.2 in
[`31778114684`](https://github.com/TaeeunKil/kafrust/actions/runs/31778114684)
and full response decoding on Kafka 4.3.1 in
[`31778116310`](https://github.com/TaeeunKil/kafrust/actions/runs/31778116310).

## DescribeQuorum

`AdminClient::describe_quorum` exposes Kafka's flexible `DescribeQuorum` API
(API key 55) for KRaft metadata quorum inspection. It negotiates v0 through v2,
preserves the response version in `DescribeQuorumResult`, and exposes typed
leader, epoch, high-watermark, voter, observer, replica-directory, and
controller-listener fields where the broker version provides them.

When a deployment exposes a controller listener separately, configure it with
`ClientConfig::controller_bootstrap_servers`. The example and live workflow use
that path explicitly. Kafka 3.7.2 selected v0 and Kafka 4.3.1 selected v2 in
the current-source qualification runs
[`31781263986`](https://github.com/TaeeunKil/kafrust/actions/runs/31781263986)
and [`31781264035`](https://github.com/TaeeunKil/kafrust/actions/runs/31781264035).
The protocol request includes tagged fields at the partition, topic, and
top-level boundaries; focused tests keep this flexible wire shape auditable.

## KRaft Voter Management

`AdminClient::add_raft_voter` and `AdminClient::remove_raft_voter` expose the
KRaft controller APIs 80 and 81. They route through the configured controller,
negotiate AddRaftVoter v1 when available, and retain typed throttle, error-code,
and error-message outcomes. AddRaftVoter v1 supports
`ack_when_committed(true)`; kafrust returns `Error::Unsupported` instead of
silently weakening that request when only v0 is advertised. RemoveRaftVoter
currently uses its stable v0 request shape.

These are controller mutations. A transport failure after transmission is
reported as `Error::AdminMutationOutcomeUnknown` and the request is never
replayed automatically. Protocol, low-level Client, and injected controller
routing tests cover the flexible request and response shapes. The isolated
Kafka 4.3.1 dynamic-quorum gate
[`32344895847`](https://github.com/TaeeunKil/kafrust/actions/runs/32344895847)
verified observer-to-voter convergence after AddRaftVoter and voter removal
after RemoveRaftVoter. The separate
[`live-dynamic-quorum-authorization.yml`](../.github/workflows/live-dynamic-quorum-authorization.yml)
gate passed in [`32364161150`](https://github.com/TaeeunKil/kafrust/actions/runs/32364161150)
with a SASL/PLAIN controller listener: a restricted principal with only cluster
`Describe` received `ClusterAuthorizationFailed` (31), and the quorum remained
unchanged; the administrator then completed AddRaftVoter and RemoveRaftVoter.
Broader controller failure workloads remain a separate qualification gate.

## Unregister Broker

`AdminClient::unregister_broker` exposes Kafka's `UnregisterBroker` API 64 v0
through the active KRaft controller. It accepts the broker ID, preserves the
broker throttle and error message, and returns a typed
`UnregisterBrokerResult`. The method is a controller mutation and therefore
does not replay a request after transmission; an ambiguous transport failure
is reported as `Error::AdminMutationOutcomeUnknown`.

The protocol, low-level `Client`, and injected controller-routing tests cover
the flexible request header, broker ID, response error fields, and capability
negotiation. The live `live-unregister-broker-rejoin.yml` matrix now qualifies
the operation against three-node KRaft clusters for Kafka 3.7.2, 3.8.1, 3.9.1,
and 4.3.1: each job stops broker 1, unregisters it through the surviving
controller quorum, restarts the same node, and verifies re-registration plus
controller quorum health in
[`32359316032`](https://github.com/TaeeunKil/kafrust/actions/runs/32359316032).

The reusable current-source Admin response-drop gate also covers API 64: it
drops the first `UnregisterBroker` response and reconciles the missing broker
through `DescribeCluster` without replaying the mutation. This is a transport
ambiguity proof, not the multi-controller unregister/re-registration gate. The
Kafka 3.7.2 and 4.3.1 runs are
[`32357381909`](https://github.com/TaeeunKil/kafrust/actions/runs/32357381909)
and
[`32357381879`](https://github.com/TaeeunKil/kafrust/actions/runs/32357381879).

The multi-controller unregister/re-registration gate is now qualified for the
four tested broker versions. This is separate from the response-drop proof
above and does not claim behavior for every authorization policy or failure
workload.

The `live-unregister-broker-authorization.yml` gate also qualifies the
operation-specific permission boundary with Kafka `StandardAuthorizer` and
SASL/PLAIN. On Kafka 3.7.2 and 4.3.1, a restricted principal that can only
discover the cluster is rejected with `ClusterAuthorizationFailed` (error
code 31), while the configured administrator principal is allowed to perform
the mutation in [`32360499520`](https://github.com/TaeeunKil/kafrust/actions/runs/32360499520).
This verifies the client preserves the broker's authorization result; it does
not establish the ACL policy required by a production principal or parity for
other Admin mutations.

## Describe Share Groups

`AdminClient::describe_share_groups` exposes Kafka's stable KIP-932
`ShareGroupDescribe` v1 API (API key 77). It routes each group ID to its active
coordinator, negotiates the capability before sending the request, and returns
typed group state, group and assignment epochs, assignor, authorized
operations, member identity, subscribed topics, and assigned topic partitions.

The response is flexible and includes topic UUIDs as well as topic names. The
method returns `Error::Unsupported` when the broker does not advertise v1; it
does not claim compatibility with Kafka 4.0's removed early-access v0. Read
errors and retryable coordinator responses use the same bounded recovery policy
as the other coordinator-scoped Admin reads.

The Kafka 4.3.1 ShareConsumer smoke also keeps a member active while calling
this method and verifies the returned group and member state in
[`32223573332`](https://github.com/TaeeunKil/kafrust/actions/runs/32223573332).

`AdminClient::alter_share_group_offsets` and
`AdminClient::delete_share_group_offsets` cover Kafka's flexible v0 APIs 91
and 92. Both operations require an empty share group, preserve top-level and
per-topic or per-partition errors, and classify a post-transmission transport
failure as `AdminMutationOutcomeUnknown` without replay. The Kafka 4.3.1
smoke sets and then deletes a real share-group offset in
[`32224302754`](https://github.com/TaeeunKil/kafrust/actions/runs/32224302754).

`AdminClient::list_share_group_offsets` exposes Kafka's API 90
`DescribeShareGroupOffsets`. It negotiates v1 when the broker advertises it so
partition lag is available, and falls back to v0 for Kafka 4.1-era brokers.
Pass `None` for all known topic-partitions or use `ShareGroupOffsetQuery` to
filter topics and partitions. Kafka uses the existing API 42 `DeleteGroups`
wire path for share-group deletion; `AdminClient::delete_share_groups` provides
that intent-specific name while preserving the typed per-group result.

The focused protocol and coordinator-routing tests cover API 90 v0/v1 and
share-group deletion. The Kafka 4.3.1 live lifecycle gate covers alter, list,
delete-offsets, and delete-group together in
[`32225957928`](https://github.com/TaeeunKil/kafrust/actions/runs/32225957928).

## Describe Streams Groups

`AdminClient::describe_streams_groups` exposes Kafka's flexible
`StreamsGroupDescribe` v0 API (API key 89). It routes each Streams group ID to
its active coordinator and returns typed group state and epochs, the initialized
topology, state-changelog and repartition topics, member endpoints and tags,
task offsets, active/target assignments, and authorized operations.

The protocol, low-level `Client`, and coordinator-routing Admin path are now
covered by focused wire and injected-broker tests. API 89 is a Kafka 4.x
Streams-group capability and therefore returns `Error::Unsupported` when the
broker does not advertise it. A live Kafka Streams application qualification is
still open: a real application must initialize a topology and exercise member,
task, and assignment fields before this path is included in the published
compatibility claim. The complementary `StreamsGroupHeartbeat` API 88 remains
the next implementation slice for full Streams group lifecycle coverage.

The low-level `Client::streams_group_heartbeat_v0` path now exposes the
flexible API 88 request and response wire shape, including topology
initialization, task state, status, and Interactive Queries endpoint
assignment. It is intentionally low-level for now: a high-level Streams
consumer lifecycle still needs member-epoch management, topology validation,
reconciliation, shutdown handling, and a live Kafka Streams application gate.

## Share Group State

The protocol and low-level `Client` expose Kafka 4.3 Share Group State APIs 83
through 87: `InitializeShareGroupState`, `ReadShareGroupState`,
`WriteShareGroupState`, `DeleteShareGroupState`, and
`ReadShareGroupStateSummary`. The high-level `AdminClient` routes these calls
to the share-group coordinator and returns typed topic, partition, state-batch,
and delivery-count results.

`WriteShareGroupState` prefers v1 when the broker advertises it. A requested
`delivery_complete_count` is never silently discarded: a broker that only
advertises v0 returns `Error::Unsupported`. The summary path likewise prefers
v1 so its delivery-completion count is retained, while v0 remains available
for brokers that only expose the earlier response shape. Initialize, Write,
and Delete classify a response lost after transmission as
`Error::AdminMutationOutcomeUnknown` and do not replay the mutation.

The wire and injected-client tests cover compact arrays, UUID topic IDs,
state batches, flexible tagged fields, and the v0/v1 field boundaries. The
manual/scheduled Kafka 4.3.1 Share smoke workflow now also exercises metadata
UUID discovery, broker-side state initialization, v1 write, full read, v1
summary, and deletion. A successful run is still required before this API is
included in the compatibility claim; coordinator failure and replicated state
recovery remain separate qualification gates.

```rust
use kafrust::{AdminClient, ClientConfig};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let offsets = admin.list_share_group_offsets("orders-share", None).await?;
for topic in offsets.topics() {
    for partition in topic.partitions() {
        println!("{}-{}: {}", topic.topic_name(), partition.partition(), partition.start_offset());
    }
}
# Ok(())
# }
```

```rust
use kafrust::{AdminClient, ClientConfig};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let groups = admin
    .describe_share_groups(&["orders-share".to_owned()], true)
    .await?;

for group in groups {
    println!("{}: {}", group.group_id(), group.state());
    for member in group.members() {
        println!("member {} on {}", member.member_id(), member.client_host());
    }
}
# Ok(())
# }
```

## Mutation Outcome Ambiguity

For mutating Admin operations, a transport failure, request timeout, response
size rejection, or response framing/protocol failure after the request may have
been transmitted returns `Error::AdminMutationOutcomeUnknown { operation }`.
The operation is not replayed automatically because Kafka may already have
applied it. Reconcile with a read operation such as metadata, describe, list,
or offset inspection, then retry only when the application has established that
the retry is safe. Broker errors returned in a valid response remain available
as typed per-operation results and are not converted to this error.

`DeleteRecords` is the explicit exception: deleting before a fixed offset is
idempotent, so its leader-refresh path may retry the request. Callers must
still inspect its per-partition results and treat a final transport failure as
unconfirmed until the log state is checked.

The current-source response-drop qualification proves this boundary against a
real Kafka broker: `CreateTopics` reached Kafka, its response was discarded by
an intervening proxy, `Error::AdminMutationOutcomeUnknown { operation:
"CreateTopics" }` was returned, and a follow-up `list_topics` observed the
applied topic. The gate passed on Kafka 3.7.2 in
[`31770443512`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443512)
and Kafka 4.3.1 in
[`31770443484`](https://github.com/TaeeunKil/kafrust/actions/runs/31770443484).
The same current-source gate now qualifies `DeleteTopics`: the topic was
created, the delete request reached Kafka, its response was discarded, the
typed `DeleteTopics` ambiguity was returned, and `list_topics` confirmed the
topic was gone. The gate passed on Kafka 3.7.2 in
[`31771419625`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419625)
and Kafka 4.3.1 in
[`31771419124`](https://github.com/TaeeunKil/kafrust/actions/runs/31771419124).
These qualify CreateTopics and DeleteTopics only; they are not a claim that
every Admin mutation has an identical broker-side failure policy. The same
workflow also qualifies `CreatePartitions`: it expanded a real topic from one
to two partitions, dropped the response, returned the typed ambiguity, and
reconciled the new partition count on Kafka 3.7.2 in
[`31771635710`](https://github.com/TaeeunKil/kafrust/actions/runs/31771635710)
and Kafka 4.3.1 in
[`31771636082`](https://github.com/TaeeunKil/kafrust/actions/runs/31771636082).
The same gate qualifies `IncrementalAlterConfigs`: it set a topic's
`retention.ms`, dropped the response, and confirmed the value with
`DescribeConfigs` on Kafka 3.7.2 in
[`31771864914`](https://github.com/TaeeunKil/kafrust/actions/runs/31771864914)
and Kafka 4.3.1 in
[`31771865024`](https://github.com/TaeeunKil/kafrust/actions/runs/31771865024).
Classic `AlterConfigs` is also qualified: it replaced the topic's
`retention.ms`, dropped the response, and confirmed the value with
`DescribeConfigs` on Kafka 3.7.2 in
[`31772009182`](https://github.com/TaeeunKil/kafrust/actions/runs/31772009182)
and Kafka 4.3.1 in
[`31772008771`](https://github.com/TaeeunKil/kafrust/actions/runs/31772008771).
The same gate now qualifies ACL mutations with Kafka's
`StandardAuthorizer` enabled and `User:ANONYMOUS` configured as the test
superuser. `CreateAcls` dropped its response and reconciled the binding
through `DescribeAcls` on Kafka 3.7.2 in
[`31772403290`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403290)
and Kafka 4.3.1 in
[`31772403077`](https://github.com/TaeeunKil/kafrust/actions/runs/31772403077).
`DeleteAcls` dropped its response and reconciled that the binding was gone on
Kafka 3.7.2 in
[`31772470761`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470761)
and Kafka 4.3.1 in
[`31772470590`](https://github.com/TaeeunKil/kafrust/actions/runs/31772470590).
These remain operation-specific proofs and do not replace target-policy
authorization testing.
The same response-drop gate now qualifies `AlterClientQuotas`: it set the
`producer_byte_rate` quota for a user, dropped the response, and confirmed the
applied value through `DescribeClientQuotas` on Kafka 3.7.2 in
[`31772731756`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731756)
and Kafka 4.3.1 in
[`31772731963`](https://github.com/TaeeunKil/kafrust/actions/runs/31772731963).
This is an operation-specific proof; target quota policy and authorization
must still be qualified for production.
The same gate qualifies `AlterUserScramCredentials`: it created a deterministic
SCRAM-SHA-256 credential for a test user, dropped the response, and confirmed
the mechanism and iteration count through `DescribeUserScramCredentials` on
Kafka 3.7.2 in
[`31772992221`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992221)
and Kafka 4.3.1 in
[`31772992381`](https://github.com/TaeeunKil/kafrust/actions/runs/31772992381).
This proves the mutation/reconciliation boundary only; credential policy and
authenticated production administration remain target-specific.
The same response-drop gate now qualifies `CreateDelegationToken` over an
authenticated SASL/PLAIN channel. It intentionally loses the create response,
then uses `DescribeDelegationTokens` to find a new token owned by
`User:admin` on Kafka 3.7.2 in
[`31773884142`](https://github.com/TaeeunKil/kafrust/actions/runs/31773884142)
and Kafka 4.3.1 in
[`31773883953`](https://github.com/TaeeunKil/kafrust/actions/runs/31773883953).
The gate never logs the token HMAC. This is an operation-specific
reconciliation proof; token authorization, secret distribution, renewal, and
expiration policy remain target-specific.
The current-source response-drop gate also qualifies administrative
`OffsetCommit` v2. It waits for the group coordinator to become ready, loses
the commit response, returns `AdminMutationOutcomeUnknown`, and confirms the
committed offset through `OffsetFetch` at `42` on Kafka 3.7.2 in
[`31774729128`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729128)
and Kafka 4.3.1 in
[`31774729263`](https://github.com/TaeeunKil/kafrust/actions/runs/31774729263).
The client does not replay the transmitted mutation. This remains an
operation-specific proof; DeleteGroups, member-aware workloads, and target
authorization are separate gates.
The same gate qualifies `OffsetDelete` v0 after first establishing a committed
offset. It loses the delete response, returns `AdminMutationOutcomeUnknown`,
and confirms the partition no longer has a committed offset through
`OffsetFetch` on Kafka 3.7.2 in
[`31774990676`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990676)
and Kafka 4.3.1 in
[`31774990554`](https://github.com/TaeeunKil/kafrust/actions/runs/31774990554).
This is still an operation-specific proof; member-aware workloads and target
authorization remain separate gates.
The same current-source gate qualifies `DeleteGroups` v1 after making the
group visible through `ListGroups`. It loses the delete response, returns
`AdminMutationOutcomeUnknown`, and confirms the group disappears through
`ListGroups` on Kafka 3.7.2 in
[`31775333815`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333815)
and Kafka 4.3.1 in
[`31775333736`](https://github.com/TaeeunKil/kafrust/actions/runs/31775333736).
This is an operation-specific ambiguity proof; active-member behavior,
member-aware workloads, and target authorization remain separate gates.
The same current-source gate qualifies `AlterPartitionReassignments` v0. It
submits a real three-broker reassignment, drops the response, returns
`AdminMutationOutcomeUnknown`, and reconciles completion through
`ListPartitionReassignments` plus final topic metadata. Kafka 3.7.2 passed in
[`31776694068`](https://github.com/TaeeunKil/kafrust/actions/runs/31776694068)
and Kafka 4.3.1 passed in
[`31776695970`](https://github.com/TaeeunKil/kafrust/actions/runs/31776695970).
The final check compares the requested replica order and the ISR broker set;
Kafka may report ISR members in a different order. Authorization, cancellation,
broker-loss, and data-movement qualification remain separate gates.
The current-source KIP-848 member-aware gate also qualifies `OffsetCommit` v9.
It joins a real consumer-protocol member, drops the member-aware commit
response, returns `AdminMutationOutcomeUnknown` without replay, and reconciles
the committed offset through member-aware `OffsetFetch` on Kafka 4.3.1 in
[`31777089953`](https://github.com/TaeeunKil/kafrust/actions/runs/31777089953).
The job also confirms offset `42` through Kafka's consumer-groups CLI; active
member deletion, member-aware offset deletion, and target authorization remain
separate gates.

## Inspect Cluster and Topics

```rust
use kafrust::{AdminClient, ClientConfig};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let cluster = admin.describe_cluster().await?;

println!("controller node: {}", cluster.controller_id());
for broker in cluster.brokers() {
    println!(
        "broker {} at {}:{} rack {:?}",
        broker.id(),
        broker.host(),
        broker.port(),
        broker.rack()
    );
}

for topic in admin.list_topics().await? {
    println!(
        "{}: {} partitions, internal={}, error={}",
        topic.name(),
        topic.partition_count(),
        topic.is_internal(),
        topic.error_code()
    );
}
# Ok(())
# }
```

`describe_cluster` sends Metadata v1 with an empty topic list so Kafka returns
broker and controller data without enumerating topics. For the dedicated Kafka
API 60 response, use `describe_cluster_with_options`; it negotiates
DescribeCluster v1 when available and preserves the cluster ID, endpoint type,
rack values, and optional cluster authorized-operations bitfield. It falls
back to Metadata when API 60 is unavailable. `list_topics` requests all visible
topics. Topic-level metadata errors remain available through
`TopicListing::error_code` and `broker_error_kind` instead of aborting the
entire listing.

```rust,no_run
use kafrust::{
    AdminClient, ClientConfig, DescribeClusterEndpointType, DescribeClusterOptions,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let cluster = admin
    .describe_cluster_with_options(
        DescribeClusterOptions::new()
            .include_cluster_authorized_operations(true)
            .endpoint_type(DescribeClusterEndpointType::Brokers),
    )
    .await?;
println!("cluster={:?} authorized={:?}", cluster.cluster_id(), cluster.cluster_authorized_operations());
# Ok(())
# }
```

## Describe Topic Configurations

```rust
use kafrust::{
    AdminClient, ClientConfig, DescribeConfigsOptions, TopicConfigResource,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .describe_topic_configs(
        &[
            TopicConfigResource::with_keys(
                "orders",
                ["cleanup.policy", "retention.ms"],
            ),
            TopicConfigResource::new("payments"),
        ],
        DescribeConfigsOptions::new().include_synonyms(true),
    )
    .await?;

for resource in result.resources() {
    for entry in resource.entries() {
        println!(
            "{}={} source={:?} sensitive={}",
            entry.name(),
            entry.value().unwrap_or("<redacted or null>"),
            entry.source(),
            entry.is_sensitive()
        );
    }
}
# Ok(())
# }
```

DescribeConfigs v1 can request all keys or a selected key set and optionally
include Kafka's config synonyms. Set `include_documentation(true)` to opt into
DescribeConfigs v4 on Kafka 4.0+; the returned `ConfigEntry` then preserves
Kafka's raw config type and documentation text through `config_type()` and
`documentation()`. Resource failures remain in
`ConfigResourceResult`; unknown future config-source values are preserved as
`ConfigSource::Other(raw_code)`. This API intentionally accepts topic resources
only until broker-specific routing is implemented.

## Incrementally Alter Topic Configurations

```rust
use kafrust::{
    AdminClient, AlterConfigsOptions, ClientConfig, TopicConfigAlteration,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .incremental_alter_topic_configs(
        &[
            TopicConfigAlteration::new("orders")
                .set("retention.ms", "60000")
                .append("cleanup.policy", "compact"),
            TopicConfigAlteration::new("payments")
                .delete("retention.ms")
                .subtract("cleanup.policy", "delete"),
        ],
        AlterConfigsOptions::new().validate_only(false),
    )
    .await?;

for resource in result.resources() {
    if !resource.is_success() {
        eprintln!(
            "{}: Kafka error {}: {}",
            resource.name(),
            resource.error_code(),
            resource.error_message().unwrap_or("no broker message")
        );
    }
}
# Ok(())
# }
```

IncrementalAlterConfigs v0 represents Kafka's Set, Delete, Append, and Subtract
operations without replacing unrelated settings. Kafka applies operations
atomically within each resource, but resources can succeed or fail
independently. `AlterConfigsResult` therefore preserves every resource outcome.
Use `validate_only(true)` to ask Kafka to validate without applying changes.

## Replace Topic Configurations

```rust
use kafrust::{
    AdminClient, AlterConfigsOptions, ClientConfig, TopicConfigUpdate,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .alter_topic_configs(
        &[TopicConfigUpdate::new("orders")
            .set("cleanup.policy", "delete")
            .set("retention.ms", "60000")
            .delete("segment.ms")],
        AlterConfigsOptions::new(),
    )
    .await?;

for resource in result.resources() {
    if !resource.is_success() {
        eprintln!(
            "{}: Kafka error {}: {}",
            resource.name(),
            resource.error_code(),
            resource.error_message().unwrap_or("no broker message")
        );
    }
}
# Ok(())
# }
```

Classic AlterConfigs v1 replaces the complete dynamic configuration map for
each resource. Include every dynamic key that must remain set; keys omitted
from the request may return to their lower-precedence value. `delete` sends a
null value so Kafka removes that dynamic key. Use incremental alterations when
unrelated dynamic settings must be preserved automatically. Resource failures,
broker throttle time, and `validate_only(true)` are exposed through the same
typed `AlterConfigsResult` used by the incremental API.

The current-source
[`live-alter-configs-authorization.yml`](../.github/workflows/live-alter-configs-authorization.yml)
gate passed on Kafka 3.7.2 and 4.3.1 in
[`32365666970`](https://github.com/TaeeunKil/kafrust/actions/runs/32365666970).
A restricted SASL/PLAIN principal with cluster/topic discovery and
`DescribeConfigs`, but without `AlterConfigs`, received
`TopicAuthorizationFailed` (error 29) and the existing `retention.ms` value
remained unchanged. The administrator then applied the replacement value and
cleaned up the topic. This is an operation-specific current-source
authorization proof, not a universal ACL or Admin mutation parity claim.

The same example can run the incremental path through
[`live-incremental-alter-configs-authorization.yml`](../.github/workflows/live-incremental-alter-configs-authorization.yml).
That matrix passed on Kafka 3.7.2 and 4.3.1 in
[`32366418605`](https://github.com/TaeeunKil/kafrust/actions/runs/32366418605):
the restricted principal received `TopicAuthorizationFailed` (29) and the
existing value remained unchanged, while the administrator applied the
incremental alteration. This closes the incremental authorization sub-gate
only.

## Describe Consumer Groups

```rust
use kafrust::{AdminClient, ClientConfig};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let descriptions = admin
    .describe_consumer_groups(&[
        "orders-service".to_owned(),
        "payments-service".to_owned(),
    ])
    .await?;

for group in descriptions {
    println!(
        "{} state={} protocol={}/{} members={}",
        group.group_id(),
        group.state(),
        group.protocol_type(),
        group.protocol_name(),
        group.members().len()
    );
}
# Ok(())
# }
```

DescribeGroups v1 discovers and connects to each group coordinator
independently, so one call can safely contain group IDs assigned to different
brokers. Member IDs, clients, hosts, protocol metadata, and assignments remain
available; metadata and assignment payloads are raw bytes because their schema
depends on the selected group protocol.

For KIP-848 consumer-protocol groups, use the typed modern path instead of
parsing classic protocol metadata:

```rust
use kafrust::{AdminClient, ClientConfig};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let descriptions = admin
    .describe_consumer_groups_modern(&["orders-service".to_owned()], true)
    .await?;

for group in descriptions {
    println!(
        "{} state={} epoch={}/{} members={}",
        group.group_id(),
        group.state(),
        group.group_epoch(),
        group.assignment_epoch(),
        group.members().len()
    );
    for member in group.members() {
        for topic in member.assignment().topic_partitions() {
            println!("{}: {:?}", topic.topic_name(), topic.partitions());
        }
    }
}
# Ok(())
# }
```

`describe_consumer_groups_modern` negotiates ConsumerGroupDescribe v0/v1
(API key 69) with the group coordinator and preserves group/assignment epochs,
member type, topic UUIDs, current and target assignments, authorized-operation
bits, and per-group broker errors. It is intentionally separate from
`describe_consumer_groups`, whose classic DescribeGroups response contains
protocol-specific raw assignment bytes. The KIP-848 live gate runs this method
while a real consumer-protocol member is joined.

## List and Delete Groups

```rust
use kafrust::{AdminClient, ClientConfig};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
for group in admin.list_groups().await? {
    println!(
        "{} protocol={} coordinator={}",
        group.group_id(),
        group.protocol_type(),
        group.coordinator_id()
    );
}

let results = admin
    .delete_consumer_groups(&["retired-service".to_owned()])
    .await?;
for result in results {
    println!("{}: Kafka error {}", result.group_id(), result.error_code());
}
# Ok(())
# }
```

ListGroups is broker-scoped, so `list_groups` discovers the cluster and queries
every advertised broker before sorting and deduplicating the results. It
negotiates v5, v4, or v1 per broker; the low-level `Client::list_groups_v1`
method remains available when an exact legacy wire shape is required.
For modern group metadata, use `list_groups_with_options`:

```rust,no_run
use kafrust::{AdminClient, ClientConfig, ListGroupsOptions};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let groups = admin
    .list_groups_with_options(
        ListGroupsOptions::new()
            .state("Stable")
            .group_type("consumer"),
    )
    .await?;

for group in groups {
    println!(
        "{} state={:?} type={:?} api_version={}",
        group.group_id(),
        group.group_state(),
        group.group_type(),
        group.api_version()
    );
}
# Ok(())
# }
```

The options path negotiates ListGroups v5 when available, v4 when only the
state filter is representable, and v1 for an older broker. A state
filter requires v4 and a group-type filter requires v5; kafrust returns
`Error::Unsupported` instead of silently dropping a requested filter. The
unfiltered high-level `list_groups` method uses the same negotiated path.
Results from v4/v5 preserve broker-reported group state/type and the negotiated
API version through `GroupListing`.

DeleteGroups v1 discovers each requested group's coordinator independently and
preserves per-group errors. Kafka returns `NonEmptyGroup` when active members
still belong to a group; members should leave or expire before deletion.
Deleting a group's last committed offset can remove the empty group first, in
which case a subsequent DeleteGroups request returns `GroupIdNotFound`.
Transient coordinator responses such as `CoordinatorLoadInProgress`,
`CoordinatorNotAvailable`, and `NotCoordinator` are retried through fresh
coordinator discovery within `AdminClient::max_retries`. A transport failure
after DeleteGroups is sent is returned instead of being replayed, because the
broker-side deletion outcome is ambiguous.

## Delete Consumer Group Offsets

```rust
use kafrust::{AdminClient, ClientConfig, ConsumerGroupOffsetDelete};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .delete_consumer_group_offsets(
        "orders-service",
        &[
            ConsumerGroupOffsetDelete::new("orders", [0, 1]),
            ConsumerGroupOffsetDelete::new("payments", [2]),
        ],
    )
    .await?;

for topic in result.topics() {
    for partition in topic.partitions() {
        if !partition.is_success() {
            eprintln!(
                "{}-{}: Kafka error {}",
                topic.topic(),
                partition.partition_index(),
                partition.error_code(),
            );
        }
    }
}
# Ok(())
# }
```

OffsetDelete v0 is routed to the consumer group's active coordinator. Its
top-level group error and every partition outcome remain available separately.
Kafka rejects deletion for a topic while the group is actively subscribed to
it with error 86 (`GroupSubscribedToTopic`), so stop the group or remove that
topic from its subscription before deleting committed offsets. A member can
remain visible until its broker-side session timeout expires after an
unclean process exit. Retryable coordinator responses are retried with fresh
discovery within `AdminClient::max_retries`; transport failures after the
mutation is transmitted remain single-attempt because replaying an ambiguous
deletion request is not transparent.

## List And Alter Consumer Group Offsets

```rust
use kafrust::{
    AdminClient, ClientConfig, ConsumerGroupOffset, ConsumerGroupOffsetQuery,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let query = [ConsumerGroupOffsetQuery::new("orders", [0, 1])];
let before = admin
    .list_consumer_group_offsets("orders-service", Some(&query))
    .await?;

for topic in before.topics() {
    for partition in topic.partitions() {
        println!(
            "{}-{} offset={} metadata={:?} error={}",
            topic.topic(),
            partition.partition_index(),
            partition.committed_offset(),
            partition.metadata(),
            partition.error_code(),
        );
    }
}

let altered = admin
    .alter_consumer_group_offsets(
        "orders-service",
        &[ConsumerGroupOffset::new("orders", 0, 42).metadata("operator-reset")],
    )
    .await?;
assert!(altered.is_success());
# Ok(())
# }
```

`list_consumer_group_offsets` routes OffsetFetch v2 to the group's active
coordinator. `Some` requests selected topic partitions; `None` requests all
topics known to the group. `alter_consumer_group_offsets` routes an
administrative OffsetCommit v2 using generation `-1`, an empty member ID, and
no retention override. Both APIs preserve top-level and partition-level Kafka
errors instead of collapsing partial results. These admin methods use classic
consumer-group offset semantics. The results expose broker throttle time; the
classic v2 path reports zero because that schema has no throttle field.

For a joined KIP-848 member, pass a fresh `ConsumerGroup::metadata()` snapshot
to the member-aware methods. The snapshot must be refreshed after every rejoin
because both the member ID and member epoch can change:

```rust
use kafrust::{
    AdminClient, ConsumerGroupOffset, ConsumerGroupOffsetQuery, ConsumerGroupMetadata,
};

# async fn example(
#     admin: AdminClient,
#     metadata: ConsumerGroupMetadata,
# ) -> kafrust::Result<()> {
let query = [ConsumerGroupOffsetQuery::new("orders", [0])];
let offsets = admin
    .list_consumer_group_offsets_with_member(
        metadata.group_id(),
        Some(metadata.member_id()),
        metadata.generation_id(),
        Some(&query),
        true,
    )
    .await?;

let altered = admin
    .alter_consumer_group_offsets_with_member(
        metadata.group_id(),
        metadata.member_id(),
        metadata.generation_id(),
        metadata.group_instance_id(),
        &[ConsumerGroupOffset::new("orders", 0, 42).leader_epoch(-1)],
    )
    .await?;
assert!(offsets.is_success() && altered.is_success());
# Ok(())
# }
```

`list_consumer_group_offsets_with_member` prefers OffsetFetch v10 when the
coordinator advertises v10 and Metadata v12 can resolve the requested topic
names to non-zero UUIDs. Callers may provide UUIDs directly through
`ConsumerGroupOffsetQuery::topic_id` to avoid that metadata lookup. The
response UUIDs are mapped back to the supplied topic names. If Metadata v12 is
unavailable or cannot resolve every requested name, it sends the name-based
OffsetFetch v9 fallback. The alteration method uses the same policy for
OffsetCommit v10/v9, including optional static-member identity and the
committed leader epoch. Transient coordinator movement is retried within
`AdminClient::max_retries`; a stale member epoch is returned to the caller and
is never silently retried with an invalid membership identity.

Topic UUIDs may come from a Metadata v12 response or the joined group's
metadata snapshot. A zero or missing UUID allows the Admin method to resolve
the name through Metadata v12; if the broker cannot provide that capability,
the same API remains usable through the v9 path on Kafka 3.x brokers.

## Delete Records

```rust
use kafrust::{AdminClient, ClientConfig, DeleteRecordsOptions, DeleteRecordsTopic};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new([
    "localhost:19092",
    "localhost:19093",
]));
let result = admin
    .delete_records(
        &[
            DeleteRecordsTopic::new("orders")
                .partition(0, 100)
                .partition(1, -1),
            DeleteRecordsTopic::new("payments").partition(0, 50),
        ],
        DeleteRecordsOptions::new(),
    )
    .await?;

for topic in result.topics() {
    for partition in topic.partitions() {
        println!(
            "{}-{} low_watermark={} error={}",
            topic.name(),
            partition.partition_index(),
            partition.low_watermark(),
            partition.error_code(),
        );
    }
}
# Ok(())
# }
```

`AdminClient::delete_records` sends Metadata v1 first, groups the requested
partitions by their current leaders, and sends DeleteRecords v1 to each leader.
This matters in multi-broker clusters because a bootstrap broker is not
necessarily the leader for every requested partition. The result preserves
each partition's low watermark and broker error, including partial success.
Because deleting through a fixed offset is idempotent, transient transport,
leader-movement, and retryable partition errors are retried through fresh
metadata within the configured Admin retry budget.
An offset of `-1` asks Kafka to delete through the current high watermark.

## Describe Producers

```rust
use kafrust::{AdminClient, ClientConfig, DescribeProducersTopic};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:19092"]));
let result = admin
    .describe_producers(&[
        DescribeProducersTopic::new("orders")
            .partition(0)
            .partition(1),
    ])
    .await?;

for topic in result.topics() {
    for partition in topic.partitions() {
        for producer in partition.active_producers() {
            println!(
                "{}-{} producer={} epoch={} sequence={}",
                topic.name(),
                partition.partition_index(),
                producer.producer_id(),
                producer.producer_epoch(),
                producer.last_sequence(),
            );
        }
    }
}
# Ok(())
# }
```

`AdminClient::describe_producers` resolves Metadata v1 first and groups
DescribeProducers v0 requests by current partition leader. Each partition
retains its error code/message and active producer sequence state, so a
leader-specific authorization or availability failure does not erase results
for other partitions. Transient leader movement, metadata convergence errors,
transport disconnects, and request timeouts are retried through fresh metadata
within the configured `AdminClient::max_retries` budget. Set the budget to
zero to disable this recovery.

## Describe Transactions

```rust
use kafrust::{AdminClient, ClientConfig};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:19092"]));
let result = admin
    .describe_transactions(&["payments-tx".to_owned()])
    .await?;

for transaction in result.transactions() {
    println!(
        "{} state={} producer={} epoch={}",
        transaction.transactional_id(),
        transaction.state(),
        transaction.producer_id(),
        transaction.producer_epoch(),
    );
}
# Ok(())
# }
```

`AdminClient::describe_transactions` discovers the transaction coordinator
for each transactional ID, groups IDs by coordinator, and sends
DescribeTransactions v0. Transaction state, timeout, producer identity, and
the topic partitions currently in the transaction remain available in the
typed response. Coordinator movement, transport disconnects, request timeouts,
and transient coordinator responses are retried through fresh discovery using
the same bounded `max_retries` budget.

## List Transactions

```rust
use std::time::Duration;
use kafrust::{AdminClient, ClientConfig, ListTransactionsOptions};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:19092"]));
let result = admin
    .list_transactions(
        ListTransactionsOptions::new()
            .state("Ongoing")
            .duration_filter(Duration::from_secs(30)),
    )
    .await?;

for transaction in result.transactions() {
    println!(
        "{} state={} producer={}",
        transaction.transactional_id(),
        transaction.state(),
        transaction.producer_id(),
    );
}
# Ok(())
# }
```

`AdminClient::list_transactions` queries every broker returned by metadata,
because each broker owns a shard of the transaction-state topic, and merges
the results. State and producer-ID filters work with ListTransactions v0. A
duration filter selects v1 when advertised; it returns `Unsupported` rather
than silently dropping the filter if a broker only supports v0. Coordinator
movement, transport disconnects, request timeouts, and transient coordinator
responses use the bounded `max_retries` budget.

## Reassign Partitions

Partition reassignment requests are routed to the active controller. A target
replica list changes the preferred replica order; `cancel` sends Kafka's
nullable replica sentinel to cancel a pending reassignment. The status API
returns only reassignments still in progress, including replicas being added
and removed.

```rust
use kafrust::{
    AdminClient, ClientConfig, PartitionReassignment, PartitionReassignmentOptions,
    PartitionReassignmentQuery,
};
use std::time::Duration;

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new([
    "localhost:19092",
    "localhost:19093",
    "localhost:19094",
]));
let options = PartitionReassignmentOptions::new().timeout(Duration::from_secs(30));
let request = [PartitionReassignment::new("orders").partition(0, [3, 1, 2])];
let submitted = admin
    .alter_partition_reassignments(&request, options)
    .await?;
if !submitted.is_success() {
    eprintln!("reassignment rejected: {:?}", submitted.error_message());
}

let query = [PartitionReassignmentQuery::new("orders").partition(0)];
let status = admin
    .list_partition_reassignments(Some(&query), options)
    .await?;
for topic in status.topics() {
    for partition in topic.partitions() {
        println!(
            "{}-{} replicas={:?} adding={:?} removing={:?}",
            topic.name(),
            partition.partition_index(),
            partition.replicas(),
            partition.adding_replicas(),
            partition.removing_replicas(),
        );
    }
}
# Ok(())
# }
```

`list_partition_reassignments(None, options)` asks Kafka for every ongoing
reassignment. An empty topic result means the selected reassignment is no
longer in progress, but callers should verify final metadata when they need
to assert the broker's completed replica assignment. The repository's
`admin_reassign_partitions` example performs bounded status polling and is
live-verified on the Kafka 3.7.2 three-broker profile.

## Elect Leaders

`AdminClient::elect_leaders` routes Kafka's ElectLeaders request to the active
controller and negotiates API v0, v1, or v2. Pass `None` to ask Kafka to
consider every eligible partition, or pass explicit topic and partition
filters. A `LeaderElection` must contain at least one partition; an empty
filter is rejected rather than being confused with the all-partitions form.

```rust
use kafrust::{
    AdminClient, ClientConfig, ElectionType, ElectLeadersOptions, LeaderElection,
};
use std::time::Duration;

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new([
    "localhost:19092",
    "localhost:19093",
    "localhost:19094",
]));
let elections = [LeaderElection::new("orders").partition(0)];
let result = admin
    .elect_leaders(
        Some(&elections),
        ElectionType::Preferred,
        ElectLeadersOptions::new().timeout(Duration::from_secs(30)),
    )
    .await?;

for topic in result.topics() {
    for partition in topic.partitions() {
        println!(
            "{}-{}: Kafka error {}",
            topic.name(),
            partition.partition_index(),
            partition.error_code(),
        );
    }
}
# Ok(())
# }
```

Preferred elections are safe to repeat as an operational no-op; Kafka may
return `ELECTION_NOT_NEEDED` (84) when the preferred replica is already the
leader. `ElectionType::Unclean` is exposed for compatibility with Kafka's
one-shot unclean election operation, but it can select an out-of-sync replica
and lose records. Use it only with an explicit recovery policy. API v0 cannot
represent unclean elections, so kafrust returns `Unsupported` instead of
silently downgrading that request. The `admin_elect_leaders` example accepts
`KAFRUST_ELECTION_TYPE=preferred|unclean`, `KAFRUST_ELECTION_TOPIC`,
`KAFRUST_ELECTION_PARTITION`, and `KAFRUST_ELECTION_ALL`.

## Describe Log Directories

`AdminClient::describe_log_dirs` queries broker-local storage state. Pass
`None` for `broker_ids` to query every broker discovered from Metadata, and
pass `None` for `topics` to query every topic. An empty partition list on a
`LogDirTopic` means all partitions of that topic. The client negotiates
DescribeLogDirs v1-v5, preserving log-directory errors, replica sizes, offset
lag, future-log state, and v4+ volume capacity fields.

```rust
use kafrust::{AdminClient, ClientConfig, LogDirTopic};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new([
    "localhost:19092",
    "localhost:19093",
    "localhost:19094",
]));
let topics = [LogDirTopic::new("orders").partition(0)];
let brokers = admin.describe_log_dirs(None, Some(&topics)).await?;
for broker in brokers {
    println!("broker={}", broker.broker_id());
    for log_dir in broker.log_dirs() {
        println!(
            "path={} usable_bytes={} cordoned={}",
            log_dir.path(),
            log_dir.usable_bytes(),
            log_dir.is_cordoned(),
        );
        for topic in log_dir.topics() {
            for partition in topic.partitions() {
                println!(
                    "{}-{} size={} lag={} future={}",
                    topic.name(),
                    partition.partition_index(),
                    partition.partition_size(),
                    partition.offset_lag(),
                    partition.is_future(),
                );
            }
        }
    }
}
# Ok(())
# }
```

The `admin_describe_log_dirs` example accepts `KAFRUST_LOG_DIR_BROKERS` as a
comma-separated broker ID list, plus `KAFRUST_LOG_DIR_TOPIC` and the optional
`KAFRUST_LOG_DIR_PARTITION` filter. Broker-local paths and capacity values are
operational metadata; they should not be treated as portable filesystem
locations across clusters.

## Alter Replica Log Directories

`AdminClient::alter_replica_log_dirs` submits broker-local replica storage
moves. The broker ID is explicit because the destination path is local to that
broker, and assignments are grouped by destination path before encoding.
Kafka's v1 baseline and flexible v2 are negotiated from ApiVersions.

```rust
use kafrust::{AdminClient, ClientConfig, ReplicaLogDirAssignment};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let assignments = [ReplicaLogDirAssignment::new(
    "orders",
    0,
    "/var/lib/kafka-2",
)];
let result = admin.alter_replica_log_dirs(1, &assignments).await?;
if !result.is_success() {
    for topic in result.topics() {
        for partition in topic.partitions() {
            eprintln!(
                "{}-{} failed with Kafka error {}",
                topic.name(),
                partition.partition_index(),
                partition.error_code(),
            );
        }
    }
}
# Ok(())
# }
```

This is a mutating operation. kafrust retries only broker connection and
ApiVersions discovery before transmission; it never replays a request after a
send-side transport failure because the broker may already have started the
move. Poll `describe_log_dirs` after the request to observe `is_future`, lag,
and completion on the destination directory. The
`admin_alter_replica_log_dirs` example requires
`KAFRUST_REPLICA_LOG_DIR_BROKER`, `KAFRUST_REPLICA_LOG_DIR_TOPIC`,
`KAFRUST_REPLICA_LOG_DIR_PARTITION`, and
`KAFRUST_REPLICA_LOG_DIR_DESTINATION` explicitly.

## Create Topics

```rust
use kafrust::{AdminClient, ClientConfig, CreateTopicsOptions, NewTopic};
use std::time::Duration;

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .create_topics(
        &[
            NewTopic::new("orders", 6, 3)
                .config("cleanup.policy", "compact"),
            NewTopic::with_assignments(
                "payments",
                [(0, vec![1, 2, 3]), (1, vec![2, 3, 1])],
            ),
        ],
        CreateTopicsOptions::new()
            .timeout(Duration::from_secs(30))
            .validate_only(false),
    )
    .await?;

for topic in result.topics() {
    if topic.is_success() {
        println!("created {}", topic.name());
    } else {
        eprintln!(
            "{} failed with Kafka error {}: {}",
            topic.name(),
            topic.error_code(),
            topic.error_message().unwrap_or("no broker message"),
        );
    }
}
# Ok(())
# }
```

Kafka CreateTopics responses are independently successful or unsuccessful per
topic. `CreateTopicsResult` therefore preserves every `CreateTopicResult`
instead of returning the first topic error as the operation error. Connection,
timeout, framing, and decoding failures still return `kafrust::Error`.

`NewTopic::new` requests automatic replica placement. Use
`NewTopic::with_assignments` for explicit partition-to-broker placement; it
sets Kafka's partition count and replication factor fields to `-1` as required
by the protocol.

The current alpha path uses CreateTopics v2, which keeps the request
non-flexible and compatible with the project's Kafka 3.7-to-current support
window.

The current-source
[`live-create-topics-authorization.yml`](../.github/workflows/live-create-topics-authorization.yml)
gate passed on Kafka 3.7.2 and 4.3.1 in
[`32364633106`](https://github.com/TaeeunKil/kafrust/actions/runs/32364633106).
A SASL/PLAIN principal with only cluster `Describe` received the per-topic
`TopicAuthorizationFailed` result (error 29) and the topic was not created;
the administrator then created and deleted the same topic. This is an
operation-specific current-source authorization proof, not a universal ACL
policy claim.

## Expand Topic Partitions

```rust
use kafrust::{
    AdminClient, ClientConfig, CreatePartitionsOptions, NewPartitions,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .create_partitions(
        &[
            NewPartitions::new("orders", 12),
            NewPartitions::with_assignments(
                "payments",
                4,
                [vec![1, 2, 3], vec![2, 3, 1]],
            ),
        ],
        CreatePartitionsOptions::new(),
    )
    .await?;

for topic in result.topics() {
    println!("{}: error={}", topic.name(), topic.error_code());
}
# Ok(())
# }
```

The count is the new total partition count and must be greater than the
topic's current count. `NewPartitions::new` delegates replica placement to
Kafka. `with_assignments` supplies one broker list for each newly added
partition in ascending partition order. CreatePartitions v0 is
controller-scoped, supports validation-only requests, and preserves per-topic
errors in `CreatePartitionsResult`.

The current-source
[`live-create-partitions-authorization.yml`](../.github/workflows/live-create-partitions-authorization.yml)
gate passed on Kafka 3.7.2 and 4.3.1 in
[`32366048755`](https://github.com/TaeeunKil/kafrust/actions/runs/32366048755).
A restricted SASL/PLAIN principal with cluster/topic discovery, but without
the partition-change permission, received `TopicAuthorizationFailed` (error
29) and the one-partition topic remained unchanged. The administrator then
expanded it to two partitions and cleaned it up. This is an operation-specific
current-source authorization proof, not a universal ACL or Admin mutation
parity claim.

## Delete Topics

```rust
use kafrust::{AdminClient, ClientConfig, DeleteTopicsOptions};
use std::time::Duration;

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let result = admin
    .delete_topics(
        &["orders".to_owned(), "payments".to_owned()],
        DeleteTopicsOptions::new().timeout(Duration::from_secs(30)),
    )
    .await?;

for topic in result.topics() {
    if !topic.is_success() {
        eprintln!("{}: Kafka error {}", topic.name(), topic.error_code());
    }
}
# Ok(())
# }
```

DeleteTopics v3 also routes to the active controller and preserves independent
topic outcomes. Version 3 responses contain topic names and error codes but no
broker error-message field.

The current-source
[`live-delete-topics-authorization.yml`](../.github/workflows/live-delete-topics-authorization.yml)
gate passed on Kafka 3.7.2 and 4.3.1 in
[`32365120994`](https://github.com/TaeeunKil/kafrust/actions/runs/32365120994).
A restricted SASL/PLAIN principal with cluster and target-topic `Describe`, but
without delete permission, received `TopicAuthorizationFailed` (error 29) and
the topic remained present. The administrator then deleted the topic. This is
an operation-specific current-source authorization proof, not a universal ACL
or Admin mutation parity claim.

## Describe, Create, and Delete ACLs

```rust
use kafrust::{
    AclBinding, AclFilter, AclOperation, AclPatternType, AclPermissionType,
    AclResourceType, AdminClient, ClientConfig,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let binding = AclBinding::new(
    AclResourceType::Topic,
    "orders",
    AclPatternType::Literal,
    "User:orders-service",
    "*",
    AclOperation::Read,
    AclPermissionType::Allow,
);

let created = admin.create_acls(&[binding.clone()]).await?;
for result in created.results() {
    println!(
        "{}: Kafka error {}",
        result.binding().resource_name(),
        result.error_code(),
    );
}

let filter = AclFilter::any()
    .resource_type(AclResourceType::Topic)
    .resource_name("orders")
    .operation(AclOperation::Read);
let described = admin.describe_acls(&filter).await?;
println!("{} ACLs matched", described.bindings().len());

let deleted = admin.delete_acls(&[filter]).await?;
for result in deleted.filter_results() {
    println!("deleted {} matching ACLs", result.matching_acls().len());
}
# Ok(())
# }
```

The ACL methods use Kafka DescribeAcls v1, CreateAcls v1, and DeleteAcls v1.
They preserve top-level, per-binding, per-filter, and matching-ACL outcomes so
authorization failures are not collapsed into a single transport error.
The broker must grant the caller the corresponding authorizer permissions;
these methods do not bypass Kafka authorization.

The wire encoders, decoders, and mock-broker AdminClient paths are tested. The
focused `Live Kafka Smoke` ACL authorizer job passed against Kafka 3.7.2
StandardAuthorizer in manual run `31457478358` on 2026-08-11 using an
explicitly provisioned `User:ANONYMOUS` superuser. Production migrations must
still qualify the target broker's authorizer policy and service principal.

## Describe and Alter Client Quotas

```rust
use kafrust::{
    AdminClient, ClientConfig, ClientQuotaAlteration, ClientQuotaEntity,
    ClientQuotaFilter, ClientQuotaFilterComponent, ClientQuotaMatchType,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let entity = ClientQuotaEntity::user("alice");
admin
    .alter_client_quotas(
        &[ClientQuotaAlteration::new(entity).set("producer_byte_rate", 1_048_576.0)],
        false,
    )
    .await?;

let filter = ClientQuotaFilter::any().component(ClientQuotaFilterComponent::new(
    "user",
    ClientQuotaMatchType::Exact,
    Some("alice"),
));
let result = admin.describe_client_quotas(&filter).await?;
for entry in result.entries() {
    for value in entry.values() {
        println!("{}={}", value.key(), value.value());
    }
}
# Ok(())
# }
```

Client quota operations use DescribeClientQuotas v0 and AlterClientQuotas v0.
Entity components, filter match modes, floating-point quota values, validation
mode, throttle time, and per-entity error outcomes remain typed. Use
`ClientQuotaAlteration::remove` to restore a broker default. The wire value is
`FLOAT64`, but Kafka validates individual quota keys; for example,
`producer_byte_rate` must be a whole number of bytes per second.

## Manage SCRAM Credentials

```rust
use kafrust::{
    AdminClient, ClientConfig, ScramCredentialDeletion, ScramCredentialMechanism,
    ScramCredentialUpsertion,
};

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let username = "orders-service";
let mechanism = ScramCredentialMechanism::Sha256;

let upsertion = ScramCredentialUpsertion::new(
    username,
    mechanism,
    4096,
    b"secret-from-a-secret-manager",
)?;
let altered = admin
    .alter_user_scram_credentials(&[], &[upsertion])
    .await?;
assert!(altered.is_success());

let users = [username.to_owned()];
let described = admin
    .describe_user_scram_credentials(Some(&users))
    .await?;
for user in described.users() {
    for credential in user.credentials() {
        println!(
            "{} {:?} iterations={}",
            user.username(),
            credential.mechanism(),
            credential.iterations()
        );
    }
}

let deletion = ScramCredentialDeletion::new(username, mechanism)?;
let removed = admin
    .alter_user_scram_credentials(&[deletion], &[])
    .await?;
assert!(removed.is_success());
# Ok(())
# }
```

These methods use DescribeUserScramCredentials v0 and AlterUserScramCredentials
v0. Describe accepts `None` to request every user or an explicit user slice;
alter returns one typed outcome per affected user and routes through the active
controller. `ScramCredentialUpsertion` derives Kafka's salted password with
PBKDF2 and retains no plaintext password. Its debug output reports only lengths,
never salts or derived credential bytes. Kafka authorization still applies, and
the caller must have the broker permissions required for credential changes.

## Manage Delegation Tokens

Delegation token operations must use an authenticated SASL or mutual-TLS
channel; Kafka rejects token management over unauthenticated PLAINTEXT,
one-way TLS, and delegation-token authenticated channels. The broker must also
be configured with the same
delegation token secret on every broker and, for KRaft, every controller. See
Kafka's [broker configuration](https://kafka.apache.org/38/configuration/broker-configs/)
for the version-specific secret-key name and defaults.

```rust
use kafrust::{
    AdminClient, ClientConfig, CreateDelegationTokenOptions, DelegationTokenPrincipal,
};
use std::time::Duration;

# async fn example() -> kafrust::Result<()> {
let admin = AdminClient::new(ClientConfig::new(["localhost:9092"]));
let renewer = DelegationTokenPrincipal::new("User", "orders-service");
let created = admin
    .create_delegation_token(
        CreateDelegationTokenOptions::new().renewer(renewer),
    )
    .await?;
assert!(created.is_success());

let described = admin.describe_delegation_tokens(None).await?;
assert!(described.is_success());
for token in described.tokens() {
    println!(
        "{} expires at {} (HMAC length {})",
        token.token_id(),
        token.expiry_timestamp_ms(),
        token.hmac().len(),
    );
}

let renewed = admin
    .renew_delegation_token(created.hmac(), Duration::from_secs(60))
    .await?;
assert!(renewed.is_success());

let expired = admin
    .expire_delegation_token(created.hmac(), Duration::ZERO)
    .await?;
assert!(expired.is_success());
# Ok(())
# }
```

`CreateDelegationToken`, `RenewDelegationToken`, `ExpireDelegationToken`, and
`DescribeDelegationToken` negotiate the highest supported Kafka API version in
the client-supported ranges. The current implementation uses v1-v3 for create
and describe, and v1-v2 for renew and expire; flexible encoding is used from
v2 onward, while create/describe v3 preserves requester and explicit-owner
details. Controller discovery and ApiVersions negotiation are retried before a
request is transmitted. Mutating requests are never replayed after a send,
because a lost response leaves the broker-side outcome ambiguous.

The HMAC returned by create and describe is credential material. It is exposed
only through an explicit `hmac()` accessor, while `Debug` and tracing redact
the bytes. Store it in a secret manager and do not include it in application
logs, metrics labels, error messages, or crash reports. The
`admin_delegation_tokens` example performs the complete lifecycle without
printing the HMAC.
