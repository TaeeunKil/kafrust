# Admin API

`AdminClient` uses the same `ClientConfig` as low-level broker connections, so
TLS, SASL, request timeouts, decode limits, and shared metrics apply to admin
operations. Controller-scoped operations discover the current controller from
cluster metadata before opening the request connection.

Call `AdminClient::validate()` during startup to check the connection settings
without opening a broker connection. This includes bootstrap entries, request
and decode limits, required SASL credentials, and an explicitly configured TLS
server name.

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
broker and controller data without enumerating topics. `list_topics` requests
all visible topics. Topic-level metadata errors remain available through
`TopicListing::error_code` and `broker_error_kind` instead of aborting the
entire listing.

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
include Kafka's config synonyms. Resource failures remain in
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

ListGroups v1 is broker-scoped, so `list_groups` discovers the cluster and
queries every advertised broker before sorting and deduplicating the results.
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

`list_consumer_group_offsets_with_member` sends OffsetFetch v9 with the
member ID, member epoch, and optional `require_stable` flag. The alteration
method sends OffsetCommit v9, including optional static-member identity and
the committed leader epoch. Transient coordinator movement is retried within
`AdminClient::max_retries`; a stale member epoch is returned to the caller and
is never silently retried with an invalid membership identity.

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
