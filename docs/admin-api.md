# Admin API

`AdminClient` uses the same `ClientConfig` as low-level broker connections, so
TLS, SASL, request timeouts, decode limits, and shared metrics apply to admin
operations. Controller-scoped operations discover the current controller from
cluster metadata before opening the request connection.

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
unclean process exit.

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
