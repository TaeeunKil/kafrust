# Admin API

`AdminClient` uses the same `ClientConfig` as low-level broker connections, so
TLS, SASL, request timeouts, decode limits, and shared metrics apply to admin
operations. Controller-scoped operations discover the current controller from
cluster metadata before opening the request connection.

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

Config inspection and alteration and consumer-group administration remain on
the M16 roadmap.
