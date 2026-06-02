# Consumer Group Direction

Consumer groups should be added incrementally after the direct consumer path is stable. The first group milestone is coordinator discovery; group membership and offset commits build on top of it.

```rust
use kafrust::ClientConfig;

let mut client = ClientConfig::new(["localhost:9092"])
    .client_id("orders-reader")
    .connect()
    .await?;

let coordinator = client.find_group_coordinator("orders-group").await?;
println!("coordinator {}:{}", coordinator.host, coordinator.port);
```

Offset fetches and commits are coordinator-scoped Kafka requests. Discover the coordinator first, connect to `coordinator.host:coordinator.port`, then issue `offset_fetch_v2` or `offset_commit_v2` on that coordinator connection.

Current implementation status:

- FindCoordinator v1 request/response protocol types exist.
- `Client::find_group_coordinator` can ask Kafka for a group coordinator.
- JoinGroup v2 request/response protocol types exist.
- SyncGroup v2 request/response protocol types exist.
- Heartbeat v2 request/response protocol types exist.
- OffsetFetch v2 request/response protocol types exist.
- OffsetCommit v2 request/response protocol types exist.
- `Client::offset_fetch_v2` and `Client::offset_commit_v2` can send coordinator-scoped offset requests.
- Rebalance handling is not implemented yet.

Run the opt-in coordinator example against a local broker:

```bash
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 KAFRUST_GROUP_ID=orders-group cargo run -p kafrust --example find_group_coordinator
```
