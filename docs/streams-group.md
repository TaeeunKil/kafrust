# Streams Group

`StreamsGroupSession` is kafrust's alpha wrapper for Kafka's dedicated Streams
group membership protocol (`StreamsGroupHeartbeat`, API key 88). It manages the
broker-facing member lifecycle and leaves record processing, state stores, and
the Kafka Streams DSL to the application.

## Scope

The current session supports:

- topology publication during the initial heartbeat
- broker-assigned member and endpoint information epochs
- nullable active, standby, warmup, task-offset, and end-offset updates
- a typed `StreamsGroupSessionAssignment` snapshot containing the latest
  broker status entries, task assignment, recovery-lag, offset-interval, and
  Interactive Queries endpoint information
- bounded coordinator reconnect and member rejoin
- graceful leave with member epoch `-1` and `shutdown_application=true`

It does not implement a Kafka Streams processor, state store, task scheduler,
source/sink processing, or a background heartbeat worker yet. The caller must
invoke `heartbeat()` within the interval returned by
`StreamsGroupSession::heartbeat_interval()` and reconcile the returned
`StreamsGroupSessionAssignment` with its task runtime.

## Example

The repository smoke example exercises the initial join, task-state heartbeat,
and graceful leave:

```powershell
$env:KAFRUST_BOOTSTRAP_SERVERS = "localhost:9092"
cargo run -p kafrust --example streams_group_smoke
```

The manual/weekly live qualification is defined in
`.github/workflows/live-streams-group.yml` and uses Kafka 4.3.1 with the
Streams group protocol enabled. Source-level and injected-broker coverage does
not yet establish compatibility with a complete Kafka Streams application.

## Stability

This is an alpha, expert-level API. The session now preserves the latest
successful assignment snapshot without collapsing nullable fields. Before
`1.0`, the project still needs a cancellable background heartbeat lifecycle,
automatic assignment/task-runtime reconciliation, multi-member and
coordinator-failure qualification, and a real Kafka Streams application test.
