# Streams Group

`StreamsGroupSession` is kafrust's alpha wrapper for Kafka's dedicated Streams
group membership protocol (`StreamsGroupHeartbeat`, API key 88). It manages the
broker-facing member lifecycle and leaves record processing, state stores, and
the Kafka Streams DSL to the application.

## Scope

The current session supports:

- topology publication during the initial heartbeat
- client-generated member ID and broker-provided member and endpoint epochs
- nullable active, standby, warmup, task-offset, and end-offset updates
- a typed `StreamsGroupSessionAssignment` snapshot containing the latest
  broker status entries, task assignment, recovery-lag, offset-interval, and
  Interactive Queries endpoint information
- bounded coordinator reconnect and member rejoin
- graceful leave with member epoch `-1` and `shutdown_application=true`

It does not implement a Kafka Streams processor, state store, task scheduler,
or source/sink processing. After joining, callers can move the session into a
`StreamsGroupSessionHandle` with `spawn_heartbeat_task()`. That handle owns a
bounded command queue, sends heartbeats at Kafka's advertised interval, and
publishes the latest assignment through `subscribe_assignment()`. The caller
still owns task-runtime reconciliation and must await `close()` when a graceful
member leave is required; dropping the handle aborts the task without a broker
leave guarantee.

## Example

The repository smoke example exercises the initial join, background task-state
heartbeat, assignment watch notification, nullable offset omission, and
graceful leave:

```powershell
$env:KAFRUST_BOOTSTRAP_SERVERS = "localhost:9092"
cargo run -p kafrust --example streams_group_smoke
```

The manual/weekly live qualification is defined in
`.github/workflows/live-streams-group.yml` and uses Kafka 4.3.1 with the
Streams group protocol enabled. The gate passed on commit `eae39b4` in
[run 32334600160](https://github.com/TaeeunKil/kafrust/actions/runs/32334600160),
covering join, background task-state heartbeat, assignment notification,
nullable task-offset omission, and graceful leave. This qualifies the bounded
Streams membership lifecycle; it does not establish compatibility with a
complete Kafka Streams application.

## Stability

This is an alpha, expert-level API. The session now preserves the latest
successful assignment snapshot without collapsing nullable fields. Before
`1.0`, the project still needs published-artifact qualification of the
background handle, automatic assignment/task-runtime reconciliation,
multi-member and coordinator-failure qualification, and a real Kafka Streams
application test.
