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
- a `StreamsTaskRuntime` reconciliation primitive with canonical task IDs,
  active/standby/warmup roles, deterministic add/remove/role-change
  transitions, and assignment conflict validation
- bounded coordinator reconnect and member rejoin
- graceful leave with member epoch `-1` and `shutdown_application=true`

It does not implement a Kafka Streams processor, state store, task scheduler,
or source/sink processing. After joining, callers can move the session into a
`StreamsGroupSessionHandle` with `spawn_heartbeat_task()`. That handle owns a
bounded command queue, sends heartbeats at Kafka's advertised interval, and
publishes the latest assignment through `subscribe_assignment()`. The caller
can reconcile the latest snapshot with `reconcile_task_runtime()` and remains
responsible for creating, processing, and closing the actual stream tasks. It
must await `close()` when a graceful member leave is required; dropping the
handle aborts the task without a broker leave guarantee.

## Task runtime reconciliation

`StreamsTaskRuntime::reconcile_assignment()` follows the Kafka response
contract: a `null` active, standby, or warmup field means that role is
unchanged, while `Some(Vec::new())` explicitly removes every task in that
role. Task IDs are canonicalized as `(subtopology_id, sorted partitions)` so a
partition-order-only broker update produces no lifecycle event. Before the
state is committed, the runtime rejects empty or invalid task IDs, duplicate
partitions, and a partition claimed by more than one local task. A failed
reconciliation leaves the previous runtime state intact.

The returned `StreamsTaskTransition` values are an application boundary, not a
processor implementation. Applications still need to apply them to their
consumer assignments, state stores, changelog restoration, and processing
loops. The runtime is intentionally bounded to assignment lifecycle and does
not claim Kafka Streams DSL compatibility.

## Example

The repository smoke example exercises the initial join, background task-state
heartbeat, assignment watch notification, nullable offset omission, and
graceful leave:

```powershell
$env:KAFRUST_BOOTSTRAP_SERVERS = "localhost:9092"
cargo run -p kafrust --example streams_group_smoke
```

The live qualification is defined in `.github/workflows/live-streams-group.yml`
and uses Kafka 4.3.1 with the Streams group protocol enabled. The current
source gate passed on commit `55e0d8b` in
[run 32373425539](https://github.com/TaeeunKil/kafrust/actions/runs/32373425539),
covering join, background task-state heartbeat, assignment notification,
nullable task-offset omission, two-member membership, member departure
convergence, and graceful leave. The log confirmed the broker-advertised
`5000ms` heartbeat interval, `members=2`, `remaining_members=1`, and a clean
lifecycle close. The three-broker coordinator-stop gate passed on commit
`21ec3fd` in [run 32374858753](https://github.com/TaeeunKil/kafrust/actions/runs/32374858753),
covering a coordinator node stop, post-stop heartbeat recovery, and clean
leave. Together these qualify the bounded Streams membership and background
heartbeat lifecycle on single- and multi-broker plaintext clusters. The
published `kafrust 0.3.3` surface was also compiled from a fresh external Cargo
project with no workspace path dependency on both stable Rust and Rust 1.81 in
[run 32380345199](https://github.com/TaeeunKil/kafrust/actions/runs/32380345199);
this proves package/API availability, not broker runtime compatibility or
compatibility with a complete Kafka Streams application. The published
single-broker runtime gate also passed on Rust 1.81 in
[run 32381356444](https://github.com/TaeeunKil/kafrust/actions/runs/32381356444),
covering published-crate join, background heartbeat, task-runtime
reconciliation, dependency verification, and graceful leave against Kafka
4.3.1.

## Stability

This is an alpha, expert-level API. The session preserves the latest
successful assignment snapshot without collapsing nullable fields, and the
task-runtime reconciliation primitive now provides deterministic local
lifecycle transitions. Before `1.0`, the project still needs a real Kafka
Streams application test and
integration of transitions with consumer assignment, state restoration, and
processing execution. The published artifact surface check is defined in
`.github/workflows/published-streams-surface.yml`; the published broker runtime
gate is defined in `.github/workflows/published-streams-group-runtime.yml`.
