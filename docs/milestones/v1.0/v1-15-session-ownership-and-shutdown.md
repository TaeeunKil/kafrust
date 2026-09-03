# V1-15 Session Ownership And Shutdown

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-04-V1-14

## User-Visible Objective

Ensure every client, connection, bounded queue, background worker, heartbeat,
and broker session has one explicit owner and reaches a deterministic terminal
state on success, cancellation, timeout, fault, and shutdown.

## Non-Goals

- No universal connection pool shared across incompatible Kafka sessions.
- No detached background task without a join/error path.
- No broad `Arc<Mutex<_>>` redesign.
- No alternate async-runtime abstraction unless V1-02 explicitly makes it a
  stable requirement.

## Scope

- `crates/kafrust/src/{client,broker_client_cache,producer,consumer,group,
  share_consumer,streams,admin,telemetry,blocking,metrics,error}.rs`
- producer shared idle cache, Admin clone-local cache, direct consumer
  instance-local Fetch sessions, classic/KIP-848 membership, Share broker
  epochs, Streams member epochs, telemetry same-connection subscription, and
  blocking adapter runtimes
- all bounded channels, spawned tasks, cancellation tokens/signals, timeouts,
  join handles, connection poison/return paths, and clone/drop behavior
- scripted-broker connection observation and live connection-churn workflows

## Work Packages

1. Add an ownership table for every connection/session/task/channel with owner,
   identity lease, capacity, saturation policy, cancellation, and join path.
2. Audit every `.await` scope for lock guards and every spawn for lifecycle/error
   handling.
3. Instrument connection opens/reuse/discards, task starts/joins, queue peaks,
   and final gauges without high-cardinality labels.
4. Fault or cancel each owner before connect, during request, after response
   loss, during rejoin, and during close.
5. Verify cache boundaries: producer and Admin stateless reuse; direct Fetch,
   group, Share, Streams, and telemetry session identities never leak across
   owners.
6. Qualify repeated construct/use/fault/close cycles from a published artifact.

## Current Execution Record (2026-08-22)

V1-15 is now `In progress`. The ownership audit on source commit `e6de5c5`
keeps the stateless producer/Admin idle-cache paths separate from direct Fetch,
classic/KIP-848, Share, Streams, and telemetry sessions that carry membership,
epoch, or subscription identity. Buffered producers, group heartbeats, Share
heartbeats, Streams heartbeats, and the blocking adapter retain explicit task or
runtime handles rather than detached lifecycle claims.

The deterministic slice currently covers FIFO/zero-limit cache behavior
(`evicts_idle_connections_in_fifo_order`,
`zero_limit_keeps_one_connection_instead_of_growing_unbounded`, and
`connection_identity_changes_reset_the_shared_cache`), poisoned-connection
non-reuse after timeout, classic and KIP-848 heartbeat cancellation, Share
heartbeat cancellation/unknown-ack close, Streams graceful close, bounded
producer enqueue/shutdown behavior, and blocking nested-runtime rejection.
These tests establish the owner boundaries but do not close the required
100-cycle gauge audit or published secured churn profile.

The remaining work is to turn the static owner inventory into a checked table,
record task/queue/connection peaks and final gauges for every stable surface,
and run the 100-cycle and exact-artifact churn gates. No stale-session or
detached-task completion claim is made by this record.

### Checked ownership inventory (2026-09-03)

The static owner inventory is now machine-readable at
[`v1-15-ownership-inventory.json`](../../evidence/v1-15-ownership-inventory.json)
and validated by [`check_v1_ownership_inventory.py`](../../../scripts/check_v1_ownership_inventory.py).
It enumerates the producer and Admin stateless caches, buffered worker, direct
Fetch session, classic and KIP-848 memberships, Share and Streams sessions,
telemetry subscription, and blocking runtime. Every row declares a finite
capacity, saturation policy, identity lease, cancellation rule, join path,
fault points, and deterministic verification references. Five focused checker
tests and the CI check pass. This closes the static inventory criterion only;
cycle gauges and published secured churn remain required.

## Failure And Lifecycle Contract

- A connection is checked out by one request and returned only after framing,
  protocol, authentication, and owner state remain valid.
- Transport/framing/timeout/cancellation ambiguity poisons the socket when
  continuation is unsafe.
- Bounded queues define fail, wait, or backpressure behavior and are woken on
  shutdown.
- Membership/session tasks stop before their identity owner is reused or
  dropped; stale epochs/generations cannot write.
- Blocking adapters reject nested Tokio contexts explicitly, stop their runtime,
  and surface task errors without panic.
- Cleanup errors do not erase an earlier unknown mutation/transaction outcome.

## Verification

- Deterministic owner-by-owner fault table with connection and request counts.
- Tests prove no mutex/RwLock guard is intentionally held across `.await` unless
  its documented primitive permits it and the invariant is reviewed.
- 100 construct/use/fault/close cycles for every stable high-level client and
  feature-gated adapter; final task, queue, connection, in-flight, and buffered
  gauges are zero after each cycle.
- Published pinned-current three-broker SASL_SSL/SCRAM-SHA-256 churn with
  producer, consumer, classic/KIP-848, Share, retained Streams, Admin, telemetry,
  and blocking surfaces selected by V1-02.

## Exit Criteria

1. Every stable owner appears in the ownership table with finite capacity and
   lifecycle rules.
2. Deterministic fault coverage proves no stale session identity or poisoned
   connection is reused.
3. All stable clients pass 100 local lifecycle cycles with zero final gauges.
4. The exact published artifact passes the secured multi-client churn profile.
5. Ownership docs, metrics, API audit, and evidence ledger are complete.

## Migration And Rollback

Document which handles are cloneable, which owners must be closed, and how long
shutdown may block. A rollback must not reintroduce generic caching for
membership/session sockets or detached task behavior. Preserve epoch/generation
identity across adapter changes.

## Conventional Commit Plan

1. `test(runtime): audit session ownership and shutdown faults`
2. `fix(runtime): close task and connection lifecycle gaps`
3. `ci(runtime): qualify secured connection churn`
4. `docs(runtime): record ownership and saturation contracts`

## Evidence Record On Completion

Record each owner/session identity, queue capacity/peak, connection opens/reuse/
discards, task starts/joins, cycle count, fault points, final gauges, artifact,
security profile, and excluded-runtime non-claim.
