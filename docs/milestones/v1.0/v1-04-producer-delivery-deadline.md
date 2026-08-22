# V1-04 Producer Delivery Deadline

- Status: Planned
- Target evidence: Published artifact
- Dependencies: V1-03

## User-Visible Objective

Guarantee one documented total delivery deadline from API acceptance through
metadata, batching, retries, backoff, Produce, and shutdown for immediate,
batch, and buffered producers.

## Non-Goals

- No exactly-once or idempotent sequence claim; V1-05 owns it.
- No guarantee that `acks=0` records reached the broker.
- No use of `.clone()` or detached tasks to bypass producer ownership.
- No throughput target; V1-22 owns performance SLOs.

## Scope

- `crates/kafrust/src/{config,producer,client,broker_client_cache,error}.rs`
- `crates/kafrust/tests/fault_injection.rs` and scripted-broker support
- immediate, batch, buffered owner/handle, linger, queue, flush, and close paths
- Produce API 0 versions selected by V1-03 plus Metadata and ApiVersions time
- producer examples, `docs/producer-api.md`, `docs/producer-buffering.md`, and
  migration timeout mapping
- live/published producer workflows

The current source default `delivery_timeout_ms` is 120,000. This milestone
must either stabilize that value or change it with an explicit migration note.

## Work Packages

1. Freeze the distinction among request timeout, linger, queue waiting, and
   total delivery deadline. Add a typed delivery-deadline error carrying a
   finite `DeliveryPhase` and `possibly_transmitted` flag; do not reuse the
   request-timeout error for both replay-safe and ambiguous expiry.
2. Add deterministic clock-controlled tests for every producer entry point.
3. Verify expired buffered records cause zero Produce transmissions and do not
   hold capacity indefinitely.
4. Verify an in-flight deadline poisons the connection and invalidates bounded
   routing/capability state without replay after the caller's deadline.
5. Define close behavior for accepted, expired, in-flight, and handle-owned
   records; final resource gauges must drain.
6. Publish a new pre-1.0 artifact and run external deadline profiles.

## Failure And Lifecycle Contract

| Condition | Required result |
| --- | --- |
| Deadline before transmission | `Error::DeliveryDeadlineExceeded { phase, possibly_transmitted: false, timeout_ms }`; zero Produce frames. |
| Deadline during/after write | `Error::DeliveryDeadlineExceeded { phase, possibly_transmitted: true, timeout_ms }`; connection discarded; non-idempotent duplicate risk documented. |
| Per-request timeout | Remains a distinct transport-attempt error and may retry only inside the remaining delivery budget and replay policy. |
| Retry/backoff | Uses remaining total budget, never restarts 120 seconds. |
| Buffered cancellation | Queue slot and delivery sender are released deterministically. |
| Shutdown | Known deliveries finish within remaining budgets; expired entries fail; worker joins and gauges reach zero. |

## Verification

- Clock-controlled tests cover immediate, mixed batch, buffered queue expiry,
  linger greater than remaining budget, metadata delay, capability delay,
  retry backoff, delayed Produce response, cancellation, flush, and close.
- For every pre-send expiry case, the scripted broker observes zero Produce
  requests.
- For every post-write expiry, the old socket is never returned to a cache.
- Public/error snapshot tests distinguish pre-send delivery expiry,
  possibly-transmitted delivery expiry, and per-request timeout without parsing
  strings.
- Published external projects run at least floor plaintext and pinned-current
  three-broker SASL_SSL/SCRAM-SHA-256 profiles with 1,000 intentionally mixed
  success/expiry records and exact per-record reconciliation.
- Final `in_flight_requests` and `buffered_records` are zero.

## Exit Criteria

1. All three producer modes share the documented total-budget calculation.
2. No test path exceeds its configured deadline by more than deterministic
   scheduler tolerance recorded in the fixture.
3. Pre-send expiry transmits zero records; post-send ambiguity is not described
   as broker rejection.
4. Both published profiles reconcile all 1,000 outcomes and end with zero
   resource gauges.
5. Callers can mechanically distinguish replay-safe pre-send expiry,
   possibly-transmitted expiry, and per-request timeout.
6. Defaults, errors, migration notes, and evidence ledger agree.

## Migration And Rollback

Map rust-rdkafka `delivery.timeout.ms` to this total deadline and keep
`linger_ms` separate. A rollback may restore prior scheduling only if it also
restores docs/tests; never return a timed-out possibly interrupted socket to the
cache. Callers requiring reconciliation must retain record identity.

## Conventional Commit Plan

1. `test(producer): cover total delivery deadline boundaries`
2. `fix(producer): enforce one delivery budget across all modes`
3. `ci(producer): qualify published delivery deadlines`
4. `docs(producer): stabilize timeout and shutdown semantics`

## Evidence Record On Completion

Record artifact versions, timeout/linger/request values, mode, transmissions,
success/expiry/ambiguous counts, final gauges, broker/security profiles, and
the explicit non-claim for `acks=0` and non-idempotent duplicates.
