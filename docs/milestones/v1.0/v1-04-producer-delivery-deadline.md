# V1-04 Producer Delivery Deadline

- Status: In progress
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

## Current Execution Record (2026-08-22)

The public error contract now separates a total delivery deadline from a
per-request transport timeout. `DeliveryDeadlineExceeded` carries the finite
`DeliveryPhase` and a `possibly_transmitted` boolean; queue expiry is classified
as pre-send while an immediate/batch outer deadline conservatively retires the
producer and reports the Produce phase. Buffered expiry and producer retry
classification have focused regressions, and the full public API snapshot was
regenerated for the new root export. Local producer tests pass (including the
deadline, poisoning, and no-retry cases). The pushed candidate's stable and
Rust 1.81.0 jobs are green in
[CI run 32548809314](https://github.com/TaeeunKil/kafrust/actions/runs/32548809314).

The published artifact profiles, clock-controlled coverage for every producer
entry point, and exact CI/live evidence remain open. V1-03's live/golden gates
also remain a prerequisite for closing this milestone.

The current-source company workstation short smoke also passed immediate and
buffered producer examples (normal and idempotent) against isolated Kafka
4.3.1; it is recorded in
[`v1-company-workstation-current-short-smoke-2026-09-03.md`](../../evidence/v1-company-workstation-current-short-smoke-2026-09-03.md).
This does not replace the published mixed-outcome reconciliation or long SLO
requirements.

The same short workstation run also passed normal and idempotent buffered
delivery against Kafka 3.7.2; see
[`v1-company-floor-short-smoke-2026-09-03.md`](../../evidence/v1-company-floor-short-smoke-2026-09-03.md).
This is diagnostic support for the planned floor line, not published deadline
or SLO evidence.

### Buffered budget clock-anchor slice (2026-09-03)

`buffered_remaining_delivery_timeout` now receives an explicit clock anchor;
the worker passes `Instant::now()`, while deterministic tests can assert the
oldest request's exact remaining budget. Producer unit coverage now includes
oldest/newer requests (80 ms remaining from a 100 ms budget), an expired
request (zero remaining), and an empty pending batch (configured timeout
retained). All 116 producer unit tests and the required local Rust validation
passed on source commit `b838fa3`. This narrows the clock-controlled gap only;
every producer entry point, published mixed-outcome reconciliation, and live
evidence remain required.

### Buffered queue expiry through the worker (2026-09-03)

At source commit `fb2778b370e466af01f08d60e6d0ec26053cc1e5`,
`buffered_delivery_deadline_expires_before_produce_without_transmission`
exercises the real `BufferedProducer` worker with a 20 ms delivery budget and
10 second linger. The delivery returns the typed queue-phase deadline,
`possibly_transmitted=false`, drains the buffered-record gauge, and the
scripted broker observes zero Produce requests. The detailed record is
[`v1-buffered-queue-expiry-2026-09-03.md`](../../evidence/v1-buffered-queue-expiry-2026-09-03.md).

This closes the buffered queue-expiry/no-transmission slice only; delayed
metadata/capability, post-write deadlines, cancellation/shutdown ambiguity,
published mixed-outcome reconciliation, and live evidence remain open.

### Close flushes accepted buffered data (2026-09-03)

At source commit `40905f137e5a1f13cb99e4923d95e35e4aa3f1c8`,
`buffered_close_flushes_accepted_record_before_worker_shutdown` enqueues a
record with a long linger and calls `BufferedProducer::close()` directly. The
close path performs Metadata, ApiVersions, and Produce, resolves offset 42,
drains the buffered gauge, and joins the worker. The detailed record is
[`v1-buffered-close-flush-2026-09-03.md`](../../evidence/v1-buffered-close-flush-2026-09-03.md).

This closes the accepted-record close/flush slice only; expired or in-flight
close ambiguity, cancellation during transmission, published mixed-outcome
reconciliation, and live evidence remain open.

### Buffered in-flight deadline after Produce transmission (2026-09-03)

At source commit `ed71f6d3d1ac50aa0f27e3a89d3a626238a452bb`,
`buffered_delivery_deadline_expires_after_produce_without_response` holds the
Produce response after the request is observed. The configured 100 ms total
budget expires in the Produce phase with `possibly_transmitted=true`; both
`flush()` and the delivery handle report the typed deadline error, the
buffered gauge drains, and the worker closes cleanly. The detailed record is
[`v1-buffered-inflight-deadline-2026-09-03.md`](../../evidence/v1-buffered-inflight-deadline-2026-09-03.md).

This closes the deterministic buffered post-write deadline slice only. Close
while a request is still blocked, delayed metadata/capability, cancellation
during transmission, published mixed-outcome reconciliation, and live
evidence remain open.

The complete deterministic `fault_injection` target was then re-run from the
company Windows/Ubuntu-T9 WSL2 x86_64 environment at source
`ed71f6d3d1ac50aa0f27e3a89d3a626238a452bb`; all 26 tests passed, including the
buffered queue, close-flush, and post-write deadline cases. This short
reproduction is recorded in
[`v1-company-fault-matrix-short-smoke-2026-09-03.md`](../../evidence/v1-company-fault-matrix-short-smoke-2026-09-03.md)
and does not change the published, multi-broker, or long-campaign gates.

The company WSL2 short smoke was refreshed at source
`c1dc20943dd9ae7e7f9971a665c4ca15dfd3b8cc`; all 29 deterministic
`fault_injection` tests passed, including delivery-receiver cancellation and
the close edge cases. The latest record is
[`v1-company-fault-matrix-29test-smoke-2026-09-03.md`](../../evidence/v1-company-fault-matrix-29test-smoke-2026-09-03.md).
This remains current-source scripted evidence only.

### Close edge cases (2026-09-03)

At source commit `b88564748c56e1bcc8ae4c5944235b4cb1bb95e4`,
`buffered_close_reports_in_flight_deadline_and_joins_worker` verifies that
owner `close()` reports an in-flight Produce deadline with
`possibly_transmitted=true`, completes the delivery, drains the gauge, and
joins the worker. `buffered_close_flushes_handle_owned_record_before_worker_shutdown`
verifies that an accepted record sent through `BufferedProducerHandle` is
flushed by the owner close and resolves successfully. The detailed record is
[`v1-buffered-close-edge-cases-2026-09-03.md`](../../evidence/v1-buffered-close-edge-cases-2026-09-03.md).

This closes deterministic close handling for these two paths only;
cancellation during socket I/O, delayed metadata/capability, published
mixed-outcome reconciliation, and live evidence remain open.

### Buffered delivery receiver cancellation (2026-09-03)

At source commit `c1dc20943dd9ae7e7f9971a665c4ca15dfd3b8cc`,
`buffered_delivery_sender_cancellation_releases_record_after_flush` drops a
delivery receiver after enqueue. The accepted record is still transmitted and
acknowledged, `flush()` completes, the buffered gauge reaches zero, and owner
close joins the worker. The detailed record is
[`v1-buffered-delivery-cancellation-2026-09-03.md`](../../evidence/v1-buffered-delivery-cancellation-2026-09-03.md).

This closes receiver-cancellation cleanup only; cancellation while socket I/O
is blocked, partial client request writes, delayed metadata/capability,
published mixed-outcome reconciliation, and live evidence remain open.

### Canceled low-level request cannot be reused (2026-09-03)

At source commit `2c28eec77911420e8dbb2d5d94bb96400f0148b9`,
`does_not_reuse_connection_after_canceled_request` cancels an `api_versions()`
future after the request is observed by a scripted broker. The client marks the
connection unusable and rejects the next request with `NotConnected`, so a
possibly partial response cannot be consumed by a different operation. The
detailed record is
[`v1-client-cancellation-poisoning-2026-09-03.md`](../../evidence/v1-client-cancellation-poisoning-2026-09-03.md).

This closes low-level connection reuse after caller cancellation only. It does
not close producer delivery receiver cancellation, partial client request
writes, delayed metadata/capability, published mixed-outcome reconciliation,
or live qualification gates.

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
