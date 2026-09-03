# V1-17 Telemetry And Metrics Contract

- Status: In progress
- Target evidence: Published artifact
- Dependencies: V1-02, V1-15
- Conditional evidence: retained telemetry joins the published-artifact gate;
  excluded telemetry remains mechanically isolated while stable metrics still
  require published-artifact evidence.

## User-Visible Objective

Provide a stable, bounded, low-cardinality metrics contract and, if retained for
v1, a KIP-714 telemetry lifecycle that survives subscription changes,
throttling, payload limits, secured multi-broker movement, and shutdown without
leaking secrets.

## Non-Goals

- No general OpenTelemetry SDK, collector, exporter, or observability backend.
- No per-topic/partition/member labels that create unbounded cardinality.
- No Kafka 4.0 early-access telemetry shape removed from stable releases.
- No guarantee for metrics not listed in the v1 contract.

## Scope

- `crates/kafrust/src/{metrics,telemetry,client,config,error}.rs`
- GetTelemetrySubscriptions API 71 and PushTelemetry API 72 v0
- optional `otlp` provider, provider trait, subscription ID, requested metrics,
  interval/jitter, delta/cumulative temporality, compression, payload ceiling,
  throttle/cooldown, unknown subscription, terminating push, and connection
  ownership
- request/retry/failure/ambiguity/backpressure/queue/connection/task metrics
- telemetry broker plugin, payload-limit workflow, live/published workflows,
  `docs/telemetry.md`, performance/compatibility docs

## Work Packages

1. Inventory metric names, type, unit, meaning, reset/aggregation behavior, and
   maximum cardinality.
2. Freeze snapshot and percentile semantics; test counter saturation/overflow
   behavior and concurrent observation.
3. If telemetry is stable, complete provider scheduling, subscription refresh,
   broker throttle, compression, payload ceiling, termination, cancellation,
   and reconnection tests. Assert one initial null ClientInstanceId handshake,
   then preserve the broker-returned ID across serving-broker replacement;
   same-connection reuse is preferred but not required by the identity gate.
4. Map every retry, ambiguous outcome, queue saturation, and shutdown event to
   observable metrics without payload/credential labels.
5. Qualify secured multi-broker and long collection from a published artifact;
   otherwise isolate telemetry as experimental and state the exclusion.

## Current Execution Record (2026-08-22)

V1-17 is now `In progress`. `ClientMetricsSnapshot` and the telemetry provider
already expose a bounded, low-cardinality set of request, retry, error, queue,
in-flight, buffered-record, and latency observations. Deterministic tests cover
filtered cumulative/delta OTLP serialization, metric selection, payload
configuration validation, codec selection, subscription negotiation, and the
single-connection push path. The typed `TelemetryPayloadTooLarge` boundary is
checked before transmission and again after compression.

The live workflows for telemetry negotiation and payload limits are available,
but the exact coordinated candidate still needs the 60-minute floor/current
published profiles, broker replacement with stable ClientInstanceId, mutation
and throttle coverage, the published terminating-push count, and seeded secret
scan. Until
those gates pass, telemetry remains an explicitly bounded qualification slice,
not a general OpenTelemetry backend or a completed stable-support claim.

### Frozen metric contract (2026-09-03)

The public `ClientMetricsSnapshot` field set is now frozen in
[`v1-17-metrics-contract.json`](../../evidence/v1-17-metrics-contract.json).
[`check_v1_metrics_contract.py`](../../../scripts/check_v1_metrics_contract.py)
parses the Rust snapshot directly and requires all 19 fields to retain their
declared type, unit, aggregation/lifecycle semantics, and maximum cardinality
of one. Five focused checker tests and the CI check pass. This closes the
deterministic metric inventory criterion only; published collection, broker
replacement, throttling, and the published terminating-push gate remain
required.

### Saturating metric arithmetic (2026-09-04)

Source commit `c526c412460af17cacd3816f6c04709b34ca31f9` changes all metric
counter and current-gauge updates to atomic compare-exchange loops with
saturating arithmetic. This prevents cumulative `u64` counters from wrapping
to zero and prevents current gauges from underflowing during cleanup. The
`metric_atomic_updates_saturate_at_u64_boundaries` regression covers both
overflow and underflow boundaries. This closes the deterministic arithmetic
slice only; published collection, broker replacement, throttling, secure
transport, and long-duration gates remain required.

### Concurrent metric updates (2026-09-04)

Source commit `91c5592c6599eeb16df661616efa3fe0d5c7e0b4` adds a deterministic
four-thread shared-`ClientMetrics` regression. A barrier starts four workers,
each worker performs 100 updates, and the test verifies exact cumulative
request/byte/error totals, latency-bucket conservation, and a zero final
in-flight gauge. The focused test passes on the company Windows x64 checkout
and Ubuntu-T9 WSL2 with Rust 1.81.0; the required workspace validation passes
on Windows. See
[`v1-metrics-concurrency-2026-09-04.md`](../../evidence/v1-metrics-concurrency-2026-09-04.md).
This closes the in-process atomic-update consistency slice only; published
collection, broker replacement, throttling, secure transport, and
long-duration qualification remain open.

### Push cancellation after transmission (2026-09-04)

The KIP-714 `push_once` path now has a deterministic cancellation regression:
after the scripted broker observes PushTelemetry, dropping the caller future
leaves the persistent connection unusable and a subsequent push returns
`NotConnected` instead of reusing an uncertain response. The focused test
passed on Windows and company Ubuntu-T9 WSL2 (Rust 1.81.0); WSL2 also passed
the 29-test fault-injection target. The evidence record is
[`v1-telemetry-push-cancellation-2026-09-04.md`](../../evidence/v1-telemetry-push-cancellation-2026-09-04.md).
This closes cancellation/reuse safety only; broker replacement, throttling,
published terminating-push count, secure published collection, and
long-duration gates remain open.

### Deterministic terminating push (2026-09-04)

`TelemetryClient::terminate` now has a focused regression that decodes the
single PushTelemetry v0 shutdown request and verifies its terminating bit,
subscription ID, compression, payload, tagged fields, successful response,
and joined broker task. The test passed on Windows and company Ubuntu-T9 WSL2
(Rust 1.81.0); WSL2 also passed the 29-test fault-injection target. This closes
the local request-encoding/shutdown boundary only. Published 60-minute
collection, broker replacement, mutation/throttle behavior, secure transport,
and final task/resource/secret checks remain open. See
[`v1-telemetry-terminating-push-2026-09-04.md`](../../evidence/v1-telemetry-terminating-push-2026-09-04.md).

## Failure And Lifecycle Contract

- Metrics updates never block the data path on network I/O.
- Cardinality is bounded by a documented constant or closed label set.
- Oversized telemetry fails before transmission with a typed limit error; it is
  never silently truncated.
- Subscription mutation/unknown ID refreshes on the required connection and
  respects broker throttling.
- A broker replacement cannot silently mint a new client instance identity;
  null is used only for the initial handshake of that client lifecycle.
- Provider failure is observable and bounded; it does not fail unrelated Kafka
  requests unless the API explicitly says so.
- Shutdown sends at most one terminating push, joins its task, and records the
  result without secrets.

## Verification

- Deterministic metric name/unit/cardinality snapshot and concurrency tests.
- Telemetry negotiation, subscription change, throttle, unknown ID, each codec,
  exact/oversized payload, provider failure, response loss, cancellation, and
  terminating-push tests.
- Published lowest accepted telemetry-capable KRaft profile (never a 3.3/3.6
  global floor that lacks KIP-714) and pinned-current three-broker
  SASL_SSL/SCRAM-SHA-256 profile collect for at least 60 minutes, replace the
  serving broker, preserve ClientInstanceId, mutate a subscription, and end
  with one terminating push.
- Zero secret-marker matches and zero final telemetry tasks/connections.

## Exit Criteria

1. Every stable metric has a frozen name/type/unit/lifecycle/cardinality entry.
2. Required failure and saturation events are observable without secret or
   high-cardinality labels.
3. If telemetry is stable, both 60-minute published profiles pass with exactly
   one initial null-ID handshake and a stable returned ID across broker
   replacement; otherwise the surface is mechanically experimental and
   excluded from V1-20 core gates.
4. Payload limits and terminating shutdown are deterministic.
5. Telemetry/metrics docs, API audit, SLO inputs, and ledger rows agree.

## Migration And Rollback

Map rust-rdkafka statistics/callback fields to stable metrics or documented
gaps. Metric rename/removal requires a deprecation/mapping period. Rolling back
telemetry stops and joins the provider task; it must not change producer/
consumer correctness.

## Conventional Commit Plan

1. `test(metrics): snapshot v1 metric contracts`
2. `fix(telemetry): bound subscription and shutdown lifecycle`
3. `ci(telemetry): qualify secured long collection`
4. `docs(observability): define metrics and telemetry support`

## Evidence Record On Completion

Record metric snapshot hash/cardinality, telemetry API versions, duration,
subscription/throttle/broker events, payload/codec, provider failures,
terminating pushes, secret scan/final gauges, artifact, and backend non-claim.
