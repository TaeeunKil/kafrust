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
and throttle coverage, terminating-push count, and seeded secret scan. Until
those gates pass, telemetry remains an explicitly bounded qualification slice,
not a general OpenTelemetry backend or a completed stable-support claim.

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
