# Client Telemetry

kafrust exposes Kafka KIP-714 client telemetry at two levels. The low-level
`Client` methods cover the flexible v0 wire schemas for
`GetTelemetrySubscriptions` (API key 71) and `PushTelemetry` (API key 72), and
`TelemetryClient` adds subscription state, bounded payloads, refresh/retry,
jittered scheduling, and a terminating push.

## Current Status

Implemented:

- request encoding for the zero UUID handshake and subsequent client instance
  ID requests;
- subscription response decoding, including accepted compression types,
  push interval, telemetry byte limit, delta temporality, and requested metric
  prefixes;
- payload request encoding for terminating state, compression type, and raw
  OpenTelemetry MetricsData bytes;
- response decoding and an injected-broker request/response test through the
  public low-level `Client` methods.
- `TelemetryMetricsProvider` for application-owned OTLP serialization;
- capability negotiation for both telemetry APIs before subscription fetch;
- broker subscription state with client-instance ID retention;
- local and broker-advertised payload ceilings;
- one same-connection refresh/retry for `UNKNOWN_SUBSCRIPTION_ID` and
  `UNSUPPORTED_COMPRESSION_TYPE`;
- broker-negotiated gzip, Snappy, LZ4, and Zstd payload compression using the
  protocol crate's pure Rust codecs;
- KIP-714 jittered push scheduling and a shutdown-triggered terminating push.

The runtime accepts raw OTLP MetricsData v1 protobuf bytes from the provider. It
compresses those bytes with the strongest codec accepted by the broker, using
Zstd, LZ4, Snappy, Gzip, then uncompressed as the preference order. It does not
yet ship a built-in OTLP serializer, so applications still provide the
MetricsData v1 protobuf bytes.
The scheduler is deliberately bounded to one in-flight push on one persistent
connection. It does not queue unbounded payloads or silently split a payload
that exceeds the broker limit.

## Runtime Usage

```rust
use kafrust::{ClientConfig, TelemetryClient, TelemetryConfig};
use tokio::sync::watch;

let provider = |requested: &[String], delta: bool| {
    build_otlp_metrics(requested, delta)
};
let client = TelemetryClient::connect(
    ClientConfig::new(["localhost:9092"]),
    provider,
    TelemetryConfig::new().max_payload_bytes(1024 * 1024),
).await?;

let (shutdown_tx, shutdown_rx) = watch::channel(false);
let task = tokio::spawn(client.run_until_shutdown(shutdown_rx));
// ... application work ...
shutdown_tx.send(true)?;
task.await??;
```

`run_until_shutdown` performs an immediate push, uses the broker's interval
with 0.5x..1.5x jitter by default, refreshes an outdated subscription once,
and sends one terminating push before returning. Dropping the shutdown sender
also ends the loop and sends the terminating push.

## Low-Level Usage

```rust
let mut client = config.connect().await?;
let subscription = client.get_telemetry_subscriptions_v0([0; 16]).await?;

if !subscription.requested_metrics.is_empty() {
    client
        .push_telemetry_v0(
            subscription.client_instance_id,
            subscription.subscription_id,
            false,
            0,
            metrics_data,
        )
        .await?;
}
```

The all-zero UUID requests a broker-assigned client instance ID. The returned
ID must be reused for later subscription and push requests. `metrics_data` is
expected to be an OpenTelemetry MetricsData v1 protobuf payload. The low-level
method accepts a compression identifier, while the high-level runtime selects
and applies a broker-accepted codec automatically.

## Release Gate

Before KIP-714 can count as a production replacement feature, kafrust must add
a built-in OTLP metrics provider or a documented integration package,
metrics-to-OTLP mapping from kafrust's own counters, and a Kafka KRaft live test
with an enabled client telemetry plugin. The live gate must cover subscription
changes, throttling, `UNKNOWN_SUBSCRIPTION_ID`, payload-limit handling,
compression, and terminating pushes.
