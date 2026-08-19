//! High-level KIP-714 client telemetry scheduling.

use crate::client::Client;
use crate::config::ClientConfig;
use crate::error::{Error, Result};
#[cfg(feature = "otlp")]
use crate::metrics::{ClientMetrics, ClientMetricsSnapshot};
use kafrust_protocol::api::telemetry::{
    GET_TELEMETRY_SUBSCRIPTIONS_API_KEY, PUSH_TELEMETRY_API_KEY,
};
use kafrust_protocol::record_batch::{compress_bytes, RecordBatchCompression};
#[cfg(feature = "otlp")]
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum,
};
#[cfg(feature = "otlp")]
use prost::Message;
use rand::Rng;
use std::time::{Duration, Instant};
#[cfg(feature = "otlp")]
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
const TELEMETRY_TOO_LARGE: i16 = 118;
const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
const THROTTLING_QUOTA_EXCEEDED: i16 = 89;

/// Supplies an OpenTelemetry MetricsData payload for a broker subscription.
///
/// The provider owns serialization. `requested_metrics` contains the metric
/// name prefixes currently requested by Kafka and `delta_temporality` mirrors
/// the broker subscription. The returned bytes must be an OTLP MetricsData v1
/// protobuf payload. Compression is selected by the telemetry runtime, so a
/// provider returns uncompressed bytes.
pub trait TelemetryMetricsProvider: Send {
    /// Builds one payload for the current broker subscription.
    fn collect(&mut self, requested_metrics: &[String], delta_temporality: bool)
        -> Result<Vec<u8>>;
}

impl<F> TelemetryMetricsProvider for F
where
    F: FnMut(&[String], bool) -> Result<Vec<u8>> + Send,
{
    fn collect(
        &mut self,
        requested_metrics: &[String],
        delta_temporality: bool,
    ) -> Result<Vec<u8>> {
        self(requested_metrics, delta_temporality)
    }
}

/// Serializes [`ClientMetrics`] as an OTLP MetricsData payload.
///
/// This provider is available with the `otlp` feature. Metric names are formed
/// by concatenating `metric_prefix` and the documented suffixes, and broker
/// metric-prefix subscriptions are applied before serialization. Counters are
/// exported as monotonic OTLP sums; gauges represent the current client state.
#[cfg(feature = "otlp")]
#[derive(Clone, Debug)]
pub struct ClientMetricsTelemetryProvider {
    metrics: ClientMetrics,
    metric_prefix: String,
    start_time_unix_nano: u64,
    previous_snapshot: Option<ClientMetricsSnapshot>,
    previous_collect_time_unix_nano: Option<u64>,
}

#[cfg(feature = "otlp")]
impl ClientMetricsTelemetryProvider {
    /// Creates a provider backed by shared client metrics.
    pub fn new(metrics: ClientMetrics) -> Self {
        Self {
            metrics,
            metric_prefix: "kafrust.client.".to_owned(),
            start_time_unix_nano: unix_time_nanos(),
            previous_snapshot: None,
            previous_collect_time_unix_nano: None,
        }
    }

    /// Sets the prefix prepended to every exported metric name.
    pub fn metric_prefix(mut self, metric_prefix: impl Into<String>) -> Self {
        self.metric_prefix = metric_prefix.into();
        self
    }

    /// Returns the shared metrics handle used by this provider.
    pub fn metrics(&self) -> &ClientMetrics {
        &self.metrics
    }
}

#[cfg(feature = "otlp")]
impl TelemetryMetricsProvider for ClientMetricsTelemetryProvider {
    fn collect(
        &mut self,
        requested_metrics: &[String],
        delta_temporality: bool,
    ) -> Result<Vec<u8>> {
        let current = self.metrics.snapshot();
        let now = unix_time_nanos();
        let previous = self.previous_snapshot.replace(current);
        let previous_time = self
            .previous_collect_time_unix_nano
            .replace(now)
            .unwrap_or(now);
        let temporality = if delta_temporality {
            AggregationTemporality::Delta as i32
        } else {
            AggregationTemporality::Cumulative as i32
        };
        let mut metrics = Vec::new();

        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "requests_started",
            "Kafka request roundtrips started",
            current.requests_started,
            previous.map(|snapshot| snapshot.requests_started),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "requests_succeeded",
            "Kafka request roundtrips completed successfully",
            current.requests_succeeded,
            previous.map(|snapshot| snapshot.requests_succeeded),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "requests_failed",
            "Kafka request roundtrips that returned an error",
            current.requests_failed,
            previous.map(|snapshot| snapshot.requests_failed),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "requests_timed_out",
            "Kafka request roundtrips that timed out",
            current.requests_timed_out,
            previous.map(|snapshot| snapshot.requests_timed_out),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "requests_cancelled",
            "Kafka request futures cancelled before completion",
            current.requests_cancelled,
            previous.map(|snapshot| snapshot.requests_cancelled),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "broker_errors",
            "Non-zero Kafka broker error codes observed",
            current.broker_errors,
            previous.map(|snapshot| snapshot.broker_errors),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "retries",
            "Additional high-level Kafka operation attempts",
            current.retries,
            previous.map(|snapshot| snapshot.retries),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "produced_records",
            "Records acknowledged by Kafka Produce operations",
            current.produced_records,
            previous.map(|snapshot| snapshot.produced_records),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "produce_batches",
            "Successful topic-partition Produce chunks",
            current.produce_batches,
            previous.map(|snapshot| snapshot.produce_batches),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "consumed_records",
            "Records returned by consumer poll and fetch operations",
            current.consumed_records,
            previous.map(|snapshot| snapshot.consumed_records),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "request_bytes",
            "Kafka request payload bytes",
            current.request_bytes,
            previous.map(|snapshot| snapshot.request_bytes),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );
        append_sum(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "response_bytes",
            "Kafka response payload bytes",
            current.response_bytes,
            previous.map(|snapshot| snapshot.response_bytes),
            delta_temporality,
            self.start_time_unix_nano,
            previous_time,
            now,
            temporality,
        );

        append_gauge(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "buffered_records",
            "Buffered producer records currently outstanding",
            current.buffered_records,
            now,
        );
        append_gauge(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "max_buffered_records",
            "Highest observed buffered producer record count",
            current.max_buffered_records,
            now,
        );
        append_gauge(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "in_flight_requests",
            "Kafka request roundtrips currently awaiting completion",
            current.in_flight_requests,
            now,
        );
        append_gauge(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "total_latency_ns",
            "Sum of completed and cancelled request latency",
            duration_nanos(current.total_latency),
            now,
        );
        append_gauge(
            &mut metrics,
            &self.metric_prefix,
            requested_metrics,
            "max_latency_ns",
            "Highest observed completed or cancelled request latency",
            duration_nanos(current.max_latency),
            now,
        );

        let payload = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        Ok(payload.encode_to_vec())
    }
}

#[cfg(feature = "otlp")]
#[allow(clippy::too_many_arguments)]
fn append_sum(
    output: &mut Vec<Metric>,
    metric_prefix: &str,
    requested_metrics: &[String],
    suffix: &str,
    description: &str,
    current: u64,
    previous: Option<u64>,
    delta_temporality: bool,
    cumulative_start: u64,
    delta_start: u64,
    now: u64,
    temporality: i32,
) {
    let name = format!("{metric_prefix}{suffix}");
    if !metric_requested(&name, requested_metrics) {
        return;
    }
    let value = if delta_temporality {
        previous.map_or(current, |previous| current.saturating_sub(previous))
    } else {
        current
    };
    output.push(Metric {
        name,
        description: description.to_owned(),
        unit: "1".to_owned(),
        metadata: Vec::new(),
        data: Some(metric::Data::Sum(Sum {
            data_points: vec![number_data_point(
                value,
                if delta_temporality {
                    delta_start
                } else {
                    cumulative_start
                },
                now,
            )],
            aggregation_temporality: temporality,
            is_monotonic: true,
        })),
    });
}

#[cfg(feature = "otlp")]
fn append_gauge(
    output: &mut Vec<Metric>,
    metric_prefix: &str,
    requested_metrics: &[String],
    suffix: &str,
    description: &str,
    value: u64,
    now: u64,
) {
    let name = format!("{metric_prefix}{suffix}");
    if !metric_requested(&name, requested_metrics) {
        return;
    }
    output.push(Metric {
        name,
        description: description.to_owned(),
        unit: "1".to_owned(),
        metadata: Vec::new(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![number_data_point(value, 0, now)],
        })),
    });
}

#[cfg(feature = "otlp")]
fn metric_requested(name: &str, requested_metrics: &[String]) -> bool {
    requested_metrics.is_empty()
        || requested_metrics
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

#[cfg(feature = "otlp")]
fn number_data_point(
    value: u64,
    start_time_unix_nano: u64,
    time_unix_nano: u64,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes: Vec::new(),
        start_time_unix_nano,
        time_unix_nano,
        exemplars: Vec::new(),
        flags: 0,
        value: Some(number_data_point::Value::AsInt(u64_to_i64(value))),
    }
}

#[cfg(feature = "otlp")]
fn u64_to_i64(value: u64) -> i64 {
    match i64::try_from(value) {
        Ok(value) => value,
        Err(_) => i64::MAX,
    }
}

#[cfg(feature = "otlp")]
fn duration_nanos(duration: Duration) -> u64 {
    match u64::try_from(duration.as_nanos()) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[cfg(feature = "otlp")]
fn unix_time_nanos() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_nanos(duration),
        Err(_) => 0,
    }
}

/// Limits and lifecycle options for [`TelemetryClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryConfig {
    client_instance_id: [u8; 16],
    max_payload_bytes: usize,
    jitter: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            client_instance_id: [0; 16],
            max_payload_bytes: 1024 * 1024,
            jitter: true,
        }
    }
}

impl TelemetryConfig {
    /// Creates telemetry options with a zero client instance ID.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the stable client instance ID. The zero value asks Kafka to assign
    /// one and the assigned value is retained for the lifetime of the client.
    pub fn client_instance_id(mut self, client_instance_id: [u8; 16]) -> Self {
        self.client_instance_id = client_instance_id;
        self
    }

    /// Sets the local payload ceiling before the broker's limit is applied.
    pub fn max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }

    /// Enables or disables the KIP-714 0.5x..1.5x push-interval jitter.
    pub fn jitter(mut self, enabled: bool) -> Self {
        self.jitter = enabled;
        self
    }

    fn validate(self) -> Result<()> {
        if self.max_payload_bytes == 0 {
            return Err(Error::InvalidConfiguration {
                field: "telemetry_max_payload_bytes",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

/// The broker subscription currently used by a telemetry client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySubscription {
    /// Broker-assigned client instance ID.
    pub client_instance_id: [u8; 16],
    /// Broker-assigned subscription ID.
    pub subscription_id: i32,
    /// Compression identifiers accepted by the broker.
    pub accepted_compression_types: Vec<i8>,
    /// Broker-requested push interval.
    pub push_interval: Duration,
    /// Broker-side maximum payload size, when positive.
    pub telemetry_max_bytes: usize,
    /// Whether the broker requested delta temporality.
    pub delta_temporality: bool,
    /// Metric name prefixes requested by the broker.
    pub requested_metrics: Vec<String>,
}

impl TelemetrySubscription {
    fn from_response(
        response: kafrust_protocol::api::telemetry::GetTelemetrySubscriptionsResponseV0,
    ) -> Result<Self> {
        let push_interval_ms =
            u64::try_from(response.push_interval_ms).map_err(|_| Error::InvalidConfiguration {
                field: "telemetry_push_interval_ms",
                reason: "broker returned a negative push interval",
            })?;
        let telemetry_max_bytes = usize::try_from(response.telemetry_max_bytes).map_err(|_| {
            Error::InvalidConfiguration {
                field: "telemetry_max_bytes",
                reason: "broker returned a negative payload limit",
            }
        })?;
        Ok(Self {
            client_instance_id: response.client_instance_id,
            subscription_id: response.subscription_id,
            accepted_compression_types: response.accepted_compression_types,
            push_interval: Duration::from_millis(push_interval_ms),
            telemetry_max_bytes,
            delta_temporality: response.delta_temporality,
            requested_metrics: response.requested_metrics,
        })
    }
}

/// Summary of one successful telemetry push.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryPushSummary {
    /// Subscription ID used by the push.
    pub subscription_id: i32,
    /// Uncompressed payload bytes supplied by the provider.
    pub payload_bytes: usize,
}

/// A KIP-714 client telemetry runtime over one persistent broker connection.
///
/// The same connection is reused for subscription and push requests so Kafka's
/// telemetry throttling state remains attached to one broker connection. The
/// runtime refreshes an invalid subscription, honors broker throttle windows,
/// bounds payload allocation, and sends one terminating push when the shutdown
/// channel is set.
pub struct TelemetryClient<P> {
    client: Client,
    provider: P,
    config: TelemetryConfig,
    client_instance_id: [u8; 16],
    subscription: Option<TelemetrySubscription>,
    subscription_refresh_pending: bool,
    throttle_until: Option<Instant>,
}

impl<P> TelemetryClient<P>
where
    P: TelemetryMetricsProvider,
{
    /// Connects to Kafka and creates a telemetry runtime.
    pub async fn connect(
        client_config: ClientConfig,
        provider: P,
        config: TelemetryConfig,
    ) -> Result<Self> {
        config.validate()?;
        let client = client_config.connect().await?;
        Ok(Self::from_client(client, provider, config))
    }

    /// Wraps an already connected low-level client.
    pub fn from_client(client: Client, provider: P, config: TelemetryConfig) -> Self {
        Self {
            client,
            provider,
            client_instance_id: config.client_instance_id,
            config,
            subscription: None,
            subscription_refresh_pending: false,
            throttle_until: None,
        }
    }

    /// Returns the last broker subscription, if one has been acquired.
    pub fn subscription(&self) -> Option<&TelemetrySubscription> {
        self.subscription.as_ref()
    }

    /// Fetches a new broker subscription and replaces local subscription state.
    pub async fn refresh_subscription(&mut self) -> Result<&TelemetrySubscription> {
        self.wait_for_throttle().await;
        let api_versions = self
            .client
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        if api_versions.error_code != 0 {
            return Err(self
                .client
                .broker_error(api_versions.error_code, "telemetry ApiVersions".to_owned()));
        }
        for (api_key, operation) in [
            (
                GET_TELEMETRY_SUBSCRIPTIONS_API_KEY,
                "GetTelemetrySubscriptions v0",
            ),
            (PUSH_TELEMETRY_API_KEY, "PushTelemetry v0"),
        ] {
            if api_versions.highest_supported_version(api_key, 0).is_none() {
                return Err(Error::Unsupported(operation));
            }
        }

        for attempt in 0..=2 {
            self.wait_for_throttle().await;
            let response = self
                .client
                .get_telemetry_subscriptions_v0(self.client_instance_id)
                .await?;
            self.record_throttle(response.throttle_time_ms);
            if response.error_code != 0 {
                if response.error_code == THROTTLING_QUOTA_EXCEEDED && attempt < 2 {
                    self.wait_for_throttle().await;
                    continue;
                }
                return Err(self
                    .client
                    .broker_error(response.error_code, "GetTelemetrySubscriptions".to_owned()));
            }
            let subscription = TelemetrySubscription::from_response(response)?;
            self.client_instance_id = subscription.client_instance_id;
            self.subscription = Some(subscription);
            self.subscription_refresh_pending = false;
            return self
                .subscription
                .as_ref()
                .ok_or(Error::Unsupported("telemetry subscription was not stored"));
        }
        Err(Error::Unsupported(
            "telemetry subscription throttle retry exhausted",
        ))
    }

    /// Collects and pushes one non-terminating telemetry payload.
    ///
    /// An empty broker metric subscription returns `Ok(None)` and does not send
    /// a payload. An outdated subscription or compression selection causes one
    /// refresh and retry on the same connection.
    pub async fn push_once(&mut self) -> Result<Option<TelemetryPushSummary>> {
        if self.subscription.is_none() || self.subscription_refresh_pending {
            self.refresh_subscription().await?;
        }
        if self
            .subscription
            .as_ref()
            .is_some_and(|subscription| subscription.requested_metrics.is_empty())
        {
            // Kafka keeps a no-match subscription in SUBSCRIPTION_NEEDED and
            // checks it again after the broker interval. Preserve the current
            // interval while requesting the same lifecycle on the next poll.
            self.subscription_refresh_pending = true;
            return Ok(None);
        }
        self.push_with_refresh(false).await.map(Some)
    }

    /// Sends one terminating payload, if a subscription has been acquired.
    pub async fn terminate(&mut self) -> Result<Option<TelemetryPushSummary>> {
        if self.subscription.is_none() {
            return Ok(None);
        }
        if self
            .subscription
            .as_ref()
            .is_some_and(|subscription| subscription.requested_metrics.is_empty())
        {
            return Ok(None);
        }
        self.push_with_refresh(true).await.map(Some)
    }

    /// Runs the telemetry loop until `shutdown` becomes true or is dropped.
    ///
    /// The first push is immediate. Subsequent pushes use the broker interval
    /// with optional jitter in the range recommended by KIP-714. Shutdown sends
    /// a terminating push before returning.
    pub async fn run_until_shutdown(mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                self.terminate().await?;
                return Ok(());
            }
            self.push_once().await?;
            let interval = self
                .subscription
                .as_ref()
                .map_or(Duration::from_secs(1), |subscription| {
                    subscription.push_interval
                });
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        self.terminate().await?;
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(jittered_interval(interval, self.config.jitter)) => {}
            }
        }
    }

    async fn push_with_refresh(&mut self, terminating: bool) -> Result<TelemetryPushSummary> {
        for attempt in 0..=2 {
            match self.push_current(terminating).await {
                Err(Error::Broker { code, .. })
                    if attempt < 2
                        && (code == UNKNOWN_SUBSCRIPTION_ID
                            || code == UNSUPPORTED_COMPRESSION_TYPE) =>
                {
                    self.refresh_subscription().await?;
                }
                Err(Error::Broker {
                    code: THROTTLING_QUOTA_EXCEEDED,
                    ..
                }) if attempt < 2 => {
                    self.wait_for_throttle().await;
                }
                result => return result,
            }
        }
        Err(Error::Unsupported("telemetry retry exhausted"))
    }

    async fn push_current(&mut self, terminating: bool) -> Result<TelemetryPushSummary> {
        self.wait_for_throttle().await;
        let subscription = self
            .subscription
            .as_ref()
            .ok_or(Error::Unsupported(
                "telemetry subscription is not initialized",
            ))?
            .clone();
        let compression = select_compression(&subscription.accepted_compression_types)?;
        let metrics = self.provider.collect(
            &subscription.requested_metrics,
            subscription.delta_temporality,
        )?;
        let max_payload_bytes = self
            .config
            .max_payload_bytes
            .min(subscription.telemetry_max_bytes.max(1));
        if metrics.len() > max_payload_bytes {
            return Err(Error::TelemetryPayloadTooLarge {
                size: metrics.len(),
                max: max_payload_bytes,
            });
        }
        let payload_bytes = metrics.len();
        let metrics = compress_bytes(compression, &metrics)?;
        if metrics.len() > max_payload_bytes {
            return Err(Error::TelemetryPayloadTooLarge {
                size: metrics.len(),
                max: max_payload_bytes,
            });
        }
        let response = self
            .client
            .push_telemetry_v0(
                subscription.client_instance_id,
                subscription.subscription_id,
                terminating,
                compression.attributes() as i8,
                metrics,
            )
            .await?;
        self.record_throttle(response.throttle_time_ms);
        if response.error_code != 0 {
            if response.error_code == TELEMETRY_TOO_LARGE {
                return Err(Error::TelemetryPayloadTooLarge {
                    size: payload_bytes,
                    max: max_payload_bytes,
                });
            }
            return Err(self
                .client
                .broker_error(response.error_code, "PushTelemetry".to_owned()));
        }
        Ok(TelemetryPushSummary {
            subscription_id: subscription.subscription_id,
            payload_bytes,
        })
    }

    fn record_throttle(&mut self, throttle_time_ms: i32) {
        let Ok(throttle_time_ms) = u64::try_from(throttle_time_ms) else {
            return;
        };
        let until = Instant::now() + Duration::from_millis(throttle_time_ms);
        self.throttle_until = Some(
            self.throttle_until
                .map_or(until, |current| current.max(until)),
        );
    }

    async fn wait_for_throttle(&mut self) {
        let Some(until) = self.throttle_until.take() else {
            return;
        };
        let remaining = until.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining).await;
        }
    }
}

fn select_compression(accepted: &[i8]) -> Result<RecordBatchCompression> {
    [
        RecordBatchCompression::Zstd,
        RecordBatchCompression::Lz4,
        RecordBatchCompression::Snappy,
        RecordBatchCompression::Gzip,
        RecordBatchCompression::None,
    ]
    .into_iter()
    .find(|compression| accepted.contains(&(compression.attributes() as i8)))
    .ok_or(Error::Unsupported(
        "telemetry subscription has no supported compression type",
    ))
}

fn jittered_interval(interval: Duration, enabled: bool) -> Duration {
    if !enabled {
        return interval;
    }
    let interval_ms = interval.as_millis();
    if interval_ms == 0 {
        return Duration::from_millis(1);
    }
    let factor_percent = rand::thread_rng().gen_range(50_u128..=150_u128);
    let jittered_ms = interval_ms
        .saturating_mul(factor_percent)
        .checked_div(100)
        .unwrap_or(u128::from(u64::MAX));
    Duration::from_millis(u64::try_from(jittered_ms).unwrap_or(u64::MAX).max(1))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::TelemetryClient;
    use super::{jittered_interval, select_compression, TelemetryConfig, TelemetryMetricsProvider};
    use crate::client::Client;
    use crate::error::Error;
    use kafrust_protocol::codec::{Decoder, Encoder};
    use kafrust_protocol::record_batch::RecordBatchCompression;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn validates_payload_limit() {
        let error = TelemetryConfig::new()
            .max_payload_bytes(0)
            .validate()
            .unwrap_err();
        assert!(matches!(
            error,
            Error::InvalidConfiguration {
                field: "telemetry_max_payload_bytes",
                ..
            }
        ));
    }

    #[test]
    fn selects_the_strongest_supported_telemetry_codec() {
        assert_eq!(
            select_compression(&[0, 2]).unwrap(),
            RecordBatchCompression::Snappy
        );
        assert_eq!(
            select_compression(&[1, 3]).unwrap(),
            RecordBatchCompression::Lz4
        );
        assert!(select_compression(&[5]).is_err());
    }

    #[test]
    fn jitter_stays_within_kip_714_bounds() {
        let interval = Duration::from_millis(10_000);
        for _ in 0..100 {
            let actual = jittered_interval(interval, true);
            assert!((5_000..=15_000).contains(&actual.as_millis()));
        }
        assert_eq!(jittered_interval(interval, false), interval);
    }

    #[test]
    fn closure_is_a_metrics_provider() {
        let mut provider = |prefixes: &[String], delta: bool| {
            assert_eq!(prefixes, &["org.apache.kafka.".to_owned()]);
            assert!(delta);
            Ok(vec![1, 2, 3])
        };
        let payload = provider
            .collect(&["org.apache.kafka.".to_owned()], true)
            .unwrap();
        assert_eq!(payload, vec![1, 2, 3]);
    }

    #[cfg(feature = "otlp")]
    #[test]
    fn built_in_provider_serializes_filtered_delta_metrics() {
        use super::ClientMetricsTelemetryProvider;
        use crate::metrics::ClientMetrics;
        use opentelemetry_proto::tonic::metrics::v1::{
            metric, number_data_point, AggregationTemporality, MetricsData,
        };
        use prost::Message;

        let metrics = ClientMetrics::new();
        metrics.record_produce_batch(3);
        let mut provider =
            ClientMetricsTelemetryProvider::new(metrics.clone()).metric_prefix("test.");
        let requested = vec!["test.produced_records".to_owned()];

        let first = provider.collect(&requested, true).unwrap();
        let first = MetricsData::decode(first.as_slice()).unwrap();
        let first_metric = &first.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(first_metric.name, "test.produced_records");
        assert!(matches!(
            first_metric.data.as_ref(),
            Some(metric::Data::Sum(_))
        ));
        let Some(metric::Data::Sum(sum)) = first_metric.data.as_ref() else {
            return;
        };
        assert_eq!(
            sum.aggregation_temporality,
            AggregationTemporality::Delta as i32
        );
        assert!(sum.is_monotonic);
        assert!(matches!(
            sum.data_points[0].value.as_ref(),
            Some(number_data_point::Value::AsInt(_))
        ));
        let Some(number_data_point::Value::AsInt(value)) = sum.data_points[0].value.as_ref() else {
            return;
        };
        assert_eq!(*value, 3);
        assert_eq!(first.resource_metrics[0].scope_metrics[0].metrics.len(), 1);

        metrics.record_produce_batch(2);
        let second = provider.collect(&requested, true).unwrap();
        let second = MetricsData::decode(second.as_slice()).unwrap();
        let second_metric = &second.resource_metrics[0].scope_metrics[0].metrics[0];
        assert!(matches!(
            second_metric.data.as_ref(),
            Some(metric::Data::Sum(_))
        ));
        let Some(metric::Data::Sum(sum)) = second_metric.data.as_ref() else {
            return;
        };
        assert!(matches!(
            sum.data_points[0].value.as_ref(),
            Some(number_data_point::Value::AsInt(_))
        ));
        let Some(number_data_point::Value::AsInt(value)) = sum.data_points[0].value.as_ref() else {
            return;
        };
        assert_eq!(*value, 2);
    }

    #[tokio::test]
    async fn negotiates_subscription_and_pushes_through_one_connection() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(4096);
        let broker = tokio::spawn(async move {
            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 18);
            let mut versions = Vec::new();
            versions.extend_from_slice(&1_i32.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(3);
            versions.extend_from_slice(&71_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(0);
            versions.extend_from_slice(&72_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(0);
            versions.extend_from_slice(&0_i32.to_be_bytes());
            versions.push(0);
            write_frame(&mut broker_stream, &versions).await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 71);
            let mut subscription = vec![0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
            subscription.extend_from_slice(&[7; 16]);
            subscription.extend_from_slice(&4_i32.to_be_bytes());
            subscription.push(3);
            subscription.push(2);
            subscription.push(0);
            subscription.extend_from_slice(&50_i32.to_be_bytes());
            subscription.extend_from_slice(&1024_i32.to_be_bytes());
            subscription.push(0);
            subscription.push(2);
            subscription.extend_from_slice(&[5, b'o', b'r', b'g', b'.']);
            subscription.push(0);
            write_frame(&mut broker_stream, &subscription).await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 72);
            let mut decoder = Decoder::new(&request);
            decoder.read_i16().unwrap();
            decoder.read_i16().unwrap();
            decoder.read_i32().unwrap();
            decoder.read_nullable_string().unwrap();
            decoder.read_tagged_fields().unwrap();
            decoder.read_uuid().unwrap();
            assert_eq!(decoder.read_i32().unwrap(), 4);
            assert!(!decoder.read_bool().unwrap());
            assert_eq!(decoder.read_i8().unwrap(), 2);
            let metrics = decoder.read_compact_bytes().unwrap();
            assert!(metrics.len() > 3);
            decoder.read_tagged_fields().unwrap();
            let mut response = Encoder::new();
            response.write_i32(3);
            response.write_empty_tagged_fields();
            response.write_i32(3);
            response.write_i16(0);
            response.write_empty_tagged_fields();
            write_frame(&mut broker_stream, &response.into_bytes()).await;
        });

        let client = Client::from_stream(
            Box::new(client_stream),
            Some("telemetry-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let mut telemetry = TelemetryClient::from_client(
            client,
            |_requested: &[String], _delta: bool| Ok(vec![1, 2, 3]),
            TelemetryConfig::new().jitter(false),
        );
        let pushed = telemetry.push_once().await.unwrap().unwrap();
        assert_eq!(pushed.subscription_id, 4);
        assert_eq!(pushed.payload_bytes, 3);
        assert_eq!(
            telemetry.subscription().unwrap().client_instance_id,
            [7; 16]
        );
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn refreshes_empty_subscription_before_the_next_push() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(4096);
        let broker = tokio::spawn(async move {
            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 18);
            let mut versions = Vec::new();
            versions.extend_from_slice(&1_i32.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(3);
            versions.extend_from_slice(&71_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(0);
            versions.extend_from_slice(&72_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(0);
            versions.extend_from_slice(&0_i32.to_be_bytes());
            versions.push(0);
            write_frame(&mut broker_stream, &versions).await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 71);
            write_frame(
                &mut broker_stream,
                &empty_telemetry_subscription_response([7; 16], 4),
            )
            .await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 71);
            write_frame(
                &mut broker_stream,
                &telemetry_subscription_response([8; 16], 9),
            )
            .await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 72);
            let mut decoder = Decoder::new(&request);
            decoder.read_i16().unwrap();
            decoder.read_i16().unwrap();
            decoder.read_i32().unwrap();
            decoder.read_nullable_string().unwrap();
            decoder.read_tagged_fields().unwrap();
            decoder.read_uuid().unwrap();
            assert_eq!(decoder.read_i32().unwrap(), 9);

            let mut response = Encoder::new();
            response.write_i32(0);
            response.write_empty_tagged_fields();
            response.write_i32(3);
            response.write_i16(0);
            response.write_empty_tagged_fields();
            write_frame(&mut broker_stream, &response.into_bytes()).await;
        });

        let client = Client::from_stream(
            Box::new(client_stream),
            Some("telemetry-empty-refresh-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let mut telemetry = TelemetryClient::from_client(
            client,
            |_requested: &[String], _delta: bool| Ok(vec![1, 2, 3]),
            TelemetryConfig::new().jitter(false),
        );

        assert!(telemetry.push_once().await.unwrap().is_none());
        let pushed = telemetry.push_once().await.unwrap().unwrap();
        assert_eq!(pushed.subscription_id, 9);
        assert_eq!(
            telemetry.subscription().unwrap().client_instance_id,
            [8; 16]
        );
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn refreshes_subscription_after_unknown_subscription_id() {
        refreshes_subscription_after_push_error(117).await;
    }

    #[tokio::test]
    async fn refreshes_subscription_after_unsupported_compression_type() {
        refreshes_subscription_after_push_error(76).await;
    }

    async fn refreshes_subscription_after_push_error(error_code: i16) {
        let (client_stream, mut broker_stream) = tokio::io::duplex(4096);
        let broker = tokio::spawn(async move {
            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 18);
            let mut versions = Vec::new();
            versions.extend_from_slice(&1_i32.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(3);
            versions.extend_from_slice(&71_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(0);
            versions.extend_from_slice(&72_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.extend_from_slice(&0_i16.to_be_bytes());
            versions.push(0);
            versions.extend_from_slice(&0_i32.to_be_bytes());
            versions.push(0);
            write_frame(&mut broker_stream, &versions).await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 71);
            write_frame(
                &mut broker_stream,
                &telemetry_subscription_response([7; 16], 4),
            )
            .await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 72);
            let mut response = Encoder::new();
            response.write_i32(3);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response.write_i16(error_code);
            response.write_empty_tagged_fields();
            write_frame(&mut broker_stream, &response.into_bytes()).await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 71);
            write_frame(
                &mut broker_stream,
                &telemetry_subscription_response([8; 16], 9),
            )
            .await;

            let request = read_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 72);
            let mut response = Encoder::new();
            response.write_i32(5);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response.write_i16(0);
            response.write_empty_tagged_fields();
            write_frame(&mut broker_stream, &response.into_bytes()).await;
        });

        let client = Client::from_stream(
            Box::new(client_stream),
            Some("telemetry-retry-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let mut telemetry = TelemetryClient::from_client(
            client,
            |_requested: &[String], _delta: bool| Ok(vec![4, 5, 6]),
            TelemetryConfig::new().jitter(false),
        );

        let pushed = telemetry.push_once().await.unwrap().unwrap();
        assert_eq!(pushed.subscription_id, 9);
        assert_eq!(pushed.payload_bytes, 3);
        assert_eq!(
            telemetry.subscription().unwrap().client_instance_id,
            [8; 16]
        );
        broker.await.unwrap();
    }

    fn telemetry_subscription_response(
        client_instance_id: [u8; 16],
        subscription_id: i32,
    ) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(4);
        response.write_empty_tagged_fields();
        response.write_i32(0);
        response.write_i16(0);
        response.write_uuid(&client_instance_id);
        response.write_i32(subscription_id);
        response
            .write_compact_array(Some(&[0_i8]), |encoder, value| {
                encoder.write_i8(*value);
                Ok(())
            })
            .unwrap();
        response.write_i32(50);
        response.write_i32(1024);
        response.write_bool(false);
        response
            .write_compact_array(Some(&["org.apache.kafka.".to_owned()]), |encoder, value| {
                encoder.write_compact_string(value)
            })
            .unwrap();
        response.write_empty_tagged_fields();
        response.into_bytes()
    }

    fn empty_telemetry_subscription_response(
        client_instance_id: [u8; 16],
        subscription_id: i32,
    ) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(4);
        response.write_empty_tagged_fields();
        response.write_i32(0);
        response.write_i16(0);
        response.write_uuid(&client_instance_id);
        response.write_i32(subscription_id);
        response
            .write_compact_array(Some(&[0_i8]), |encoder, value| {
                encoder.write_i8(*value);
                Ok(())
            })
            .unwrap();
        response.write_i32(50);
        response.write_i32(1024);
        response.write_bool(false);
        let requested_metrics: Vec<String> = Vec::new();
        response
            .write_compact_array(Some(&requested_metrics), |encoder, value| {
                encoder.write_compact_string(value)
            })
            .unwrap();
        response.write_empty_tagged_fields();
        response.into_bytes()
    }

    async fn read_frame(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
        let mut size = [0; 4];
        stream.read_exact(&mut size).await.unwrap();
        let size = usize::try_from(i32::from_be_bytes(size)).unwrap();
        let mut frame = vec![0; size];
        stream.read_exact(&mut frame).await.unwrap();
        frame
    }

    async fn write_frame(stream: &mut tokio::io::DuplexStream, frame: &[u8]) {
        stream
            .write_all(&(i32::try_from(frame.len()).unwrap()).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(frame).await.unwrap();
        stream.flush().await.unwrap();
    }
}
