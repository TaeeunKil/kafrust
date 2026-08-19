//! High-level KIP-714 client telemetry scheduling.

use crate::client::Client;
use crate::config::ClientConfig;
use crate::error::{Error, Result};
use kafrust_protocol::api::telemetry::{
    GET_TELEMETRY_SUBSCRIPTIONS_API_KEY, PUSH_TELEMETRY_API_KEY,
};
use kafrust_protocol::record_batch::{compress_bytes, RecordBatchCompression};
use rand::Rng;
use std::time::Duration;
use tokio::sync::watch;

const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
const TELEMETRY_TOO_LARGE: i16 = 118;
const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;

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
/// runtime refreshes an invalid subscription once, bounds payload allocation,
/// and sends one terminating push when the shutdown channel is set.
pub struct TelemetryClient<P> {
    client: Client,
    provider: P,
    config: TelemetryConfig,
    client_instance_id: [u8; 16],
    subscription: Option<TelemetrySubscription>,
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
        }
    }

    /// Returns the last broker subscription, if one has been acquired.
    pub fn subscription(&self) -> Option<&TelemetrySubscription> {
        self.subscription.as_ref()
    }

    /// Fetches a new broker subscription and replaces local subscription state.
    pub async fn refresh_subscription(&mut self) -> Result<&TelemetrySubscription> {
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

        let response = self
            .client
            .get_telemetry_subscriptions_v0(self.client_instance_id)
            .await?;
        if response.error_code != 0 {
            return Err(self
                .client
                .broker_error(response.error_code, "GetTelemetrySubscriptions".to_owned()));
        }
        let subscription = TelemetrySubscription::from_response(response)?;
        self.client_instance_id = subscription.client_instance_id;
        self.subscription = Some(subscription);
        self.subscription
            .as_ref()
            .ok_or(Error::Unsupported("telemetry subscription was not stored"))
    }

    /// Collects and pushes one non-terminating telemetry payload.
    ///
    /// An empty broker metric subscription returns `Ok(None)` and does not send
    /// a payload. An outdated subscription or compression selection causes one
    /// refresh and retry on the same connection.
    pub async fn push_once(&mut self) -> Result<Option<TelemetryPushSummary>> {
        if self.subscription.is_none() {
            self.refresh_subscription().await?;
        }
        if self
            .subscription
            .as_ref()
            .is_some_and(|subscription| subscription.requested_metrics.is_empty())
        {
            return Ok(None);
        }
        self.push_with_refresh(false).await.map(Some)
    }

    /// Sends one terminating payload, if a subscription has been acquired.
    pub async fn terminate(&mut self) -> Result<Option<TelemetryPushSummary>> {
        if self.subscription.is_none() {
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
        for attempt in 0..=1 {
            match self.push_current(terminating).await {
                Err(Error::Broker { code, .. })
                    if attempt == 0
                        && (code == UNKNOWN_SUBSCRIPTION_ID
                            || code == UNSUPPORTED_COMPRESSION_TYPE) =>
                {
                    self.refresh_subscription().await?;
                }
                result => return result,
            }
        }
        Err(Error::Unsupported("telemetry retry exhausted"))
    }

    async fn push_current(&mut self, terminating: bool) -> Result<TelemetryPushSummary> {
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
