//! Kafka client telemetry protocol types from KIP-714.

use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka API key for `GetTelemetrySubscriptions`.
pub const GET_TELEMETRY_SUBSCRIPTIONS_API_KEY: i16 = 71;
/// Kafka API key for `PushTelemetry`.
pub const PUSH_TELEMETRY_API_KEY: i16 = 72;

/// KIP-714 telemetry subscription request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTelemetrySubscriptionsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    /// The all-zero UUID requests a new broker-assigned client instance ID.
    pub client_instance_id: [u8; 16],
}

impl GetTelemetrySubscriptionsRequestV0 {
    /// Encodes the flexible v0 request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: GET_TELEMETRY_SUBSCRIPTIONS_API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_uuid(&self.client_instance_id);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// KIP-714 telemetry subscription response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTelemetrySubscriptionsResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    /// A non-zero value is returned when the request supplied the zero UUID.
    pub client_instance_id: [u8; 16],
    pub subscription_id: i32,
    pub accepted_compression_types: Vec<i8>,
    pub push_interval_ms: i32,
    pub telemetry_max_bytes: i32,
    pub delta_temporality: bool,
    pub requested_metrics: Vec<String>,
}

impl GetTelemetrySubscriptionsResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let client_instance_id = decoder.read_uuid()?;
        let subscription_id = decoder.read_i32()?;
        let accepted_compression_types = decoder
            .read_compact_array("accepted telemetry compression types", |decoder| {
                decoder.read_i8()
            })?
            .unwrap_or_default();
        let push_interval_ms = decoder.read_i32()?;
        let telemetry_max_bytes = decoder.read_i32()?;
        let delta_temporality = decoder.read_bool()?;
        let requested_metrics = decoder
            .read_compact_array("requested telemetry metrics", |decoder| {
                decoder.read_compact_string()
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
        })
    }
}

/// KIP-714 telemetry payload request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub client_instance_id: [u8; 16],
    pub subscription_id: i32,
    pub terminating: bool,
    /// Compression type identifier from Kafka's record-batch codec values.
    pub compression_type: i8,
    /// OpenTelemetry MetricsData v1 protobuf bytes, optionally compressed as
    /// declared by `compression_type`.
    pub metrics: Vec<u8>,
}

impl PushTelemetryRequestV0 {
    /// Encodes the flexible v0 request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: PUSH_TELEMETRY_API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_uuid(&self.client_instance_id);
        encoder.write_i32(self.subscription_id);
        encoder.write_bool(self.terminating);
        encoder.write_i8(self.compression_type);
        encoder.write_compact_bytes(&self.metrics)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// KIP-714 telemetry payload response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTelemetryResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

impl PushTelemetryResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn encodes_get_telemetry_subscriptions_request() {
        let request = GetTelemetrySubscriptionsRequestV0 {
            correlation_id: 12,
            client_id: Some("kafrust".to_owned()),
            client_instance_id: [0; 16],
        };
        let encoded = request.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        assert_eq!(
            decoder.read_i16().unwrap(),
            GET_TELEMETRY_SUBSCRIPTIONS_API_KEY
        );
        assert_eq!(decoder.read_i16().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 12);
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("kafrust".to_owned())
        );
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert_eq!(decoder.read_uuid().unwrap(), [0; 16]);
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_telemetry_subscription_response() {
        let mut encoder = Encoder::new();
        encoder.write_i32(4);
        encoder.write_i16(0);
        encoder.write_uuid(&[1; 16]);
        encoder.write_i32(7);
        encoder
            .write_compact_array(Some(&[0_i8, 2_i8]), |encoder, value| {
                encoder.write_i8(*value);
                Ok(())
            })
            .unwrap();
        encoder.write_i32(30_000);
        encoder.write_i32(1024 * 1024);
        encoder.write_bool(true);
        encoder
            .write_compact_array(Some(&["org.apache.kafka.".to_owned()]), |encoder, value| {
                encoder.write_compact_string(value)
            })
            .unwrap();
        encoder.write_empty_tagged_fields();

        let response = GetTelemetrySubscriptionsResponseV0::decode_body(&mut Decoder::new(
            &encoder.into_bytes(),
        ))
        .unwrap();
        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.client_instance_id, [1; 16]);
        assert_eq!(response.subscription_id, 7);
        assert_eq!(response.accepted_compression_types, vec![0, 2]);
        assert_eq!(response.push_interval_ms, 30_000);
        assert_eq!(response.telemetry_max_bytes, 1024 * 1024);
        assert!(response.delta_temporality);
        assert_eq!(
            response.requested_metrics,
            vec!["org.apache.kafka.".to_owned()]
        );
    }

    #[test]
    fn encodes_push_telemetry_request_with_payload() {
        let request = PushTelemetryRequestV0 {
            correlation_id: 13,
            client_id: None,
            client_instance_id: [2; 16],
            subscription_id: 7,
            terminating: false,
            compression_type: 0,
            metrics: vec![1, 2, 3],
        };
        let encoded = request.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        assert_eq!(decoder.read_i16().unwrap(), PUSH_TELEMETRY_API_KEY);
        assert_eq!(decoder.read_i16().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 13);
        assert_eq!(decoder.read_nullable_string().unwrap(), None);
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert_eq!(decoder.read_uuid().unwrap(), [2; 16]);
        assert_eq!(decoder.read_i32().unwrap(), 7);
        assert!(!decoder.read_bool().unwrap());
        assert_eq!(decoder.read_i8().unwrap(), 0);
        assert_eq!(decoder.read_compact_bytes().unwrap(), vec![1, 2, 3]);
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_push_telemetry_response() {
        let mut encoder = Encoder::new();
        encoder.write_i32(9);
        encoder.write_i16(0);
        encoder.write_empty_tagged_fields();
        let response =
            PushTelemetryResponseV0::decode_body(&mut Decoder::new(&encoder.into_bytes())).unwrap();
        assert_eq!(response.throttle_time_ms, 9);
        assert_eq!(response.error_code, 0);
    }
}
