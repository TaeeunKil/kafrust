use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka DescribeCluster API key.
pub const API_KEY: i16 = 60;

/// Describe broker endpoints.
pub const BROKER_ENDPOINT_TYPE: i8 = 1;
/// Describe controller endpoints.
pub const CONTROLLER_ENDPOINT_TYPE: i8 = 2;

/// Kafka DescribeCluster request for versions 0 and 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub api_version: i16,
    pub include_cluster_authorized_operations: bool,
    pub endpoint_type: i8,
}

impl DescribeClusterRequest {
    /// Encodes the flexible request header and body for the selected version.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: self.api_version,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_bool(self.include_cluster_authorized_operations);
        if self.api_version >= 1 {
            encoder.write_i8(self.endpoint_type);
        }
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// One broker endpoint returned by DescribeCluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterBroker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// Kafka DescribeCluster response for versions 0 and 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterResponse {
    pub api_version: i16,
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub endpoint_type: Option<i8>,
    pub cluster_id: String,
    pub controller_id: i32,
    pub brokers: Vec<DescribeClusterBroker>,
    pub cluster_authorized_operations: i32,
}

impl DescribeClusterResponse {
    /// Decodes a flexible response body for the selected version.
    pub fn decode_body(decoder: &mut Decoder<'_>, api_version: i16) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let endpoint_type = (api_version >= 1).then(|| decoder.read_i8()).transpose()?;
        let cluster_id = decoder.read_compact_string()?;
        let controller_id = decoder.read_i32()?;
        let brokers = decoder
            .read_compact_array("describe cluster brokers", |decoder| {
                let node_id = decoder.read_i32()?;
                let host = decoder.read_compact_string()?;
                let port = decoder.read_i32()?;
                let rack = decoder.read_compact_nullable_string()?;
                decoder.read_tagged_fields()?;
                Ok(DescribeClusterBroker {
                    node_id,
                    host,
                    port,
                    rack,
                })
            })?
            .unwrap_or_default();
        let cluster_authorized_operations = decoder.read_i32()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            api_version,
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DescribeClusterRequest, DescribeClusterResponse, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_cluster_v1_request() {
        let request = DescribeClusterRequest {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            api_version: 1,
            include_cluster_authorized_operations: true,
            endpoint_type: 2,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 60, // API key
                0, 1, // API version
                0, 0, 0, 7, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't',
                0, // request header tagged fields
                1, // include cluster authorized operations
                2, // controller endpoint
                0, // request tagged fields
            ]
        );
        assert_eq!(API_KEY, 60);
    }

    #[test]
    fn decodes_describe_cluster_v1_response() {
        let mut body = Encoder::new();
        body.write_i32(11);
        body.write_i16(0);
        body.write_compact_nullable_string(None).unwrap();
        body.write_i8(2);
        body.write_compact_string("cluster").unwrap();
        body.write_i32(2);
        body.write_compact_array(Some(&["broker-a".to_owned()]), |encoder, host| {
            encoder.write_i32(1);
            encoder.write_compact_string(host)?;
            encoder.write_i32(9092);
            encoder.write_compact_nullable_string(Some("rack-a"))?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
        body.write_i32(0x1234);
        body.write_empty_tagged_fields();

        let bytes = body.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = DescribeClusterResponse::decode_body(&mut decoder, 1).unwrap();

        assert_eq!(response.api_version, 1);
        assert_eq!(response.throttle_time_ms, 11);
        assert_eq!(response.endpoint_type, Some(2));
        assert_eq!(response.cluster_id, "cluster");
        assert_eq!(response.controller_id, 2);
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.brokers[0].host, "broker-a");
        assert_eq!(response.brokers[0].rack.as_deref(), Some("rack-a"));
        assert_eq!(response.cluster_authorized_operations, 0x1234);
        assert!(decoder.is_empty());
    }
}
