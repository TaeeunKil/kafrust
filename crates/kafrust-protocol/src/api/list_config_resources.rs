use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka ListConfigResources API key.
pub const API_KEY: i16 = 74;

/// Kafka configuration resource type for topics.
pub const TOPIC_RESOURCE_TYPE: i8 = 2;
/// Kafka configuration resource type for brokers.
pub const BROKER_RESOURCE_TYPE: i8 = 4;
/// Kafka configuration resource type for broker loggers.
pub const BROKER_LOGGER_RESOURCE_TYPE: i8 = 8;
/// Kafka configuration resource type for client metrics.
pub const CLIENT_METRICS_RESOURCE_TYPE: i8 = 16;
/// Kafka configuration resource type for consumer groups.
pub const GROUP_RESOURCE_TYPE: i8 = 32;

/// Kafka ListConfigResources v1 request.
///
/// Version 0 uses the same API key for the older ListClientMetricsResources
/// operation and has no resource-type request field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl ListConfigResourcesRequestV0 {
    /// Encodes the flexible v0 request header and empty body.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// Kafka ListClientMetricsResources v0 response carried by API key 74.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub resources: Vec<ListedClientMetricsResourceV0>,
}

impl ListConfigResourcesResponseV0 {
    /// Decodes the flexible v0 response body.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let resources = decoder
            .read_compact_array("list client metrics resources", |decoder| {
                let name = decoder.read_compact_string()?;
                decoder.read_tagged_fields()?;
                Ok(ListedClientMetricsResourceV0 { name })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            resources,
        })
    }
}

/// One client-metrics resource returned by the API 74 v0 compatibility path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedClientMetricsResourceV0 {
    pub name: String,
}

/// Kafka ListConfigResources v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    /// Empty requests ask Kafka for every supported configuration resource type.
    pub resource_types: Vec<i8>,
}

impl ListConfigResourcesRequestV1 {
    /// Encodes the flexible request header and body.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&self.resource_types), |encoder, resource_type| {
            encoder.write_i8(*resource_type);
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// Kafka ListConfigResources v1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub resources: Vec<ListedConfigResourceV1>,
}

impl ListConfigResourcesResponseV1 {
    /// Decodes a flexible response body.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let resources = decoder
            .read_compact_array("list config resources", |decoder| {
                let resource_name = decoder.read_compact_string()?;
                let resource_type = decoder.read_i8()?;
                decoder.read_tagged_fields()?;
                Ok(ListedConfigResourceV1 {
                    resource_name,
                    resource_type,
                })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            resources,
        })
    }
}

/// One configuration resource returned by Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedConfigResourceV1 {
    pub resource_name: String,
    pub resource_type: i8,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ListConfigResourcesRequestV0, ListConfigResourcesRequestV1, ListConfigResourcesResponseV0,
        ListConfigResourcesResponseV1, API_KEY, BROKER_RESOURCE_TYPE, GROUP_RESOURCE_TYPE,
        TOPIC_RESOURCE_TYPE,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_list_config_resources_v1_request() {
        let request = ListConfigResourcesRequestV1 {
            correlation_id: 12,
            client_id: Some("kafrust".to_owned()),
            resource_types: vec![TOPIC_RESOURCE_TYPE, GROUP_RESOURCE_TYPE],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 74, // API key
                0, 1, // API version
                0, 0, 0, 12, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0,    // request header tagged fields
                3,    // compact resource type count
                2, 32, // topic and group
                0,  // request tagged fields
            ]
        );
        assert_eq!(API_KEY, 74);
    }

    #[test]
    fn encodes_list_client_metrics_resources_v0_request() {
        let request = ListConfigResourcesRequestV0 {
            correlation_id: 13,
            client_id: None,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 74, // API key
                0, 0, // API version
                0, 0, 0, 13, // correlation ID
                0xff, 0xff, // nullable client ID
                0,    // request header tagged fields
                0,    // empty v0 request body tagged fields
            ]
        );
    }

    #[test]
    fn decodes_list_config_resources_v1_response() {
        let mut body = Encoder::new();
        body.write_i32(9);
        body.write_i16(0);
        body.write_compact_array(
            Some(&["orders".to_owned(), "payments".to_owned()]),
            |encoder, name| {
                encoder.write_compact_string(name)?;
                encoder.write_i8(if name == "orders" {
                    TOPIC_RESOURCE_TYPE
                } else {
                    BROKER_RESOURCE_TYPE
                });
                encoder.write_empty_tagged_fields();
                Ok(())
            },
        )
        .unwrap();
        body.write_empty_tagged_fields();

        let bytes = body.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ListConfigResourcesResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 9);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.resources.len(), 2);
        assert_eq!(response.resources[0].resource_name, "orders");
        assert_eq!(response.resources[0].resource_type, TOPIC_RESOURCE_TYPE);
        assert_eq!(response.resources[1].resource_type, BROKER_RESOURCE_TYPE);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_list_client_metrics_resources_v0_response() {
        let mut body = Encoder::new();
        body.write_i32(5);
        body.write_i16(0);
        body.write_compact_array(Some(&["latency", "throughput"]), |encoder, name| {
            encoder.write_compact_string(name)?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
        body.write_empty_tagged_fields();

        let bytes = body.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ListConfigResourcesResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 5);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.resources.len(), 2);
        assert_eq!(response.resources[0].name, "latency");
        assert_eq!(response.resources[1].name, "throughput");
        assert!(decoder.is_empty());
    }
}
