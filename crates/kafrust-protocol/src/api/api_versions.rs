use crate::codec::{Decoder, Encoder, TaggedField};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 18;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiKeyVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// A feature version range advertised by one broker in ApiVersions v3+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedFeature {
    /// Feature name defined by Kafka's feature registry.
    pub name: String,
    /// Minimum version level supported by the broker.
    pub min_version: i16,
    /// Maximum version level supported by the broker.
    pub max_version: i16,
}

impl SupportedFeature {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let feature = Self {
            name: decoder.read_compact_string()?,
            min_version: decoder.read_i16()?,
            max_version: decoder.read_i16()?,
        };
        decoder.read_tagged_fields()?;
        Ok(feature)
    }
}

/// A cluster-wide finalized feature range from ApiVersions v3+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedFeature {
    /// Feature name defined by Kafka's feature registry.
    pub name: String,
    /// Finalized minimum version level.
    pub min_version_level: i16,
    /// Finalized maximum version level.
    pub max_version_level: i16,
}

impl FinalizedFeature {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let feature = Self {
            name: decoder.read_compact_string()?,
            max_version_level: decoder.read_i16()?,
            min_version_level: decoder.read_i16()?,
        };
        decoder.read_tagged_fields()?;
        Ok(feature)
    }
}

/// Common capability lookup implemented by fixed and flexible ApiVersions responses.
pub trait ApiVersionsLookup {
    /// Returns the highest broker-supported version not exceeding the client limit.
    fn highest_supported_version(&self, api_key: i16, max_supported: i16) -> Option<i16>;
}

impl ApiKeyVersion {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            api_key: decoder.read_i16()?,
            min_version: decoder.read_i16()?,
            max_version: decoder.read_i16()?,
        })
    }

    fn decode_flexible(decoder: &mut Decoder<'_>) -> Result<Self> {
        let version = Self::decode(decoder)?;
        decoder.read_tagged_fields()?;
        Ok(version)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl ApiVersionsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        Ok(encoder.into_bytes())
    }
}

/// ApiVersions v3 request with client software identification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequestV3 {
    /// Request correlation ID.
    pub correlation_id: i32,
    /// Optional Kafka client ID from the request header.
    pub client_id: Option<String>,
    /// Client software name reported through KIP-511.
    pub client_software_name: String,
    /// Client software version reported through KIP-511.
    pub client_software_version: String,
}

impl ApiVersionsRequestV3 {
    /// Encodes an ApiVersions v3 request frame without the outer frame length.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_flexible_request(
            3,
            self.correlation_id,
            self.client_id.as_deref(),
            &self.client_software_name,
            &self.client_software_version,
            None,
            None,
        )
    }
}

/// ApiVersions v4 request.
///
/// Version 4 keeps the v3 wire shape while allowing the broker to report
/// feature minimum versions of zero correctly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequestV4 {
    /// Request correlation ID.
    pub correlation_id: i32,
    /// Optional Kafka client ID from the request header.
    pub client_id: Option<String>,
    /// Client software name reported through KIP-511.
    pub client_software_name: String,
    /// Client software version reported through KIP-511.
    pub client_software_version: String,
}

impl ApiVersionsRequestV4 {
    /// Encodes an ApiVersions v4 request frame without the outer frame length.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_flexible_request(
            4,
            self.correlation_id,
            self.client_id.as_deref(),
            &self.client_software_name,
            &self.client_software_version,
            None,
            None,
        )
    }
}

/// ApiVersions v5 request with optional cluster and node identity checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequestV5 {
    /// Request correlation ID.
    pub correlation_id: i32,
    /// Optional Kafka client ID from the request header.
    pub client_id: Option<String>,
    /// Client software name reported through KIP-511.
    pub client_software_name: String,
    /// Client software version reported through KIP-511.
    pub client_software_version: String,
    /// Expected cluster ID, when the client already knows it.
    pub cluster_id: Option<String>,
    /// Expected broker node ID, or `-1` when it is unknown.
    pub node_id: i32,
}

impl ApiVersionsRequestV5 {
    /// Encodes an ApiVersions v5 request frame without the outer frame length.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_flexible_request(
            5,
            self.correlation_id,
            self.client_id.as_deref(),
            &self.client_software_name,
            &self.client_software_version,
            Some(self.cluster_id.as_deref()),
            Some(self.node_id),
        )
    }
}

fn encode_flexible_request(
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    client_software_name: &str,
    client_software_version: &str,
    cluster_id: Option<Option<&str>>,
    node_id: Option<i32>,
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: API_KEY,
        api_version,
        correlation_id,
        client_id: client_id.map(str::to_owned),
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_string(client_software_name)?;
    encoder.write_compact_string(client_software_version)?;
    if let (Some(cluster_id), Some(node_id)) = (cluster_id, node_id) {
        encoder.write_compact_nullable_string(cluster_id)?;
        encoder.write_i32(node_id);
    }
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponseV0 {
    pub error_code: i16,
    pub api_keys: Vec<ApiKeyVersion>,
}

impl ApiVersionsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let api_keys = decoder
            .read_array("api versions", ApiKeyVersion::decode)?
            .unwrap_or_default();
        let response = Self {
            error_code,
            api_keys,
        };
        decoder.finish()?;
        Ok(response)
    }

    pub fn highest_supported_version(&self, api_key: i16, max_supported: i16) -> Option<i16> {
        self.api_keys
            .iter()
            .find(|version| version.api_key == api_key)
            .and_then(|version| {
                let selected = version.max_version.min(max_supported);
                (selected >= version.min_version).then_some(selected)
            })
    }
}

impl ApiVersionsLookup for ApiVersionsResponseV0 {
    fn highest_supported_version(&self, api_key: i16, max_supported: i16) -> Option<i16> {
        highest_supported_version(&self.api_keys, api_key, max_supported)
    }
}

/// ApiVersions v3 response with flexible encoding and forward-compatible tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponseV3 {
    /// Top-level Kafka error code.
    pub error_code: i16,
    /// API version ranges advertised by the broker.
    pub api_keys: Vec<ApiKeyVersion>,
    /// Broker throttle duration in milliseconds.
    pub throttle_time_ms: i32,
    /// Feature ranges supported by the broker.
    pub supported_features: Vec<SupportedFeature>,
    /// Monotonically increasing finalized-feature metadata epoch, or `-1` if unknown.
    pub finalized_features_epoch: i64,
    /// Cluster-wide finalized feature ranges when the epoch is known.
    pub finalized_features: Vec<FinalizedFeature>,
    /// Whether the broker reports that ZooKeeper migration is ready.
    pub zk_migration_ready: bool,
    /// Unknown or future top-level tagged fields preserved for inspection.
    pub tagged_fields: Vec<TaggedField>,
}

impl ApiVersionsResponseV3 {
    /// Decodes an ApiVersions v3 response body after its response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let api_keys = decoder
            .read_compact_array("api versions", ApiKeyVersion::decode_flexible)?
            .unwrap_or_default();
        let throttle_time_ms = decoder.read_i32()?;
        let tagged_fields = decoder.read_tagged_fields()?;
        let limits = decoder.limits();
        let mut supported_features = Vec::new();
        let mut finalized_features_epoch = -1;
        let mut finalized_features = Vec::new();
        let mut zk_migration_ready = false;
        let mut unknown_tagged_fields = Vec::new();
        for field in tagged_fields {
            match field.tag {
                0 => {
                    let mut field_decoder = Decoder::with_limits(&field.data, limits);
                    supported_features = field_decoder
                        .read_compact_array("supported features", SupportedFeature::decode)?
                        .unwrap_or_default();
                }
                1 => {
                    let mut field_decoder = Decoder::with_limits(&field.data, limits);
                    finalized_features_epoch = field_decoder.read_i64()?;
                }
                2 => {
                    let mut field_decoder = Decoder::with_limits(&field.data, limits);
                    finalized_features = field_decoder
                        .read_compact_array("finalized features", FinalizedFeature::decode)?
                        .unwrap_or_default();
                }
                3 => {
                    let mut field_decoder = Decoder::with_limits(&field.data, limits);
                    zk_migration_ready = field_decoder.read_bool()?;
                }
                _ => unknown_tagged_fields.push(field),
            }
        }
        let response = Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
            finalized_features_epoch,
            finalized_features,
            zk_migration_ready,
            tagged_fields: unknown_tagged_fields,
        };
        decoder.finish()?;
        Ok(response)
    }

    /// Returns the highest broker-supported version not exceeding the client limit.
    pub fn highest_supported_version(&self, api_key: i16, max_supported: i16) -> Option<i16> {
        highest_supported_version(&self.api_keys, api_key, max_supported)
    }
}

impl ApiVersionsLookup for ApiVersionsResponseV3 {
    fn highest_supported_version(&self, api_key: i16, max_supported: i16) -> Option<i16> {
        highest_supported_version(&self.api_keys, api_key, max_supported)
    }
}

/// ApiVersions v4 response. The body schema is identical to v3.
pub type ApiVersionsResponseV4 = ApiVersionsResponseV3;

/// ApiVersions v5 response. The body schema is identical to v4.
pub type ApiVersionsResponseV5 = ApiVersionsResponseV3;

/// Kafka's `UNSUPPORTED_VERSION` protocol error code.
pub const UNSUPPORTED_VERSION_ERROR_CODE: i16 = 35;

fn highest_supported_version(
    api_keys: &[ApiKeyVersion],
    api_key: i16,
    max_supported: i16,
) -> Option<i16> {
    api_keys
        .iter()
        .find(|version| version.api_key == api_key)
        .and_then(|version| {
            let selected = version.max_version.min(max_supported);
            (selected >= version.min_version).then_some(selected)
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ApiVersionsRequestV0, ApiVersionsRequestV3, ApiVersionsRequestV4, ApiVersionsRequestV5,
        ApiVersionsResponseV0, ApiVersionsResponseV3, ApiVersionsResponseV4, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_api_versions_request_v0() {
        let request = ApiVersionsRequestV0 {
            correlation_id: 42,
            client_id: Some("kafrust".to_owned()),
        };
        assert_eq!(
            request.encode().unwrap(),
            [
                0, 18, // api key
                0, 0, // api version
                0, 0, 0, 42, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't',
            ]
        );
        assert_eq!(API_KEY, 18);
    }

    #[test]
    fn decodes_api_versions_response_v0() {
        let bytes = [
            0, 0, // error code
            0, 0, 0, 2, // api key count
            0, 18, 0, 0, 0, 4, // ApiVersions min/max
            0, 3, 0, 1, 0, 9, // Metadata min/max
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = ApiVersionsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.api_keys.len(), 2);
        assert_eq!(response.highest_supported_version(18, 3), Some(3));
        assert_eq!(response.highest_supported_version(3, 12), Some(9));
        assert_eq!(response.highest_supported_version(1, 1), None);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_api_versions_request_v3() {
        let request = ApiVersionsRequestV3 {
            correlation_id: 42,
            client_id: Some("kafrust".to_owned()),
            client_software_name: "kafrust".to_owned(),
            client_software_version: "0.3.0".to_owned(),
        };
        assert_eq!(
            request.encode().unwrap(),
            [
                0, 18, // api key
                0, 3, // api version
                0, 0, 0, 42, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // nullable client id
                0,    // request tagged fields
                8, b'k', b'a', b'f', b'r', b'u', b's', b't', // software name
                6, b'0', b'.', b'3', b'.', b'0', // software version
                0,    // request body tagged fields
            ]
        );
    }

    #[test]
    fn encodes_api_versions_request_v4_with_the_v4_header_version() {
        let request = ApiVersionsRequestV4 {
            correlation_id: 42,
            client_id: Some("kafrust".to_owned()),
            client_software_name: "kafrust".to_owned(),
            client_software_version: "0.3.0".to_owned(),
        };
        assert_eq!(
            request.encode().unwrap(),
            [
                0, 18, // api key
                0, 4, // api version
                0, 0, 0, 42, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // nullable client id
                0,    // request tagged fields
                8, b'k', b'a', b'f', b'r', b'u', b's', b't', // software name
                6, b'0', b'.', b'3', b'.', b'0', // software version
                0,    // request body tagged fields
            ]
        );
    }

    #[test]
    fn encodes_api_versions_request_v5_cluster_identity() {
        let request = ApiVersionsRequestV5 {
            correlation_id: 42,
            client_id: None,
            client_software_name: "kafrust".to_owned(),
            client_software_version: "0.3.0".to_owned(),
            cluster_id: Some("cluster-1".to_owned()),
            node_id: 3,
        };
        assert_eq!(
            request.encode().unwrap(),
            [
                0, 18, // api key
                0, 5, // api version
                0, 0, 0, 42, // correlation id
                0xff, 0xff, // nullable client id
                0,    // request header tagged fields
                8, b'k', b'a', b'f', b'r', b'u', b's', b't', // software name
                6, b'0', b'.', b'3', b'.', b'0', // software version
                10, b'c', b'l', b'u', b's', b't', b'e', b'r', b'-', b'1', // cluster id
                0, 0, 0, 3, // node id
                0, // request body tagged fields
            ]
        );
    }

    #[test]
    fn decodes_api_versions_response_v3() {
        let bytes = [
            0, 0, // error code
            3, // compact api key count: two entries
            0, 18, 0, 0, 0, 4, 0, // ApiVersions entry + tagged fields
            0, 3, 0, 1, 0, 9, 0, // Metadata entry + tagged fields
            0, 0, 0, 17, // throttle time
            0,  // top-level tagged fields
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = ApiVersionsResponseV3::decode_body(&mut decoder).unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.throttle_time_ms, 17);
        assert_eq!(response.api_keys.len(), 2);
        assert_eq!(response.highest_supported_version(18, 3), Some(3));
        assert_eq!(response.highest_supported_version(3, 12), Some(9));
        assert_eq!(response.highest_supported_version(1, 1), None);
        assert!(response.tagged_fields.is_empty());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_api_versions_v4_feature_minimum_zero() {
        let mut supported_features = Encoder::new();
        supported_features
            .write_compact_array(Some(&["metadata.version"]), |encoder, name| {
                encoder.write_compact_string(name)?;
                encoder.write_i16(0);
                encoder.write_i16(1);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();

        let supported_features = supported_features.into_bytes();
        let mut body = Encoder::new();
        body.write_i16(0);
        body.write_unsigned_varint(1); // empty compact ApiKeys array
        body.write_i32(0);
        body.write_unsigned_varint(1); // one top-level tagged field
        body.write_unsigned_varint(0); // supported features
        body.write_unsigned_varint(u32::try_from(supported_features.len()).unwrap());
        body.write_raw(&supported_features);

        let bytes = body.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ApiVersionsResponseV4::decode_body(&mut decoder).unwrap();

        assert_eq!(response.supported_features.len(), 1);
        assert_eq!(response.supported_features[0].min_version, 0);
        assert_eq!(response.supported_features[0].max_version, 1);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_api_versions_feature_metadata_and_preserves_unknown_tags() {
        let mut supported_features = Encoder::new();
        supported_features
            .write_compact_array(Some(&["group_coordinator"]), |encoder, name| {
                encoder.write_compact_string(name)?;
                encoder.write_i16(1);
                encoder.write_i16(3);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        let supported_features = supported_features.into_bytes();

        let mut finalized_features = Encoder::new();
        finalized_features
            .write_compact_array(Some(&["metadata.version"]), |encoder, name| {
                encoder.write_compact_string(name)?;
                encoder.write_i16(4);
                encoder.write_i16(1);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        let finalized_features = finalized_features.into_bytes();

        let mut body = Encoder::new();
        body.write_i16(0);
        body.write_unsigned_varint(1); // empty compact ApiKeys array
        body.write_i32(0);
        body.write_unsigned_varint(5); // four known tags and one unknown tag
        body.write_unsigned_varint(0);
        body.write_unsigned_varint(u32::try_from(supported_features.len()).unwrap());
        body.write_raw(&supported_features);
        body.write_unsigned_varint(1);
        body.write_unsigned_varint(8);
        body.write_i64(42);
        body.write_unsigned_varint(2);
        body.write_unsigned_varint(u32::try_from(finalized_features.len()).unwrap());
        body.write_raw(&finalized_features);
        body.write_unsigned_varint(3);
        body.write_unsigned_varint(1);
        body.write_bool(true);
        body.write_unsigned_varint(99);
        body.write_unsigned_varint(2);
        body.write_raw(&[7, 8]);

        let bytes = body.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ApiVersionsResponseV3::decode_body(&mut decoder).unwrap();

        assert_eq!(response.supported_features.len(), 1);
        assert_eq!(response.supported_features[0].name, "group_coordinator");
        assert_eq!(response.supported_features[0].min_version, 1);
        assert_eq!(response.supported_features[0].max_version, 3);
        assert_eq!(response.finalized_features_epoch, 42);
        assert_eq!(response.finalized_features.len(), 1);
        assert_eq!(response.finalized_features[0].name, "metadata.version");
        assert_eq!(response.finalized_features[0].min_version_level, 1);
        assert_eq!(response.finalized_features[0].max_version_level, 4);
        assert!(response.zk_migration_ready);
        assert_eq!(response.tagged_fields.len(), 1);
        assert_eq!(response.tagged_fields[0].tag, 99);
        assert_eq!(response.tagged_fields[0].data, vec![7, 8]);
        assert!(decoder.is_empty());
    }
}
