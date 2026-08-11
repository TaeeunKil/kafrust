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
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 3,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.client_software_name)?;
        encoder.write_compact_string(&self.client_software_version)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
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
        Ok(Self {
            error_code,
            api_keys,
        })
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
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            tagged_fields,
        })
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
        ApiVersionsRequestV0, ApiVersionsRequestV3, ApiVersionsResponseV0, ApiVersionsResponseV3,
        API_KEY,
    };
    use crate::codec::Decoder;

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
}
