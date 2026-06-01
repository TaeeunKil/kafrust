use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 18;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiKeyVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiKeyVersion {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            api_key: decoder.read_i16()?,
            min_version: decoder.read_i16()?,
            max_version: decoder.read_i16()?,
        })
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{ApiVersionsRequestV0, ApiVersionsResponseV0, API_KEY};
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
}
