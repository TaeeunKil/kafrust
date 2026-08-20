use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 57;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub timeout_ms: i32,
    pub updates: Vec<FeatureUpdateV0>,
}

impl UpdateFeaturesRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i32(self.timeout_ms);
        encoder.write_compact_array(Some(&self.updates), |encoder, update| {
            encoder.write_compact_string(&update.feature)?;
            encoder.write_i16(update.max_version_level);
            encoder.write_bool(update.allow_downgrade);
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// Kafka UpdateFeatures v1 request.
///
/// Version 1 replaces the v0 boolean downgrade flag with Kafka's three-way
/// upgrade type and adds validation-only execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub timeout_ms: i32,
    pub updates: Vec<FeatureUpdateV1>,
    pub validate_only: bool,
}

impl UpdateFeaturesRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i32(self.timeout_ms);
        encoder.write_compact_array(Some(&self.updates), |encoder, update| {
            encoder.write_compact_string(&update.feature)?;
            encoder.write_i16(update.max_version_level);
            encoder.write_i8(update.upgrade_type);
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_bool(self.validate_only);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateV0 {
    pub feature: String,
    pub max_version_level: i16,
    pub allow_downgrade: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateV1 {
    pub feature: String,
    pub max_version_level: i16,
    /// Kafka UpgradeType: 1 upgrade, 2 safe downgrade, 3 unsafe downgrade.
    pub upgrade_type: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub results: Vec<FeatureUpdateResultV0>,
}

impl UpdateFeaturesResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let results = decoder
            .read_compact_array("update features results", |decoder| {
                let result = FeatureUpdateResultV0 {
                    feature: decoder.read_compact_string()?,
                    error_code: decoder.read_i16()?,
                    error_message: decoder.read_compact_nullable_string()?,
                };
                decoder.read_tagged_fields()?;
                Ok(result)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            results,
        })
    }
}

/// UpdateFeatures v1 has the same response body as v0.
pub type UpdateFeaturesResponseV1 = UpdateFeaturesResponseV0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateResultV0 {
    pub feature: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        FeatureUpdateV0, FeatureUpdateV1, UpdateFeaturesRequestV0, UpdateFeaturesRequestV1,
        UpdateFeaturesResponseV0, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_update_features_v0_request() {
        let request = UpdateFeaturesRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            timeout_ms: 60_000,
            updates: vec![FeatureUpdateV0 {
                feature: "metadata.version".to_owned(),
                max_version_level: 21,
                allow_downgrade: false,
            }],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[..2], &API_KEY.to_be_bytes());
        assert_eq!(&bytes[2..4], &[0, 0]);
        assert_eq!(&bytes[4..8], &23_i32.to_be_bytes());
        assert!(bytes.ends_with(&[0]));
    }

    #[test]
    fn encodes_update_features_v1_request_with_validation() {
        let request = UpdateFeaturesRequestV1 {
            correlation_id: 23,
            client_id: None,
            timeout_ms: 60_000,
            updates: vec![FeatureUpdateV1 {
                feature: "metadata.version".to_owned(),
                max_version_level: 21,
                upgrade_type: 2,
            }],
            validate_only: true,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(
            bytes,
            [
                0,
                API_KEY as u8, // api key
                0,
                1, // api version
                0,
                0,
                0,
                23, // correlation id
                0xff,
                0xff, // nullable client id
                0,    // request header tagged fields
                0,
                0,
                234,
                96, // timeout ms
                2,  // compact update count
                17, // compact feature string length
                b'm',
                b'e',
                b't',
                b'a',
                b'd',
                b'a',
                b't',
                b'a',
                b'.',
                b'v',
                b'e',
                b'r',
                b's',
                b'i',
                b'o',
                b'n',
                0,
                21, // max version level
                2,  // safe downgrade
                0,  // update tagged fields
                1,  // validate only
                0,  // request tagged fields
            ]
        );
    }

    #[test]
    fn decodes_update_features_v0_response() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&12_i32.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&[3, b'o', b'k']);
        bytes.push(2);
        bytes.push(17);
        bytes.extend_from_slice(b"metadata.version");
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&[3, b'o', b'k']);
        bytes.push(0);
        bytes.push(0);

        let mut decoder = Decoder::new(&bytes);
        let response = UpdateFeaturesResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message.as_deref(), Some("ok"));
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].feature, "metadata.version");
        assert_eq!(response.results[0].error_message.as_deref(), Some("ok"));
        assert!(decoder.is_empty());
    }
}
