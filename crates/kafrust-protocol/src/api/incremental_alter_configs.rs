use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 44;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub resources: Vec<IncrementalAlterConfigsResourceV0>,
    pub validate_only: bool,
}

impl IncrementalAlterConfigsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.resources), |encoder, resource| {
            resource.encode(encoder)
        })?;
        encoder.write_bool(self.validate_only);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResourceV0 {
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<IncrementalAlterConfigsEntryV0>,
}

impl IncrementalAlterConfigsResourceV0 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i8(self.resource_type);
        encoder.write_string(&self.resource_name)?;
        encoder.write_array(Some(&self.configs), |encoder, config| {
            config.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsEntryV0 {
    pub name: String,
    pub operation: i8,
    pub value: Option<String>,
}

impl IncrementalAlterConfigsEntryV0 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_i8(self.operation);
        encoder.write_nullable_string(self.value.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResponseV0 {
    pub throttle_time_ms: i32,
    pub responses: Vec<IncrementalAlterConfigsResourceResponseV0>,
}

impl IncrementalAlterConfigsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            responses: decoder
                .read_array(
                    "incremental alter configs responses",
                    IncrementalAlterConfigsResourceResponseV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAlterConfigsResourceResponseV0 {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

impl IncrementalAlterConfigsResourceResponseV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            resource_type: decoder.read_i8()?,
            resource_name: decoder.read_string()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        IncrementalAlterConfigsEntryV0, IncrementalAlterConfigsRequestV0,
        IncrementalAlterConfigsResourceV0, IncrementalAlterConfigsResponseV0, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_incremental_alter_configs_v0_request() {
        let request = IncrementalAlterConfigsRequestV0 {
            correlation_id: 13,
            client_id: Some("kafrust".to_owned()),
            resources: vec![IncrementalAlterConfigsResourceV0 {
                resource_type: 2,
                resource_name: "orders".to_owned(),
                configs: vec![
                    IncrementalAlterConfigsEntryV0 {
                        name: "retention.ms".to_owned(),
                        operation: 0,
                        value: Some("60000".to_owned()),
                    },
                    IncrementalAlterConfigsEntryV0 {
                        name: "cleanup.policy".to_owned(),
                        operation: 1,
                        value: None,
                    },
                ],
            }],
            validate_only: true,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 44, // API key
                0, 0, // API version
                0, 0, 0, 13, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 0, 0, 1, // resource count
                2, // topic resource
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
                0, 0, 0, 2, // config count
                0, 12, b'r', b'e', b't', b'e', b'n', b't', b'i', b'o', b'n', b'.', b'm', b's',
                0, // SET
                0, 5, b'6', b'0', b'0', b'0', b'0', // value
                0, 14, b'c', b'l', b'e', b'a', b'n', b'u', b'p', b'.', b'p', b'o', b'l', b'i',
                b'c', b'y', // config name
                1,    // DELETE
                0xff, 0xff, // null value
                1,    // validate only
            ]
        );
        assert_eq!(API_KEY, 44);
    }

    #[test]
    fn decodes_incremental_alter_configs_v0_response() {
        let bytes = [
            0, 0, 0, 6, // throttle time
            0, 0, 0, 2, // response count
            0, 0, // success
            0xff, 0xff, // null error message
            2,    // topic resource
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
            0, 40, // invalid config
            0, 7, b'i', b'n', b'v', b'a', b'l', b'i', b'd', // error message
            2,    // topic resource
            0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // resource name
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = IncrementalAlterConfigsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 6);
        assert_eq!(response.responses.len(), 2);
        assert_eq!(response.responses[0].resource_name, "orders");
        assert_eq!(response.responses[0].error_code, 0);
        assert_eq!(response.responses[1].resource_name, "payments");
        assert_eq!(response.responses[1].error_code, 40);
        assert_eq!(
            response.responses[1].error_message.as_deref(),
            Some("invalid")
        );
        assert!(decoder.is_empty());
    }
}
