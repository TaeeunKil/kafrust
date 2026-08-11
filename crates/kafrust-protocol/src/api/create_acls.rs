use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub creations: Vec<CreateAclsCreationV1>,
}

impl CreateAclsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.creations), |encoder, creation| {
            encoder.write_i8(creation.resource_type);
            encoder.write_string(&creation.resource_name)?;
            encoder.write_i8(creation.resource_pattern_type);
            encoder.write_string(&creation.principal)?;
            encoder.write_string(&creation.host)?;
            encoder.write_i8(creation.operation);
            encoder.write_i8(creation.permission_type);
            Ok(())
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsCreationV1 {
    pub resource_type: i8,
    pub resource_name: String,
    pub resource_pattern_type: i8,
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsResponseV1 {
    pub throttle_time_ms: i32,
    pub results: Vec<CreateAclsResultV1>,
}

impl CreateAclsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            results: decoder
                .read_array("create ACL results", CreateAclsResultV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsResultV1 {
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl CreateAclsResultV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{CreateAclsCreationV1, CreateAclsRequestV1, CreateAclsResponseV1, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_create_acls_v1_request() {
        let request = CreateAclsRequestV1 {
            correlation_id: 9,
            client_id: None,
            creations: vec![CreateAclsCreationV1 {
                resource_type: 2,
                resource_name: "orders".to_owned(),
                resource_pattern_type: 3,
                principal: "User:alice".to_owned(),
                host: "*".to_owned(),
                operation: 3,
                permission_type: 1,
            }],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 1]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 9]);
        assert_eq!(bytes.last(), Some(&1));
    }

    #[test]
    fn decodes_create_acls_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(5);
        bytes.write_i32(2);
        bytes.write_i16(0);
        bytes.write_nullable_string(None).unwrap();
        bytes.write_i16(29);
        bytes.write_nullable_string(Some("denied")).unwrap();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = CreateAclsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 5);
        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].error_code, 0);
        assert_eq!(response.results[1].error_code, 29);
        assert_eq!(response.results[1].error_message.as_deref(), Some("denied"));
        assert!(decoder.is_empty());
    }
}
