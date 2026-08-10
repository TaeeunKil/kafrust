use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 29;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub resource_type_filter: i8,
    pub resource_name_filter: Option<String>,
    pub pattern_type_filter: i8,
    pub principal_filter: Option<String>,
    pub host_filter: Option<String>,
    pub operation: i8,
    pub permission_type: i8,
}

impl DescribeAclsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_i8(self.resource_type_filter);
        encoder.write_nullable_string(self.resource_name_filter.as_deref())?;
        encoder.write_i8(self.pattern_type_filter);
        encoder.write_nullable_string(self.principal_filter.as_deref())?;
        encoder.write_nullable_string(self.host_filter.as_deref())?;
        encoder.write_i8(self.operation);
        encoder.write_i8(self.permission_type);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resources: Vec<DescribeAclsResourceV1>,
}

impl DescribeAclsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            resources: decoder
                .read_array("describe ACL resources", DescribeAclsResourceV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsResourceV1 {
    pub resource_type: i8,
    pub resource_name: String,
    pub pattern_type: i8,
    pub acls: Vec<DescribeAclsEntryV1>,
}

impl DescribeAclsResourceV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            resource_type: decoder.read_i8()?,
            resource_name: decoder.read_string()?,
            pattern_type: decoder.read_i8()?,
            acls: decoder
                .read_array("describe ACL entries", DescribeAclsEntryV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsEntryV1 {
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission_type: i8,
}

impl DescribeAclsEntryV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            principal: decoder.read_string()?,
            host: decoder.read_string()?,
            operation: decoder.read_i8()?,
            permission_type: decoder.read_i8()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DescribeAclsRequestV1, DescribeAclsResponseV1, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_acls_v1_request() {
        let request = DescribeAclsRequestV1 {
            correlation_id: 17,
            client_id: Some("kafrust".to_owned()),
            resource_type_filter: 2,
            resource_name_filter: Some("orders".to_owned()),
            pattern_type_filter: 3,
            principal_filter: Some("User:alice".to_owned()),
            host_filter: Some("*".to_owned()),
            operation: 3,
            permission_type: 1,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 1]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 17]);
        assert_eq!(bytes.last(), Some(&1));
    }

    #[test]
    fn decodes_describe_acls_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(7);
        bytes.write_i16(0);
        bytes.write_nullable_string(None).unwrap();
        bytes.write_i32(1);
        bytes.write_i8(2);
        bytes.write_string("orders").unwrap();
        bytes.write_i8(3);
        bytes.write_i32(1);
        bytes.write_string("User:alice").unwrap();
        bytes.write_string("*").unwrap();
        bytes.write_i8(3);
        bytes.write_i8(1);
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = DescribeAclsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(response.resources[0].resource_name, "orders");
        assert_eq!(response.resources[0].pattern_type, 3);
        assert_eq!(response.resources[0].acls[0].principal, "User:alice");
        assert_eq!(response.resources[0].acls[0].operation, 3);
        assert!(decoder.is_empty());
    }
}
