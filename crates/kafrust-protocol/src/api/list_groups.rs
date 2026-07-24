use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl ListGroupsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<ListedGroupV1>,
}

impl ListGroupsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            groups: decoder
                .read_array("listed groups", ListedGroupV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroupV1 {
    pub group_id: String,
    pub protocol_type: String,
}

impl ListedGroupV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            group_id: decoder.read_string()?,
            protocol_type: decoder.read_string()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{ListGroupsRequestV1, ListGroupsResponseV1, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_list_groups_v1_request() {
        let request = ListGroupsRequestV1 {
            correlation_id: 17,
            client_id: Some("kafrust".to_owned()),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 16, // API key
                0, 1, // API version
                0, 0, 0, 17, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
            ]
        );
        assert_eq!(API_KEY, 16);
    }

    #[test]
    fn decodes_list_groups_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i16(0);
        bytes.write_i32(2);
        bytes.write_string("orders").unwrap();
        bytes.write_string("consumer").unwrap();
        bytes.write_string("connect-cluster").unwrap();
        bytes.write_string("connect").unwrap();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = ListGroupsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.groups.len(), 2);
        assert_eq!(response.groups[0].group_id, "orders");
        assert_eq!(response.groups[0].protocol_type, "consumer");
        assert_eq!(response.groups[1].protocol_type, "connect");
        assert!(decoder.is_empty());
    }
}
