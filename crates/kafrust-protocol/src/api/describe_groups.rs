use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 15;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_ids: Vec<String>,
}

impl DescribeGroupsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.group_ids), |encoder, group_id| {
            encoder.write_string(group_id)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsResponseV1 {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribeGroupsGroupV1>,
}

impl DescribeGroupsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            groups: decoder
                .read_array("describe groups", DescribeGroupsGroupV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsGroupV1 {
    pub error_code: i16,
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<DescribeGroupsMemberV1>,
}

impl DescribeGroupsGroupV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            group_id: decoder.read_string()?,
            state: decoder.read_string()?,
            protocol_type: decoder.read_string()?,
            protocol_data: decoder.read_string()?,
            members: decoder
                .read_array("describe group members", DescribeGroupsMemberV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeGroupsMemberV1 {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Vec<u8>,
    pub member_assignment: Vec<u8>,
}

impl DescribeGroupsMemberV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            member_id: decoder.read_string()?,
            client_id: decoder.read_string()?,
            client_host: decoder.read_string()?,
            member_metadata: decoder.read_bytes()?,
            member_assignment: decoder.read_bytes()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DescribeGroupsRequestV1, DescribeGroupsResponseV1, API_KEY};
    use crate::codec::Decoder;

    #[test]
    fn encodes_describe_groups_v1_request() {
        let request = DescribeGroupsRequestV1 {
            correlation_id: 14,
            client_id: Some("kafrust".to_owned()),
            group_ids: vec!["orders-group".to_owned()],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 15, // API key
                0, 1, // API version
                0, 0, 0, 14, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 0, 0, 1, // group count
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u', b'p',
            ]
        );
        assert_eq!(API_KEY, 15);
    }

    #[test]
    fn decodes_describe_groups_v1_response() {
        let bytes = [
            0, 0, 0, 4, // throttle time
            0, 0, 0, 1, // group count
            0, 0, // success
            0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u', b'p', 0, 6,
            b'S', b't', b'a', b'b', b'l', b'e', // state
            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // protocol type
            0, 5, b'r', b'a', b'n', b'g', b'e', // protocol
            0, 0, 0, 1, // member count
            0, 8, b'm', b'e', b'm', b'b', b'e', b'r', b'-', b'1', // member ID
            0, 8, b'c', b'l', b'i', b'e', b'n', b't', b'-', b'1', // client ID
            0, 10, b'/', b'1', b'2', b'7', b'.', b'0', b'.', b'0', b'.', b'1', // client host
            0, 0, 0, 2, 1, 2, // member metadata
            0, 0, 0, 3, 3, 4, 5, // member assignment
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = DescribeGroupsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.groups.len(), 1);
        assert_eq!(response.groups[0].group_id, "orders-group");
        assert_eq!(response.groups[0].state, "Stable");
        assert_eq!(response.groups[0].protocol_type, "consumer");
        assert_eq!(response.groups[0].protocol_data, "range");
        assert_eq!(response.groups[0].members.len(), 1);
        assert_eq!(response.groups[0].members[0].client_host, "/127.0.0.1");
        assert_eq!(response.groups[0].members[0].member_metadata, [1, 2]);
        assert_eq!(response.groups[0].members[0].member_assignment, [3, 4, 5]);
        assert!(decoder.is_empty());
    }
}
