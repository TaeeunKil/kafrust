use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 11;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub protocol_type: String,
    pub protocols: Vec<JoinGroupProtocol>,
}

impl JoinGroupRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_i32(self.session_timeout_ms);
        encoder.write_i32(self.rebalance_timeout_ms);
        encoder.write_string(&self.member_id)?;
        encoder.write_string(&self.protocol_type)?;
        encoder.write_array(Some(self.protocols.as_slice()), |encoder, protocol| {
            protocol.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupRequestV5 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<JoinGroupProtocol>,
}

impl JoinGroupRequestV5 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 5,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_i32(self.session_timeout_ms);
        encoder.write_i32(self.rebalance_timeout_ms);
        encoder.write_string(&self.member_id)?;
        encoder.write_nullable_string(self.group_instance_id.as_deref())?;
        encoder.write_string(&self.protocol_type)?;
        encoder.write_array(Some(self.protocols.as_slice()), |encoder, protocol| {
            protocol.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupProtocol {
    pub name: String,
    pub metadata: Vec<u8>,
}

impl JoinGroupProtocol {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_bytes(&self.metadata)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinGroupMember>,
}

impl JoinGroupResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            generation_id: decoder.read_i32()?,
            protocol_name: decoder.read_string()?,
            leader: decoder.read_string()?,
            member_id: decoder.read_string()?,
            members: decoder
                .read_array("join group members", JoinGroupMember::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResponseV5 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinGroupMemberV5>,
}

impl JoinGroupResponseV5 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            generation_id: decoder.read_i32()?,
            protocol_name: decoder.read_string()?,
            leader: decoder.read_string()?,
            member_id: decoder.read_string()?,
            members: decoder
                .read_array("join group members", JoinGroupMemberV5::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupMember {
    pub member_id: String,
    pub metadata: Vec<u8>,
}

impl JoinGroupMember {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            member_id: decoder.read_string()?,
            metadata: decoder.read_bytes()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupMemberV5 {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Vec<u8>,
}

impl JoinGroupMemberV5 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            member_id: decoder.read_string()?,
            group_instance_id: decoder.read_nullable_string()?,
            metadata: decoder.read_bytes()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        JoinGroupMember, JoinGroupMemberV5, JoinGroupProtocol, JoinGroupRequestV2,
        JoinGroupRequestV5, JoinGroupResponseV2, JoinGroupResponseV5,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_join_group_v2_request() {
        let request = JoinGroupRequestV2 {
            correlation_id: 13,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            member_id: String::new(),
            protocol_type: "consumer".to_owned(),
            protocols: vec![JoinGroupProtocol {
                name: "range".to_owned(),
                metadata: vec![1, 2, 3],
            }],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 11, // api key
                0, 2, // api version
                0, 0, 0, 13, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0, 0, 39, 16, // session timeout
                0, 0, 117, 48, // rebalance timeout
                0, 0, // member id
                0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // protocol type
                0, 0, 0, 1, // protocol count
                0, 5, b'r', b'a', b'n', b'g', b'e', // protocol name
                0, 0, 0, 3, 1, 2, 3, // metadata
            ]
        );
    }

    #[test]
    fn decodes_join_group_v2_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i32(7);
        bytes.write_string("range").unwrap();
        bytes.write_string("member-a").unwrap();
        bytes.write_string("member-a").unwrap();
        bytes.write_i32(1);
        bytes.write_string("member-a").unwrap();
        bytes.write_bytes(&[1, 2, 3]).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = JoinGroupResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.generation_id, 7);
        assert_eq!(response.protocol_name, "range");
        assert_eq!(response.leader, "member-a");
        assert_eq!(response.member_id, "member-a");
        assert_eq!(
            response.members,
            vec![JoinGroupMember {
                member_id: "member-a".to_owned(),
                metadata: vec![1, 2, 3],
            }]
        );
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_join_group_v5_request_with_static_member() {
        let request = JoinGroupRequestV5 {
            correlation_id: 13,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            member_id: "member-a".to_owned(),
            group_instance_id: Some("orders-reader-1".to_owned()),
            protocol_type: "consumer".to_owned(),
            protocols: vec![JoinGroupProtocol {
                name: "range".to_owned(),
                metadata: vec![1, 2, 3],
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 11, 0, 5]);
        assert!(encoded
            .windows(17)
            .any(|bytes| bytes == b"\0\x0forders-reader-1"));
    }

    #[test]
    fn decodes_join_group_v5_response_with_static_member() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i32(7);
        bytes.write_string("range").unwrap();
        bytes.write_string("member-a").unwrap();
        bytes.write_string("member-a").unwrap();
        bytes.write_i32(1);
        bytes.write_string("member-a").unwrap();
        bytes
            .write_nullable_string(Some("orders-reader-1"))
            .unwrap();
        bytes.write_bytes(&[1, 2, 3]).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = JoinGroupResponseV5::decode_body(&mut decoder).unwrap();

        assert_eq!(
            response.members,
            vec![JoinGroupMemberV5 {
                member_id: "member-a".to_owned(),
                group_instance_id: Some("orders-reader-1".to_owned()),
                metadata: vec![1, 2, 3],
            }]
        );
        assert!(decoder.is_empty());
    }
}
