use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 13;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupRequestV3 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub members: Vec<LeaveGroupMemberIdentity>,
}

impl LeaveGroupRequestV3 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 3,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_array(Some(self.members.as_slice()), |encoder, member| {
            member.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupMemberIdentity {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

impl LeaveGroupMemberIdentity {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.member_id)?;
        encoder.write_nullable_string(self.group_instance_id.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupResponseV3 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub members: Vec<LeaveGroupMemberResponse>,
}

impl LeaveGroupResponseV3 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            members: decoder
                .read_array("leave group members", LeaveGroupMemberResponse::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupMemberResponse {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub error_code: i16,
}

impl LeaveGroupMemberResponse {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            member_id: decoder.read_string()?,
            group_instance_id: decoder.read_nullable_string()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        LeaveGroupMemberIdentity, LeaveGroupMemberResponse, LeaveGroupRequestV3,
        LeaveGroupResponseV3,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_leave_group_v3_request() {
        let request = LeaveGroupRequestV3 {
            correlation_id: 31,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            members: vec![LeaveGroupMemberIdentity {
                member_id: "member-a".to_owned(),
                group_instance_id: Some("orders-reader-1".to_owned()),
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 13, 0, 3]);
        assert!(encoded
            .windows(17)
            .any(|bytes| bytes == b"\0\x0forders-reader-1"));
    }

    #[test]
    fn decodes_leave_group_v3_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(5);
        bytes.write_i16(0);
        bytes.write_i32(1);
        bytes.write_string("member-a").unwrap();
        bytes
            .write_nullable_string(Some("orders-reader-1"))
            .unwrap();
        bytes.write_i16(82);
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = LeaveGroupResponseV3::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 5);
        assert_eq!(
            response.members,
            vec![LeaveGroupMemberResponse {
                member_id: "member-a".to_owned(),
                group_instance_id: Some("orders-reader-1".to_owned()),
                error_code: 82,
            }]
        );
        assert!(decoder.is_empty());
    }
}
