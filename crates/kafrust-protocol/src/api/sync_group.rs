use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub assignments: Vec<SyncGroupAssignment>,
}

impl SyncGroupRequestV2 {
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
        encoder.write_i32(self.generation_id);
        encoder.write_string(&self.member_id)?;
        encoder.write_array(Some(self.assignments.as_slice()), |encoder, assignment| {
            assignment.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupRequestV3 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub assignments: Vec<SyncGroupAssignment>,
}

impl SyncGroupRequestV3 {
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
        encoder.write_i32(self.generation_id);
        encoder.write_string(&self.member_id)?;
        encoder.write_nullable_string(self.group_instance_id.as_deref())?;
        encoder.write_array(Some(self.assignments.as_slice()), |encoder, assignment| {
            assignment.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupAssignment {
    pub member_id: String,
    pub assignment: Vec<u8>,
}

impl SyncGroupAssignment {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.member_id)?;
        encoder.write_bytes(&self.assignment)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub assignment: Vec<u8>,
}

impl SyncGroupResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            assignment: decoder.read_bytes()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{SyncGroupAssignment, SyncGroupRequestV2, SyncGroupRequestV3, SyncGroupResponseV2};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_sync_group_v2_request() {
        let request = SyncGroupRequestV2 {
            correlation_id: 19,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            generation_id: 7,
            member_id: "member-a".to_owned(),
            assignments: vec![SyncGroupAssignment {
                member_id: "member-a".to_owned(),
                assignment: vec![1, 2, 3],
            }],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 14, // api key
                0, 2, // api version
                0, 0, 0, 19, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0, 0, 0, 7, // generation id
                0, 8, b'm', b'e', b'm', b'b', b'e', b'r', b'-', b'a', // member id
                0, 0, 0, 1, // assignment count
                0, 8, b'm', b'e', b'm', b'b', b'e', b'r', b'-', b'a', // assignment member
                0, 0, 0, 3, 1, 2, 3, // assignment bytes
            ]
        );
    }

    #[test]
    fn decodes_sync_group_v2_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_bytes(&[1, 2, 3]).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = SyncGroupResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.assignment, vec![1, 2, 3]);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_sync_group_v3_request_with_static_member() {
        let request = SyncGroupRequestV3 {
            correlation_id: 19,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            generation_id: 7,
            member_id: "member-a".to_owned(),
            group_instance_id: Some("orders-reader-1".to_owned()),
            assignments: Vec::new(),
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 14, 0, 3]);
        assert!(encoded
            .windows(17)
            .any(|bytes| bytes == b"\0\x0forders-reader-1"));
    }
}
