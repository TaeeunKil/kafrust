use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

impl HeartbeatRequestV2 {
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
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

impl HeartbeatResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{HeartbeatRequestV2, HeartbeatResponseV2};
    use crate::codec::Decoder;

    #[test]
    fn encodes_heartbeat_v2_request() {
        let request = HeartbeatRequestV2 {
            correlation_id: 17,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            generation_id: 7,
            member_id: "member-a".to_owned(),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 12, // api key
                0, 2, // api version
                0, 0, 0, 17, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0, 0, 0, 7, // generation id
                0, 8, b'm', b'e', b'm', b'b', b'e', b'r', b'-', b'a', // member id
            ]
        );
    }

    #[test]
    fn decodes_heartbeat_v2_response() {
        let bytes = [
            0, 0, 0, 0, // throttle time
            0, 0, // error code
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = HeartbeatResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, 0);
        assert!(decoder.is_empty());
    }
}
