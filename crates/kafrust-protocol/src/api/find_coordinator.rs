use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorType {
    Group,
    Transaction,
}

impl CoordinatorType {
    fn as_i8(self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub coordinator_key: String,
    pub coordinator_type: CoordinatorType,
}

impl FindCoordinatorRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.coordinator_key)?;
        encoder.write_i8(self.coordinator_type.as_i8());
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl FindCoordinatorResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            node_id: decoder.read_i32()?,
            host: decoder.read_string()?,
            port: decoder.read_i32()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{CoordinatorType, FindCoordinatorRequestV1, FindCoordinatorResponseV1, API_KEY};
    use crate::codec::Decoder;

    #[test]
    fn encodes_find_coordinator_v1_for_group() {
        let request = FindCoordinatorRequestV1 {
            correlation_id: 11,
            client_id: Some("kafrust".to_owned()),
            coordinator_key: "orders-group".to_owned(),
            coordinator_type: CoordinatorType::Group,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 10, // api key
                0, 1, // api version
                0, 0, 0, 11, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // coordinator key
                0,    // group coordinator type
            ]
        );
        assert_eq!(API_KEY, 10);
    }

    #[test]
    fn decodes_find_coordinator_v1_response() {
        let bytes = [
            0, 0, 0, 0, // throttle time
            0, 0, // error code
            0xff, 0xff, // null error message
            0, 0, 0, 2, // node id
            0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // host
            0, 0, 35, 132, // port 9092
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = FindCoordinatorResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message, None);
        assert_eq!(response.node_id, 2);
        assert_eq!(response.host, "localhost");
        assert_eq!(response.port, 9092);
        assert!(decoder.is_empty());
    }
}
