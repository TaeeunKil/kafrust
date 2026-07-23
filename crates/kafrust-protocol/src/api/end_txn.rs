use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 26;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub committed: bool,
}

impl EndTxnRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.transactional_id)?;
        encoder.write_i64(self.producer_id);
        encoder.write_i16(self.producer_epoch);
        encoder.write_bool(self.committed);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

impl EndTxnResponseV0 {
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
    use super::{EndTxnRequestV0, EndTxnResponseV0, API_KEY};
    use crate::codec::Decoder;

    #[test]
    fn encodes_end_txn_v0_commit_request() {
        let request = EndTxnRequestV0 {
            correlation_id: 31,
            client_id: Some("kafrust".to_owned()),
            transactional_id: "orders-tx".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            committed: true,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 26, // api key
                0, 0, // api version
                0, 0, 0, 31, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 9, b'o', b'r', b'd', b'e', b'r', b's', b'-', b't', b'x', 0, 0, 0, 0, 0, 0, 0,
                42, // producer id
                0, 3, // producer epoch
                1, // committed
            ]
        );
        assert_eq!(API_KEY, 26);
    }

    #[test]
    fn encodes_end_txn_v0_abort_request() {
        let request = EndTxnRequestV0 {
            correlation_id: 32,
            client_id: None,
            transactional_id: "orders-tx".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            committed: false,
        };

        assert_eq!(request.encode().unwrap().last(), Some(&0));
    }

    #[test]
    fn decodes_end_txn_v0_response() {
        let bytes = [
            0, 0, 0, 12, // throttle time
            0, 47, // invalid producer epoch
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = EndTxnResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 47);
        assert!(decoder.is_empty());
    }
}
