use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 25;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub group_id: String,
}

impl AddOffsetsToTxnRequestV0 {
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
        encoder.write_string(&self.group_id)?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

impl AddOffsetsToTxnResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
        })
    }
}

/// AddOffsetsToTxn v3, the flexible form used by current Kafka brokers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnRequestV3 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub group_id: String,
}

impl AddOffsetsToTxnRequestV3 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 3,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.transactional_id)?;
        encoder.write_i64(self.producer_id);
        encoder.write_i16(self.producer_epoch);
        encoder.write_compact_string(&self.group_id)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// AddOffsetsToTxn v3 response with flexible tagged fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnResponseV3 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

impl AddOffsetsToTxnResponseV3 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let response = Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
        };
        decoder.read_tagged_fields()?;
        Ok(response)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AddOffsetsToTxnRequestV0, AddOffsetsToTxnRequestV3, AddOffsetsToTxnResponseV0,
        AddOffsetsToTxnResponseV3, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_add_offsets_to_txn_v0_request() {
        let request = AddOffsetsToTxnRequestV0 {
            correlation_id: 51,
            client_id: Some("kafrust".to_owned()),
            transactional_id: "orders-tx".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            group_id: "orders-group".to_owned(),
        };
        let encoded = request.encode().unwrap();

        assert_eq!(&encoded[0..8], &[0, 25, 0, 0, 0, 0, 0, 51]);
        assert!(encoded.ends_with(b"orders-group"));
        assert_eq!(API_KEY, 25);
    }

    #[test]
    fn decodes_add_offsets_to_txn_v0_response() {
        let bytes = [
            0, 0, 0, 7, // throttle time
            0, 16, // not coordinator
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = AddOffsetsToTxnResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(response.error_code, 16);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_add_offsets_to_txn_v3_request_with_flexible_fields() {
        let request = AddOffsetsToTxnRequestV3 {
            correlation_id: 52,
            client_id: Some("kafrust".to_owned()),
            transactional_id: "orders-tx".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            group_id: "orders-group".to_owned(),
        };
        let encoded = request.encode().unwrap();

        assert_eq!(&encoded[0..8], &[0, 25, 0, 3, 0, 0, 0, 52]);
        assert!(encoded
            .windows(b"orders-tx".len())
            .any(|window| window == b"orders-tx"));
        assert_eq!(encoded.last(), Some(&0));
    }

    #[test]
    fn decodes_add_offsets_to_txn_v3_response_with_tagged_fields() {
        let mut bytes = Encoder::new();
        bytes.write_i32(9);
        bytes.write_i16(47);
        bytes.write_empty_tagged_fields();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);

        let response = AddOffsetsToTxnResponseV3::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 9);
        assert_eq!(response.error_code, 47);
        assert!(decoder.is_empty());
    }
}
