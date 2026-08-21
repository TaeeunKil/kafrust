use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 22;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
}

impl InitProducerIdRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_nullable_string(self.transactional_id.as_deref())?;
        encoder.write_i32(self.transaction_timeout_ms);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            producer_id: decoder.read_i64()?,
            producer_epoch: decoder.read_i16()?,
        })
    }
}

/// InitProducerId v2, the flexible form used by current Kafka brokers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
}

impl InitProducerIdRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_nullable_string(self.transactional_id.as_deref())?;
        encoder.write_i32(self.transaction_timeout_ms);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// InitProducerId v2 response with flexible tagged fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let response = Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            producer_id: decoder.read_i64()?,
            producer_epoch: decoder.read_i16()?,
        };
        decoder.read_tagged_fields()?;
        Ok(response)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        InitProducerIdRequestV0, InitProducerIdRequestV2, InitProducerIdResponseV0,
        InitProducerIdResponseV2, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_non_transactional_init_producer_id_v0_request() {
        let request = InitProducerIdRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            transactional_id: None,
            transaction_timeout_ms: 60_000,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 22, // api key
                0, 0, // api version
                0, 0, 0, 23, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0xff, 0xff, // null transactional id
                0, 0, 0xea, 0x60, // transaction timeout
            ]
        );
        assert_eq!(API_KEY, 22);
    }

    #[test]
    fn encodes_transactional_init_producer_id_v0_request() {
        let request = InitProducerIdRequestV0 {
            correlation_id: 24,
            client_id: None,
            transactional_id: Some("orders-tx".to_owned()),
            transaction_timeout_ms: 30_000,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 22, // api key
                0, 0, // api version
                0, 0, 0, 24, // correlation id
                0xff, 0xff, // null client id
                0, 9, b'o', b'r', b'd', b'e', b'r', b's', b'-', b't', b'x', 0, 0, 0x75,
                0x30, // transaction timeout
            ]
        );
    }

    #[test]
    fn decodes_init_producer_id_v0_response() {
        let bytes = [
            0, 0, 0, 12, // throttle time
            0, 0, // error code
            0, 0, 0, 0, 0, 0, 0, 42, // producer id
            0, 3, // producer epoch
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = InitProducerIdResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.producer_id, 42);
        assert_eq!(response.producer_epoch, 3);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_transactional_init_producer_id_v2_request_with_flexible_fields() {
        let request = InitProducerIdRequestV2 {
            correlation_id: 25,
            client_id: Some("kafrust".to_owned()),
            transactional_id: Some("orders-tx".to_owned()),
            transaction_timeout_ms: 30_000,
        };
        let encoded = request.encode().unwrap();

        assert_eq!(&encoded[0..8], &[0, 22, 0, 2, 0, 0, 0, 25]);
        assert!(encoded
            .windows(b"orders-tx".len())
            .any(|window| window == b"orders-tx"));
        assert_eq!(encoded.last(), Some(&0));
    }

    #[test]
    fn decodes_init_producer_id_v2_response_with_tagged_fields() {
        let bytes = [
            0, 0, 0, 12, // throttle time
            0, 0, // error code
            0, 0, 0, 0, 0, 0, 0, 42, // producer id
            0, 3, // producer epoch
            0, // response tagged fields
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = InitProducerIdResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.producer_id, 42);
        assert_eq!(response.producer_epoch, 3);
        assert!(decoder.is_empty());
    }
}
