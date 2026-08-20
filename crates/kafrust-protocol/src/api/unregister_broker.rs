use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka UnregisterBroker API key.
pub const API_KEY: i16 = 64;

/// UnregisterBroker v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterBrokerRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub broker_id: i32,
}

impl UnregisterBrokerRequestV0 {
    /// Encodes the flexible v0 request header and body.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i32(self.broker_id);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// UnregisterBroker v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterBrokerResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl UnregisterBrokerResponseV0 {
    /// Decodes a flexible UnregisterBroker response body.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let response = Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_compact_nullable_string()?,
        };
        decoder.read_tagged_fields()?;
        Ok(response)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{UnregisterBrokerRequestV0, UnregisterBrokerResponseV0, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_unregister_broker_v0_wire_shape() {
        let request = UnregisterBrokerRequestV0 {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            broker_id: 4,
        };
        let bytes = request.encode().unwrap();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.read_i16().unwrap(), API_KEY);
        assert_eq!(decoder.read_i16().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 7);
        assert_eq!(
            decoder.read_nullable_string().unwrap().as_deref(),
            Some("kafrust")
        );
        decoder.read_tagged_fields().unwrap();
        assert_eq!(decoder.read_i32().unwrap(), 4);
        decoder.read_tagged_fields().unwrap();
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_unregister_broker_v0_response() {
        let mut encoder = Encoder::new();
        encoder.write_i32(12);
        encoder.write_i16(0);
        encoder.write_compact_nullable_string(Some("ok")).unwrap();
        encoder.write_empty_tagged_fields();
        let bytes = encoder.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = UnregisterBrokerResponseV0::decode_body(&mut decoder).unwrap();
        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message.as_deref(), Some("ok"));
        assert!(decoder.is_empty());
    }
}
