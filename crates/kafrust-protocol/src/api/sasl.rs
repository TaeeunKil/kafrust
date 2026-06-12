use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const SASL_HANDSHAKE_API_KEY: i16 = 17;
pub const SASL_AUTHENTICATE_API_KEY: i16 = 36;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslHandshakeRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub mechanism: String,
}

impl SaslHandshakeRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: SASL_HANDSHAKE_API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.mechanism)?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslHandshakeResponseV1 {
    pub error_code: i16,
    pub mechanisms: Vec<String>,
}

impl SaslHandshakeResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let mechanisms = decoder
            .read_array("SASL mechanisms", |decoder| decoder.read_string())?
            .unwrap_or_default();
        Ok(Self {
            error_code,
            mechanisms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslAuthenticateRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub auth_bytes: Vec<u8>,
}

impl SaslAuthenticateRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: SASL_AUTHENTICATE_API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_bytes(&self.auth_bytes)?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslAuthenticateResponseV0 {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub auth_bytes: Vec<u8>,
}

impl SaslAuthenticateResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            auth_bytes: decoder.read_bytes()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        SaslAuthenticateRequestV0, SaslAuthenticateResponseV0, SaslHandshakeRequestV1,
        SaslHandshakeResponseV1, SASL_AUTHENTICATE_API_KEY, SASL_HANDSHAKE_API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_sasl_handshake_v1_request() {
        let request = SaslHandshakeRequestV1 {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            mechanism: "PLAIN".to_owned(),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 17, // api key
                0, 1, // api version
                0, 0, 0, 7, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 5, b'P', b'L', b'A', b'I', b'N', // mechanism
            ]
        );
        assert_eq!(SASL_HANDSHAKE_API_KEY, 17);
    }

    #[test]
    fn decodes_sasl_handshake_v1_response() {
        let bytes = [
            0, 0, // error code
            0, 0, 0, 2, // mechanism count
            0, 5, b'P', b'L', b'A', b'I', b'N', // mechanism
            0, 13, b'S', b'C', b'R', b'A', b'M', b'-', b'S', b'H', b'A', b'-', b'2', b'5',
            b'6', // mechanism
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = SaslHandshakeResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.mechanisms, ["PLAIN", "SCRAM-SHA-256"]);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_sasl_authenticate_v0_request() {
        let request = SaslAuthenticateRequestV0 {
            correlation_id: 8,
            client_id: Some("kafrust".to_owned()),
            auth_bytes: b"\0user\0pass".to_vec(),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 36, // api key
                0, 0, // api version
                0, 0, 0, 8, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 0, 0, 10, // auth bytes length
                0, b'u', b's', b'e', b'r', 0, b'p', b'a', b's', b's',
            ]
        );
        assert_eq!(SASL_AUTHENTICATE_API_KEY, 36);
    }

    #[test]
    fn decodes_sasl_authenticate_v0_response() {
        let bytes = [
            0, 0, // error code
            0xff, 0xff, // null error message
            0, 0, 0, 2, // auth bytes length
            1, 2, // auth bytes
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = SaslAuthenticateResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message, None);
        assert_eq!(response.auth_bytes, [1, 2]);
        assert!(decoder.is_empty());
    }
}
