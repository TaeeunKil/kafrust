use crate::codec::{Decoder, Encoder};
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn encode_v1(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i16(self.api_key);
        encoder.write_i16(self.api_version);
        encoder.write_i32(self.correlation_id);
        encoder.write_nullable_string(self.client_id.as_deref())
    }

    pub fn encode_v2(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i16(self.api_key);
        encoder.write_i16(self.api_version);
        encoder.write_i32(self.correlation_id);
        encoder.write_compact_nullable_string(self.client_id.as_deref())?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn decode_v0(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            correlation_id: decoder.read_i32()?,
        })
    }

    pub fn decode_v1(decoder: &mut Decoder<'_>) -> Result<Self> {
        let header = Self::decode_v0(decoder)?;
        let _tagged_fields = decoder.read_tagged_fields()?;
        Ok(header)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{RequestHeader, ResponseHeader};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_request_header_v1() {
        let header = RequestHeader {
            api_key: 18,
            api_version: 0,
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
        };
        let mut encoder = Encoder::new();
        header.encode_v1(&mut encoder).unwrap();
        assert_eq!(
            encoder.into_bytes(),
            [0, 18, 0, 0, 0, 0, 0, 7, 0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't']
        );
    }

    #[test]
    fn encodes_request_header_v2_with_compact_client_id() {
        let header = RequestHeader {
            api_key: 3,
            api_version: 12,
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
        };
        let mut encoder = Encoder::new();
        header.encode_v2(&mut encoder).unwrap();
        assert_eq!(
            encoder.into_bytes(),
            [0, 3, 0, 12, 0, 0, 0, 7, 8, b'k', b'a', b'f', b'r', b'u', b's', b't', 0]
        );
    }

    #[test]
    fn decodes_response_header_v0() {
        let mut decoder = Decoder::new(&[0, 0, 0, 7]);
        let header = ResponseHeader::decode_v0(&mut decoder).unwrap();
        assert_eq!(header.correlation_id, 7);
    }
}
