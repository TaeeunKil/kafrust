use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub users: Option<Vec<String>>,
}

impl DescribeUserScramCredentialsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(self.users.as_deref(), |encoder, user| {
            encoder.write_string(user)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub results: Vec<DescribeUserScramCredentialsResultV0>,
}

impl DescribeUserScramCredentialsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            results: decoder
                .read_array(
                    "describe user SCRAM credential results",
                    DescribeUserScramCredentialsResultV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResultV0 {
    pub user: String,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub credential_infos: Vec<ScramCredentialInfoV0>,
}

impl DescribeUserScramCredentialsResultV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            user: decoder.read_string()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            credential_infos: decoder
                .read_array(
                    "described SCRAM credential infos",
                    ScramCredentialInfoV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialInfoV0 {
    pub mechanism: i8,
    pub iterations: i32,
}

impl ScramCredentialInfoV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            mechanism: decoder.read_i8()?,
            iterations: decoder.read_i32()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        DescribeUserScramCredentialsRequestV0, DescribeUserScramCredentialsResponseV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_all_user_scram_credentials_v0_request() {
        let request = DescribeUserScramCredentialsRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            users: None,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 23]);
        assert_eq!(&bytes[bytes.len() - 4..], &[0xff, 0xff, 0xff, 0xff]);
    }

    #[test]
    fn decodes_describe_user_scram_credentials_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(9);
        bytes.write_i16(0);
        bytes.write_nullable_string(None).unwrap();
        bytes.write_i32(1);
        bytes.write_string("alice").unwrap();
        bytes.write_i16(0);
        bytes.write_nullable_string(None).unwrap();
        bytes.write_i32(2);
        bytes.write_i8(1);
        bytes.write_i32(4096);
        bytes.write_i8(2);
        bytes.write_i32(8192);
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = DescribeUserScramCredentialsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 9);
        assert_eq!(response.results[0].user, "alice");
        assert_eq!(response.results[0].credential_infos[0].mechanism, 1);
        assert_eq!(response.results[0].credential_infos[1].iterations, 8192);
        assert!(decoder.is_empty());
    }
}
