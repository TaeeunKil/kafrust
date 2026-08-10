use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 51;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub deletions: Vec<AlterUserScramCredentialsDeletionV0>,
    pub upsertions: Vec<AlterUserScramCredentialsUpsertionV0>,
}

impl AlterUserScramCredentialsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.deletions), |encoder, deletion| {
            encoder.write_string(&deletion.name)?;
            encoder.write_i8(deletion.mechanism);
            Ok(())
        })?;
        encoder.write_array(Some(&self.upsertions), |encoder, upsertion| {
            encoder.write_string(&upsertion.name)?;
            encoder.write_i8(upsertion.mechanism);
            encoder.write_i32(upsertion.iterations);
            encoder.write_bytes(&upsertion.salt)?;
            encoder.write_bytes(&upsertion.salted_password)?;
            Ok(())
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsDeletionV0 {
    pub name: String,
    pub mechanism: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsUpsertionV0 {
    pub name: String,
    pub mechanism: i8,
    pub iterations: i32,
    pub salt: Vec<u8>,
    pub salted_password: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsResponseV0 {
    pub throttle_time_ms: i32,
    pub results: Vec<AlterUserScramCredentialsResultV0>,
}

impl AlterUserScramCredentialsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            results: decoder
                .read_array(
                    "alter user SCRAM credential results",
                    AlterUserScramCredentialsResultV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsResultV0 {
    pub user: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl AlterUserScramCredentialsResultV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            user: decoder.read_string()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AlterUserScramCredentialsDeletionV0, AlterUserScramCredentialsRequestV0,
        AlterUserScramCredentialsResponseV0, AlterUserScramCredentialsUpsertionV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_alter_user_scram_credentials_v0_request() {
        let request = AlterUserScramCredentialsRequestV0 {
            correlation_id: 29,
            client_id: None,
            deletions: vec![AlterUserScramCredentialsDeletionV0 {
                name: "alice".to_owned(),
                mechanism: 1,
            }],
            upsertions: vec![AlterUserScramCredentialsUpsertionV0 {
                name: "bob".to_owned(),
                mechanism: 2,
                iterations: 4096,
                salt: vec![1, 2, 3],
                salted_password: vec![4, 5, 6],
            }],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 29]);
        assert_eq!(bytes[bytes.len() - 6..], [0, 0, 3, 4, 5, 6]);
    }

    #[test]
    fn decodes_alter_user_scram_credentials_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i32(1);
        bytes.write_string("alice").unwrap();
        bytes.write_i16(31);
        bytes.write_nullable_string(Some("denied")).unwrap();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = AlterUserScramCredentialsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.results[0].user, "alice");
        assert_eq!(response.results[0].error_code, 31);
        assert_eq!(response.results[0].error_message.as_deref(), Some("denied"));
        assert!(decoder.is_empty());
    }
}
