use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const CREATE_API_KEY: i16 = 38;
pub const RENEW_API_KEY: i16 = 39;
pub const EXPIRE_API_KEY: i16 = 40;
pub const DESCRIBE_API_KEY: i16 = 41;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationTokenPrincipal {
    pub principal_type: String,
    pub principal_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub owner: Option<DelegationTokenPrincipal>,
    pub renewers: Vec<DelegationTokenPrincipal>,
    pub max_lifetime_ms: i64,
}

impl CreateDelegationTokenRequest {
    pub fn encode_v1(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: CREATE_API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.renewers), encode_legacy_principal)?;
        encoder.write_i64(self.max_lifetime_ms);
        Ok(encoder.into_bytes())
    }

    pub fn encode_v2(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: CREATE_API_KEY,
            api_version,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        if api_version >= 3 {
            let owner = self.owner.as_ref();
            encoder
                .write_compact_nullable_string(owner.map(|owner| owner.principal_type.as_str()))?;
            encoder
                .write_compact_nullable_string(owner.map(|owner| owner.principal_name.as_str()))?;
        }
        encoder.write_compact_array(Some(&self.renewers), encode_flexible_principal)?;
        encoder.write_i64(self.max_lifetime_ms);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenResponse {
    pub error_code: i16,
    pub owner: DelegationTokenPrincipal,
    pub requester: Option<DelegationTokenPrincipal>,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub token_id: String,
    pub hmac: Vec<u8>,
    pub throttle_time_ms: i32,
}

impl CreateDelegationTokenResponse {
    pub fn decode_body_v1(decoder: &mut Decoder<'_>) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let owner = decode_legacy_principal(decoder)?;
        let issue_timestamp_ms = decoder.read_i64()?;
        let expiry_timestamp_ms = decoder.read_i64()?;
        let max_timestamp_ms = decoder.read_i64()?;
        let token_id = decoder.read_string()?;
        let hmac = decoder.read_bytes()?;
        let throttle_time_ms = decoder.read_i32()?;
        Ok(Self {
            error_code,
            owner,
            requester: None,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }

    pub fn decode_body_v2(decoder: &mut Decoder<'_>, api_version: i16) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let owner = decode_flexible_principal(decoder)?;
        let requester = if api_version >= 3 {
            Some(decode_flexible_principal(decoder)?)
        } else {
            None
        };
        let issue_timestamp_ms = decoder.read_i64()?;
        let expiry_timestamp_ms = decoder.read_i64()?;
        let max_timestamp_ms = decoder.read_i64()?;
        let token_id = decoder.read_compact_string()?;
        let hmac = decoder.read_compact_bytes()?;
        let throttle_time_ms = decoder.read_i32()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            error_code,
            owner,
            requester,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewDelegationTokenRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub hmac: Vec<u8>,
    pub renew_period_ms: i64,
}

impl RenewDelegationTokenRequest {
    pub fn encode_v1(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: RENEW_API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_bytes(&self.hmac)?;
        encoder.write_i64(self.renew_period_ms);
        Ok(encoder.into_bytes())
    }

    pub fn encode_v2(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: RENEW_API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_bytes(&self.hmac)?;
        encoder.write_i64(self.renew_period_ms);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationTokenOperationResponse {
    pub error_code: i16,
    pub expiry_timestamp_ms: i64,
    pub throttle_time_ms: i32,
}

pub type RenewDelegationTokenResponse = DelegationTokenOperationResponse;
pub type ExpireDelegationTokenResponse = DelegationTokenOperationResponse;

impl RenewDelegationTokenResponse {
    pub fn decode_body_v1(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_operation_response(decoder, false)
    }

    pub fn decode_body_v2(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_operation_response(decoder, true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpireDelegationTokenRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub hmac: Vec<u8>,
    pub expiry_time_period_ms: i64,
}

impl ExpireDelegationTokenRequest {
    pub fn encode_v1(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: EXPIRE_API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_bytes(&self.hmac)?;
        encoder.write_i64(self.expiry_time_period_ms);
        Ok(encoder.into_bytes())
    }

    pub fn encode_v2(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: EXPIRE_API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_bytes(&self.hmac)?;
        encoder.write_i64(self.expiry_time_period_ms);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDelegationTokenRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub owners: Option<Vec<DelegationTokenPrincipal>>,
}

impl DescribeDelegationTokenRequest {
    pub fn encode_v1(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: DESCRIBE_API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(self.owners.as_deref(), encode_legacy_principal)?;
        Ok(encoder.into_bytes())
    }

    pub fn encode_v2(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: DESCRIBE_API_KEY,
            api_version,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(self.owners.as_deref(), encode_flexible_principal)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDelegationTokenResponse {
    pub error_code: i16,
    pub tokens: Vec<DescribedDelegationToken>,
    pub throttle_time_ms: i32,
}

impl DescribeDelegationTokenResponse {
    pub fn decode_body_v1(decoder: &mut Decoder<'_>) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let tokens = decoder
            .read_array("described delegation tokens", decode_legacy_token)?
            .unwrap_or_default();
        let throttle_time_ms = decoder.read_i32()?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }

    pub fn decode_body_v2(decoder: &mut Decoder<'_>, api_version: i16) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let tokens = decoder
            .read_compact_array("described delegation tokens", |decoder| {
                decode_flexible_token(decoder, api_version)
            })?
            .unwrap_or_default();
        let throttle_time_ms = decoder.read_i32()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedDelegationToken {
    pub owner: DelegationTokenPrincipal,
    pub requester: Option<DelegationTokenPrincipal>,
    pub issue_timestamp_ms: i64,
    pub expiry_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub token_id: String,
    pub hmac: Vec<u8>,
    pub renewers: Vec<DelegationTokenPrincipal>,
}

fn decode_operation_response(
    decoder: &mut Decoder<'_>,
    flexible: bool,
) -> Result<DelegationTokenOperationResponse> {
    let response = DelegationTokenOperationResponse {
        error_code: decoder.read_i16()?,
        expiry_timestamp_ms: decoder.read_i64()?,
        throttle_time_ms: decoder.read_i32()?,
    };
    if flexible {
        decoder.read_tagged_fields()?;
    }
    Ok(response)
}

fn encode_legacy_principal(
    encoder: &mut Encoder,
    principal: &DelegationTokenPrincipal,
) -> Result<()> {
    encoder.write_string(&principal.principal_type)?;
    encoder.write_string(&principal.principal_name)
}

fn encode_flexible_principal(
    encoder: &mut Encoder,
    principal: &DelegationTokenPrincipal,
) -> Result<()> {
    encoder.write_compact_string(&principal.principal_type)?;
    encoder.write_compact_string(&principal.principal_name)?;
    encoder.write_empty_tagged_fields();
    Ok(())
}

fn decode_legacy_principal(decoder: &mut Decoder<'_>) -> Result<DelegationTokenPrincipal> {
    Ok(DelegationTokenPrincipal {
        principal_type: decoder.read_string()?,
        principal_name: decoder.read_string()?,
    })
}

fn decode_flexible_principal(decoder: &mut Decoder<'_>) -> Result<DelegationTokenPrincipal> {
    Ok(DelegationTokenPrincipal {
        principal_type: decoder.read_compact_string()?,
        principal_name: decoder.read_compact_string()?,
    })
}

fn decode_flexible_principal_struct(decoder: &mut Decoder<'_>) -> Result<DelegationTokenPrincipal> {
    let principal = decode_flexible_principal(decoder)?;
    decoder.read_tagged_fields()?;
    Ok(principal)
}

fn decode_legacy_token(decoder: &mut Decoder<'_>) -> Result<DescribedDelegationToken> {
    let owner = decode_legacy_principal(decoder)?;
    let issue_timestamp_ms = decoder.read_i64()?;
    let expiry_timestamp_ms = decoder.read_i64()?;
    let max_timestamp_ms = decoder.read_i64()?;
    let token_id = decoder.read_string()?;
    let hmac = decoder.read_bytes()?;
    let renewers = decoder
        .read_array("delegation token renewers", decode_legacy_principal)?
        .unwrap_or_default();
    Ok(DescribedDelegationToken {
        owner,
        requester: None,
        issue_timestamp_ms,
        expiry_timestamp_ms,
        max_timestamp_ms,
        token_id,
        hmac,
        renewers,
    })
}

fn decode_flexible_token(
    decoder: &mut Decoder<'_>,
    api_version: i16,
) -> Result<DescribedDelegationToken> {
    let owner = decode_flexible_principal(decoder)?;
    let requester = if api_version >= 3 {
        Some(decode_flexible_principal(decoder)?)
    } else {
        None
    };
    let issue_timestamp_ms = decoder.read_i64()?;
    let expiry_timestamp_ms = decoder.read_i64()?;
    let max_timestamp_ms = decoder.read_i64()?;
    let token_id = decoder.read_compact_string()?;
    let hmac = decoder.read_compact_bytes()?;
    let renewers = decoder
        .read_compact_array(
            "delegation token renewers",
            decode_flexible_principal_struct,
        )?
        .unwrap_or_default();
    decoder.read_tagged_fields()?;
    Ok(DescribedDelegationToken {
        owner,
        requester,
        issue_timestamp_ms,
        expiry_timestamp_ms,
        max_timestamp_ms,
        token_id,
        hmac,
        renewers,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        CreateDelegationTokenRequest, CreateDelegationTokenResponse, DelegationTokenPrincipal,
        DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
        ExpireDelegationTokenRequest, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
        CREATE_API_KEY, DESCRIBE_API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    fn principal(kind: &str, name: &str) -> DelegationTokenPrincipal {
        DelegationTokenPrincipal {
            principal_type: kind.to_owned(),
            principal_name: name.to_owned(),
        }
    }

    #[test]
    fn encodes_create_delegation_token_v1() {
        let request = CreateDelegationTokenRequest {
            correlation_id: 38,
            client_id: Some("kafrust".to_owned()),
            owner: None,
            renewers: vec![principal("User", "alice")],
            max_lifetime_ms: -1,
        };
        let bytes = request.encode_v1().unwrap();
        assert_eq!(&bytes[0..4], &[0, CREATE_API_KEY as u8, 0, 1]);
        assert!(bytes.windows(12).any(|window| {
            window == [0, 4, b'U', b's', b'e', b'r', 0, 5, b'a', b'l', b'i', b'c']
        }));
        assert!(bytes.ends_with(&(-1_i64).to_be_bytes()));
    }

    #[test]
    fn encodes_create_delegation_token_v3_with_owner() {
        let request = CreateDelegationTokenRequest {
            correlation_id: 39,
            client_id: None,
            owner: Some(principal("User", "owner")),
            renewers: Vec::new(),
            max_lifetime_ms: 60_000,
        };
        let bytes = request.encode_v2(3).unwrap();
        assert_eq!(&bytes[0..4], &[0, CREATE_API_KEY as u8, 0, 3]);
        assert!(bytes.ends_with(&[0]));
    }

    #[test]
    fn decodes_create_delegation_token_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i16(0);
        bytes.write_string("User").unwrap();
        bytes.write_string("alice").unwrap();
        bytes.write_i64(10);
        bytes.write_i64(20);
        bytes.write_i64(30);
        bytes.write_string("token-1").unwrap();
        bytes.write_bytes(&[1, 2, 3]).unwrap();
        bytes.write_i32(4);
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = CreateDelegationTokenResponse::decode_body_v1(&mut decoder).unwrap();
        assert_eq!(response.owner.principal_name, "alice");
        assert_eq!(response.hmac, vec![1, 2, 3]);
        assert_eq!(response.throttle_time_ms, 4);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_create_delegation_token_v3_response_without_nested_principal_tags() {
        let mut bytes = Encoder::new();
        bytes.write_i16(0);
        bytes.write_compact_string("User").unwrap();
        bytes.write_compact_string("owner").unwrap();
        bytes.write_compact_string("User").unwrap();
        bytes.write_compact_string("requester").unwrap();
        bytes.write_i64(10);
        bytes.write_i64(20);
        bytes.write_i64(30);
        bytes.write_compact_string("token-1").unwrap();
        bytes.write_compact_bytes(b"secret-hmac").unwrap();
        bytes.write_i32(4);
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = CreateDelegationTokenResponse::decode_body_v2(&mut decoder, 3).unwrap();
        assert_eq!(response.owner.principal_name, "owner");
        assert_eq!(
            response.requester.as_ref().unwrap().principal_name,
            "requester"
        );
        assert_eq!(response.hmac, b"secret-hmac");
        assert_eq!(response.throttle_time_ms, 4);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_renew_and_expire_delegation_token_v2() {
        let renew = RenewDelegationTokenRequest {
            correlation_id: 40,
            client_id: None,
            hmac: vec![7, 8],
            renew_period_ms: 100,
        };
        let expire = ExpireDelegationTokenRequest {
            correlation_id: 41,
            client_id: None,
            hmac: vec![7, 8],
            expiry_time_period_ms: 0,
        };
        assert_eq!(&renew.encode_v2().unwrap()[0..4], &[0, 39, 0, 2]);
        assert_eq!(&expire.encode_v2().unwrap()[0..4], &[0, 40, 0, 2]);
    }

    #[test]
    fn encodes_describe_delegation_tokens_v1_for_all_owners() {
        let request = DescribeDelegationTokenRequest {
            correlation_id: 42,
            client_id: None,
            owners: None,
        };
        let bytes = request.encode_v1().unwrap();
        assert_eq!(&bytes[0..4], &[0, DESCRIBE_API_KEY as u8, 0, 1]);
        assert_eq!(&bytes[bytes.len() - 4..], &(-1_i32).to_be_bytes());
    }

    #[test]
    fn decodes_describe_delegation_tokens_v3_with_requester_and_tags() {
        let mut bytes = Encoder::new();
        bytes.write_i16(0);
        bytes.write_unsigned_varint(2); // one token
        bytes.write_compact_string("User").unwrap();
        bytes.write_compact_string("alice").unwrap();
        bytes.write_compact_string("User").unwrap();
        bytes.write_compact_string("admin").unwrap();
        bytes.write_i64(1);
        bytes.write_i64(2);
        bytes.write_i64(3);
        bytes.write_compact_string("token-1").unwrap();
        bytes.write_compact_bytes(&[9, 8]).unwrap();
        bytes.write_unsigned_varint(2); // one renewer
        bytes.write_compact_string("User").unwrap();
        bytes.write_compact_string("renew").unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields(); // token tags
        bytes.write_i32(5);
        bytes.write_empty_tagged_fields(); // response tags
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = DescribeDelegationTokenResponse::decode_body_v2(&mut decoder, 3).unwrap();
        assert_eq!(response.tokens[0].owner.principal_name, "alice");
        assert_eq!(
            response.tokens[0]
                .requester
                .as_ref()
                .unwrap()
                .principal_name,
            "admin"
        );
        assert_eq!(response.tokens[0].hmac, vec![9, 8]);
        assert_eq!(response.tokens[0].renewers[0].principal_name, "renew");
        assert_eq!(response.throttle_time_ms, 5);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_renew_delegation_token_v2_response() {
        let mut bytes = Encoder::new();
        bytes.write_i16(0);
        bytes.write_i64(99);
        bytes.write_i32(3);
        bytes.write_empty_tagged_fields();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = RenewDelegationTokenResponse::decode_body_v2(&mut decoder).unwrap();
        assert_eq!(response.expiry_timestamp_ms, 99);
        assert_eq!(response.throttle_time_ms, 3);
        assert!(decoder.is_empty());
    }
}
