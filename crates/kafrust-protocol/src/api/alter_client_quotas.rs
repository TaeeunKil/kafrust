use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 49;

#[derive(Debug, Clone, PartialEq)]
pub struct AlterClientQuotasRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub entries: Vec<AlterClientQuotasEntryV0>,
    pub validate_only: bool,
}

impl AlterClientQuotasRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.entries), |encoder, entry| {
            encoder.write_array(Some(&entry.entities), |encoder, entity| {
                encoder.write_string(&entity.entity_type)?;
                encoder.write_nullable_string(entity.entity_name.as_deref())?;
                Ok(())
            })?;
            encoder.write_array(Some(&entry.operations), |encoder, operation| {
                encoder.write_string(&operation.key)?;
                encoder.write_f64(operation.value);
                encoder.write_bool(operation.remove);
                Ok(())
            })?;
            Ok(())
        })?;
        encoder.write_bool(self.validate_only);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterClientQuotasEntryV0 {
    pub entities: Vec<AlterClientQuotasEntityV0>,
    pub operations: Vec<AlterClientQuotasOperationV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterClientQuotasEntityV0 {
    pub entity_type: String,
    pub entity_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterClientQuotasOperationV0 {
    pub key: String,
    pub value: f64,
    pub remove: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterClientQuotasResponseV0 {
    pub throttle_time_ms: i32,
    pub entries: Vec<AlterClientQuotasResultV0>,
}

impl AlterClientQuotasResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            entries: decoder
                .read_array(
                    "alter client quota results",
                    AlterClientQuotasResultV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterClientQuotasResultV0 {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub entities: Vec<AlterClientQuotasEntityV0>,
}

impl AlterClientQuotasResultV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            entities: decoder
                .read_array(
                    "alter client quota result entities",
                    AlterClientQuotasEntityV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

impl AlterClientQuotasEntityV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            entity_type: decoder.read_string()?,
            entity_name: decoder.read_nullable_string()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AlterClientQuotasEntityV0, AlterClientQuotasEntryV0, AlterClientQuotasOperationV0,
        AlterClientQuotasRequestV0, AlterClientQuotasResponseV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_alter_client_quotas_v0_request() {
        let request = AlterClientQuotasRequestV0 {
            correlation_id: 19,
            client_id: None,
            entries: vec![AlterClientQuotasEntryV0 {
                entities: vec![AlterClientQuotasEntityV0 {
                    entity_type: "user".to_owned(),
                    entity_name: Some("alice".to_owned()),
                }],
                operations: vec![AlterClientQuotasOperationV0 {
                    key: "producer_byte_rate".to_owned(),
                    value: 1024.5,
                    remove: false,
                }],
            }],
            validate_only: true,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 19]);
        assert_eq!(bytes.last(), Some(&1));
    }

    #[test]
    fn decodes_alter_client_quotas_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(5);
        bytes.write_i32(1);
        bytes.write_i16(0);
        bytes.write_nullable_string(None).unwrap();
        bytes.write_i32(1);
        bytes.write_string("user").unwrap();
        bytes.write_nullable_string(Some("alice")).unwrap();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = AlterClientQuotasResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 5);
        assert_eq!(response.entries[0].entities[0].entity_type, "user");
        assert_eq!(
            response.entries[0].entities[0].entity_name.as_deref(),
            Some("alice")
        );
        assert!(decoder.is_empty());
    }
}
