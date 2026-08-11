use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 48;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClientQuotasRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub components: Vec<DescribeClientQuotasComponentV0>,
    pub strict: bool,
}

impl DescribeClientQuotasRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.components), |encoder, component| {
            encoder.write_string(&component.entity_type)?;
            encoder.write_i8(component.match_type);
            encoder.write_nullable_string(component.match_value.as_deref())?;
            Ok(())
        })?;
        encoder.write_bool(self.strict);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClientQuotasComponentV0 {
    pub entity_type: String,
    pub match_type: i8,
    pub match_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub entries: Vec<DescribeClientQuotasEntryV0>,
}

impl DescribeClientQuotasResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            entries: decoder
                .read_array(
                    "describe client quota entries",
                    DescribeClientQuotasEntryV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasEntryV0 {
    pub entities: Vec<DescribeClientQuotasEntityV0>,
    pub values: Vec<DescribeClientQuotasValueV0>,
}

impl DescribeClientQuotasEntryV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            entities: decoder
                .read_array(
                    "describe client quota entities",
                    DescribeClientQuotasEntityV0::decode,
                )?
                .unwrap_or_default(),
            values: decoder
                .read_array(
                    "describe client quota values",
                    DescribeClientQuotasValueV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClientQuotasEntityV0 {
    pub entity_type: String,
    pub entity_name: Option<String>,
}

impl DescribeClientQuotasEntityV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            entity_type: decoder.read_string()?,
            entity_name: decoder.read_nullable_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasValueV0 {
    pub key: String,
    pub value: f64,
}

impl DescribeClientQuotasValueV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            key: decoder.read_string()?,
            value: decoder.read_f64()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        DescribeClientQuotasComponentV0, DescribeClientQuotasRequestV0,
        DescribeClientQuotasResponseV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_client_quotas_v0_request() {
        let request = DescribeClientQuotasRequestV0 {
            correlation_id: 17,
            client_id: Some("kafrust".to_owned()),
            components: vec![DescribeClientQuotasComponentV0 {
                entity_type: "user".to_owned(),
                match_type: 0,
                match_value: Some("alice".to_owned()),
            }],
            strict: true,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 17]);
        assert_eq!(bytes.last(), Some(&1));
    }

    #[test]
    fn decodes_describe_client_quotas_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(7);
        bytes.write_i16(0);
        bytes.write_nullable_string(None).unwrap();
        bytes.write_i32(1);
        bytes.write_i32(1);
        bytes.write_string("user").unwrap();
        bytes.write_nullable_string(Some("alice")).unwrap();
        bytes.write_i32(1);
        bytes.write_string("producer_byte_rate").unwrap();
        bytes.write_f64(1024.5);
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = DescribeClientQuotasResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(response.entries[0].entities[0].entity_type, "user");
        assert_eq!(
            response.entries[0].entities[0].entity_name.as_deref(),
            Some("alice")
        );
        assert_eq!(response.entries[0].values[0].key, "producer_byte_rate");
        assert_eq!(response.entries[0].values[0].value, 1024.5);
        assert!(decoder.is_empty());
    }
}
