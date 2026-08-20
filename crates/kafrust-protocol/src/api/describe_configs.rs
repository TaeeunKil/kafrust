use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub resources: Vec<DescribeConfigsResourceV1>,
    pub include_synonyms: bool,
}

impl DescribeConfigsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.resources), |encoder, resource| {
            resource.encode(encoder)
        })?;
        encoder.write_bool(self.include_synonyms);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResourceV1 {
    pub resource_type: i8,
    pub resource_name: String,
    pub configuration_keys: Option<Vec<String>>,
}

impl DescribeConfigsResourceV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i8(self.resource_type);
        encoder.write_string(&self.resource_name)?;
        encoder.write_array(
            self.configuration_keys.as_deref(),
            |encoder, configuration_key| encoder.write_string(configuration_key),
        )
    }
}

/// Kafka DescribeConfigs v4 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsRequestV4 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub resources: Vec<DescribeConfigsResourceV4>,
    pub include_synonyms: bool,
    pub include_documentation: bool,
}

impl DescribeConfigsRequestV4 {
    /// Encodes the flexible request header and body.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 4,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&self.resources), |encoder, resource| {
            encoder.write_i8(resource.resource_type);
            encoder.write_compact_string(&resource.resource_name)?;
            encoder.write_compact_array(
                resource.configuration_keys.as_deref(),
                |encoder, configuration_key| encoder.write_compact_string(configuration_key),
            )?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_bool(self.include_synonyms);
        encoder.write_bool(self.include_documentation);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// One resource in a DescribeConfigs v4 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResourceV4 {
    pub resource_type: i8,
    pub resource_name: String,
    pub configuration_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResponseV1 {
    pub throttle_time_ms: i32,
    pub results: Vec<DescribeConfigsResultV1>,
}

impl DescribeConfigsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            results: decoder
                .read_array("describe configs results", DescribeConfigsResultV1::decode)?
                .unwrap_or_default(),
        })
    }
}

/// Kafka DescribeConfigs v4 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResponseV4 {
    pub throttle_time_ms: i32,
    pub results: Vec<DescribeConfigsResultV4>,
}

impl DescribeConfigsResponseV4 {
    /// Decodes a flexible response body.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let results = decoder
            .read_compact_array("describe configs v4 results", |decoder| {
                let error_code = decoder.read_i16()?;
                let error_message = decoder.read_compact_nullable_string()?;
                let resource_type = decoder.read_i8()?;
                let resource_name = decoder.read_compact_string()?;
                let configs = decoder
                    .read_compact_array("describe configs v4 entries", |decoder| {
                        let name = decoder.read_compact_string()?;
                        let value = decoder.read_compact_nullable_string()?;
                        let read_only = decoder.read_bool()?;
                        let config_source = decoder.read_i8()?;
                        let is_sensitive = decoder.read_bool()?;
                        let synonyms = decoder
                            .read_compact_array(
                                "describe configs v4 synonyms",
                                DescribeConfigsSynonymV4::decode,
                            )?
                            .unwrap_or_default();
                        let config_type = decoder.read_i8()?;
                        let documentation = decoder.read_compact_nullable_string()?;
                        decoder.read_tagged_fields()?;
                        Ok(DescribeConfigsEntryV4 {
                            name,
                            value,
                            read_only,
                            config_source,
                            is_sensitive,
                            synonyms,
                            config_type,
                            documentation,
                        })
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok(DescribeConfigsResultV4 {
                    error_code,
                    error_message,
                    resource_type,
                    resource_name,
                    configs,
                })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

/// One resource result in a DescribeConfigs v4 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResultV4 {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<DescribeConfigsEntryV4>,
}

/// One configuration entry in a DescribeConfigs v4 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsEntryV4 {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
    pub synonyms: Vec<DescribeConfigsSynonymV4>,
    pub config_type: i8,
    pub documentation: Option<String>,
}

/// One synonym in a DescribeConfigs v4 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsSynonymV4 {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

impl DescribeConfigsSynonymV4 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let name = decoder.read_compact_string()?;
        let value = decoder.read_compact_nullable_string()?;
        let source = decoder.read_i8()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            name,
            value,
            source,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResultV1 {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<DescribeConfigsEntryV1>,
}

impl DescribeConfigsResultV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            resource_type: decoder.read_i8()?,
            resource_name: decoder.read_string()?,
            configs: decoder
                .read_array("describe configs entries", DescribeConfigsEntryV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsEntryV1 {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
    pub synonyms: Vec<DescribeConfigsSynonymV1>,
}

impl DescribeConfigsEntryV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            value: decoder.read_nullable_string()?,
            read_only: decoder.read_bool()?,
            config_source: decoder.read_i8()?,
            is_sensitive: decoder.read_bool()?,
            synonyms: decoder
                .read_array(
                    "describe configs synonyms",
                    DescribeConfigsSynonymV1::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsSynonymV1 {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

impl DescribeConfigsSynonymV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            value: decoder.read_nullable_string()?,
            source: decoder.read_i8()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        DescribeConfigsRequestV1, DescribeConfigsRequestV4, DescribeConfigsResourceV1,
        DescribeConfigsResourceV4, DescribeConfigsResponseV1, DescribeConfigsResponseV4, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_configs_v1_request() {
        let request = DescribeConfigsRequestV1 {
            correlation_id: 12,
            client_id: Some("kafrust".to_owned()),
            resources: vec![DescribeConfigsResourceV1 {
                resource_type: 2,
                resource_name: "orders".to_owned(),
                configuration_keys: Some(vec!["cleanup.policy".to_owned()]),
            }],
            include_synonyms: true,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 32, // API key
                0, 1, // API version
                0, 0, 0, 12, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 0, 0, 1, // resource count
                2, // topic resource
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
                0, 0, 0, 1, // configuration key count
                0, 14, b'c', b'l', b'e', b'a', b'n', b'u', b'p', b'.', b'p', b'o', b'l', b'i',
                b'c', b'y', // configuration key
                1,    // include synonyms
            ]
        );
        assert_eq!(API_KEY, 32);
    }

    #[test]
    fn decodes_describe_configs_v1_response() {
        let bytes = [
            0, 0, 0, 9, // throttle time
            0, 0, 0, 1, // result count
            0, 0, // success
            0xff, 0xff, // null error message
            2,    // topic resource
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
            0, 0, 0, 1, // config count
            0, 14, b'c', b'l', b'e', b'a', b'n', b'u', b'p', b'.', b'p', b'o', b'l', b'i', b'c',
            b'y', // config name
            0, 7, b'c', b'o', b'm', b'p', b'a', b'c', b't', // value
            0,    // read only
            1,    // dynamic topic config source
            0,    // not sensitive
            0, 0, 0, 1, // synonym count
            0, 14, b'c', b'l', b'e', b'a', b'n', b'u', b'p', b'.', b'p', b'o', b'l', b'i', b'c',
            b'y', // synonym name
            0, 6, b'd', b'e', b'l', b'e', b't', b'e', // synonym value
            5,    // default config source
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = DescribeConfigsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 9);
        assert_eq!(response.results.len(), 1);
        let result = &response.results[0];
        assert_eq!(result.resource_type, 2);
        assert_eq!(result.resource_name, "orders");
        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].name, "cleanup.policy");
        assert_eq!(result.configs[0].value.as_deref(), Some("compact"));
        assert_eq!(result.configs[0].config_source, 1);
        assert_eq!(result.configs[0].synonyms[0].source, 5);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_describe_configs_v4_request_with_documentation() {
        let request = DescribeConfigsRequestV4 {
            correlation_id: 12,
            client_id: Some("kafrust".to_owned()),
            resources: vec![DescribeConfigsResourceV4 {
                resource_type: 2,
                resource_name: "orders".to_owned(),
                configuration_keys: Some(vec!["cleanup.policy".to_owned()]),
            }],
            include_synonyms: true,
            include_documentation: true,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 4]);
        assert_eq!(bytes[17], 0); // request header tagged fields
        assert_eq!(bytes[18], 2); // one compact resource
        assert!(bytes.ends_with(&[1, 1, 0]));
    }

    #[test]
    fn decodes_describe_configs_v4_response_with_documentation() {
        let mut body = Encoder::new();
        body.write_i32(9);
        body.write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_i16(0);
            encoder.write_compact_nullable_string(None)?;
            encoder.write_i8(2);
            encoder.write_compact_string("orders")?;
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("cleanup.policy")?;
                encoder.write_compact_nullable_string(Some("compact"))?;
                encoder.write_bool(false);
                encoder.write_i8(1);
                encoder.write_bool(false);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("cleanup.policy")?;
                    encoder.write_compact_nullable_string(Some("delete"))?;
                    encoder.write_i8(5);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i8(7);
                encoder.write_compact_nullable_string(Some("The cleanup policy."))?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
        body.write_empty_tagged_fields();

        let bytes = body.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = DescribeConfigsResponseV4::decode_body(&mut decoder).unwrap();

        let entry = &response.results[0].configs[0];
        assert_eq!(entry.config_type, 7);
        assert_eq!(entry.documentation.as_deref(), Some("The cleanup policy."));
        assert_eq!(entry.synonyms[0].source, 5);
        assert!(decoder.is_empty());
    }
}
