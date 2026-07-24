use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 19;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Vec<CreateTopicsTopicV2>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

impl CreateTopicsRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.topics), |encoder, topic| topic.encode(encoder))?;
        encoder.write_i32(self.timeout_ms);
        encoder.write_bool(self.validate_only);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsTopicV2 {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub assignments: Vec<CreateTopicsAssignmentV2>,
    pub configs: Vec<CreateTopicsConfigV2>,
}

impl CreateTopicsTopicV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_i32(self.num_partitions);
        encoder.write_i16(self.replication_factor);
        encoder.write_array(Some(&self.assignments), |encoder, assignment| {
            assignment.encode(encoder)
        })?;
        encoder.write_array(Some(&self.configs), |encoder, config| {
            config.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsAssignmentV2 {
    pub partition_index: i32,
    pub broker_ids: Vec<i32>,
}

impl CreateTopicsAssignmentV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_array(Some(&self.broker_ids), |encoder, broker_id| {
            encoder.write_i32(*broker_id);
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsConfigV2 {
    pub name: String,
    pub value: Option<String>,
}

impl CreateTopicsConfigV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_nullable_string(self.value.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsResponseV2 {
    pub throttle_time_ms: i32,
    pub topics: Vec<CreateTopicsTopicResultV2>,
}

impl CreateTopicsResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            topics: decoder
                .read_array("create topics results", CreateTopicsTopicResultV2::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsTopicResultV2 {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl CreateTopicsTopicResultV2 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        CreateTopicsAssignmentV2, CreateTopicsConfigV2, CreateTopicsRequestV2,
        CreateTopicsResponseV2, CreateTopicsTopicV2, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_create_topics_v2_request() {
        let request = CreateTopicsRequestV2 {
            correlation_id: 9,
            client_id: Some("kafrust".to_owned()),
            topics: vec![CreateTopicsTopicV2 {
                name: "orders".to_owned(),
                num_partitions: -1,
                replication_factor: -1,
                assignments: vec![CreateTopicsAssignmentV2 {
                    partition_index: 0,
                    broker_ids: vec![1, 2],
                }],
                configs: vec![CreateTopicsConfigV2 {
                    name: "cleanup.policy".to_owned(),
                    value: Some("compact".to_owned()),
                }],
            }],
            timeout_ms: 30_000,
            validate_only: true,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 19, // API key
                0, 2, // API version
                0, 0, 0, 9, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 0, 0, 1, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
                0xff, 0xff, 0xff, 0xff, // default partition count
                0xff, 0xff, // default replication factor
                0, 0, 0, 1, // assignment count
                0, 0, 0, 0, // partition index
                0, 0, 0, 2, // broker count
                0, 0, 0, 1, // broker 1
                0, 0, 0, 2, // broker 2
                0, 0, 0, 1, // config count
                0, 14, b'c', b'l', b'e', b'a', b'n', b'u', b'p', b'.', b'p', b'o', b'l', b'i',
                b'c', b'y', // config name
                0, 7, b'c', b'o', b'm', b'p', b'a', b'c', b't', // config value
                0, 0, 117, 48, // timeout
                1,  // validate only
            ]
        );
        assert_eq!(API_KEY, 19);
    }

    #[test]
    fn decodes_create_topics_v2_response() {
        let bytes = [
            0, 0, 0, 12, // throttle time
            0, 0, 0, 2, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, // success
            0xff, 0xff, // null error message
            0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
            0, 36, // topic already exists
            0, 6, b'e', b'x', b'i', b's', b't', b's', // error message
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = CreateTopicsResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].error_code, 0);
        assert_eq!(response.topics[0].error_message, None);
        assert_eq!(response.topics[1].name, "payments");
        assert_eq!(response.topics[1].error_code, 36);
        assert_eq!(response.topics[1].error_message.as_deref(), Some("exists"));
        assert!(decoder.is_empty());
    }
}
