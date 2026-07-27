use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 37;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Vec<CreatePartitionsTopicV0>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

impl CreatePartitionsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
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
pub struct CreatePartitionsTopicV0 {
    pub name: String,
    pub count: i32,
    pub assignments: Option<Vec<CreatePartitionsAssignmentV0>>,
}

impl CreatePartitionsTopicV0 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_i32(self.count);
        encoder.write_array(self.assignments.as_deref(), |encoder, assignment| {
            encoder.write_array(Some(&assignment.broker_ids), |encoder, broker_id| {
                encoder.write_i32(*broker_id);
                Ok(())
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsAssignmentV0 {
    pub broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsResponseV0 {
    pub throttle_time_ms: i32,
    pub results: Vec<CreatePartitionsTopicResultV0>,
}

impl CreatePartitionsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            results: decoder
                .read_array(
                    "create partitions results",
                    CreatePartitionsTopicResultV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopicResultV0 {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl CreatePartitionsTopicResultV0 {
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
    use super::*;

    #[test]
    fn encodes_create_partitions_v0_request() {
        let request = CreatePartitionsRequestV0 {
            correlation_id: 9,
            client_id: Some("kafrust".to_owned()),
            topics: vec![
                CreatePartitionsTopicV0 {
                    name: "orders".to_owned(),
                    count: 4,
                    assignments: None,
                },
                CreatePartitionsTopicV0 {
                    name: "payments".to_owned(),
                    count: 3,
                    assignments: Some(vec![CreatePartitionsAssignmentV0 {
                        broker_ids: vec![1, 2],
                    }]),
                },
            ],
            timeout_ms: 30_000,
            validate_only: true,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 37, // API key
                0, 0, // API version
                0, 0, 0, 9, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 0, 0, 2, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
                0, 0, 0, 4, // new total count
                0xff, 0xff, 0xff, 0xff, // automatic assignment
                0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
                0, 0, 0, 3, // new total count
                0, 0, 0, 1, // assignment count
                0, 0, 0, 2, // broker count
                0, 0, 0, 1, // broker 1
                0, 0, 0, 2, // broker 2
                0, 0, 117, 48, // timeout
                1,  // validate only
            ]
        );
        assert_eq!(API_KEY, 37);
    }

    #[test]
    fn decodes_create_partitions_v0_response() {
        let bytes = [
            0, 0, 0, 12, // throttle time
            0, 0, 0, 2, // result count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, // success
            0xff, 0xff, // null error message
            0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
            0, 37, // invalid partitions
            0, 7, b'i', b'n', b'v', b'a', b'l', b'i', b'd', // error message
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = CreatePartitionsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].name, "orders");
        assert_eq!(response.results[0].error_code, 0);
        assert_eq!(response.results[1].error_code, 37);
        assert_eq!(
            response.results[1].error_message.as_deref(),
            Some("invalid")
        );
        assert!(decoder.is_empty());
    }
}
