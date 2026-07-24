use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsRequestV3 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

impl DeleteTopicsRequestV3 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 3,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.topic_names), |encoder, topic_name| {
            encoder.write_string(topic_name)
        })?;
        encoder.write_i32(self.timeout_ms);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsResponseV3 {
    pub throttle_time_ms: i32,
    pub topics: Vec<DeleteTopicsTopicResultV3>,
}

impl DeleteTopicsResponseV3 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            topics: decoder
                .read_array("delete topics results", DeleteTopicsTopicResultV3::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsTopicResultV3 {
    pub name: String,
    pub error_code: i16,
}

impl DeleteTopicsTopicResultV3 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DeleteTopicsRequestV3, DeleteTopicsResponseV3, API_KEY};
    use crate::codec::Decoder;

    #[test]
    fn encodes_delete_topics_v3_request() {
        let request = DeleteTopicsRequestV3 {
            correlation_id: 11,
            client_id: Some("kafrust".to_owned()),
            topic_names: vec!["orders".to_owned(), "payments".to_owned()],
            timeout_ms: 30_000,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 20, // API key
                0, 3, // API version
                0, 0, 0, 11, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 0, 0, 2, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
                0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
                0, 0, 117, 48, // timeout
            ]
        );
        assert_eq!(API_KEY, 20);
    }

    #[test]
    fn decodes_delete_topics_v3_response() {
        let bytes = [
            0, 0, 0, 8, // throttle time
            0, 0, 0, 2, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, // success
            0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
            0, 3, // unknown topic or partition
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = DeleteTopicsResponseV3::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 8);
        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].error_code, 0);
        assert_eq!(response.topics[1].name, "payments");
        assert_eq!(response.topics[1].error_code, 3);
        assert!(decoder.is_empty());
    }
}
