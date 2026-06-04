use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 9;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Option<Vec<OffsetFetchTopic>>,
}

impl OffsetFetchRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_array(self.topics.as_deref(), |encoder, topic| {
            topic.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

impl OffsetFetchTopic {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(
            Some(self.partition_indexes.as_slice()),
            |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponseV2 {
    pub topics: Vec<OffsetFetchTopicResponse>,
    pub error_code: i16,
}

impl OffsetFetchResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            topics: decoder
                .read_array(
                    "offset fetch topic responses",
                    OffsetFetchTopicResponse::decode,
                )?
                .unwrap_or_default(),
            error_code: decoder.read_i16()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

impl OffsetFetchTopicResponse {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "offset fetch partition responses",
                    OffsetFetchPartitionResponse::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
    pub error_code: i16,
}

impl OffsetFetchPartitionResponse {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            committed_offset: decoder.read_i64()?,
            metadata: decoder.read_nullable_string()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        OffsetFetchPartitionResponse, OffsetFetchRequestV2, OffsetFetchResponseV2,
        OffsetFetchTopic, OffsetFetchTopicResponse,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_offset_fetch_v2_request_for_partitions() {
        let request = OffsetFetchRequestV2 {
            correlation_id: 29,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            topics: Some(vec![OffsetFetchTopic {
                name: "orders".to_owned(),
                partition_indexes: vec![0, 1],
            }]),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 9, // api key
                0, 2, // api version
                0, 0, 0, 29, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0, 0, 0, 1, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
                0, 0, 0, 2, // partition count
                0, 0, 0, 0, // partition 0
                0, 0, 0, 1, // partition 1
            ]
        );
    }

    #[test]
    fn encodes_offset_fetch_v2_request_for_all_topics() {
        let request = OffsetFetchRequestV2 {
            correlation_id: 31,
            client_id: None,
            group_id: "orders-group".to_owned(),
            topics: None,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 9, // api key
                0, 2, // api version
                0, 0, 0, 31, // correlation id
                0xff, 0xff, // null client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0xff, 0xff, 0xff, 0xff, // null topics
            ]
        );
    }

    #[test]
    fn decodes_offset_fetch_v2_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i64(42);
        bytes.write_nullable_string(Some("processed")).unwrap();
        bytes.write_i16(0);
        bytes.write_i16(0);
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = OffsetFetchResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(
            response,
            OffsetFetchResponseV2 {
                topics: vec![OffsetFetchTopicResponse {
                    name: "orders".to_owned(),
                    partitions: vec![OffsetFetchPartitionResponse {
                        partition_index: 0,
                        committed_offset: 42,
                        metadata: Some("processed".to_owned()),
                        error_code: 0,
                    }],
                }],
                error_code: 0,
            }
        );
        assert!(decoder.is_empty());
    }
}
