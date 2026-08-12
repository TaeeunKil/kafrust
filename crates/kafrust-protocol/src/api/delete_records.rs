use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 21;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Vec<DeleteRecordsTopicV1>,
    pub timeout_ms: i32,
}

impl DeleteRecordsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.topics), |encoder, topic| topic.encode(encoder))?;
        encoder.write_i32(self.timeout_ms);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopicV1 {
    pub name: String,
    pub partitions: Vec<DeleteRecordsPartitionV1>,
}

impl DeleteRecordsTopicV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(&self.partitions), |encoder, partition| {
            partition.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPartitionV1 {
    pub partition_index: i32,
    pub offset: i64,
}

impl DeleteRecordsPartitionV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i64(self.offset);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsResponseV1 {
    pub throttle_time_ms: i32,
    pub topics: Vec<DeleteRecordsTopicResponseV1>,
}

impl DeleteRecordsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            topics: decoder
                .read_array(
                    "delete records topics",
                    DeleteRecordsTopicResponseV1::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopicResponseV1 {
    pub name: String,
    pub partitions: Vec<DeleteRecordsPartitionResponseV1>,
}

impl DeleteRecordsTopicResponseV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "delete records partition results",
                    DeleteRecordsPartitionResponseV1::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsPartitionResponseV1 {
    pub partition_index: i32,
    pub low_watermark: i64,
    pub error_code: i16,
}

impl DeleteRecordsPartitionResponseV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            low_watermark: decoder.read_i64()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::codec::Decoder;

    #[test]
    fn encodes_delete_records_v1_request() {
        let request = DeleteRecordsRequestV1 {
            correlation_id: 11,
            client_id: Some("kafrust".to_owned()),
            topics: vec![
                DeleteRecordsTopicV1 {
                    name: "orders".to_owned(),
                    partitions: vec![
                        DeleteRecordsPartitionV1 {
                            partition_index: 0,
                            offset: 100,
                        },
                        DeleteRecordsPartitionV1 {
                            partition_index: 1,
                            offset: -1,
                        },
                    ],
                },
                DeleteRecordsTopicV1 {
                    name: "payments".to_owned(),
                    partitions: vec![DeleteRecordsPartitionV1 {
                        partition_index: 2,
                        offset: 40,
                    }],
                },
            ],
            timeout_ms: 30_000,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 21, // API key
                0, 1, // API version
                0, 0, 0, 11, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 0, 0, 2, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
                0, 0, 0, 2, // partition count
                0, 0, 0, 0, // partition 0
                0, 0, 0, 0, 0, 0, 0, 100, // offset 100
                0, 0, 0, 1, // partition 1
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // high watermark
                0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
                0, 0, 0, 1, // partition count
                0, 0, 0, 2, // partition 2
                0, 0, 0, 0, 0, 0, 0, 40, // offset 40
                0, 0, 117, 48, // timeout
            ]
        );
        assert_eq!(API_KEY, 21);
    }

    #[test]
    fn decodes_delete_records_v1_response() {
        let bytes = [
            0, 0, 0, 8, // throttle time
            0, 0, 0, 2, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, 0, 2, // partition count
            0, 0, 0, 0, // partition 0
            0, 0, 0, 0, 0, 0, 0, 100, // low watermark
            0, 0, // success
            0, 0, 0, 1, // partition 1
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // low watermark
            0, 3, // unknown topic or partition
            0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
            0, 0, 0, 1, // partition count
            0, 0, 0, 2, // partition 2
            0, 0, 0, 0, 0, 0, 0, 40, // low watermark
            0, 0, // success
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = DeleteRecordsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 8);
        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].partitions[0].low_watermark, 100);
        assert_eq!(response.topics[0].partitions[1].error_code, 3);
        assert_eq!(response.topics[1].partitions[0].partition_index, 2);
        assert!(decoder.is_empty());
    }
}
