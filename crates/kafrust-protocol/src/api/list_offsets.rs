use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 2;
pub const EARLIEST_TIMESTAMP: i64 = -2;
pub const LATEST_TIMESTAMP: i64 = -1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub replica_id: i32,
    pub topics: Vec<ListOffsetsTopicV1>,
}

impl ListOffsetsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_i32(self.replica_id);
        encoder.write_array(Some(&self.topics), |encoder, topic| topic.encode(encoder))?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopicV1 {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartitionV1>,
}

impl ListOffsetsTopicV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(&self.partitions), |encoder, partition| {
            encoder.write_i32(partition.partition_index);
            encoder.write_i64(partition.timestamp);
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartitionV1 {
    pub partition_index: i32,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResponseV1 {
    pub topics: Vec<ListOffsetsTopicResponseV1>,
}

impl ListOffsetsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            topics: decoder
                .read_array("list offsets topic responses", |decoder| {
                    Ok(ListOffsetsTopicResponseV1 {
                        name: decoder.read_string()?,
                        partitions: decoder
                            .read_array("list offsets partition responses", |decoder| {
                                Ok(ListOffsetsPartitionResponseV1 {
                                    partition_index: decoder.read_i32()?,
                                    error_code: decoder.read_i16()?,
                                    timestamp: decoder.read_i64()?,
                                    offset: decoder.read_i64()?,
                                })
                            })?
                            .unwrap_or_default(),
                    })
                })?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsTopicResponseV1 {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartitionResponseV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsPartitionResponseV1 {
    pub partition_index: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn encodes_list_offsets_v1_request() {
        let request = ListOffsetsRequestV1 {
            correlation_id: 7,
            client_id: None,
            replica_id: -1,
            topics: vec![ListOffsetsTopicV1 {
                name: "x".to_owned(),
                partitions: vec![ListOffsetsPartitionV1 {
                    partition_index: 2,
                    timestamp: LATEST_TIMESTAMP,
                }],
            }],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 2, 0, 1, 0, 0, 0, 7, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 1, 0, 1, b'x',
                0, 0, 0, 1, 0, 0, 0, 2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            ]
        );
    }

    #[test]
    fn decodes_list_offsets_v1_response() {
        let mut encoder = Encoder::new();
        encoder.write_i32(1);
        encoder.write_string("x").unwrap();
        encoder.write_i32(1);
        encoder.write_i32(2);
        encoder.write_i16(0);
        encoder.write_i64(123);
        encoder.write_i64(42);
        let bytes = encoder.into_bytes();

        let response = ListOffsetsResponseV1::decode_body(&mut Decoder::new(&bytes)).unwrap();
        assert_eq!(response.topics[0].partitions[0].offset, 42);
    }
}
