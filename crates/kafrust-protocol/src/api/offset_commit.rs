use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub generation_id_or_member_epoch: i32,
    pub member_id: String,
    pub retention_time_ms: i64,
    pub topics: Vec<OffsetCommitTopic>,
}

impl OffsetCommitRequestV2 {
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
        encoder.write_i32(self.generation_id_or_member_epoch);
        encoder.write_string(&self.member_id)?;
        encoder.write_i64(self.retention_time_ms);
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitRequestV7 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub generation_id_or_member_epoch: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub topics: Vec<OffsetCommitTopicV7>,
}

impl OffsetCommitRequestV7 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 7,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_i32(self.generation_id_or_member_epoch);
        encoder.write_string(&self.member_id)?;
        encoder.write_nullable_string(self.group_instance_id.as_deref())?;
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartition>,
}

impl OffsetCommitTopic {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopicV7 {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartitionV7>,
}

impl OffsetCommitTopicV7 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_metadata: Option<String>,
}

impl OffsetCommitPartition {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i64(self.committed_offset);
        encoder.write_nullable_string(self.committed_metadata.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartitionV7 {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub committed_metadata: Option<String>,
}

impl OffsetCommitPartitionV7 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i64(self.committed_offset);
        encoder.write_i32(self.committed_leader_epoch);
        encoder.write_nullable_string(self.committed_metadata.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitResponseV2 {
    pub topics: Vec<OffsetCommitTopicResponse>,
}

impl OffsetCommitResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            topics: decoder
                .read_array(
                    "offset commit topic responses",
                    OffsetCommitTopicResponse::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitResponseV7 {
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetCommitTopicResponse>,
}

impl OffsetCommitResponseV7 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            topics: decoder
                .read_array(
                    "offset commit topic responses",
                    OffsetCommitTopicResponse::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartitionResponse>,
}

impl OffsetCommitTopicResponse {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "offset commit partition responses",
                    OffsetCommitPartitionResponse::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
}

impl OffsetCommitPartitionResponse {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        OffsetCommitPartition, OffsetCommitPartitionResponse, OffsetCommitPartitionV7,
        OffsetCommitRequestV2, OffsetCommitRequestV7, OffsetCommitResponseV2,
        OffsetCommitResponseV7, OffsetCommitTopic, OffsetCommitTopicResponse, OffsetCommitTopicV7,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_offset_commit_v2_request() {
        let request = OffsetCommitRequestV2 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            generation_id_or_member_epoch: 7,
            member_id: "member-a".to_owned(),
            retention_time_ms: 86_400_000,
            topics: vec![OffsetCommitTopic {
                name: "orders".to_owned(),
                partitions: vec![OffsetCommitPartition {
                    partition_index: 0,
                    committed_offset: 42,
                    committed_metadata: Some("processed".to_owned()),
                }],
            }],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 8, // api key
                0, 2, // api version
                0, 0, 0, 23, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0, 0, 0, 7, // generation id
                0, 8, b'm', b'e', b'm', b'b', b'e', b'r', b'-', b'a', // member id
                0, 0, 0, 0, 5, 38, 92, 0, // retention time
                0, 0, 0, 1, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
                0, 0, 0, 1, // partition count
                0, 0, 0, 0, // partition
                0, 0, 0, 0, 0, 0, 0, 42, // committed offset
                0, 9, b'p', b'r', b'o', b'c', b'e', b's', b's', b'e', b'd', // metadata
            ]
        );
    }

    #[test]
    fn decodes_offset_commit_v2_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = OffsetCommitResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(
            response.topics,
            vec![OffsetCommitTopicResponse {
                name: "orders".to_owned(),
                partitions: vec![OffsetCommitPartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                }],
            }]
        );
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_offset_commit_v7_request_with_static_member() {
        let request = OffsetCommitRequestV7 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            generation_id_or_member_epoch: 7,
            member_id: "member-a".to_owned(),
            group_instance_id: Some("orders-reader-1".to_owned()),
            topics: vec![OffsetCommitTopicV7 {
                name: "orders".to_owned(),
                partitions: vec![OffsetCommitPartitionV7 {
                    partition_index: 0,
                    committed_offset: 42,
                    committed_leader_epoch: -1,
                    committed_metadata: None,
                }],
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 8, 0, 7]);
        assert!(encoded
            .windows(17)
            .any(|bytes| bytes == b"\0\x0forders-reader-1"));
        assert!(encoded.windows(4).any(|bytes| bytes == [u8::MAX; 4]));
    }

    #[test]
    fn decodes_offset_commit_v7_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = OffsetCommitResponseV7::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.topics[0].partitions[0].error_code, 0);
        assert!(decoder.is_empty());
    }
}
