use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 28;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<TxnOffsetCommitTopic>,
}

impl TxnOffsetCommitRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.transactional_id)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_i64(self.producer_id);
        encoder.write_i16(self.producer_epoch);
        encoder.write_array(Some(&self.topics), |encoder, topic| topic.encode(encoder))?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitRequestV3 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub topics: Vec<TxnOffsetCommitTopicV3>,
}

impl TxnOffsetCommitRequestV3 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 3,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.transactional_id)?;
        encoder.write_compact_string(&self.group_id)?;
        encoder.write_i64(self.producer_id);
        encoder.write_i16(self.producer_epoch);
        encoder.write_i32(self.generation_id);
        encoder.write_compact_string(&self.member_id)?;
        encoder.write_compact_nullable_string(self.group_instance_id.as_deref())?;
        encoder.write_compact_array(Some(&self.topics), |encoder, topic| topic.encode(encoder))?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitTopicV3 {
    pub name: String,
    pub partitions: Vec<TxnOffsetCommitPartitionV3>,
}

impl TxnOffsetCommitTopicV3 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.name)?;
        encoder.write_compact_array(Some(&self.partitions), |encoder, partition| {
            partition.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitPartitionV3 {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_leader_epoch: i32,
    pub committed_metadata: Option<String>,
}

impl TxnOffsetCommitPartitionV3 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i64(self.committed_offset);
        encoder.write_i32(self.committed_leader_epoch);
        encoder.write_compact_nullable_string(self.committed_metadata.as_deref())?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitTopic {
    pub name: String,
    pub partitions: Vec<TxnOffsetCommitPartition>,
}

impl TxnOffsetCommitTopic {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(&self.partitions), |encoder, partition| {
            partition.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_metadata: Option<String>,
}

impl TxnOffsetCommitPartition {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i64(self.committed_offset);
        encoder.write_nullable_string(self.committed_metadata.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitResponseV0 {
    pub throttle_time_ms: i32,
    pub topics: Vec<TxnOffsetCommitTopicResult>,
}

impl TxnOffsetCommitResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            topics: decoder
                .read_array(
                    "transaction offset commit topic results",
                    TxnOffsetCommitTopicResult::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitResponseV3 {
    pub throttle_time_ms: i32,
    pub topics: Vec<TxnOffsetCommitTopicResult>,
}

impl TxnOffsetCommitResponseV3 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let topics = decoder
            .read_compact_array("transaction offset commit topic results", |decoder| {
                let name = decoder.read_compact_string()?;
                let partitions = decoder
                    .read_compact_array("transaction offset commit partition results", |decoder| {
                        let partition_index = decoder.read_i32()?;
                        let error_code = decoder.read_i16()?;
                        let _tagged_fields = decoder.read_tagged_fields()?;
                        Ok(TxnOffsetCommitPartitionResult {
                            partition_index,
                            error_code,
                        })
                    })?
                    .unwrap_or_default();
                let _tagged_fields = decoder.read_tagged_fields()?;
                Ok(TxnOffsetCommitTopicResult { name, partitions })
            })?
            .unwrap_or_default();
        let _tagged_fields = decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitTopicResult {
    pub name: String,
    pub partitions: Vec<TxnOffsetCommitPartitionResult>,
}

impl TxnOffsetCommitTopicResult {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "transaction offset commit partition results",
                    TxnOffsetCommitPartitionResult::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitPartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
}

impl TxnOffsetCommitPartitionResult {
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
        TxnOffsetCommitPartition, TxnOffsetCommitPartitionV3, TxnOffsetCommitRequestV0,
        TxnOffsetCommitRequestV3, TxnOffsetCommitResponseV0, TxnOffsetCommitResponseV3,
        TxnOffsetCommitTopic, TxnOffsetCommitTopicV3, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_txn_offset_commit_v0_request() {
        let request = TxnOffsetCommitRequestV0 {
            correlation_id: 61,
            client_id: Some("kafrust".to_owned()),
            transactional_id: "orders-tx".to_owned(),
            group_id: "orders-group".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            topics: vec![TxnOffsetCommitTopic {
                name: "orders".to_owned(),
                partitions: vec![TxnOffsetCommitPartition {
                    partition_index: 2,
                    committed_offset: 81,
                    committed_metadata: Some("processed".to_owned()),
                }],
            }],
        };
        let encoded = request.encode().unwrap();

        assert_eq!(&encoded[0..8], &[0, 28, 0, 0, 0, 0, 0, 61]);
        assert!(encoded.ends_with(b"processed"));
        assert_eq!(API_KEY, 28);
    }

    #[test]
    fn decodes_txn_offset_commit_v0_response() {
        let bytes = [
            0, 0, 0, 5, // throttle time
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', 0, 0, 0, 1, // partition count
            0, 0, 0, 2, // partition index
            0, 27, // rebalance in progress
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = TxnOffsetCommitResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 5);
        assert_eq!(response.topics[0].partitions[0].partition_index, 2);
        assert_eq!(response.topics[0].partitions[0].error_code, 27);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_txn_offset_commit_v3_with_group_generation() {
        let request = TxnOffsetCommitRequestV3 {
            correlation_id: 62,
            client_id: Some("kafrust".to_owned()),
            transactional_id: "orders-tx".to_owned(),
            group_id: "orders-group".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            generation_id: 7,
            member_id: "member-1".to_owned(),
            group_instance_id: Some("instance-1".to_owned()),
            topics: vec![TxnOffsetCommitTopicV3 {
                name: "orders".to_owned(),
                partitions: vec![TxnOffsetCommitPartitionV3 {
                    partition_index: 2,
                    committed_offset: 81,
                    committed_leader_epoch: -1,
                    committed_metadata: None,
                }],
            }],
        };
        let encoded = request.encode().unwrap();

        assert_eq!(&encoded[0..8], &[0, 28, 0, 3, 0, 0, 0, 62]);
        assert_eq!(encoded[17], 0); // request-header tagged fields
        assert!(encoded.windows(8).any(|bytes| bytes == b"member-1"));
        assert!(encoded.windows(10).any(|bytes| bytes == b"instance-1"));
        assert_eq!(encoded.last(), Some(&0)); // request tagged fields
    }

    #[test]
    fn decodes_txn_offset_commit_v3_flexible_response() {
        let bytes = [
            0, 0, 0, 5, // throttle time
            2, // compact topic count (1 + 1)
            7, b'o', b'r', b'd', b'e', b'r', b's', // compact topic name
            2,    // compact partition count
            0, 0, 0, 2, // partition index
            0, 22, // illegal generation
            0,  // partition tagged fields
            0,  // topic tagged fields
            0,  // response tagged fields
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = TxnOffsetCommitResponseV3::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 5);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].partitions[0].partition_index, 2);
        assert_eq!(response.topics[0].partitions[0].error_code, 22);
        assert!(decoder.is_empty());
    }
}
