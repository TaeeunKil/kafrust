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
        TxnOffsetCommitPartition, TxnOffsetCommitRequestV0, TxnOffsetCommitResponseV0,
        TxnOffsetCommitTopic, API_KEY,
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
}
