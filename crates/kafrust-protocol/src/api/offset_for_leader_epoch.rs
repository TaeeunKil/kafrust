use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka OffsetForLeaderEpoch API key.
pub const API_KEY: i16 = 23;

/// OffsetForLeaderEpoch v3 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderEpochRequestV3 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub replica_id: i32,
    pub topics: Vec<OffsetForLeaderEpochTopicV3>,
}

impl OffsetForLeaderEpochRequestV3 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 3,
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
pub struct OffsetForLeaderEpochTopicV3 {
    pub name: String,
    pub partitions: Vec<OffsetForLeaderEpochPartitionV3>,
}

impl OffsetForLeaderEpochTopicV3 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(&self.partitions), |encoder, partition| {
            encoder.write_i32(partition.partition_index);
            encoder.write_i32(partition.current_leader_epoch);
            encoder.write_i32(partition.leader_epoch);
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderEpochPartitionV3 {
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderEpochResponseV3 {
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetForLeaderEpochTopicResponseV3>,
}

impl OffsetForLeaderEpochResponseV3 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let response = Self {
            throttle_time_ms: decoder.read_i32()?,
            topics: decoder
                .read_array("offset for leader epoch topic responses", |decoder| {
                    Ok(OffsetForLeaderEpochTopicResponseV3 {
                        name: decoder.read_string()?,
                        partitions: decoder
                            .read_array("offset for leader epoch partition responses", |decoder| {
                                Ok(OffsetForLeaderEpochPartitionResponseV3 {
                                    error_code: decoder.read_i16()?,
                                    partition_index: decoder.read_i32()?,
                                    leader_epoch: decoder.read_i32()?,
                                    end_offset: decoder.read_i64()?,
                                })
                            })?
                            .unwrap_or_default(),
                    })
                })?
                .unwrap_or_default(),
        };
        decoder.finish()?;
        Ok(response)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderEpochTopicResponseV3 {
    pub name: String,
    pub partitions: Vec<OffsetForLeaderEpochPartitionResponseV3>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetForLeaderEpochPartitionResponseV3 {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_epoch: i32,
    pub end_offset: i64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn encodes_offset_for_leader_epoch_v3_request() {
        let request = OffsetForLeaderEpochRequestV3 {
            correlation_id: 7,
            client_id: None,
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopicV3 {
                name: "orders".to_owned(),
                partitions: vec![OffsetForLeaderEpochPartitionV3 {
                    partition_index: 2,
                    current_leader_epoch: 8,
                    leader_epoch: 7,
                }],
            }],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 23, 0, 3, 0, 0, 0, 7, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 1, 0, 6,
                b'o', b'r', b'd', b'e', b'r', b's', 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 0, 7,
            ]
        );
    }

    #[test]
    fn decodes_offset_for_leader_epoch_v3_response() {
        let mut encoder = Encoder::new();
        encoder.write_i32(12);
        encoder.write_i32(1);
        encoder.write_string("orders").unwrap();
        encoder.write_i32(1);
        encoder.write_i16(0);
        encoder.write_i32(2);
        encoder.write_i32(8);
        encoder.write_i64(42);

        let response =
            OffsetForLeaderEpochResponseV3::decode_body(&mut Decoder::new(&encoder.into_bytes()))
                .unwrap();
        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.topics[0].partitions[0].leader_epoch, 8);
        assert_eq!(response.topics[0].partitions[0].end_offset, 42);
    }
}
