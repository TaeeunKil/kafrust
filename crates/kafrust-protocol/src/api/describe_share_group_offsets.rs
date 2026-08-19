use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka DescribeShareGroupOffsets API key.
pub const API_KEY: i16 = 90;

/// One topic and partition filter for a share-group offset query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsTopic {
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

/// One share group in a DescribeShareGroupOffsets request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsGroup {
    pub group_id: String,
    pub topics: Option<Vec<DescribeShareGroupOffsetsTopic>>,
}

fn encode_request(
    correlation_id: i32,
    client_id: Option<String>,
    groups: &[DescribeShareGroupOffsetsGroup],
    api_version: i16,
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: API_KEY,
        api_version,
        correlation_id,
        client_id,
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_array(Some(groups), |encoder, group| {
        encoder.write_compact_string(&group.group_id)?;
        encoder.write_compact_array(group.topics.as_deref(), |encoder, topic| {
            encoder.write_compact_string(&topic.topic_name)?;
            encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    })?;
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

/// DescribeShareGroupOffsets v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub groups: Vec<DescribeShareGroupOffsetsGroup>,
}

impl DescribeShareGroupOffsetsRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(self.correlation_id, self.client_id.clone(), &self.groups, 0)
    }
}

/// DescribeShareGroupOffsets v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub groups: Vec<DescribeShareGroupOffsetsGroup>,
}

impl DescribeShareGroupOffsetsRequestV1 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(self.correlation_id, self.client_id.clone(), &self.groups, 1)
    }
}

/// One partition result returned by DescribeShareGroupOffsets v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsPartitionV0 {
    pub partition_index: i32,
    pub start_offset: i64,
    pub leader_epoch: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// One topic result returned by DescribeShareGroupOffsets v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsTopicResultV0 {
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub partitions: Vec<DescribeShareGroupOffsetsPartitionV0>,
}

/// One group result returned by DescribeShareGroupOffsets v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsGroupResultV0 {
    pub group_id: String,
    pub topics: Vec<DescribeShareGroupOffsetsTopicResultV0>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// DescribeShareGroupOffsets v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsResponseV0 {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribeShareGroupOffsetsGroupResultV0>,
}

impl DescribeShareGroupOffsetsResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let groups = decoder
            .read_compact_array("share group offset groups", |decoder| {
                decode_group_v0(decoder)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

fn decode_group_v0(decoder: &mut Decoder<'_>) -> Result<DescribeShareGroupOffsetsGroupResultV0> {
    let group_id = decoder.read_compact_string()?;
    let topics = decoder
        .read_compact_array("share group offset topics", |decoder| {
            decode_topic_v0(decoder)
        })?
        .unwrap_or_default();
    let error_code = decoder.read_i16()?;
    let error_message = decoder.read_compact_nullable_string()?;
    decoder.read_tagged_fields()?;
    Ok(DescribeShareGroupOffsetsGroupResultV0 {
        group_id,
        topics,
        error_code,
        error_message,
    })
}

fn decode_topic_v0(decoder: &mut Decoder<'_>) -> Result<DescribeShareGroupOffsetsTopicResultV0> {
    let topic_name = decoder.read_compact_string()?;
    let topic_id = decoder.read_uuid()?;
    let partitions = decoder
        .read_compact_array("share group offset partitions", |decoder| {
            let partition = DescribeShareGroupOffsetsPartitionV0 {
                partition_index: decoder.read_i32()?,
                start_offset: decoder.read_i64()?,
                leader_epoch: decoder.read_i32()?,
                error_code: decoder.read_i16()?,
                error_message: decoder.read_compact_nullable_string()?,
            };
            decoder.read_tagged_fields()?;
            Ok(partition)
        })?
        .unwrap_or_default();
    decoder.read_tagged_fields()?;
    Ok(DescribeShareGroupOffsetsTopicResultV0 {
        topic_name,
        topic_id,
        partitions,
    })
}

/// One partition result returned by DescribeShareGroupOffsets v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsPartitionV1 {
    pub partition_index: i32,
    pub start_offset: i64,
    pub leader_epoch: i32,
    pub lag: i64,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// One topic result returned by DescribeShareGroupOffsets v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsTopicResultV1 {
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub partitions: Vec<DescribeShareGroupOffsetsPartitionV1>,
}

/// One group result returned by DescribeShareGroupOffsets v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsGroupResultV1 {
    pub group_id: String,
    pub topics: Vec<DescribeShareGroupOffsetsTopicResultV1>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// DescribeShareGroupOffsets v1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeShareGroupOffsetsResponseV1 {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribeShareGroupOffsetsGroupResultV1>,
}

impl DescribeShareGroupOffsetsResponseV1 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let groups = decoder
            .read_compact_array("share group offset groups", |decoder| {
                let group_id = decoder.read_compact_string()?;
                let topics = decoder
                    .read_compact_array("share group offset topics", |decoder| {
                        let topic_name = decoder.read_compact_string()?;
                        let topic_id = decoder.read_uuid()?;
                        let partitions = decoder
                            .read_compact_array("share group offset partitions", |decoder| {
                                let result = DescribeShareGroupOffsetsPartitionV1 {
                                    partition_index: decoder.read_i32()?,
                                    start_offset: decoder.read_i64()?,
                                    leader_epoch: decoder.read_i32()?,
                                    lag: decoder.read_i64()?,
                                    error_code: decoder.read_i16()?,
                                    error_message: decoder.read_compact_nullable_string()?,
                                };
                                decoder.read_tagged_fields()?;
                                Ok(result)
                            })?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok(DescribeShareGroupOffsetsTopicResultV1 {
                            topic_name,
                            topic_id,
                            partitions,
                        })
                    })?
                    .unwrap_or_default();
                let error_code = decoder.read_i16()?;
                let error_message = decoder.read_compact_nullable_string()?;
                decoder.read_tagged_fields()?;
                Ok(DescribeShareGroupOffsetsGroupResultV1 {
                    group_id,
                    topics,
                    error_code,
                    error_message,
                })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        DescribeShareGroupOffsetsGroup, DescribeShareGroupOffsetsGroupResultV0,
        DescribeShareGroupOffsetsPartitionV0, DescribeShareGroupOffsetsRequestV0,
        DescribeShareGroupOffsetsResponseV0, DescribeShareGroupOffsetsTopic,
        DescribeShareGroupOffsetsTopicResultV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_share_group_offsets_v0_request() {
        let request = DescribeShareGroupOffsetsRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            groups: vec![DescribeShareGroupOffsetsGroup {
                group_id: "share-orders".to_owned(),
                topics: Some(vec![DescribeShareGroupOffsetsTopic {
                    topic_name: "orders".to_owned(),
                    partitions: vec![0, 2],
                }]),
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..4], &[0, 90, 0, 0]);
        assert_eq!(API_KEY, 90);
        assert_eq!(encoded.last(), Some(&0));
    }

    #[test]
    fn decodes_describe_share_group_offsets_v0_response() -> crate::error::Result<()> {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes.write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_compact_string("share-orders")?;
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_uuid(&[7; 16]);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(0);
                    encoder.write_i64(42);
                    encoder.write_i32(3);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_i16(0);
            encoder.write_compact_nullable_string(None)?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = DescribeShareGroupOffsetsResponseV0::decode_body(&mut decoder)?;
        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(
            response.groups,
            vec![DescribeShareGroupOffsetsGroupResultV0 {
                group_id: "share-orders".to_owned(),
                topics: vec![DescribeShareGroupOffsetsTopicResultV0 {
                    topic_name: "orders".to_owned(),
                    topic_id: [7; 16],
                    partitions: vec![DescribeShareGroupOffsetsPartitionV0 {
                        partition_index: 0,
                        start_offset: 42,
                        leader_epoch: 3,
                        error_code: 0,
                        error_message: None,
                    }],
                }],
                error_code: 0,
                error_message: None,
            }]
        );
        assert!(decoder.is_empty());
        Ok(())
    }

    #[test]
    fn decodes_describe_share_group_offsets_v1_lag() -> crate::error::Result<()> {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_compact_string("share-orders")?;
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_uuid(&[9; 16]);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(1);
                    encoder.write_i64(100);
                    encoder.write_i32(4);
                    encoder.write_i64(7);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_i16(0);
            encoder.write_compact_nullable_string(None)?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = super::DescribeShareGroupOffsetsResponseV1::decode_body(&mut decoder)?;
        assert_eq!(response.groups[0].topics[0].partitions[0].lag, 7);
        assert!(decoder.is_empty());
        Ok(())
    }
}
