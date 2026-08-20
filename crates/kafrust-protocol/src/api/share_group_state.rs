use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka InitializeShareGroupState API key.
pub const INITIALIZE_API_KEY: i16 = 83;
/// Kafka ReadShareGroupState API key.
pub const READ_API_KEY: i16 = 84;
/// Kafka WriteShareGroupState API key.
pub const WRITE_API_KEY: i16 = 85;
/// Kafka DeleteShareGroupState API key.
pub const DELETE_API_KEY: i16 = 86;
/// Kafka ReadShareGroupStateSummary API key.
pub const SUMMARY_API_KEY: i16 = 87;

/// One partition in an InitializeShareGroupState request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitializeShareGroupStatePartition {
    pub partition: i32,
    pub state_epoch: i32,
    pub start_offset: i64,
}

/// One topic in an InitializeShareGroupState request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeShareGroupStateTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<InitializeShareGroupStatePartition>,
}

/// InitializeShareGroupState v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeShareGroupStateRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<InitializeShareGroupStateTopic>,
}

impl InitializeShareGroupStateRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(
            INITIALIZE_API_KEY,
            0,
            self.correlation_id,
            self.client_id.clone(),
            &self.group_id,
            &self.topics,
            |encoder, topic| {
                encoder.write_uuid(&topic.topic_id);
                encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                    encoder.write_i32(partition.partition);
                    encoder.write_i32(partition.state_epoch);
                    encoder.write_i64(partition.start_offset);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            },
        )
    }
}

/// One partition in a ReadShareGroupState or ReadShareGroupStateSummary request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadShareGroupStatePartition {
    pub partition: i32,
    pub leader_epoch: i32,
}

/// One topic in a ReadShareGroupState or ReadShareGroupStateSummary request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ReadShareGroupStatePartition>,
}

fn encode_read_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    group_id: &str,
    topics: &[ReadShareGroupStateTopic],
) -> Result<Vec<u8>> {
    encode_request(
        api_key,
        api_version,
        correlation_id,
        client_id,
        group_id,
        topics,
        |encoder, topic| {
            encoder.write_uuid(&topic.topic_id);
            encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                encoder.write_i32(partition.partition);
                encoder.write_i32(partition.leader_epoch);
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        },
    )
}

fn encode_request<T>(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    group_id: &str,
    topics: &[T],
    mut encode_topic: impl FnMut(&mut Encoder, &T) -> Result<()>,
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_string(group_id)?;
    encoder.write_compact_array(Some(topics), |encoder, topic| encode_topic(encoder, topic))?;
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

/// ReadShareGroupState v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<ReadShareGroupStateTopic>,
}

impl ReadShareGroupStateRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_read_request(
            READ_API_KEY,
            0,
            self.correlation_id,
            self.client_id.clone(),
            &self.group_id,
            &self.topics,
        )
    }
}

/// One state batch returned by ReadShareGroupState.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupStateBatch {
    pub first_offset: i64,
    pub last_offset: i64,
    pub delivery_state: i8,
    pub delivery_count: i16,
}

/// One partition result returned by an Initialize, Write, or Delete operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStatePartitionResult {
    pub partition: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// One topic result returned by an Initialize, Write, or Delete operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateTopicResult {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ShareGroupStatePartitionResult>,
}

fn decode_state_results(
    decoder: &mut Decoder<'_>,
    kind: &'static str,
) -> Result<Vec<ShareGroupStateTopicResult>> {
    let results = decoder
        .read_compact_array(kind, |decoder| {
            let topic_id = decoder.read_uuid()?;
            let partitions = decoder
                .read_compact_array("share group state result partitions", |decoder| {
                    let result = ShareGroupStatePartitionResult {
                        partition: decoder.read_i32()?,
                        error_code: decoder.read_i16()?,
                        error_message: decoder.read_compact_nullable_string()?,
                    };
                    decoder.read_tagged_fields()?;
                    Ok(result)
                })?
                .unwrap_or_default();
            decoder.read_tagged_fields()?;
            Ok(ShareGroupStateTopicResult {
                topic_id,
                partitions,
            })
        })?
        .unwrap_or_default();
    Ok(results)
}

/// Response body shared by InitializeShareGroupState, WriteShareGroupState,
/// and DeleteShareGroupState v0/v1 responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateResultResponse {
    pub results: Vec<ShareGroupStateTopicResult>,
}

impl ShareGroupStateResultResponse {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let results = decode_state_results(decoder, "share group state results")?;
        decoder.read_tagged_fields()?;
        Ok(Self { results })
    }
}

/// InitializeShareGroupState v0 response.
pub type InitializeShareGroupStateResponseV0 = ShareGroupStateResultResponse;

/// One partition result returned by ReadShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStatePartitionResult {
    pub partition: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub state_epoch: i32,
    pub start_offset: i64,
    pub state_batches: Vec<ShareGroupStateBatch>,
}

/// One topic result returned by ReadShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateTopicResult {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ReadShareGroupStatePartitionResult>,
}

/// ReadShareGroupState v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateResponseV0 {
    pub results: Vec<ReadShareGroupStateTopicResult>,
}

impl ReadShareGroupStateResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            results: decode_read_results(decoder, "read share group state results")?,
        })
    }
}

fn decode_read_results(
    decoder: &mut Decoder<'_>,
    kind: &'static str,
) -> Result<Vec<ReadShareGroupStateTopicResult>> {
    let results = decoder
        .read_compact_array(kind, |decoder| {
            let topic_id = decoder.read_uuid()?;
            let partitions = decoder
                .read_compact_array("read share group state partitions", |decoder| {
                    let partition = ReadShareGroupStatePartitionResult {
                        partition: decoder.read_i32()?,
                        error_code: decoder.read_i16()?,
                        error_message: decoder.read_compact_nullable_string()?,
                        state_epoch: decoder.read_i32()?,
                        start_offset: decoder.read_i64()?,
                        state_batches: decoder
                            .read_compact_array("share group state batches", |decoder| {
                                let batch = ShareGroupStateBatch {
                                    first_offset: decoder.read_i64()?,
                                    last_offset: decoder.read_i64()?,
                                    delivery_state: decoder.read_i8()?,
                                    delivery_count: decoder.read_i16()?,
                                };
                                decoder.read_tagged_fields()?;
                                Ok(batch)
                            })?
                            .unwrap_or_default(),
                    };
                    decoder.read_tagged_fields()?;
                    Ok(partition)
                })?
                .unwrap_or_default();
            decoder.read_tagged_fields()?;
            Ok(ReadShareGroupStateTopicResult {
                topic_id,
                partitions,
            })
        })?
        .unwrap_or_default();
    decoder.read_tagged_fields()?;
    Ok(results)
}

/// One partition in a WriteShareGroupState v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteShareGroupStatePartitionV0 {
    pub partition: i32,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: i64,
    pub state_batches: Vec<ShareGroupStateBatch>,
}

/// One topic in a WriteShareGroupState v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteShareGroupStateTopicV0 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<WriteShareGroupStatePartitionV0>,
}

/// One partition in a WriteShareGroupState v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteShareGroupStatePartitionV1 {
    pub partition: i32,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: i64,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<ShareGroupStateBatch>,
}

/// One topic in a WriteShareGroupState v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteShareGroupStateTopicV1 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<WriteShareGroupStatePartitionV1>,
}

fn encode_write_partition(
    encoder: &mut Encoder,
    partition: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: Option<i32>,
    state_batches: &[ShareGroupStateBatch],
) -> Result<()> {
    encoder.write_i32(partition);
    encoder.write_i32(state_epoch);
    encoder.write_i32(leader_epoch);
    encoder.write_i64(start_offset);
    if let Some(delivery_complete_count) = delivery_complete_count {
        encoder.write_i32(delivery_complete_count);
    }
    encoder.write_compact_array(Some(state_batches), |encoder, batch| {
        encoder.write_i64(batch.first_offset);
        encoder.write_i64(batch.last_offset);
        encoder.write_i8(batch.delivery_state);
        encoder.write_i16(batch.delivery_count);
        encoder.write_empty_tagged_fields();
        Ok(())
    })?;
    encoder.write_empty_tagged_fields();
    Ok(())
}

/// WriteShareGroupState v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteShareGroupStateRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<WriteShareGroupStateTopicV0>,
}

impl WriteShareGroupStateRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(
            WRITE_API_KEY,
            0,
            self.correlation_id,
            self.client_id.clone(),
            &self.group_id,
            &self.topics,
            |encoder, topic| {
                encoder.write_uuid(&topic.topic_id);
                encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                    encode_write_partition(
                        encoder,
                        partition.partition,
                        partition.state_epoch,
                        partition.leader_epoch,
                        partition.start_offset,
                        None,
                        &partition.state_batches,
                    )
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            },
        )
    }
}

/// WriteShareGroupState v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteShareGroupStateRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<WriteShareGroupStateTopicV1>,
}

impl WriteShareGroupStateRequestV1 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(
            WRITE_API_KEY,
            1,
            self.correlation_id,
            self.client_id.clone(),
            &self.group_id,
            &self.topics,
            |encoder, topic| {
                encoder.write_uuid(&topic.topic_id);
                encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                    encode_write_partition(
                        encoder,
                        partition.partition,
                        partition.state_epoch,
                        partition.leader_epoch,
                        partition.start_offset,
                        Some(partition.delivery_complete_count),
                        &partition.state_batches,
                    )
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            },
        )
    }
}

/// WriteShareGroupState v0 response.
pub type WriteShareGroupStateResponseV0 = ShareGroupStateResultResponse;
/// WriteShareGroupState v1 response.
pub type WriteShareGroupStateResponseV1 = ShareGroupStateResultResponse;

/// One topic in a DeleteShareGroupState request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupStateTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<i32>,
}

/// DeleteShareGroupState v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupStateRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<DeleteShareGroupStateTopic>,
}

impl DeleteShareGroupStateRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(
            DELETE_API_KEY,
            0,
            self.correlation_id,
            self.client_id.clone(),
            &self.group_id,
            &self.topics,
            |encoder, topic| {
                encoder.write_uuid(&topic.topic_id);
                encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                    encoder.write_i32(*partition);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            },
        )
    }
}

/// DeleteShareGroupState v0 response.
pub type DeleteShareGroupStateResponseV0 = ShareGroupStateResultResponse;

/// ReadShareGroupStateSummary v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<ReadShareGroupStateTopic>,
}

impl ReadShareGroupStateSummaryRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_read_request(
            SUMMARY_API_KEY,
            0,
            self.correlation_id,
            self.client_id.clone(),
            &self.group_id,
            &self.topics,
        )
    }
}

/// ReadShareGroupStateSummary v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<ReadShareGroupStateTopic>,
}

impl ReadShareGroupStateSummaryRequestV1 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_read_request(
            SUMMARY_API_KEY,
            1,
            self.correlation_id,
            self.client_id.clone(),
            &self.group_id,
            &self.topics,
        )
    }
}

/// One partition result returned by ReadShareGroupStateSummary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryPartitionResult {
    pub partition: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: i64,
    pub delivery_complete_count: Option<i32>,
}

/// One topic result returned by ReadShareGroupStateSummary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryTopicResult {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ReadShareGroupStateSummaryPartitionResult>,
}

fn decode_summary_results(
    decoder: &mut Decoder<'_>,
    api_version: i16,
) -> Result<Vec<ReadShareGroupStateSummaryTopicResult>> {
    let results = decoder
        .read_compact_array("read share group state summary results", |decoder| {
            let topic_id = decoder.read_uuid()?;
            let partitions = decoder
                .read_compact_array("share group state summary partitions", |decoder| {
                    let partition = ReadShareGroupStateSummaryPartitionResult {
                        partition: decoder.read_i32()?,
                        error_code: decoder.read_i16()?,
                        error_message: decoder.read_compact_nullable_string()?,
                        state_epoch: decoder.read_i32()?,
                        leader_epoch: decoder.read_i32()?,
                        start_offset: decoder.read_i64()?,
                        delivery_complete_count: (api_version >= 1)
                            .then(|| decoder.read_i32())
                            .transpose()?,
                    };
                    decoder.read_tagged_fields()?;
                    Ok(partition)
                })?
                .unwrap_or_default();
            decoder.read_tagged_fields()?;
            Ok(ReadShareGroupStateSummaryTopicResult {
                topic_id,
                partitions,
            })
        })?
        .unwrap_or_default();
    decoder.read_tagged_fields()?;
    Ok(results)
}

/// ReadShareGroupStateSummary v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryResponseV0 {
    pub results: Vec<ReadShareGroupStateSummaryTopicResult>,
}

impl ReadShareGroupStateSummaryResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            results: decode_summary_results(decoder, 0)?,
        })
    }
}

/// ReadShareGroupStateSummary v1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryResponseV1 {
    pub results: Vec<ReadShareGroupStateSummaryTopicResult>,
}

impl ReadShareGroupStateSummaryResponseV1 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            results: decode_summary_results(decoder, 1)?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::codec::{Decoder, Encoder};

    fn read_header(decoder: &mut Decoder<'_>, api_key: i16, api_version: i16) {
        assert_eq!(decoder.read_i16().unwrap(), api_key);
        assert_eq!(decoder.read_i16().unwrap(), api_version);
        assert_eq!(decoder.read_i32().unwrap(), 7);
        assert_eq!(
            decoder.read_nullable_string().unwrap().as_deref(),
            Some("kafrust")
        );
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
    }

    fn batch() -> ShareGroupStateBatch {
        ShareGroupStateBatch {
            first_offset: 10,
            last_offset: 12,
            delivery_state: 2,
            delivery_count: 3,
        }
    }

    #[test]
    fn encodes_initialize_share_group_state_v0() {
        let request = InitializeShareGroupStateRequestV0 {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            group_id: "share-orders".to_owned(),
            topics: vec![InitializeShareGroupStateTopic {
                topic_id: [1; 16],
                partitions: vec![InitializeShareGroupStatePartition {
                    partition: 2,
                    state_epoch: 4,
                    start_offset: 10,
                }],
            }],
        };
        let encoded = request.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        read_header(&mut decoder, INITIALIZE_API_KEY, 0);
        assert_eq!(decoder.read_compact_string().unwrap(), "share-orders");
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_uuid().unwrap(), [1; 16]);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_i32().unwrap(), 2);
        assert_eq!(decoder.read_i32().unwrap(), 4);
        assert_eq!(decoder.read_i64().unwrap(), 10);
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_write_v1_delivery_complete_count_without_v0_field() {
        let topic = WriteShareGroupStateTopicV1 {
            topic_id: [2; 16],
            partitions: vec![WriteShareGroupStatePartitionV1 {
                partition: 1,
                state_epoch: 3,
                leader_epoch: 4,
                start_offset: 5,
                delivery_complete_count: 6,
                state_batches: vec![batch()],
            }],
        };
        let request = WriteShareGroupStateRequestV1 {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            group_id: "share-orders".to_owned(),
            topics: vec![topic],
        };
        let encoded = request.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        read_header(&mut decoder, WRITE_API_KEY, 1);
        assert_eq!(decoder.read_compact_string().unwrap(), "share-orders");
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_uuid().unwrap(), [2; 16]);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_i32().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 3);
        assert_eq!(decoder.read_i32().unwrap(), 4);
        assert_eq!(decoder.read_i64().unwrap(), 5);
        assert_eq!(decoder.read_i32().unwrap(), 6);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_i64().unwrap(), 10);
        assert_eq!(decoder.read_i64().unwrap(), 12);
        assert_eq!(decoder.read_i8().unwrap(), 2);
        assert_eq!(decoder.read_i16().unwrap(), 3);
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_read_and_summary_results_by_version() -> crate::error::Result<()> {
        let mut body = Encoder::new();
        body.write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_uuid(&[3; 16]);
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i32(1);
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_i32(2);
                encoder.write_i64(10);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i64(10);
                    encoder.write_i64(12);
                    encoder.write_i8(0);
                    encoder.write_i16(1);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        body.write_empty_tagged_fields();
        let encoded = body.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ReadShareGroupStateResponseV0::decode_body(&mut decoder)?;
        assert_eq!(
            response.results[0].partitions[0].state_batches[0],
            ShareGroupStateBatch {
                first_offset: 10,
                last_offset: 12,
                delivery_state: 0,
                delivery_count: 1,
            }
        );
        assert!(decoder.is_empty());

        let mut summary = Encoder::new();
        summary.write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_uuid(&[4; 16]);
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i32(1);
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_i32(2);
                encoder.write_i32(3);
                encoder.write_i64(10);
                encoder.write_i32(9);
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        summary.write_empty_tagged_fields();
        let encoded = summary.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ReadShareGroupStateSummaryResponseV1::decode_body(&mut decoder)?;
        assert_eq!(
            response.results[0].partitions[0].delivery_complete_count,
            Some(9)
        );
        assert!(decoder.is_empty());
        Ok(())
    }

    #[test]
    fn encodes_delete_share_group_state_v0() {
        let request = DeleteShareGroupStateRequestV0 {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            group_id: "share-orders".to_owned(),
            topics: vec![DeleteShareGroupStateTopic {
                topic_id: [5; 16],
                partitions: vec![0, 2],
            }],
        };
        let encoded = request.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        read_header(&mut decoder, DELETE_API_KEY, 0);
        assert_eq!(decoder.read_compact_string().unwrap(), "share-orders");
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_uuid().unwrap(), [5; 16]);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 3);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert_eq!(decoder.read_i32().unwrap(), 2);
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.read_tagged_fields().unwrap().is_empty());
        assert!(decoder.is_empty());
    }
}
