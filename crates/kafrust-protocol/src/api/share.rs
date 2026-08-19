use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::header::RequestHeader;

/// Kafka ShareGroupHeartbeat API key.
pub const SHARE_GROUP_HEARTBEAT_API_KEY: i16 = 76;
/// Kafka ShareFetch API key.
pub const SHARE_FETCH_API_KEY: i16 = 78;
/// Kafka ShareAcknowledge API key.
pub const SHARE_ACKNOWLEDGE_API_KEY: i16 = 79;

/// A topic and its partition indexes in a share-group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareTopicPartitionsV1 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<i32>,
}

impl ShareTopicPartitionsV1 {
    #[cfg(test)]
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_uuid(&self.topic_id);
        encoder.write_compact_array(Some(&self.partitions), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_id = decoder.read_uuid()?;
        let partitions = decoder
            .read_compact_array("share topic partitions", |decoder| decoder.read_i32())?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

/// ShareGroupHeartbeat v1 request.
///
/// Version 1 is the stable KIP-932 wire shape. Kafka 4.0's early-access v0 is
/// intentionally not exposed here because it was removed in Kafka 4.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub rack_id: Option<String>,
    pub subscribed_topic_names: Option<Vec<String>>,
}

impl ShareGroupHeartbeatRequestV1 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: SHARE_GROUP_HEARTBEAT_API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.group_id)?;
        encoder.write_compact_string(&self.member_id)?;
        encoder.write_i32(self.member_epoch);
        encoder.write_compact_nullable_string(self.rack_id.as_deref())?;
        encoder.write_compact_array(self.subscribed_topic_names.as_deref(), |encoder, topic| {
            encoder.write_compact_string(topic)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// Assignment returned by ShareGroupHeartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatAssignmentV1 {
    pub topic_partitions: Vec<ShareTopicPartitionsV1>,
}

impl ShareGroupHeartbeatAssignmentV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_partitions = decoder
            .read_compact_array("share heartbeat assignment", ShareTopicPartitionsV1::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self { topic_partitions })
    }
}

/// ShareGroupHeartbeat v1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupHeartbeatResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub assignment: Option<ShareGroupHeartbeatAssignmentV1>,
}

impl ShareGroupHeartbeatResponseV1 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let member_id = decoder.read_compact_nullable_string()?;
        let member_epoch = decoder.read_i32()?;
        let heartbeat_interval_ms = decoder.read_i32()?;
        let assignment = match decoder.read_i8()? {
            -1 => None,
            1 => Some(ShareGroupHeartbeatAssignmentV1::decode(decoder)?),
            marker => return Err(Error::InvalidNullableStruct(marker)),
        };
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            member_id,
            member_epoch,
            heartbeat_interval_ms,
            assignment,
        })
    }
}

/// One batch of records acknowledged by a share consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgementBatchV1 {
    pub first_offset: i64,
    pub last_offset: i64,
    /// Kafka acknowledgement types: 0 gap, 1 accept, 2 release, 3 reject.
    pub acknowledgement_types: Vec<i8>,
}

impl ShareAcknowledgementBatchV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i64(self.first_offset);
        encoder.write_i64(self.last_offset);
        encoder.write_compact_array(
            Some(&self.acknowledgement_types),
            |encoder, acknowledgement_type| {
                encoder.write_i8(*acknowledgement_type);
                Ok(())
            },
        )?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    #[cfg(test)]
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let first_offset = decoder.read_i64()?;
        let last_offset = decoder.read_i64()?;
        let acknowledgement_types = decoder
            .read_compact_array("share acknowledgement types", |decoder| decoder.read_i8())?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            first_offset,
            last_offset,
            acknowledgement_types,
        })
    }
}

/// Acknowledgement batches for one share-group partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchPartitionV1 {
    pub partition_index: i32,
    pub acknowledgement_batches: Vec<ShareAcknowledgementBatchV1>,
}

impl ShareFetchPartitionV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_compact_array(Some(&self.acknowledgement_batches), |encoder, batch| {
            batch.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

/// One topic in a ShareFetch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchTopicV1 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ShareFetchPartitionV1>,
}

impl ShareFetchTopicV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_uuid(&self.topic_id);
        encoder.write_compact_array(Some(&self.partitions), |encoder, partition| {
            partition.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

/// A topic and partitions to remove from a share fetch session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareForgottenTopicV1 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<i32>,
}

impl ShareForgottenTopicV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_uuid(&self.topic_id);
        encoder.write_compact_array(Some(&self.partitions), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

/// ShareFetch v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: Option<String>,
    pub member_id: Option<String>,
    pub share_session_epoch: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_records: i32,
    pub batch_size: i32,
    pub topics: Vec<ShareFetchTopicV1>,
    pub forgotten_topics: Vec<ShareForgottenTopicV1>,
}

impl ShareFetchRequestV1 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_share_fetch_request(
            1,
            self.correlation_id,
            self.client_id.clone(),
            self.group_id.clone(),
            self.member_id.clone(),
            self.share_session_epoch,
            self.max_wait_ms,
            self.min_bytes,
            self.max_bytes,
            self.max_records,
            self.batch_size,
            None,
            false,
            &self.topics,
            &self.forgotten_topics,
        )
    }
}

/// ShareFetch v2 request with KIP-1206 acquisition and KIP-1222 renewal fields.
///
/// The response schema remains the ShareFetch v1 response shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: Option<String>,
    pub member_id: Option<String>,
    pub share_session_epoch: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_records: i32,
    pub batch_size: i32,
    /// KIP-1206 mode: `0` for batch-optimized or `1` for record-limit.
    pub share_acquire_mode: i8,
    /// KIP-1222 renew marker. KIP-1206 callers must send `false`.
    pub is_renew_ack: bool,
    pub topics: Vec<ShareFetchTopicV1>,
    pub forgotten_topics: Vec<ShareForgottenTopicV1>,
}

impl ShareFetchRequestV2 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_share_fetch_request(
            2,
            self.correlation_id,
            self.client_id.clone(),
            self.group_id.clone(),
            self.member_id.clone(),
            self.share_session_epoch,
            self.max_wait_ms,
            self.min_bytes,
            self.max_bytes,
            self.max_records,
            self.batch_size,
            Some(self.share_acquire_mode),
            self.is_renew_ack,
            &self.topics,
            &self.forgotten_topics,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_share_fetch_request(
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    group_id: Option<String>,
    member_id: Option<String>,
    share_session_epoch: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    batch_size: i32,
    share_acquire_mode: Option<i8>,
    is_renew_ack: bool,
    topics: &[ShareFetchTopicV1],
    forgotten_topics: &[ShareForgottenTopicV1],
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: SHARE_FETCH_API_KEY,
        api_version,
        correlation_id,
        client_id,
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_nullable_string(group_id.as_deref())?;
    encoder.write_compact_nullable_string(member_id.as_deref())?;
    encoder.write_i32(share_session_epoch);
    encoder.write_i32(max_wait_ms);
    encoder.write_i32(min_bytes);
    encoder.write_i32(max_bytes);
    encoder.write_i32(max_records);
    encoder.write_i32(batch_size);
    if let Some(share_acquire_mode) = share_acquire_mode {
        encoder.write_i8(share_acquire_mode);
        encoder.write_bool(is_renew_ack);
    }
    encoder.write_compact_array(Some(topics), |encoder, topic| topic.encode(encoder))?;
    encoder.write_compact_array(Some(forgotten_topics), |encoder, topic| {
        topic.encode(encoder)
    })?;
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

/// Current leader information returned for a share partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareLeaderIdAndEpochV1 {
    pub leader_id: i32,
    pub leader_epoch: i32,
}

impl ShareLeaderIdAndEpochV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let leader_id = decoder.read_i32()?;
        let leader_epoch = decoder.read_i32()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            leader_id,
            leader_epoch,
        })
    }
}

/// Acquired record range returned by ShareFetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcquiredRecordsV1 {
    pub first_offset: i64,
    pub last_offset: i64,
    pub delivery_count: i16,
}

impl ShareAcquiredRecordsV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let first_offset = decoder.read_i64()?;
        let last_offset = decoder.read_i64()?;
        let delivery_count = decoder.read_i16()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            first_offset,
            last_offset,
            delivery_count,
        })
    }
}

/// One partition returned by ShareFetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchPartitionResponseV1 {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub acknowledgement_error_code: i16,
    pub acknowledgement_error_message: Option<String>,
    pub current_leader: ShareLeaderIdAndEpochV1,
    /// Raw Kafka record bytes. The value is nullable when no records were fetched.
    pub records: Option<Vec<u8>>,
    pub acquired_records: Vec<ShareAcquiredRecordsV1>,
}

impl ShareFetchPartitionResponseV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let partition_index = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let acknowledgement_error_code = decoder.read_i16()?;
        let acknowledgement_error_message = decoder.read_compact_nullable_string()?;
        let current_leader = ShareLeaderIdAndEpochV1::decode(decoder)?;
        let records = decoder.read_compact_nullable_bytes()?;
        let acquired_records = decoder
            .read_compact_array("share acquired records", ShareAcquiredRecordsV1::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            partition_index,
            error_code,
            error_message,
            acknowledgement_error_code,
            acknowledgement_error_message,
            current_leader,
            records,
            acquired_records,
        })
    }
}

/// One topic returned by ShareFetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchTopicResponseV1 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ShareFetchPartitionResponseV1>,
}

impl ShareFetchTopicResponseV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_id = decoder.read_uuid()?;
        let partitions = decoder
            .read_compact_array(
                "share fetch partitions",
                ShareFetchPartitionResponseV1::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

/// Broker endpoint returned when a share partition leader changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareNodeEndpointV1 {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

impl ShareNodeEndpointV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let node_id = decoder.read_i32()?;
        let host = decoder.read_compact_string()?;
        let port = decoder.read_i32()?;
        let rack = decoder.read_compact_nullable_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}

/// ShareFetch v1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareFetchResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub acquisition_lock_timeout_ms: i32,
    pub responses: Vec<ShareFetchTopicResponseV1>,
    pub node_endpoints: Vec<ShareNodeEndpointV1>,
}

impl ShareFetchResponseV1 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let acquisition_lock_timeout_ms = decoder.read_i32()?;
        let responses = decoder
            .read_compact_array("share fetch responses", ShareFetchTopicResponseV1::decode)?
            .unwrap_or_default();
        let node_endpoints = decoder
            .read_compact_array("share node endpoints", ShareNodeEndpointV1::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            acquisition_lock_timeout_ms,
            responses,
            node_endpoints,
        })
    }
}

/// One topic and its acknowledgement batches in a ShareAcknowledge request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeTopicV1 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ShareAcknowledgePartitionV1>,
}

impl ShareAcknowledgeTopicV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_uuid(&self.topic_id);
        encoder.write_compact_array(Some(&self.partitions), |encoder, partition| {
            partition.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

/// One partition's acknowledgement batches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgePartitionV1 {
    pub partition_index: i32,
    pub acknowledgement_batches: Vec<ShareAcknowledgementBatchV1>,
}

impl ShareAcknowledgePartitionV1 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_compact_array(Some(&self.acknowledgement_batches), |encoder, batch| {
            batch.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

/// ShareAcknowledge v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: Option<String>,
    pub member_id: Option<String>,
    pub share_session_epoch: i32,
    pub topics: Vec<ShareAcknowledgeTopicV1>,
}

impl ShareAcknowledgeRequestV1 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_share_acknowledge_request(ShareAcknowledgeRequestParts {
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.as_deref(),
            group_id: self.group_id.as_deref(),
            member_id: self.member_id.as_deref(),
            share_session_epoch: self.share_session_epoch,
            is_renew_ack: false,
            topics: &self.topics,
        })
    }
}

/// ShareAcknowledge v2 request with KIP-1222 renewal support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: Option<String>,
    pub member_id: Option<String>,
    pub share_session_epoch: i32,
    /// True when one or more acknowledgement batches contain `Renew` (4).
    pub is_renew_ack: bool,
    pub topics: Vec<ShareAcknowledgeTopicV1>,
}

impl ShareAcknowledgeRequestV2 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_share_acknowledge_request(ShareAcknowledgeRequestParts {
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.as_deref(),
            group_id: self.group_id.as_deref(),
            member_id: self.member_id.as_deref(),
            share_session_epoch: self.share_session_epoch,
            is_renew_ack: self.is_renew_ack,
            topics: &self.topics,
        })
    }
}

struct ShareAcknowledgeRequestParts<'a> {
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&'a str>,
    group_id: Option<&'a str>,
    member_id: Option<&'a str>,
    share_session_epoch: i32,
    is_renew_ack: bool,
    topics: &'a [ShareAcknowledgeTopicV1],
}

fn encode_share_acknowledge_request(parts: ShareAcknowledgeRequestParts<'_>) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: SHARE_ACKNOWLEDGE_API_KEY,
        api_version: parts.api_version,
        correlation_id: parts.correlation_id,
        client_id: parts.client_id.map(str::to_owned),
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_nullable_string(parts.group_id)?;
    encoder.write_compact_nullable_string(parts.member_id)?;
    encoder.write_i32(parts.share_session_epoch);
    if parts.api_version >= 2 {
        encoder.write_bool(parts.is_renew_ack);
    }
    encoder.write_compact_array(Some(parts.topics), |encoder, topic| topic.encode(encoder))?;
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

/// One partition result returned by ShareAcknowledge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgePartitionResponseV1 {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub current_leader: ShareLeaderIdAndEpochV1,
}

impl ShareAcknowledgePartitionResponseV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let partition_index = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let current_leader = ShareLeaderIdAndEpochV1::decode(decoder)?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            partition_index,
            error_code,
            error_message,
            current_leader,
        })
    }
}

/// One topic result returned by ShareAcknowledge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeTopicResponseV1 {
    pub topic_id: [u8; 16],
    pub partitions: Vec<ShareAcknowledgePartitionResponseV1>,
}

impl ShareAcknowledgeTopicResponseV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_id = decoder.read_uuid()?;
        let partitions = decoder
            .read_compact_array(
                "share acknowledgement partitions",
                ShareAcknowledgePartitionResponseV1::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

/// ShareAcknowledge v1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub responses: Vec<ShareAcknowledgeTopicResponseV1>,
    pub node_endpoints: Vec<ShareNodeEndpointV1>,
}

impl ShareAcknowledgeResponseV1 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let responses = decoder
            .read_compact_array(
                "share acknowledgement responses",
                ShareAcknowledgeTopicResponseV1::decode,
            )?
            .unwrap_or_default();
        let node_endpoints = decoder
            .read_compact_array("share node endpoints", ShareNodeEndpointV1::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            responses,
            node_endpoints,
        })
    }
}

/// ShareAcknowledge v2 response with the current acquisition lock timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAcknowledgeResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub acquisition_lock_timeout_ms: i32,
    pub responses: Vec<ShareAcknowledgeTopicResponseV1>,
    pub node_endpoints: Vec<ShareNodeEndpointV1>,
}

impl ShareAcknowledgeResponseV2 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let acquisition_lock_timeout_ms = decoder.read_i32()?;
        let responses = decoder
            .read_compact_array(
                "share acknowledgement responses",
                ShareAcknowledgeTopicResponseV1::decode,
            )?
            .unwrap_or_default();
        let node_endpoints = decoder
            .read_compact_array("share node endpoints", ShareNodeEndpointV1::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            acquisition_lock_timeout_ms,
            responses,
            node_endpoints,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_share_group_heartbeat_v1_request() {
        let request = ShareGroupHeartbeatRequestV1 {
            correlation_id: 11,
            client_id: Some("kafrust".to_owned()),
            group_id: "share-orders".to_owned(),
            member_id: "member-1".to_owned(),
            member_epoch: 3,
            rack_id: Some("rack-a".to_owned()),
            subscribed_topic_names: Some(vec!["orders".to_owned()]),
        };

        let encoded = request.encode().unwrap();
        let mut decoder = Decoder::new(&encoded);
        assert_eq!(decoder.read_i16().unwrap(), SHARE_GROUP_HEARTBEAT_API_KEY);
        assert_eq!(decoder.read_i16().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 11);
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("kafrust".to_owned())
        );
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert_eq!(decoder.read_compact_string().unwrap(), "share-orders");
        assert_eq!(decoder.read_compact_string().unwrap(), "member-1");
        assert_eq!(decoder.read_i32().unwrap(), 3);
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("rack-a".to_owned())
        );
        assert_eq!(
            decoder
                .read_compact_array("topics", |decoder| decoder.read_compact_string())
                .unwrap(),
            Some(vec!["orders".to_owned()])
        );
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_share_group_heartbeat_v1_assignment() {
        let mut bytes = Encoder::new();
        bytes.write_i32(9);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes
            .write_compact_nullable_string(Some("member-1"))
            .unwrap();
        bytes.write_i32(4);
        bytes.write_i32(2500);
        bytes.write_i8(1);
        bytes
            .write_compact_array(
                Some(&[ShareTopicPartitionsV1 {
                    topic_id: [7; 16],
                    partitions: vec![0, 2],
                }]),
                |encoder, assignment| assignment.encode(encoder),
            )
            .unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ShareGroupHeartbeatResponseV1::decode_body(&mut decoder).unwrap();
        assert_eq!(response.member_id.as_deref(), Some("member-1"));
        assert_eq!(response.member_epoch, 4);
        assert_eq!(
            response.assignment.unwrap().topic_partitions[0].partitions,
            vec![0, 2]
        );
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_share_fetch_v1_request_with_acknowledgements() {
        let request = ShareFetchRequestV1 {
            correlation_id: 22,
            client_id: Some("kafrust".to_owned()),
            group_id: Some("share-orders".to_owned()),
            member_id: Some("member-1".to_owned()),
            share_session_epoch: 2,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1024,
            max_records: 100,
            batch_size: 10,
            topics: vec![ShareFetchTopicV1 {
                topic_id: [3; 16],
                partitions: vec![ShareFetchPartitionV1 {
                    partition_index: 0,
                    acknowledgement_batches: vec![ShareAcknowledgementBatchV1 {
                        first_offset: 10,
                        last_offset: 12,
                        acknowledgement_types: vec![1, 2, 3],
                    }],
                }],
            }],
            forgotten_topics: vec![ShareForgottenTopicV1 {
                topic_id: [4; 16],
                partitions: vec![1],
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 78, 0, 1]);
        assert!(encoded.ends_with(&[0]));
        let mut decoder = Decoder::new(&encoded[4..]);
        assert_eq!(decoder.read_i32().unwrap(), 22);
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("kafrust".to_owned())
        );
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("share-orders".to_owned())
        );
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("member-1".to_owned())
        );
        assert_eq!(decoder.read_i32().unwrap(), 2);
        assert_eq!(decoder.read_i32().unwrap(), 500);
        assert_eq!(decoder.read_i32().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 1024);
        assert_eq!(decoder.read_i32().unwrap(), 100);
        assert_eq!(decoder.read_i32().unwrap(), 10);
        assert!(decoder
            .read_compact_array("topics", |decoder| {
                let topic_id = decoder.read_uuid()?;
                let partitions = decoder.read_compact_array("partitions", |decoder| {
                    let partition_index = decoder.read_i32()?;
                    let batches = decoder
                        .read_compact_array("batches", ShareAcknowledgementBatchV1::decode)?;
                    decoder.read_tagged_fields()?;
                    Ok((partition_index, batches))
                })?;
                decoder.read_tagged_fields()?;
                Ok((topic_id, partitions))
            })
            .unwrap()
            .is_some());
    }

    #[test]
    fn encodes_share_fetch_v2_request_with_record_limit_mode() {
        let request = ShareFetchRequestV2 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_id: Some("share-orders".to_owned()),
            member_id: Some("member-1".to_owned()),
            share_session_epoch: 3,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1024,
            max_records: 6,
            batch_size: 2,
            share_acquire_mode: 1,
            is_renew_ack: false,
            topics: Vec::new(),
            forgotten_topics: Vec::new(),
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 78, 0, 2]);
        let mut decoder = Decoder::new(&encoded[4..]);
        assert_eq!(decoder.read_i32().unwrap(), 23);
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("kafrust".to_owned())
        );
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("share-orders".to_owned())
        );
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("member-1".to_owned())
        );
        assert_eq!(decoder.read_i32().unwrap(), 3);
        assert_eq!(decoder.read_i32().unwrap(), 500);
        assert_eq!(decoder.read_i32().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 1024);
        assert_eq!(decoder.read_i32().unwrap(), 6);
        assert_eq!(decoder.read_i32().unwrap(), 2);
        assert_eq!(decoder.read_i8().unwrap(), 1);
        assert!(!decoder.read_bool().unwrap());
        assert!(decoder
            .read_compact_array("topics", |_decoder| Ok::<(), Error>(()))
            .unwrap()
            .is_some());
        assert!(decoder
            .read_compact_array("forgotten topics", |_decoder| Ok::<(), Error>(()))
            .unwrap()
            .is_some());
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_share_fetch_v2_renew_request_with_zero_fetch_limits() {
        let request = ShareFetchRequestV2 {
            correlation_id: 24,
            client_id: Some("kafrust".to_owned()),
            group_id: Some("share-orders".to_owned()),
            member_id: Some("member-1".to_owned()),
            share_session_epoch: 7,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0,
            max_records: 0,
            batch_size: 0,
            share_acquire_mode: 0,
            is_renew_ack: true,
            topics: Vec::new(),
            forgotten_topics: Vec::new(),
        };

        let encoded = request.encode().unwrap();
        let mut decoder = Decoder::new(&encoded[4..]);
        assert_eq!(decoder.read_i32().unwrap(), 24);
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("kafrust".to_owned())
        );
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("share-orders".to_owned())
        );
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("member-1".to_owned())
        );
        assert_eq!(decoder.read_i32().unwrap(), 7);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i8().unwrap(), 0);
        assert!(decoder.read_bool().unwrap());
        assert!(decoder
            .read_compact_array("topics", |_decoder| Ok::<(), Error>(()))
            .unwrap()
            .is_some());
        assert!(decoder
            .read_compact_array("forgotten topics", |_decoder| Ok::<(), Error>(()))
            .unwrap()
            .is_some());
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_share_fetch_v1_response_and_preserves_record_bytes() {
        let mut bytes = Encoder::new();
        bytes.write_i32(7);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_i32(30_000);
        bytes
            .write_compact_array(
                Some(&[ShareFetchTopicResponseV1 {
                    topic_id: [5; 16],
                    partitions: vec![ShareFetchPartitionResponseV1 {
                        partition_index: 0,
                        error_code: 0,
                        error_message: None,
                        acknowledgement_error_code: 0,
                        acknowledgement_error_message: None,
                        current_leader: ShareLeaderIdAndEpochV1 {
                            leader_id: 1,
                            leader_epoch: 8,
                        },
                        records: Some(vec![1, 2, 3]),
                        acquired_records: vec![ShareAcquiredRecordsV1 {
                            first_offset: 10,
                            last_offset: 12,
                            delivery_count: 1,
                        }],
                    }],
                }]),
                |encoder, topic| {
                    encoder.write_uuid(&topic.topic_id);
                    encoder.write_compact_array(
                        Some(&topic.partitions),
                        |encoder, partition| {
                            encoder.write_i32(partition.partition_index);
                            encoder.write_i16(partition.error_code);
                            encoder.write_compact_nullable_string(
                                partition.error_message.as_deref(),
                            )?;
                            encoder.write_i16(partition.acknowledgement_error_code);
                            encoder.write_compact_nullable_string(
                                partition.acknowledgement_error_message.as_deref(),
                            )?;
                            encoder.write_i32(partition.current_leader.leader_id);
                            encoder.write_i32(partition.current_leader.leader_epoch);
                            encoder.write_empty_tagged_fields();
                            encoder.write_compact_nullable_bytes(partition.records.as_deref())?;
                            encoder.write_compact_array(
                                Some(&partition.acquired_records),
                                |encoder, acquired| {
                                    encoder.write_i64(acquired.first_offset);
                                    encoder.write_i64(acquired.last_offset);
                                    encoder.write_i16(acquired.delivery_count);
                                    encoder.write_empty_tagged_fields();
                                    Ok(())
                                },
                            )?;
                            encoder.write_empty_tagged_fields();
                            Ok(())
                        },
                    )?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                },
            )
            .unwrap();
        bytes
            .write_compact_array::<ShareNodeEndpointV1>(Some(&[]), |_encoder, _| Ok(()))
            .unwrap();
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ShareFetchResponseV1::decode_body(&mut decoder).unwrap();
        let partition = &response.responses[0].partitions[0];
        assert_eq!(partition.records.as_deref(), Some(&[1, 2, 3][..]));
        assert_eq!(partition.acquired_records[0].delivery_count, 1);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_share_acknowledge_v1_request() {
        let request = ShareAcknowledgeRequestV1 {
            correlation_id: 33,
            client_id: None,
            group_id: Some("share-orders".to_owned()),
            member_id: Some("member-1".to_owned()),
            share_session_epoch: 3,
            topics: vec![ShareAcknowledgeTopicV1 {
                topic_id: [9; 16],
                partitions: vec![ShareAcknowledgePartitionV1 {
                    partition_index: 2,
                    acknowledgement_batches: vec![ShareAcknowledgementBatchV1 {
                        first_offset: 20,
                        last_offset: 20,
                        acknowledgement_types: vec![1],
                    }],
                }],
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 79, 0, 1]);
        assert!(encoded.ends_with(&[0]));
    }

    #[test]
    fn encodes_share_acknowledge_v2_renew_request() {
        let request = ShareAcknowledgeRequestV2 {
            correlation_id: 34,
            client_id: None,
            group_id: Some("share-orders".to_owned()),
            member_id: Some("member-1".to_owned()),
            share_session_epoch: 3,
            is_renew_ack: true,
            topics: vec![ShareAcknowledgeTopicV1 {
                topic_id: [9; 16],
                partitions: vec![ShareAcknowledgePartitionV1 {
                    partition_index: 2,
                    acknowledgement_batches: vec![ShareAcknowledgementBatchV1 {
                        first_offset: 20,
                        last_offset: 20,
                        acknowledgement_types: vec![4],
                    }],
                }],
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 79, 0, 2]);
        let mut decoder = Decoder::new(&encoded[4..]);
        assert_eq!(decoder.read_i32().unwrap(), 34);
        assert_eq!(decoder.read_nullable_string().unwrap(), None);
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("share-orders".to_owned())
        );
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap(),
            Some("member-1".to_owned())
        );
        assert_eq!(decoder.read_i32().unwrap(), 3);
        assert!(decoder.read_bool().unwrap());
        let topics = decoder
            .read_compact_array("topics", |decoder| {
                let topic_id = decoder.read_uuid()?;
                let partitions = decoder
                    .read_compact_array("partitions", |decoder| {
                        let partition_index = decoder.read_i32()?;
                        let batches = decoder
                            .read_compact_array("batches", ShareAcknowledgementBatchV1::decode)?;
                        decoder.read_tagged_fields()?;
                        Ok((partition_index, batches.unwrap_or_default()))
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok((topic_id, partitions))
            })
            .unwrap()
            .unwrap();
        assert_eq!(topics[0].1[0].1[0].acknowledgement_types, vec![4]);
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_share_acknowledge_v1_response_with_leader_endpoint() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes
            .write_compact_array::<ShareAcknowledgeTopicResponseV1>(Some(&[]), |_encoder, _| Ok(()))
            .unwrap();
        bytes
            .write_compact_array(
                Some(&[ShareNodeEndpointV1 {
                    node_id: 2,
                    host: "broker".to_owned(),
                    port: 9092,
                    rack: None,
                }]),
                |encoder, endpoint| {
                    encoder.write_i32(endpoint.node_id);
                    encoder.write_compact_string(&endpoint.host)?;
                    encoder.write_i32(endpoint.port);
                    encoder.write_compact_nullable_string(endpoint.rack.as_deref())?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                },
            )
            .unwrap();
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ShareAcknowledgeResponseV1::decode_body(&mut decoder).unwrap();
        assert_eq!(response.node_endpoints[0].host, "broker");
        assert_eq!(response.node_endpoints[0].port, 9092);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_share_acknowledge_v2_response_with_lock_timeout() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_i32(45_000);
        bytes
            .write_compact_array::<ShareAcknowledgeTopicResponseV1>(Some(&[]), |_encoder, _| Ok(()))
            .unwrap();
        bytes
            .write_compact_array::<ShareNodeEndpointV1>(Some(&[]), |_encoder, _| Ok(()))
            .unwrap();
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ShareAcknowledgeResponseV2::decode_body(&mut decoder).unwrap();
        assert_eq!(response.acquisition_lock_timeout_ms, 45_000);
        assert!(decoder.is_empty());
    }
}
