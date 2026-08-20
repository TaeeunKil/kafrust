#![allow(dead_code)]

// The request topology types carry encode-only nested structures while the
// response has a separate, smaller assignment shape. Keeping both wire shapes
// typed here avoids lossy reuse and leaves the protocol boundary auditable.

use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::header::RequestHeader;

/// Kafka StreamsGroupHeartbeat API key.
pub const API_KEY: i16 = 88;

/// A Kafka Streams topology sent while initializing a Streams group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatTopology {
    pub epoch: i32,
    pub subtopologies: Vec<StreamsGroupHeartbeatSubtopology>,
}

impl StreamsGroupHeartbeatTopology {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.epoch);
        encoder.write_compact_array(Some(&self.subtopologies), |encoder, subtopology| {
            subtopology.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let epoch = decoder.read_i32()?;
        let subtopologies = decoder
            .read_compact_array(
                "streams heartbeat subtopologies",
                StreamsGroupHeartbeatSubtopology::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            epoch,
            subtopologies,
        })
    }
}

/// One subtopology sent while initializing a Streams group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatSubtopology {
    pub subtopology_id: String,
    pub source_topics: Vec<String>,
    pub source_topic_regex: Vec<String>,
    pub state_changelog_topics: Vec<StreamsGroupHeartbeatTopic>,
    pub repartition_sink_topics: Vec<String>,
    pub repartition_source_topics: Vec<StreamsGroupHeartbeatTopic>,
    pub copartition_groups: Vec<StreamsGroupHeartbeatCopartitionGroup>,
}

impl StreamsGroupHeartbeatSubtopology {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.subtopology_id)?;
        write_string_array(encoder, &self.source_topics)?;
        write_string_array(encoder, &self.source_topic_regex)?;
        encoder.write_compact_array(Some(&self.state_changelog_topics), |encoder, topic| {
            topic.encode(encoder)
        })?;
        write_string_array(encoder, &self.repartition_sink_topics)?;
        encoder.write_compact_array(Some(&self.repartition_source_topics), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_compact_array(Some(&self.copartition_groups), |encoder, group| {
            group.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let subtopology_id = decoder.read_compact_string()?;
        let source_topics = read_string_array(decoder, "streams heartbeat source topics")?;
        let source_topic_regex = read_string_array(decoder, "streams heartbeat source regex")?;
        let state_changelog_topics = decoder
            .read_compact_array(
                "streams heartbeat state changelog topics",
                StreamsGroupHeartbeatTopic::decode,
            )?
            .unwrap_or_default();
        let repartition_sink_topics =
            read_string_array(decoder, "streams heartbeat repartition sinks")?;
        let repartition_source_topics = decoder
            .read_compact_array(
                "streams heartbeat repartition sources",
                StreamsGroupHeartbeatTopic::decode,
            )?
            .unwrap_or_default();
        let copartition_groups = decoder
            .read_compact_array(
                "streams heartbeat copartition groups",
                StreamsGroupHeartbeatCopartitionGroup::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            subtopology_id,
            source_topics,
            source_topic_regex,
            state_changelog_topics,
            repartition_sink_topics,
            repartition_source_topics,
            copartition_groups,
        })
    }
}

/// A topic created or managed by a Streams topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatTopic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: Vec<StreamsGroupHeartbeatTopicConfig>,
}

impl StreamsGroupHeartbeatTopic {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.name)?;
        encoder.write_i32(self.partitions);
        encoder.write_i16(self.replication_factor);
        encoder.write_compact_array(Some(&self.topic_configs), |encoder, config| {
            config.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let name = decoder.read_compact_string()?;
        let partitions = decoder.read_i32()?;
        let replication_factor = decoder.read_i16()?;
        let topic_configs = decoder
            .read_compact_array("streams heartbeat topic configs", |decoder| {
                StreamsGroupHeartbeatTopicConfig::decode(decoder)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            name,
            partitions,
            replication_factor,
            topic_configs,
        })
    }
}

/// One topic-level configuration in a Streams topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatTopicConfig {
    pub key: String,
    pub value: String,
}

impl StreamsGroupHeartbeatTopicConfig {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.key)?;
        encoder.write_compact_string(&self.value)?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let key = decoder.read_compact_string()?;
        let value = decoder.read_compact_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self { key, value })
    }
}

/// A copartition constraint represented by subtopology-level indexes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatCopartitionGroup {
    pub source_topics: Vec<i16>,
    pub source_topic_regex: Vec<i16>,
    pub repartition_source_topics: Vec<i16>,
}

impl StreamsGroupHeartbeatCopartitionGroup {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        write_i16_array(encoder, &self.source_topics)?;
        write_i16_array(encoder, &self.source_topic_regex)?;
        write_i16_array(encoder, &self.repartition_source_topics)?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let source_topics = read_i16_array(decoder, "streams heartbeat copartition sources")?;
        let source_topic_regex = read_i16_array(decoder, "streams heartbeat copartition regex")?;
        let repartition_source_topics =
            read_i16_array(decoder, "streams heartbeat copartition repartition sources")?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            source_topics,
            source_topic_regex,
            repartition_source_topics,
        })
    }
}

/// A Streams task and its input partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatTask {
    pub subtopology_id: String,
    pub partitions: Vec<i32>,
}

impl StreamsGroupHeartbeatTask {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.subtopology_id)?;
        write_i32_array(encoder, &self.partitions)?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let subtopology_id = decoder.read_compact_string()?;
        let partitions = read_i32_array(decoder, "streams heartbeat task partitions")?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            subtopology_id,
            partitions,
        })
    }
}

/// A cumulative changelog offset for a Streams task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatTaskOffset {
    pub subtopology_id: String,
    pub partition: i32,
    pub offset: i64,
}

impl StreamsGroupHeartbeatTaskOffset {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.subtopology_id)?;
        encoder.write_i32(self.partition);
        encoder.write_i64(self.offset);
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let subtopology_id = decoder.read_compact_string()?;
        let partition = decoder.read_i32()?;
        let offset = decoder.read_i64()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            subtopology_id,
            partition,
            offset,
        })
    }
}

/// A host and port exposed for Interactive Queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatEndpoint {
    pub host: String,
    pub port: u16,
}

impl StreamsGroupHeartbeatEndpoint {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.host)?;
        encoder.write_i16(i16::from_be_bytes(self.port.to_be_bytes()));
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let host = decoder.read_compact_string()?;
        let port = u16::from_be_bytes(decoder.read_i16()?.to_be_bytes());
        decoder.read_tagged_fields()?;
        Ok(Self { host, port })
    }
}

/// A rack-aware client tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatKeyValue {
    pub key: String,
    pub value: String,
}

impl StreamsGroupHeartbeatKeyValue {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.key)?;
        encoder.write_compact_string(&self.value)?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let key = decoder.read_compact_string()?;
        let value = decoder.read_compact_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self { key, value })
    }
}

/// StreamsGroupHeartbeat v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub endpoint_information_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub topology: Option<StreamsGroupHeartbeatTopology>,
    pub active_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pub standby_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pub warmup_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pub process_id: Option<String>,
    pub user_endpoint: Option<StreamsGroupHeartbeatEndpoint>,
    pub client_tags: Option<Vec<StreamsGroupHeartbeatKeyValue>>,
    pub task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    pub task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    pub shutdown_application: bool,
}

impl StreamsGroupHeartbeatRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.group_id)?;
        encoder.write_compact_string(&self.member_id)?;
        encoder.write_i32(self.member_epoch);
        encoder.write_i32(self.endpoint_information_epoch);
        encoder.write_compact_nullable_string(self.instance_id.as_deref())?;
        encoder.write_compact_nullable_string(self.rack_id.as_deref())?;
        encoder.write_i32(self.rebalance_timeout_ms);
        write_nullable_struct(&mut encoder, self.topology.as_ref(), |encoder, topology| {
            topology.encode(encoder)
        })?;
        encoder.write_compact_array(self.active_tasks.as_deref(), |encoder, task| {
            task.encode(encoder)
        })?;
        encoder.write_compact_array(self.standby_tasks.as_deref(), |encoder, task| {
            task.encode(encoder)
        })?;
        encoder.write_compact_array(self.warmup_tasks.as_deref(), |encoder, task| {
            task.encode(encoder)
        })?;
        encoder.write_compact_nullable_string(self.process_id.as_deref())?;
        write_nullable_struct(
            &mut encoder,
            self.user_endpoint.as_ref(),
            |encoder, endpoint| endpoint.encode(encoder),
        )?;
        encoder.write_compact_array(self.client_tags.as_deref(), |encoder, tag| {
            tag.encode(encoder)
        })?;
        encoder.write_compact_array(self.task_offsets.as_deref(), |encoder, offset| {
            offset.encode(encoder)
        })?;
        encoder.write_compact_array(self.task_end_offsets.as_deref(), |encoder, offset| {
            offset.encode(encoder)
        })?;
        encoder.write_bool(self.shutdown_application);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// A status entry returned by StreamsGroupHeartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatStatus {
    pub status_code: i8,
    pub status_detail: String,
}

impl StreamsGroupHeartbeatStatus {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let status_code = decoder.read_i8()?;
        let status_detail = decoder.read_compact_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            status_code,
            status_detail,
        })
    }
}

/// Topic partitions materialized by one Interactive Queries endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatTopicPartitions {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl StreamsGroupHeartbeatTopicPartitions {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic = decoder.read_compact_string()?;
        let partitions = read_i32_array(decoder, "streams heartbeat endpoint partitions")?;
        decoder.read_tagged_fields()?;
        Ok(Self { topic, partitions })
    }
}

/// Assignment information grouped by an Interactive Queries endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatEndpointPartitions {
    pub user_endpoint: StreamsGroupHeartbeatEndpoint,
    pub active_partitions: Vec<StreamsGroupHeartbeatTopicPartitions>,
    pub standby_partitions: Vec<StreamsGroupHeartbeatTopicPartitions>,
}

impl StreamsGroupHeartbeatEndpointPartitions {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let user_endpoint = StreamsGroupHeartbeatEndpoint::decode(decoder)?;
        let active_partitions = decoder
            .read_compact_array(
                "streams heartbeat active endpoint partitions",
                StreamsGroupHeartbeatTopicPartitions::decode,
            )?
            .unwrap_or_default();
        let standby_partitions = decoder
            .read_compact_array(
                "streams heartbeat standby endpoint partitions",
                StreamsGroupHeartbeatTopicPartitions::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            user_endpoint,
            active_partitions,
            standby_partitions,
        })
    }
}

/// StreamsGroupHeartbeat v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupHeartbeatResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub member_id: String,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub acceptable_recovery_lag: i32,
    pub task_offset_interval_ms: i32,
    pub status: Option<StreamsGroupHeartbeatStatus>,
    pub active_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pub standby_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pub warmup_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pub endpoint_information_epoch: i32,
    pub partitions_by_user_endpoint: Option<Vec<StreamsGroupHeartbeatEndpointPartitions>>,
}

impl StreamsGroupHeartbeatResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let member_id = decoder.read_compact_string()?;
        let member_epoch = decoder.read_i32()?;
        let heartbeat_interval_ms = decoder.read_i32()?;
        let acceptable_recovery_lag = decoder.read_i32()?;
        let task_offset_interval_ms = decoder.read_i32()?;
        let status = decode_nullable_struct(decoder, StreamsGroupHeartbeatStatus::decode)?;
        let active_tasks = decoder.read_compact_array(
            "streams heartbeat active tasks",
            StreamsGroupHeartbeatTask::decode,
        )?;
        let standby_tasks = decoder.read_compact_array(
            "streams heartbeat standby tasks",
            StreamsGroupHeartbeatTask::decode,
        )?;
        let warmup_tasks = decoder.read_compact_array(
            "streams heartbeat warmup tasks",
            StreamsGroupHeartbeatTask::decode,
        )?;
        let endpoint_information_epoch = decoder.read_i32()?;
        let partitions_by_user_endpoint = decoder.read_compact_array(
            "streams heartbeat endpoint assignments",
            StreamsGroupHeartbeatEndpointPartitions::decode,
        )?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            member_id,
            member_epoch,
            heartbeat_interval_ms,
            acceptable_recovery_lag,
            task_offset_interval_ms,
            status,
            active_tasks,
            standby_tasks,
            warmup_tasks,
            endpoint_information_epoch,
            partitions_by_user_endpoint,
        })
    }
}

fn write_nullable_struct<T>(
    encoder: &mut Encoder,
    value: Option<&T>,
    mut write: impl FnMut(&mut Encoder, &T) -> Result<()>,
) -> Result<()> {
    match value {
        Some(value) => {
            encoder.write_i8(1);
            write(encoder, value)?;
        }
        None => encoder.write_i8(-1),
    }
    Ok(())
}

fn decode_nullable_struct<T>(
    decoder: &mut Decoder<'_>,
    decode: impl FnOnce(&mut Decoder<'_>) -> Result<T>,
) -> Result<Option<T>> {
    match decoder.read_i8()? {
        -1 => Ok(None),
        1 => decode(decoder).map(Some),
        marker => Err(Error::InvalidNullableStruct(marker)),
    }
}

fn write_string_array(encoder: &mut Encoder, values: &[String]) -> Result<()> {
    encoder.write_compact_array(Some(values), |encoder, value| {
        encoder.write_compact_string(value)
    })
}

fn read_string_array(decoder: &mut Decoder<'_>, kind: &'static str) -> Result<Vec<String>> {
    Ok(decoder
        .read_compact_array(kind, |decoder| decoder.read_compact_string())?
        .unwrap_or_default())
}

fn write_i16_array(encoder: &mut Encoder, values: &[i16]) -> Result<()> {
    encoder.write_compact_array(Some(values), |encoder, value| {
        encoder.write_i16(*value);
        Ok(())
    })
}

fn read_i16_array(decoder: &mut Decoder<'_>, kind: &'static str) -> Result<Vec<i16>> {
    Ok(decoder
        .read_compact_array(kind, |decoder| decoder.read_i16())?
        .unwrap_or_default())
}

fn write_i32_array(encoder: &mut Encoder, values: &[i32]) -> Result<()> {
    encoder.write_compact_array(Some(values), |encoder, value| {
        encoder.write_i32(*value);
        Ok(())
    })
}

fn read_i32_array(decoder: &mut Decoder<'_>, kind: &'static str) -> Result<Vec<i32>> {
    Ok(decoder
        .read_compact_array(kind, |decoder| decoder.read_i32())?
        .unwrap_or_default())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        StreamsGroupHeartbeatRequestV0, StreamsGroupHeartbeatResponseV0, StreamsGroupHeartbeatTask,
        StreamsGroupHeartbeatTopology, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_streams_group_heartbeat_v0_request() {
        let request = StreamsGroupHeartbeatRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_id: "streams-orders".to_owned(),
            member_id: "member-a".to_owned(),
            member_epoch: 0,
            endpoint_information_epoch: 0,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 30_000,
            topology: Some(StreamsGroupHeartbeatTopology {
                epoch: 1,
                subtopologies: Vec::new(),
            }),
            active_tasks: Some(vec![StreamsGroupHeartbeatTask {
                subtopology_id: "subtopology-0".to_owned(),
                partitions: vec![0, 2],
            }]),
            standby_tasks: None,
            warmup_tasks: None,
            process_id: Some("process-a".to_owned()),
            user_endpoint: None,
            client_tags: None,
            task_offsets: None,
            task_end_offsets: None,
            shutdown_application: false,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..4], &[0, 88, 0, 0]);
        assert_eq!(API_KEY, 88);
        assert!(encoded.windows(14).any(|bytes| bytes == b"streams-orders"));
        assert!(encoded.windows(8).any(|bytes| bytes == b"member-a"));
        assert!(encoded.ends_with(&[0]));
    }

    #[test]
    fn decodes_streams_group_heartbeat_v0_response() -> crate::error::Result<()> {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(Some("ok"))?;
        bytes.write_compact_string("member-a")?;
        bytes.write_i32(3);
        bytes.write_i32(2500);
        bytes.write_i32(10);
        bytes.write_i32(1000);
        bytes.write_i8(1);
        bytes.write_i8(2);
        bytes.write_compact_string("running")?;
        bytes.write_empty_tagged_fields();
        bytes.write_compact_array(
            Some(&[StreamsGroupHeartbeatTask {
                subtopology_id: "subtopology-0".to_owned(),
                partitions: vec![0, 1],
            }]),
            |encoder, task| task.encode(encoder),
        )?;
        bytes.write_compact_array::<StreamsGroupHeartbeatTask>(Some(&[]), |_, _| Ok(()))?;
        bytes.write_compact_array::<StreamsGroupHeartbeatTask>(None, |_, _| Ok(()))?;
        bytes.write_i32(4);
        bytes.write_compact_array::<i8>(None, |_, _| Ok(()))?;
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = StreamsGroupHeartbeatResponseV0::decode_body(&mut decoder)?;

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.member_id, "member-a");
        assert_eq!(response.status.as_ref().unwrap().status_code, 2);
        assert_eq!(
            response.active_tasks.as_ref().unwrap()[0].partitions,
            [0, 1]
        );
        assert!(response.standby_tasks.as_ref().unwrap().is_empty());
        assert!(response.warmup_tasks.is_none());
        assert!(response.partitions_by_user_endpoint.is_none());
        assert!(decoder.is_empty());
        Ok(())
    }
}
