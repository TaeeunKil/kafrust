use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka StreamsGroupDescribe API key.
pub const API_KEY: i16 = 89;

/// StreamsGroupDescribe v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupDescribeRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_ids: Vec<String>,
    pub include_authorized_operations: bool,
}

impl StreamsGroupDescribeRequestV0 {
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
        encoder.write_compact_array(Some(&self.group_ids), |encoder, group_id| {
            encoder.write_compact_string(group_id)
        })?;
        encoder.write_bool(self.include_authorized_operations);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// StreamsGroupDescribe v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupDescribeResponseV0 {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedStreamsGroup>,
}

impl StreamsGroupDescribeResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let groups = decoder
            .read_compact_array("streams group descriptions", DescribedStreamsGroup::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

/// One Streams group returned by StreamsGroupDescribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedStreamsGroup {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub group_id: String,
    pub group_state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub topology: Option<StreamsGroupTopology>,
    pub members: Vec<DescribedStreamsGroupMember>,
    pub authorized_operations: i32,
}

impl DescribedStreamsGroup {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let group_id = decoder.read_compact_string()?;
        let group_state = decoder.read_compact_string()?;
        let group_epoch = decoder.read_i32()?;
        let assignment_epoch = decoder.read_i32()?;
        let topology = decode_nullable_struct(decoder, StreamsGroupTopology::decode)?;
        let members = decoder
            .read_compact_array("streams group members", DescribedStreamsGroupMember::decode)?
            .unwrap_or_default();
        let authorized_operations = decoder.read_i32()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            topology,
            members,
            authorized_operations,
        })
    }
}

/// The topology currently initialized for a Streams group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTopology {
    pub epoch: i32,
    pub subtopologies: Option<Vec<StreamsGroupSubtopology>>,
}

impl StreamsGroupTopology {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let epoch = decoder.read_i32()?;
        let subtopologies = decoder.read_compact_array(
            "streams group subtopologies",
            StreamsGroupSubtopology::decode,
        )?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            epoch,
            subtopologies,
        })
    }
}

/// One subtopology in a Streams group topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupSubtopology {
    pub subtopology_id: String,
    pub source_topics: Vec<String>,
    pub repartition_sink_topics: Vec<String>,
    pub state_changelog_topics: Vec<StreamsGroupTopic>,
    pub repartition_source_topics: Vec<StreamsGroupTopic>,
}

impl StreamsGroupSubtopology {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let subtopology_id = decoder.read_compact_string()?;
        let source_topics = decoder
            .read_compact_array("streams group source topics", |decoder| {
                decoder.read_compact_string()
            })?
            .unwrap_or_default();
        let repartition_sink_topics = decoder
            .read_compact_array("streams group repartition sink topics", |decoder| {
                decoder.read_compact_string()
            })?
            .unwrap_or_default();
        let state_changelog_topics = decoder
            .read_compact_array(
                "streams group state changelog topics",
                StreamsGroupTopic::decode,
            )?
            .unwrap_or_default();
        let repartition_source_topics = decoder
            .read_compact_array(
                "streams group repartition source topics",
                StreamsGroupTopic::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            subtopology_id,
            source_topics,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
        })
    }
}

/// A topic managed by a Streams group topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTopic {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: Vec<StreamsGroupTopicConfig>,
}

impl StreamsGroupTopic {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let name = decoder.read_compact_string()?;
        let partitions = decoder.read_i32()?;
        let replication_factor = decoder.read_i16()?;
        let topic_configs = decoder
            .read_compact_array("streams group topic configs", |decoder| {
                let config = StreamsGroupTopicConfig {
                    key: decoder.read_compact_string()?,
                    value: decoder.read_compact_string()?,
                };
                decoder.read_tagged_fields()?;
                Ok(config)
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

/// One configuration entry for a Streams group-managed topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTopicConfig {
    pub key: String,
    pub value: String,
}

/// One member returned by StreamsGroupDescribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedStreamsGroupMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub topology_epoch: i32,
    pub process_id: String,
    pub user_endpoint: Option<StreamsGroupEndpoint>,
    pub client_tags: Vec<StreamsGroupKeyValue>,
    pub task_offsets: Vec<StreamsGroupTaskOffset>,
    pub task_end_offsets: Vec<StreamsGroupTaskOffset>,
    pub assignment: StreamsGroupAssignment,
    pub target_assignment: StreamsGroupAssignment,
    pub is_classic: bool,
}

impl DescribedStreamsGroupMember {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let member_id = decoder.read_compact_string()?;
        let member_epoch = decoder.read_i32()?;
        let instance_id = decoder.read_compact_nullable_string()?;
        let rack_id = decoder.read_compact_nullable_string()?;
        let client_id = decoder.read_compact_string()?;
        let client_host = decoder.read_compact_string()?;
        let topology_epoch = decoder.read_i32()?;
        let process_id = decoder.read_compact_string()?;
        let user_endpoint = decode_nullable_struct(decoder, StreamsGroupEndpoint::decode)?;
        let client_tags = decode_key_values(decoder, "streams group client tags")?;
        let task_offsets = decoder
            .read_compact_array("streams group task offsets", StreamsGroupTaskOffset::decode)?
            .unwrap_or_default();
        let task_end_offsets = decoder
            .read_compact_array(
                "streams group task end offsets",
                StreamsGroupTaskOffset::decode,
            )?
            .unwrap_or_default();
        let assignment = StreamsGroupAssignment::decode(decoder)?;
        let target_assignment = StreamsGroupAssignment::decode(decoder)?;
        let is_classic = decoder.read_bool()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            member_id,
            member_epoch,
            instance_id,
            rack_id,
            client_id,
            client_host,
            topology_epoch,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            assignment,
            target_assignment,
            is_classic,
        })
    }
}

/// A host and port exposed by a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupEndpoint {
    pub host: String,
    pub port: u16,
}

impl StreamsGroupEndpoint {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let host = decoder.read_compact_string()?;
        let port = decoder.read_i16()? as u16;
        decoder.read_tagged_fields()?;
        Ok(Self { host, port })
    }
}

/// A compact key/value entry attached to a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupKeyValue {
    pub key: String,
    pub value: String,
}

fn decode_key_values(
    decoder: &mut Decoder<'_>,
    kind: &'static str,
) -> Result<Vec<StreamsGroupKeyValue>> {
    Ok(decoder
        .read_compact_array(kind, |decoder| {
            let key = decoder.read_compact_string()?;
            let value = decoder.read_compact_string()?;
            decoder.read_tagged_fields()?;
            Ok(StreamsGroupKeyValue { key, value })
        })?
        .unwrap_or_default())
}

/// Current or target task assignment for a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupAssignment {
    pub active_tasks: Vec<StreamsGroupTask>,
    pub standby_tasks: Vec<StreamsGroupTask>,
    pub warmup_tasks: Vec<StreamsGroupTask>,
}

impl StreamsGroupAssignment {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let active_tasks = decoder
            .read_compact_array("streams group active tasks", StreamsGroupTask::decode)?
            .unwrap_or_default();
        let standby_tasks = decoder
            .read_compact_array("streams group standby tasks", StreamsGroupTask::decode)?
            .unwrap_or_default();
        let warmup_tasks = decoder
            .read_compact_array("streams group warmup tasks", StreamsGroupTask::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            active_tasks,
            standby_tasks,
            warmup_tasks,
        })
    }
}

/// A Streams task assignment identified by subtopology and partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTask {
    pub subtopology_id: String,
    pub partitions: Vec<i32>,
}

impl StreamsGroupTask {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let subtopology_id = decoder.read_compact_string()?;
        let partitions = decoder
            .read_compact_array("streams group task partitions", |decoder| {
                decoder.read_i32()
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            subtopology_id,
            partitions,
        })
    }
}

/// A cumulative changelog offset reported by a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTaskOffset {
    pub subtopology_id: String,
    pub partition: i32,
    pub offset: i64,
}

impl StreamsGroupTaskOffset {
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

fn decode_nullable_struct<T>(
    decoder: &mut Decoder<'_>,
    decode: impl FnOnce(&mut Decoder<'_>) -> Result<T>,
) -> Result<Option<T>> {
    match decoder.read_i8()? {
        -1 => Ok(None),
        1 => decode(decoder).map(Some),
        marker => Err(crate::error::Error::InvalidNullableStruct(marker)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{StreamsGroupDescribeRequestV0, StreamsGroupDescribeResponseV0, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_streams_group_describe_v0_request() {
        let request = StreamsGroupDescribeRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_ids: vec!["streams-orders".to_owned()],
            include_authorized_operations: true,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..4], &[0, 89, 0, 0]);
        assert_eq!(API_KEY, 89);
    }

    #[test]
    fn decodes_streams_group_describe_v0_response() -> crate::error::Result<()> {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes.write_compact_array(Some(&[1_i8]), |encoder, _| {
            encoder.write_i16(0);
            encoder.write_compact_nullable_string(Some("ok"))?;
            encoder.write_compact_string("streams-orders")?;
            encoder.write_compact_string("Stable")?;
            encoder.write_i32(4);
            encoder.write_i32(5);
            encoder.write_i8(1);
            encoder.write_i32(3);
            encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                encoder.write_compact_string("subtopology-0")?;
                encoder.write_compact_array(Some(&["orders".to_owned()]), |encoder, topic| {
                    encoder.write_compact_string(topic)
                })?;
                encoder.write_compact_array::<String>(Some(&[]), |_, _| Ok(()))?;
                encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                    encoder.write_compact_string("orders-store")?;
                    encoder.write_i32(3);
                    encoder.write_i16(1);
                    encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                        encoder.write_compact_string("cleanup.policy")?;
                        encoder.write_compact_string("compact")?;
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            bytes_for_member(encoder)?;
            encoder.write_i32(-2147483648);
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = StreamsGroupDescribeResponseV0::decode_body(&mut decoder)?;

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.groups[0].group_id, "streams-orders");
        assert_eq!(response.groups[0].topology.as_ref().unwrap().epoch, 3);
        assert_eq!(
            response.groups[0]
                .topology
                .as_ref()
                .unwrap()
                .subtopologies
                .as_ref()
                .unwrap()[0]
                .state_changelog_topics[0]
                .name,
            "orders-store"
        );
        assert_eq!(response.groups[0].members[0].member_id, "member-1");
        assert_eq!(
            response.groups[0].members[0].assignment.active_tasks[0].partitions,
            [0, 2]
        );
        assert!(decoder.is_empty());
        Ok(())
    }

    fn bytes_for_member(encoder: &mut Encoder) -> crate::error::Result<()> {
        encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
            encoder.write_compact_string("member-1")?;
            encoder.write_i32(7);
            encoder.write_compact_nullable_string(None)?;
            encoder.write_compact_nullable_string(Some("rack-a"))?;
            encoder.write_compact_string("client-a")?;
            encoder.write_compact_string("/127.0.0.1")?;
            encoder.write_i32(3);
            encoder.write_compact_string("process-1")?;
            encoder.write_i8(1);
            encoder.write_compact_string("127.0.0.1")?;
            encoder.write_i16(7000);
            encoder.write_empty_tagged_fields();
            encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                encoder.write_compact_string("rack")?;
                encoder.write_compact_string("a")?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                encoder.write_compact_string("subtopology-0")?;
                encoder.write_i32(0);
                encoder.write_i64(10);
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))?;
            for _ in 0..2 {
                encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                    encoder.write_compact_string("subtopology-0")?;
                    encoder.write_compact_array(Some(&[0_i32, 2]), |encoder, partition| {
                        encoder.write_i32(*partition);
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))?;
                encoder.write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))?;
                encoder.write_empty_tagged_fields();
            }
            encoder.write_bool(false);
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        Ok(())
    }
}
