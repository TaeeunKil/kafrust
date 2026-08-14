use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::header::RequestHeader;

/// Kafka DescribeTopicPartitions API key.
pub const API_KEY: i16 = 75;

/// Nullable cursor used to page through topic partition metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsCursorV0 {
    pub topic_name: String,
    pub partition_index: i32,
}

/// One topic requested by DescribeTopicPartitions v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsTopicV0 {
    pub name: String,
}

/// DescribeTopicPartitions v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Vec<DescribeTopicPartitionsTopicV0>,
    pub response_partition_limit: i32,
    pub cursor: Option<DescribeTopicPartitionsCursorV0>,
}

impl DescribeTopicPartitionsRequestV0 {
    /// Encodes the flexible v0 request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&self.topics), |encoder, topic| {
            encoder.write_compact_string(&topic.name)?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_i32(self.response_partition_limit);
        match &self.cursor {
            Some(cursor) => {
                encoder.write_i8(1);
                encoder.write_compact_string(&cursor.topic_name)?;
                encoder.write_i32(cursor.partition_index);
                encoder.write_empty_tagged_fields();
            }
            None => encoder.write_i8(-1),
        }
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// One partition returned by DescribeTopicPartitions v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsPartitionResponseV0 {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub eligible_leader_replicas: Option<Vec<i32>>,
    pub last_known_elr: Option<Vec<i32>>,
    pub offline_replicas: Vec<i32>,
}

/// One topic returned by DescribeTopicPartitions v0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsTopicResponseV0 {
    pub error_code: i16,
    pub name: Option<String>,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<DescribeTopicPartitionsPartitionResponseV0>,
    pub topic_authorized_operations: i32,
}

/// DescribeTopicPartitions v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsResponseV0 {
    pub throttle_time_ms: i32,
    pub topics: Vec<DescribeTopicPartitionsTopicResponseV0>,
    pub next_cursor: Option<DescribeTopicPartitionsCursorV0>,
}

impl DescribeTopicPartitionsResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let topics = decoder
            .read_compact_array("describe topic partitions topics", |decoder| {
                let error_code = decoder.read_i16()?;
                let name = decoder.read_compact_nullable_string()?;
                let topic_id = decoder.read_uuid()?;
                let is_internal = decoder.read_bool()?;
                let partitions = decoder
                    .read_compact_array("describe topic partitions partitions", |decoder| {
                        let error_code = decoder.read_i16()?;
                        let partition_index = decoder.read_i32()?;
                        let leader_id = decoder.read_i32()?;
                        let leader_epoch = decoder.read_i32()?;
                        let replica_nodes = decoder
                            .read_compact_array("describe topic partitions replicas", |decoder| {
                                decoder.read_i32()
                            })?
                            .unwrap_or_default();
                        let isr_nodes = decoder
                            .read_compact_array("describe topic partitions isr", |decoder| {
                                decoder.read_i32()
                            })?
                            .unwrap_or_default();
                        let eligible_leader_replicas = decoder.read_compact_array(
                            "describe topic partitions eligible leader replicas",
                            |decoder| decoder.read_i32(),
                        )?;
                        let last_known_elr = decoder.read_compact_array(
                            "describe topic partitions last known elr",
                            |decoder| decoder.read_i32(),
                        )?;
                        let offline_replicas = decoder
                            .read_compact_array(
                                "describe topic partitions offline replicas",
                                |decoder| decoder.read_i32(),
                            )?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok(DescribeTopicPartitionsPartitionResponseV0 {
                            error_code,
                            partition_index,
                            leader_id,
                            leader_epoch,
                            replica_nodes,
                            isr_nodes,
                            eligible_leader_replicas,
                            last_known_elr,
                            offline_replicas,
                        })
                    })?
                    .unwrap_or_default();
                let topic_authorized_operations = decoder.read_i32()?;
                decoder.read_tagged_fields()?;
                Ok(DescribeTopicPartitionsTopicResponseV0 {
                    error_code,
                    name,
                    topic_id,
                    is_internal,
                    partitions,
                    topic_authorized_operations,
                })
            })?
            .unwrap_or_default();
        let next_cursor = match decoder.read_i8()? {
            -1 => None,
            1 => {
                let topic_name = decoder.read_compact_string()?;
                let partition_index = decoder.read_i32()?;
                decoder.read_tagged_fields()?;
                Some(DescribeTopicPartitionsCursorV0 {
                    topic_name,
                    partition_index,
                })
            }
            marker => return Err(Error::InvalidNullableStruct(marker)),
        };
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        DescribeTopicPartitionsCursorV0, DescribeTopicPartitionsRequestV0,
        DescribeTopicPartitionsResponseV0, DescribeTopicPartitionsTopicV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_topic_partitions_v0_request_with_cursor() {
        let request = DescribeTopicPartitionsRequestV0 {
            correlation_id: 31,
            client_id: Some("kafrust".to_owned()),
            topics: vec![DescribeTopicPartitionsTopicV0 {
                name: "orders".to_owned(),
            }],
            response_partition_limit: 2000,
            cursor: Some(DescribeTopicPartitionsCursorV0 {
                topic_name: "orders".to_owned(),
                partition_index: 2,
            }),
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 31]);
        assert!(bytes.windows(6).any(|value| value == b"orders"));
        assert_eq!(&bytes[bytes.len() - 1..], &[0]);
    }

    #[test]
    fn decodes_describe_topic_partitions_v0_response_with_nullable_fields() {
        let mut bytes = Encoder::new();
        bytes.write_i32(17);
        bytes.write_unsigned_varint(2);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(Some("orders")).unwrap();
        bytes.write_uuid(&[7; 16]);
        bytes.write_bool(false);
        bytes.write_unsigned_varint(2);
        bytes.write_i16(0);
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_i32(8);
        bytes.write_unsigned_varint(2);
        bytes.write_i32(1);
        bytes.write_unsigned_varint(2);
        bytes.write_i32(1);
        bytes.write_unsigned_varint(0);
        bytes.write_unsigned_varint(0);
        bytes.write_unsigned_varint(2);
        bytes.write_i32(2);
        bytes.write_empty_tagged_fields();
        bytes.write_i32(-2147483648);
        bytes.write_empty_tagged_fields();
        bytes.write_i8(-1);
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = DescribeTopicPartitionsResponseV0::decode_body(&mut decoder).unwrap();
        assert_eq!(response.throttle_time_ms, 17);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name.as_deref(), Some("orders"));
        assert_eq!(response.topics[0].topic_id, [7; 16]);
        assert_eq!(response.topics[0].partitions[0].leader_id, 1);
        assert_eq!(
            response.topics[0].partitions[0].eligible_leader_replicas,
            None
        );
        assert_eq!(response.next_cursor, None);
        assert!(decoder.is_empty());
    }
}
