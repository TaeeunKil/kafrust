use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 46;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub timeout_ms: i32,
    pub topics: Option<Vec<ListPartitionReassignmentsTopicV0>>,
}

impl ListPartitionReassignmentsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i32(self.timeout_ms);
        encoder.write_compact_array(self.topics.as_deref(), |encoder, topic| {
            encoder.write_compact_string(&topic.name)?;
            encoder.write_compact_array(Some(&topic.partition_indexes), |encoder, index| {
                encoder.write_i32(*index);
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsTopicV0 {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub topics: Vec<ListPartitionReassignmentsTopicResponseV0>,
}

impl ListPartitionReassignmentsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let topics = decoder
            .read_compact_array("list partition reassignment topics", |decoder| {
                let name = decoder.read_compact_string()?;
                let partitions = decoder
                    .read_compact_array("list partition reassignment partitions", |decoder| {
                        let partition_index = decoder.read_i32()?;
                        let replicas = decoder
                            .read_array("partition reassignment replicas", |decoder| {
                                decoder.read_i32()
                            })?
                            .unwrap_or_default();
                        let adding_replicas = decoder
                            .read_array("partition reassignment adding replicas", |decoder| {
                                decoder.read_i32()
                            })?
                            .unwrap_or_default();
                        let removing_replicas = decoder
                            .read_array("partition reassignment removing replicas", |decoder| {
                                decoder.read_i32()
                            })?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok(ListPartitionReassignmentsPartitionResponseV0 {
                            partition_index,
                            replicas,
                            adding_replicas,
                            removing_replicas,
                        })
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok(ListPartitionReassignmentsTopicResponseV0 { name, partitions })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            topics,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsTopicResponseV0 {
    pub name: String,
    pub partitions: Vec<ListPartitionReassignmentsPartitionResponseV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsPartitionResponseV0 {
    pub partition_index: i32,
    pub replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ListPartitionReassignmentsRequestV0, ListPartitionReassignmentsResponseV0,
        ListPartitionReassignmentsTopicV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_list_partition_reassignments_v0_request_with_nullable_topics() {
        let request = ListPartitionReassignmentsRequestV0 {
            correlation_id: 13,
            client_id: None,
            timeout_ms: 10_000,
            topics: Some(vec![ListPartitionReassignmentsTopicV0 {
                name: "orders".to_owned(),
                partition_indexes: vec![0, 2],
            }]),
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 13]);
        assert_eq!(bytes[bytes.len() - 1], 0);
    }

    #[test]
    fn decodes_list_partition_reassignments_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(9);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("orders").unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_i32(2);
        bytes
            .write_array(Some(&[1, 2, 3]), |encoder, value| {
                encoder.write_i32(*value);
                Ok(())
            })
            .unwrap();
        bytes
            .write_array(Some(&[3]), |encoder, value| {
                encoder.write_i32(*value);
                Ok(())
            })
            .unwrap();
        bytes
            .write_array(Some(&[1]), |encoder, value| {
                encoder.write_i32(*value);
                Ok(())
            })
            .unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = ListPartitionReassignmentsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 9);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].partitions[0].replicas, [1, 2, 3]);
        assert_eq!(response.topics[0].partitions[0].adding_replicas, [3]);
        assert_eq!(response.topics[0].partitions[0].removing_replicas, [1]);
        assert!(decoder.is_empty());
    }
}
