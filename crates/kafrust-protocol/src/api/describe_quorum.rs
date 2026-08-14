use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka DescribeQuorum API key.
pub const API_KEY: i16 = 55;

/// One topic and partition selection in a DescribeQuorum request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

/// DescribeQuorum request shared by versions 0 through 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Vec<DescribeQuorumTopic>,
}

impl DescribeQuorumRequest {
    /// Encodes the flexible request header and body.
    pub fn encode(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&self.topics), |encoder, topic| {
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

/// Replica state returned by DescribeQuorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumReplicaState {
    pub replica_id: i32,
    pub replica_directory_id: Option<[u8; 16]>,
    pub log_end_offset: i64,
    pub last_fetch_timestamp: Option<i64>,
    pub last_caught_up_timestamp: Option<i64>,
}

/// One partition returned by DescribeQuorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumPartition {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub current_voters: Vec<DescribeQuorumReplicaState>,
    pub observers: Vec<DescribeQuorumReplicaState>,
}

/// One topic returned by DescribeQuorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumTopicResponse {
    pub name: String,
    pub partitions: Vec<DescribeQuorumPartition>,
}

/// One controller listener returned by DescribeQuorum v2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumListener {
    pub name: String,
    pub host: String,
    pub port: u16,
}

/// One controller node returned by DescribeQuorum v2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumNode {
    pub node_id: i32,
    pub listeners: Vec<DescribeQuorumListener>,
}

/// DescribeQuorum response for versions 0 through 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumResponse {
    pub api_version: i16,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub topics: Vec<DescribeQuorumTopicResponse>,
    pub nodes: Vec<DescribeQuorumNode>,
}

impl DescribeQuorumResponse {
    /// Decodes a flexible response body for the negotiated API version.
    pub fn decode_body(decoder: &mut Decoder<'_>, api_version: i16) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let error_message = if api_version >= 2 {
            decoder.read_compact_nullable_string()?
        } else {
            None
        };
        let topics = decoder
            .read_compact_array("describe quorum topics", |decoder| {
                let name = decoder.read_compact_string()?;
                let partitions = decoder
                    .read_compact_array("describe quorum partitions", |decoder| {
                        let partition_index = decoder.read_i32()?;
                        let error_code = decoder.read_i16()?;
                        let error_message = if api_version >= 2 {
                            decoder.read_compact_nullable_string()?
                        } else {
                            None
                        };
                        let leader_id = decoder.read_i32()?;
                        let leader_epoch = decoder.read_i32()?;
                        let high_watermark = decoder.read_i64()?;
                        let current_voters = decoder
                            .read_compact_array("describe quorum current voters", |decoder| {
                                decode_replica_state(decoder, api_version)
                            })?
                            .unwrap_or_default();
                        let observers = decoder
                            .read_compact_array("describe quorum observers", |decoder| {
                                decode_replica_state(decoder, api_version)
                            })?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok(DescribeQuorumPartition {
                            partition_index,
                            error_code,
                            error_message,
                            leader_id,
                            leader_epoch,
                            high_watermark,
                            current_voters,
                            observers,
                        })
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok(DescribeQuorumTopicResponse { name, partitions })
            })?
            .unwrap_or_default();
        let nodes = if api_version >= 2 {
            decoder
                .read_compact_array("describe quorum nodes", |decoder| {
                    let node_id = decoder.read_i32()?;
                    let listeners = decoder
                        .read_compact_array("describe quorum listeners", |decoder| {
                            let name = decoder.read_compact_string()?;
                            let host = decoder.read_compact_string()?;
                            let port = decoder.read_i16()? as u16;
                            decoder.read_tagged_fields()?;
                            Ok(DescribeQuorumListener { name, host, port })
                        })?
                        .unwrap_or_default();
                    decoder.read_tagged_fields()?;
                    Ok(DescribeQuorumNode { node_id, listeners })
                })?
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        decoder.read_tagged_fields()?;
        Ok(Self {
            api_version,
            error_code,
            error_message,
            topics,
            nodes,
        })
    }
}

fn decode_replica_state(
    decoder: &mut Decoder<'_>,
    api_version: i16,
) -> Result<DescribeQuorumReplicaState> {
    let replica_id = decoder.read_i32()?;
    let replica_directory_id = if api_version >= 2 {
        Some(decoder.read_uuid()?)
    } else {
        None
    };
    let log_end_offset = decoder.read_i64()?;
    let last_fetch_timestamp = if api_version >= 1 {
        Some(decoder.read_i64()?)
    } else {
        None
    };
    let last_caught_up_timestamp = if api_version >= 1 {
        Some(decoder.read_i64()?)
    } else {
        None
    };
    decoder.read_tagged_fields()?;
    Ok(DescribeQuorumReplicaState {
        replica_id,
        replica_directory_id,
        log_end_offset,
        last_fetch_timestamp,
        last_caught_up_timestamp,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DescribeQuorumRequest, DescribeQuorumResponse, DescribeQuorumTopic, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_quorum_v2_request() {
        let request = DescribeQuorumRequest {
            correlation_id: 12,
            client_id: Some("kafrust".to_owned()),
            topics: vec![DescribeQuorumTopic {
                name: "__cluster_metadata".to_owned(),
                partition_indexes: vec![0],
            }],
        };
        let bytes = request.encode(2).unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 2]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 12]);
        assert!(bytes
            .windows("__cluster_metadata".len())
            .any(|value| value == b"__cluster_metadata"));
        assert_eq!(*bytes.last().unwrap(), 0);
    }

    #[test]
    fn decodes_describe_quorum_v2_response_with_nodes_and_replica_state() {
        let mut bytes = Encoder::new();
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("__cluster_metadata").unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_i32(1);
        bytes.write_i32(4);
        bytes.write_i64(42);
        bytes.write_unsigned_varint(2);
        bytes.write_i32(1);
        bytes.write_uuid(&[8; 16]);
        bytes.write_i64(42);
        bytes.write_i64(100);
        bytes.write_i64(101);
        bytes.write_empty_tagged_fields();
        bytes.write_unsigned_varint(1);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_unsigned_varint(2);
        bytes.write_i32(1);
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("CONTROLLER").unwrap();
        bytes.write_compact_string("127.0.0.1").unwrap();
        bytes.write_i16(9093);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = DescribeQuorumResponse::decode_body(&mut decoder, 2).unwrap();
        assert_eq!(response.error_code, 0);
        assert_eq!(response.topics[0].partitions[0].leader_id, 1);
        assert_eq!(response.topics[0].partitions[0].high_watermark, 42);
        assert_eq!(
            response.topics[0].partitions[0].current_voters[0].replica_directory_id,
            Some([8; 16])
        );
        assert_eq!(response.nodes[0].listeners[0].port, 9093);
        assert!(decoder.is_empty());
    }
}
