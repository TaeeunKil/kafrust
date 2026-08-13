use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 34;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub dirs: Vec<AlterReplicaLogDir>,
}

impl AlterReplicaLogDirsRequest {
    /// Encodes the non-flexible v1 request schema.
    pub fn encode_v1(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encode_legacy_dirs(&mut encoder, &self.dirs)?;
        Ok(encoder.into_bytes())
    }

    /// Encodes a flexible v2 or newer request schema.
    pub fn encode_v2(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&self.dirs), encode_flexible_dir)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDir {
    pub path: String,
    pub topics: Vec<AlterReplicaLogDirTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResponse {
    pub throttle_time_ms: i32,
    pub results: Vec<AlterReplicaLogDirTopicResult>,
}

impl AlterReplicaLogDirsResponse {
    /// Decodes the non-flexible v1 response schema.
    pub fn decode_body_v1(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_body(decoder, false)
    }

    /// Decodes the flexible v2 response schema.
    pub fn decode_body_v2(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_body(decoder, true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirTopicResult {
    pub name: String,
    pub partitions: Vec<AlterReplicaLogDirPartitionResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirPartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
}

fn encode_legacy_dirs(encoder: &mut Encoder, dirs: &[AlterReplicaLogDir]) -> Result<()> {
    encoder.write_array(Some(dirs), |encoder, dir| {
        encoder.write_string(&dir.path)?;
        encoder.write_array(Some(&dir.topics), |encoder, topic| {
            encoder.write_string(&topic.name)?;
            encoder.write_array(Some(&topic.partitions), |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            })
        })
    })
}

fn encode_flexible_dir(encoder: &mut Encoder, dir: &AlterReplicaLogDir) -> Result<()> {
    encoder.write_compact_string(&dir.path)?;
    encoder.write_compact_array(Some(&dir.topics), |encoder, topic| {
        encoder.write_compact_string(&topic.name)?;
        encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    })?;
    encoder.write_empty_tagged_fields();
    Ok(())
}

fn decode_body(decoder: &mut Decoder<'_>, flexible: bool) -> Result<AlterReplicaLogDirsResponse> {
    let throttle_time_ms = decoder.read_i32()?;
    let results = if flexible {
        decoder
            .read_compact_array(
                "alter replica log dirs results",
                decode_flexible_topic_result,
            )?
            .unwrap_or_default()
    } else {
        decoder
            .read_array("alter replica log dirs results", decode_legacy_topic_result)?
            .unwrap_or_default()
    };
    if flexible {
        decoder.read_tagged_fields()?;
    }
    Ok(AlterReplicaLogDirsResponse {
        throttle_time_ms,
        results,
    })
}

fn decode_legacy_topic_result(decoder: &mut Decoder<'_>) -> Result<AlterReplicaLogDirTopicResult> {
    Ok(AlterReplicaLogDirTopicResult {
        name: decoder.read_string()?,
        partitions: decoder
            .read_array("alter replica log dirs partitions", decode_partition_result)?
            .unwrap_or_default(),
    })
}

fn decode_flexible_topic_result(
    decoder: &mut Decoder<'_>,
) -> Result<AlterReplicaLogDirTopicResult> {
    let result = AlterReplicaLogDirTopicResult {
        name: decoder.read_compact_string()?,
        partitions: decoder
            .read_compact_array(
                "alter replica log dirs partitions",
                decode_flexible_partition_result,
            )?
            .unwrap_or_default(),
    };
    decoder.read_tagged_fields()?;
    Ok(result)
}

fn decode_flexible_partition_result(
    decoder: &mut Decoder<'_>,
) -> Result<AlterReplicaLogDirPartitionResult> {
    let result = decode_partition_result(decoder)?;
    decoder.read_tagged_fields()?;
    Ok(result)
}

fn decode_partition_result(decoder: &mut Decoder<'_>) -> Result<AlterReplicaLogDirPartitionResult> {
    Ok(AlterReplicaLogDirPartitionResult {
        partition_index: decoder.read_i32()?,
        error_code: decoder.read_i16()?,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AlterReplicaLogDir, AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopic,
        AlterReplicaLogDirTopicResult, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
        API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_alter_replica_log_dirs_v1() {
        let request = AlterReplicaLogDirsRequest {
            correlation_id: 34,
            client_id: Some("kafrust".to_owned()),
            dirs: vec![AlterReplicaLogDir {
                path: "/var/lib/kafka-2".to_owned(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "orders".to_owned(),
                    partitions: vec![0, 2],
                }],
            }],
        };

        let bytes = request.encode_v1().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 1]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 34]);
        assert!(bytes
            .windows(8)
            .any(|window| { window == [0, 6, b'o', b'r', b'd', b'e', b'r', b's'] }));
        assert!(bytes.windows(4).any(|window| window == [0, 0, 0, 2]));
    }

    #[test]
    fn encodes_alter_replica_log_dirs_v2_with_tagged_fields() {
        let request = AlterReplicaLogDirsRequest {
            correlation_id: 35,
            client_id: None,
            dirs: vec![AlterReplicaLogDir {
                path: "/var/lib/kafka-2".to_owned(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "orders".to_owned(),
                    partitions: vec![1],
                }],
            }],
        };

        let bytes = request.encode_v2(2).unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 2]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 35]);
        assert!(bytes.ends_with(&[0]));
    }

    #[test]
    fn decodes_alter_replica_log_dirs_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(7);
        bytes
            .write_array(
                Some(&[AlterReplicaLogDirTopicResult {
                    name: "orders".to_owned(),
                    partitions: vec![AlterReplicaLogDirPartitionResult {
                        partition_index: 0,
                        error_code: 0,
                    }],
                }]),
                |encoder, topic| {
                    encoder.write_string(&topic.name)?;
                    encoder.write_array(Some(&topic.partitions), |encoder, partition| {
                        encoder.write_i32(partition.partition_index);
                        encoder.write_i16(partition.error_code);
                        Ok(())
                    })
                },
            )
            .unwrap();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = AlterReplicaLogDirsResponse::decode_body_v1(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(response.results[0].name, "orders");
        assert_eq!(response.results[0].partitions[0].partition_index, 0);
        assert_eq!(response.results[0].partitions[0].error_code, 0);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_alter_replica_log_dirs_v2_response_with_tagged_fields() {
        let mut bytes = Encoder::new();
        bytes.write_i32(8);
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("orders").unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_i32(1);
        bytes.write_i16(0);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = AlterReplicaLogDirsResponse::decode_body_v2(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 8);
        assert_eq!(response.results[0].partitions[0].partition_index, 1);
        assert!(decoder.is_empty());
    }
}
