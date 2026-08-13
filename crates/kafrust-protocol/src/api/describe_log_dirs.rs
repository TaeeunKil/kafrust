use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 35;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsRequest {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Option<Vec<DescribeLogDirsTopic>>,
}

impl DescribeLogDirsRequest {
    pub fn encode_v1(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encode_legacy_topics(&mut encoder, self.topics.as_deref())?;
        Ok(encoder.into_bytes())
    }

    pub fn encode_v2(&self, api_version: i16) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(self.topics.as_deref(), |encoder, topic| {
            encoder.write_compact_string(&topic.name)?;
            encoder.write_compact_array(Some(&topic.partition_indexes), |encoder, partition| {
                encoder.write_i32(*partition);
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
pub struct DescribeLogDirsTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub results: Vec<DescribeLogDirsResult>,
}

impl DescribeLogDirsResponse {
    pub fn decode_body_v1(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_body(decoder, false, false, false, false)
    }

    pub fn decode_body_v2(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_body(decoder, true, false, false, false)
    }

    pub fn decode_body_v3(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_body(decoder, true, true, false, false)
    }

    pub fn decode_body_v4(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_body(decoder, true, true, true, false)
    }

    pub fn decode_body_v5(decoder: &mut Decoder<'_>) -> Result<Self> {
        decode_body(decoder, true, true, true, true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsResult {
    pub error_code: i16,
    pub log_dir: String,
    pub topics: Vec<DescribeLogDirsTopicResult>,
    pub total_bytes: i64,
    pub usable_bytes: i64,
    pub is_cordoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsTopicResult {
    pub name: String,
    pub partitions: Vec<DescribeLogDirsPartitionResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsPartitionResult {
    pub partition_index: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future: bool,
}

fn encode_legacy_topics(
    encoder: &mut Encoder,
    topics: Option<&[DescribeLogDirsTopic]>,
) -> Result<()> {
    encoder.write_array(topics, |encoder, topic| {
        encoder.write_string(&topic.name)?;
        encoder.write_array(Some(&topic.partition_indexes), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })
    })
}

fn decode_body(
    decoder: &mut Decoder<'_>,
    flexible: bool,
    has_error_code: bool,
    has_capacity: bool,
    has_cordoned: bool,
) -> Result<DescribeLogDirsResponse> {
    let throttle_time_ms = decoder.read_i32()?;
    let error_code = if has_error_code {
        decoder.read_i16()?
    } else {
        0
    };
    let results = if flexible {
        decoder
            .read_compact_array("describe log dirs results", |decoder| {
                decode_flexible_result(decoder, has_capacity, has_cordoned)
            })?
            .unwrap_or_default()
    } else {
        decoder
            .read_array("describe log dirs results", |decoder| {
                decode_legacy_result(decoder, has_capacity, has_cordoned)
            })?
            .unwrap_or_default()
    };
    if flexible {
        decoder.read_tagged_fields()?;
    }
    Ok(DescribeLogDirsResponse {
        throttle_time_ms,
        error_code,
        results,
    })
}

fn decode_legacy_result(
    decoder: &mut Decoder<'_>,
    has_capacity: bool,
    has_cordoned: bool,
) -> Result<DescribeLogDirsResult> {
    let result = DescribeLogDirsResult {
        error_code: decoder.read_i16()?,
        log_dir: decoder.read_string()?,
        topics: decoder
            .read_array("describe log dirs topics", decode_legacy_topic_result)?
            .unwrap_or_default(),
        total_bytes: if has_capacity {
            decoder.read_i64()?
        } else {
            -1
        },
        usable_bytes: if has_capacity {
            decoder.read_i64()?
        } else {
            -1
        },
        is_cordoned: if has_cordoned {
            decoder.read_bool()?
        } else {
            false
        },
    };
    Ok(result)
}

fn decode_legacy_topic_result(decoder: &mut Decoder<'_>) -> Result<DescribeLogDirsTopicResult> {
    Ok(DescribeLogDirsTopicResult {
        name: decoder.read_string()?,
        partitions: decoder
            .read_array("describe log dirs partitions", decode_partition_result)?
            .unwrap_or_default(),
    })
}

fn decode_flexible_result(
    decoder: &mut Decoder<'_>,
    has_capacity: bool,
    has_cordoned: bool,
) -> Result<DescribeLogDirsResult> {
    let result = DescribeLogDirsResult {
        error_code: decoder.read_i16()?,
        log_dir: decoder.read_compact_string()?,
        topics: decoder
            .read_compact_array("describe log dirs topics", decode_flexible_topic_result)?
            .unwrap_or_default(),
        total_bytes: if has_capacity {
            decoder.read_i64()?
        } else {
            -1
        },
        usable_bytes: if has_capacity {
            decoder.read_i64()?
        } else {
            -1
        },
        is_cordoned: if has_cordoned {
            decoder.read_bool()?
        } else {
            false
        },
    };
    decoder.read_tagged_fields()?;
    Ok(result)
}

fn decode_flexible_topic_result(decoder: &mut Decoder<'_>) -> Result<DescribeLogDirsTopicResult> {
    let result = DescribeLogDirsTopicResult {
        name: decoder.read_compact_string()?,
        partitions: decoder
            .read_compact_array(
                "describe log dirs partitions",
                decode_flexible_partition_result,
            )?
            .unwrap_or_default(),
    };
    decoder.read_tagged_fields()?;
    Ok(result)
}

fn decode_flexible_partition_result(
    decoder: &mut Decoder<'_>,
) -> Result<DescribeLogDirsPartitionResult> {
    let result = decode_partition_result(decoder)?;
    decoder.read_tagged_fields()?;
    Ok(result)
}

fn decode_partition_result(decoder: &mut Decoder<'_>) -> Result<DescribeLogDirsPartitionResult> {
    Ok(DescribeLogDirsPartitionResult {
        partition_index: decoder.read_i32()?,
        partition_size: decoder.read_i64()?,
        offset_lag: decoder.read_i64()?,
        is_future: decoder.read_bool()?,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DescribeLogDirsRequest, DescribeLogDirsResponse, DescribeLogDirsTopic, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_log_dirs_v1_request_with_nullable_topics() {
        let request = DescribeLogDirsRequest {
            correlation_id: 35,
            client_id: Some("kafrust".to_owned()),
            topics: None,
        };

        let bytes = request.encode_v1().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 1]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 35]);
        assert_eq!(&bytes[17..21], &[255, 255, 255, 255]);
    }

    #[test]
    fn encodes_describe_log_dirs_v2_request_with_partition_filter() {
        let request = DescribeLogDirsRequest {
            correlation_id: 36,
            client_id: None,
            topics: Some(vec![DescribeLogDirsTopic {
                name: "orders".to_owned(),
                partition_indexes: vec![0, 2],
            }]),
        };

        let bytes = request.encode_v2(2).unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 2]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 36]);
        assert!(bytes
            .windows(7)
            .any(|window| { window == [7, b'o', b'r', b'd', b'e', b'r', b's'] }));
        assert!(bytes.ends_with(&[0]));
    }

    #[test]
    fn decodes_describe_log_dirs_v5_response_with_capacity_and_tags() {
        let mut bytes = Encoder::new();
        bytes.write_i32(11);
        bytes.write_i16(0);
        bytes.write_unsigned_varint(2); // one result
        bytes.write_i16(0);
        bytes.write_compact_string("/var/lib/kafka").unwrap();
        bytes.write_unsigned_varint(2); // one topic
        bytes.write_compact_string("orders").unwrap();
        bytes.write_unsigned_varint(2); // one partition
        bytes.write_i32(0);
        bytes.write_i64(4096);
        bytes.write_i64(3);
        bytes.write_bool(false);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_i64(1_000_000);
        bytes.write_i64(900_000);
        bytes.write_bool(false);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);

        let response = DescribeLogDirsResponse::decode_body_v5(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 11);
        assert_eq!(response.results[0].log_dir, "/var/lib/kafka");
        assert_eq!(response.results[0].topics[0].name, "orders");
        assert_eq!(
            response.results[0].topics[0].partitions[0].partition_size,
            4096
        );
        assert_eq!(response.results[0].topics[0].partitions[0].offset_lag, 3);
        assert_eq!(response.results[0].total_bytes, 1_000_000);
        assert_eq!(response.results[0].usable_bytes, 900_000);
        assert!(!response.results[0].is_cordoned);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_describe_log_dirs_v5_response_with_multiple_directories() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_unsigned_varint(3); // two log directories
        for (path, total, usable, cordoned) in [
            ("/var/lib/kafka", 100_i64, 90_i64, false),
            ("/var/lib/kafka-2", 200_i64, 180_i64, true),
        ] {
            bytes.write_i16(0);
            bytes.write_compact_string(path).unwrap();
            bytes.write_unsigned_varint(1); // no topics
            bytes.write_i64(total);
            bytes.write_i64(usable);
            bytes.write_bool(cordoned);
            bytes.write_empty_tagged_fields();
        }
        bytes.write_empty_tagged_fields();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = DescribeLogDirsResponse::decode_body_v5(&mut decoder).unwrap();

        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].log_dir, "/var/lib/kafka");
        assert_eq!(response.results[0].total_bytes, 100);
        assert_eq!(response.results[1].log_dir, "/var/lib/kafka-2");
        assert_eq!(response.results[1].usable_bytes, 180);
        assert!(response.results[1].is_cordoned);
        assert!(decoder.is_empty());
    }
}
