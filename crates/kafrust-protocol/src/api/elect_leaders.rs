use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 43;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Option<Vec<ElectLeadersTopicV0>>,
    pub timeout_ms: i32,
}

impl ElectLeadersRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encode_legacy_topics(&mut encoder, self.topics.as_deref())?;
        encoder.write_i32(self.timeout_ms);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub election_type: i8,
    pub topics: Option<Vec<ElectLeadersTopicV0>>,
    pub timeout_ms: i32,
}

impl ElectLeadersRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_i8(self.election_type);
        encode_legacy_topics(&mut encoder, self.topics.as_deref())?;
        encoder.write_i32(self.timeout_ms);
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub election_type: i8,
    pub topics: Option<Vec<ElectLeadersTopicV0>>,
    pub timeout_ms: i32,
}

impl ElectLeadersRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i8(self.election_type);
        encoder.write_compact_array(self.topics.as_deref(), |encoder, topic| {
            encoder.write_compact_string(&topic.name)?;
            encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_i32(self.timeout_ms);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersTopicV0 {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersResponseV0 {
    pub throttle_time_ms: i32,
    pub results: Vec<ElectLeadersTopicResultV0>,
}

impl ElectLeadersResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            results: decoder
                .read_array("elect leaders results", decode_legacy_topic_result)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub results: Vec<ElectLeadersTopicResultV0>,
}

impl ElectLeadersResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            results: decoder
                .read_array("elect leaders results", decode_legacy_topic_result)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersResponseV2 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub results: Vec<ElectLeadersTopicResultV0>,
}

impl ElectLeadersResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let results = decoder
            .read_compact_array("elect leaders results", |decoder| {
                let name = decoder.read_compact_string()?;
                let partitions = decoder
                    .read_compact_array("elect leaders partition results", |decoder| {
                        let partition_index = decoder.read_i32()?;
                        let error_code = decoder.read_i16()?;
                        let error_message = decoder.read_compact_nullable_string()?;
                        decoder.read_tagged_fields()?;
                        Ok(ElectLeadersPartitionResultV0 {
                            partition_index,
                            error_code,
                            error_message,
                        })
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok(ElectLeadersTopicResultV0 { name, partitions })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            results,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersTopicResultV0 {
    pub name: String,
    pub partitions: Vec<ElectLeadersPartitionResultV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersPartitionResultV0 {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

fn encode_legacy_topics(
    encoder: &mut Encoder,
    topics: Option<&[ElectLeadersTopicV0]>,
) -> Result<()> {
    encoder.write_array(topics, |encoder, topic| {
        encoder.write_string(&topic.name)?;
        encoder.write_array(Some(&topic.partitions), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })
    })
}

fn decode_legacy_topic_result(decoder: &mut Decoder<'_>) -> Result<ElectLeadersTopicResultV0> {
    let name = decoder.read_string()?;
    let partitions = decoder
        .read_array("elect leaders partition results", |decoder| {
            Ok(ElectLeadersPartitionResultV0 {
                partition_index: decoder.read_i32()?,
                error_code: decoder.read_i16()?,
                error_message: decoder.read_nullable_string()?,
            })
        })?
        .unwrap_or_default();
    Ok(ElectLeadersTopicResultV0 { name, partitions })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ElectLeadersRequestV0, ElectLeadersRequestV1, ElectLeadersRequestV2,
        ElectLeadersResponseV1, ElectLeadersResponseV2, ElectLeadersTopicV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_elect_leaders_v0_with_all_topics() {
        let request = ElectLeadersRequestV0 {
            correlation_id: 43,
            client_id: Some("kafrust".to_owned()),
            topics: None,
            timeout_ms: 30_000,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 43]);
        assert_eq!(&bytes[17..21], &[255, 255, 255, 255]);
        assert_eq!(&bytes[21..25], &30_000_i32.to_be_bytes());
    }

    #[test]
    fn encodes_elect_leaders_v1_with_partition_filter() {
        let request = ElectLeadersRequestV1 {
            correlation_id: 44,
            client_id: None,
            election_type: 1,
            topics: Some(vec![ElectLeadersTopicV0 {
                name: "orders".to_owned(),
                partitions: vec![0, 2],
            }]),
            timeout_ms: 10_000,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 1]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 44]);
        assert_eq!(bytes[10], 1);
        assert!(bytes
            .windows(6)
            .any(|window| window == [0, 6, b'o', b'r', b'd', b'e']));
        assert!(bytes.ends_with(&10_000_i32.to_be_bytes()));
    }

    #[test]
    fn encodes_elect_leaders_v2_with_flexible_fields() {
        let request = ElectLeadersRequestV2 {
            correlation_id: 45,
            client_id: None,
            election_type: 0,
            topics: Some(vec![ElectLeadersTopicV0 {
                name: "orders".to_owned(),
                partitions: vec![1],
            }]),
            timeout_ms: 5_000,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 2]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 45]);
        assert_eq!(bytes[11], 0);
        assert!(bytes.ends_with(&[0]));
    }

    #[test]
    fn decodes_elect_leaders_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(8);
        bytes.write_i16(0);
        bytes
            .write_array(
                Some(&[ElectLeadersTopicV0 {
                    name: "orders".to_owned(),
                    partitions: vec![0],
                }]),
                |encoder, topic| {
                    encoder.write_string(&topic.name)?;
                    encoder.write_array(Some(&[0_i32]), |encoder, partition| {
                        encoder.write_i32(*partition);
                        encoder.write_i16(0);
                        encoder.write_nullable_string(Some("ok"))
                    })
                },
            )
            .unwrap();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ElectLeadersResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 8);
        assert_eq!(response.results[0].name, "orders");
        assert_eq!(response.results[0].partitions[0].partition_index, 0);
        assert_eq!(
            response.results[0].partitions[0].error_message.as_deref(),
            Some("ok")
        );
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_elect_leaders_v2_response_with_tagged_fields() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i16(0);
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("orders").unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_i32(1);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ElectLeadersResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.results[0].partitions[0].partition_index, 1);
        assert!(decoder.is_empty());
    }
}
