use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 9;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Option<Vec<OffsetFetchTopic>>,
}

impl OffsetFetchRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_array(self.topics.as_deref(), |encoder, topic| {
            topic.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

/// OffsetFetch v9 request for Kafka's KIP-848 consumer group protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequestV9 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub topics: Option<Vec<OffsetFetchTopicV9>>,
    pub require_stable: bool,
}

impl OffsetFetchRequestV9 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 9,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_compact_string(&self.group_id)?;
            encoder.write_compact_nullable_string(self.member_id.as_deref())?;
            encoder.write_i32(self.member_epoch);
            encoder.write_compact_array(self.topics.as_deref(), |encoder, topic| {
                topic.encode(encoder)
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_bool(self.require_stable);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

impl OffsetFetchTopic {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(
            Some(self.partition_indexes.as_slice()),
            |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopicV9 {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

impl OffsetFetchTopicV9 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.name)?;
        encoder.write_compact_array(
            Some(self.partition_indexes.as_slice()),
            |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            },
        )?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponseV2 {
    pub topics: Vec<OffsetFetchTopicResponse>,
    pub error_code: i16,
}

impl OffsetFetchResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            topics: decoder
                .read_array(
                    "offset fetch topic responses",
                    OffsetFetchTopicResponse::decode,
                )?
                .unwrap_or_default(),
            error_code: decoder.read_i16()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponseV9 {
    pub throttle_time_ms: i32,
    pub groups: Vec<OffsetFetchGroupResponse>,
}

impl OffsetFetchResponseV9 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let groups = decoder
            .read_compact_array("offset fetch group responses", |decoder| {
                let group_id = decoder.read_compact_string()?;
                let topics = decoder
                    .read_compact_array("offset fetch topic responses", |decoder| {
                        let name = decoder.read_compact_string()?;
                        let partitions = decoder
                            .read_compact_array("offset fetch partition responses", |decoder| {
                                let partition_index = decoder.read_i32()?;
                                let committed_offset = decoder.read_i64()?;
                                let _committed_leader_epoch = decoder.read_i32()?;
                                let metadata = decoder.read_compact_nullable_string()?;
                                let error_code = decoder.read_i16()?;
                                decoder.read_tagged_fields()?;
                                Ok(OffsetFetchPartitionResponse {
                                    partition_index,
                                    committed_offset,
                                    metadata,
                                    error_code,
                                })
                            })?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok(OffsetFetchTopicResponse { name, partitions })
                    })?
                    .unwrap_or_default();
                let error_code = decoder.read_i16()?;
                decoder.read_tagged_fields()?;
                Ok(OffsetFetchGroupResponse {
                    group_id,
                    topics,
                    error_code,
                })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroupResponse {
    pub group_id: String,
    pub topics: Vec<OffsetFetchTopicResponse>,
    pub error_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

impl OffsetFetchTopicResponse {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "offset fetch partition responses",
                    OffsetFetchPartitionResponse::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
    pub error_code: i16,
}

impl OffsetFetchPartitionResponse {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            committed_offset: decoder.read_i64()?,
            metadata: decoder.read_nullable_string()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        OffsetFetchGroupResponse, OffsetFetchPartitionResponse, OffsetFetchRequestV2,
        OffsetFetchRequestV9, OffsetFetchResponseV2, OffsetFetchResponseV9, OffsetFetchTopic,
        OffsetFetchTopicResponse, OffsetFetchTopicV9,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_offset_fetch_v2_request_for_partitions() {
        let request = OffsetFetchRequestV2 {
            correlation_id: 29,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            topics: Some(vec![OffsetFetchTopic {
                name: "orders".to_owned(),
                partition_indexes: vec![0, 1],
            }]),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 9, // api key
                0, 2, // api version
                0, 0, 0, 29, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0, 0, 0, 1, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
                0, 0, 0, 2, // partition count
                0, 0, 0, 0, // partition 0
                0, 0, 0, 1, // partition 1
            ]
        );
    }

    #[test]
    fn encodes_offset_fetch_v2_request_for_all_topics() {
        let request = OffsetFetchRequestV2 {
            correlation_id: 31,
            client_id: None,
            group_id: "orders-group".to_owned(),
            topics: None,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 9, // api key
                0, 2, // api version
                0, 0, 0, 31, // correlation id
                0xff, 0xff, // null client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // group id
                0xff, 0xff, 0xff, 0xff, // null topics
            ]
        );
    }

    #[test]
    fn decodes_offset_fetch_v2_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i64(42);
        bytes.write_nullable_string(Some("processed")).unwrap();
        bytes.write_i16(0);
        bytes.write_i16(0);
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = OffsetFetchResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(
            response,
            OffsetFetchResponseV2 {
                topics: vec![OffsetFetchTopicResponse {
                    name: "orders".to_owned(),
                    partitions: vec![OffsetFetchPartitionResponse {
                        partition_index: 0,
                        committed_offset: 42,
                        metadata: Some("processed".to_owned()),
                        error_code: 0,
                    }],
                }],
                error_code: 0,
            }
        );
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_offset_fetch_v9_request_for_consumer_protocol() {
        let request = OffsetFetchRequestV9 {
            correlation_id: 29,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            member_id: Some("member-a".to_owned()),
            member_epoch: 7,
            topics: Some(vec![OffsetFetchTopicV9 {
                name: "orders".to_owned(),
                partition_indexes: vec![0, 1],
            }]),
            require_stable: false,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 9, 0, 9]);
        let mut decoder = Decoder::new(&encoded[18..]);
        let groups = decoder
            .read_compact_array("offset fetch groups", |decoder| {
                let group_id = decoder.read_compact_string()?;
                let member_id = decoder.read_compact_nullable_string()?;
                let member_epoch = decoder.read_i32()?;
                let topics = decoder
                    .read_compact_array("offset fetch topics", |decoder| {
                        let name = decoder.read_compact_string()?;
                        let partitions = decoder
                            .read_compact_array("offset fetch partitions", |decoder| {
                                decoder.read_i32()
                            })?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok((name, partitions))
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok((group_id, member_id, member_epoch, topics))
            })
            .unwrap()
            .unwrap();
        assert_eq!(groups[0].0, "orders-group");
        assert_eq!(groups[0].1, Some("member-a".to_owned()));
        assert_eq!(groups[0].2, 7);
        assert_eq!(groups[0].3[0].0, "orders");
        assert_eq!(groups[0].3[0].1, vec![0, 1]);
        assert!(!decoder.read_bool().unwrap());
        decoder.read_tagged_fields().unwrap();
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_offset_fetch_v9_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders-group")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("orders")?;
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_i32(0);
                        encoder.write_i64(42);
                        encoder.write_i32(-1);
                        encoder.write_compact_nullable_string(Some("processed"))?;
                        encoder.write_i16(0);
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i16(0);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        bytes.write_empty_tagged_fields();

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = OffsetFetchResponseV9::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(
            response.groups,
            vec![OffsetFetchGroupResponse {
                group_id: "orders-group".to_owned(),
                topics: vec![OffsetFetchTopicResponse {
                    name: "orders".to_owned(),
                    partitions: vec![OffsetFetchPartitionResponse {
                        partition_index: 0,
                        committed_offset: 42,
                        metadata: Some("processed".to_owned()),
                        error_code: 0,
                    }],
                }],
                error_code: 0,
            }]
        );
        assert!(decoder.is_empty());
    }
}
