use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka ConsumerGroupHeartbeat API key.
pub const API_KEY: i16 = 68;

/// Topic partitions owned by a member in the KIP-848 consumer group protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatTopicPartitions {
    pub topic_id: [u8; 16],
    pub partitions: Vec<i32>,
}

impl ConsumerGroupHeartbeatTopicPartitions {
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
            .read_compact_array("consumer group heartbeat partitions", |decoder| {
                decoder.read_i32()
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

/// KIP-848 ConsumerGroupHeartbeat v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub subscribed_topic_names: Option<Vec<String>>,
    pub server_assignor: Option<String>,
    pub topic_partitions: Option<Vec<ConsumerGroupHeartbeatTopicPartitions>>,
}

impl ConsumerGroupHeartbeatRequestV0 {
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
        encoder.write_compact_nullable_string(self.instance_id.as_deref())?;
        encoder.write_compact_nullable_string(self.rack_id.as_deref())?;
        encoder.write_i32(self.rebalance_timeout_ms);
        encoder.write_compact_array(self.subscribed_topic_names.as_deref(), |encoder, topic| {
            encoder.write_compact_string(topic)
        })?;
        encoder.write_compact_nullable_string(self.server_assignor.as_deref())?;
        encoder.write_compact_array(self.topic_partitions.as_deref(), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// KIP-848 ConsumerGroupHeartbeat v1 request.
///
/// Version 1 keeps the v0 flexible wire shape and adds the nullable topic
/// subscription regular expression introduced by KIP-848. It also lets the
/// consumer provide a stable member ID, which is represented by the existing
/// `member_id` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub subscribed_topic_names: Option<Vec<String>>,
    pub subscribed_topic_regex: Option<String>,
    pub server_assignor: Option<String>,
    pub topic_partitions: Option<Vec<ConsumerGroupHeartbeatTopicPartitions>>,
}

impl ConsumerGroupHeartbeatRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.group_id)?;
        encoder.write_compact_string(&self.member_id)?;
        encoder.write_i32(self.member_epoch);
        encoder.write_compact_nullable_string(self.instance_id.as_deref())?;
        encoder.write_compact_nullable_string(self.rack_id.as_deref())?;
        encoder.write_i32(self.rebalance_timeout_ms);
        encoder.write_compact_array(self.subscribed_topic_names.as_deref(), |encoder, topic| {
            encoder.write_compact_string(topic)
        })?;
        encoder.write_compact_nullable_string(self.subscribed_topic_regex.as_deref())?;
        encoder.write_compact_nullable_string(self.server_assignor.as_deref())?;
        encoder.write_compact_array(self.topic_partitions.as_deref(), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// KIP-848 ConsumerGroupHeartbeat v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub assignment: Option<Vec<ConsumerGroupHeartbeatTopicPartitions>>,
}

impl ConsumerGroupHeartbeatResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let member_id = decoder.read_compact_nullable_string()?;
        let member_epoch = decoder.read_i32()?;
        let heartbeat_interval_ms = decoder.read_i32()?;
        // Assignment is a nullable struct, not a compact nullable array. The
        // struct contains the compact topic-partitions array and its tags.
        let assignment = match decoder.read_i8()? {
            -1 => None,
            1 => {
                let topic_partitions = decoder
                    .read_compact_array("consumer group heartbeat assignment", |decoder| {
                        ConsumerGroupHeartbeatTopicPartitions::decode(decoder)
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Some(topic_partitions)
            }
            marker => return Err(crate::error::Error::InvalidNullableStruct(marker)),
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

/// ConsumerGroupHeartbeat v1 has the same response wire shape as v0.
pub type ConsumerGroupHeartbeatResponseV1 = ConsumerGroupHeartbeatResponseV0;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ConsumerGroupHeartbeatRequestV0, ConsumerGroupHeartbeatRequestV1,
        ConsumerGroupHeartbeatResponseV0, ConsumerGroupHeartbeatResponseV1,
        ConsumerGroupHeartbeatTopicPartitions,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_kip_848_heartbeat_request_with_subscription_and_assignment() {
        let request = ConsumerGroupHeartbeatRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            member_id: "member-a".to_owned(),
            member_epoch: 4,
            instance_id: Some("instance-a".to_owned()),
            rack_id: None,
            rebalance_timeout_ms: 30_000,
            subscribed_topic_names: Some(vec!["orders".to_owned()]),
            server_assignor: Some("uniform".to_owned()),
            topic_partitions: Some(vec![ConsumerGroupHeartbeatTopicPartitions {
                topic_id: [1; 16],
                partitions: vec![0, 2],
            }]),
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 68, 0, 0]);
        assert_eq!(&encoded[4..12], &[0, 0, 0, 23, 0, 7, b'k', b'a']);
        assert!(encoded.windows(12).any(|bytes| bytes == b"orders-group"));
        assert!(encoded.windows(16).any(|bytes| bytes == [1; 16]));
        assert_eq!(*encoded.last().unwrap(), 0);
    }

    #[test]
    fn decodes_kip_848_heartbeat_response_and_assignment() {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes
            .write_compact_nullable_string(Some("member-a"))
            .unwrap();
        bytes.write_i32(5);
        bytes.write_i32(2500);
        bytes.write_i8(1);
        bytes
            .write_compact_array(
                Some(&[ConsumerGroupHeartbeatTopicPartitions {
                    topic_id: [2; 16],
                    partitions: vec![1, 3],
                }]),
                |encoder, topic| topic.encode(encoder),
            )
            .unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ConsumerGroupHeartbeatResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.member_id.as_deref(), Some("member-a"));
        assert_eq!(response.member_epoch, 5);
        assert_eq!(response.heartbeat_interval_ms, 2500);
        assert_eq!(response.assignment.unwrap()[0].partitions, vec![1, 3]);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_kip_848_heartbeat_response_with_null_assignment_struct() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes
            .write_compact_nullable_string(Some("member-a"))
            .unwrap();
        bytes.write_i32(1);
        bytes.write_i32(2500);
        bytes.write_i8(-1);
        bytes.write_empty_tagged_fields();

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ConsumerGroupHeartbeatResponseV0::decode_body(&mut decoder).unwrap();

        assert!(response.assignment.is_none());
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_nullable_kip_848_fields_as_null_arrays() {
        let request = ConsumerGroupHeartbeatRequestV0 {
            correlation_id: 1,
            client_id: None,
            group_id: "g".to_owned(),
            member_id: "m".to_owned(),
            member_epoch: -1,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: -1,
            subscribed_topic_names: None,
            server_assignor: None,
            topic_partitions: None,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..10], &[0, 68, 0, 0, 0, 0, 0, 1, 0xff, 0xff]);
        assert_eq!(encoded[10], 0); // request-header tagged fields
        assert!(encoded.ends_with(&[0, 0]));
    }

    #[test]
    fn encodes_kip_848_heartbeat_v1_with_topic_regex() {
        let request = ConsumerGroupHeartbeatRequestV1 {
            correlation_id: 9,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            member_id: "member-a".to_owned(),
            member_epoch: 2,
            instance_id: None,
            rack_id: Some("rack-a".to_owned()),
            rebalance_timeout_ms: 30_000,
            subscribed_topic_names: None,
            subscribed_topic_regex: Some("orders-.*".to_owned()),
            server_assignor: Some("uniform".to_owned()),
            topic_partitions: None,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 68, 0, 1]);
        assert!(encoded.windows(9).any(|bytes| bytes == b"orders-.*"));
        assert!(encoded.ends_with(&[0, 0]));
    }

    #[test]
    fn v1_response_alias_decodes_the_v0_wire_shape() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes
            .write_compact_nullable_string(Some("member-a"))
            .unwrap();
        bytes.write_i32(3);
        bytes.write_i32(2500);
        bytes.write_i8(-1);
        bytes.write_empty_tagged_fields();

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ConsumerGroupHeartbeatResponseV1::decode_body(&mut decoder).unwrap();
        assert_eq!(response.member_epoch, 3);
        assert!(response.assignment.is_none());
        assert!(decoder.is_empty());
    }
}
