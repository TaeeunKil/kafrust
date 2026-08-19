use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka ConsumerGroupDescribe API key.
pub const API_KEY: i16 = 69;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescribeRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_ids: Vec<String>,
    pub include_authorized_operations: bool,
}

impl ConsumerGroupDescribeRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(
            0,
            self.correlation_id,
            self.client_id.as_deref(),
            &self.group_ids,
            self.include_authorized_operations,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescribeRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_ids: Vec<String>,
    pub include_authorized_operations: bool,
}

impl ConsumerGroupDescribeRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_request(
            1,
            self.correlation_id,
            self.client_id.as_deref(),
            &self.group_ids,
            self.include_authorized_operations,
        )
    }
}

fn encode_request(
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: API_KEY,
        api_version,
        correlation_id,
        client_id: client_id.map(str::to_owned),
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_array(Some(group_ids), |encoder, group_id| {
        encoder.write_compact_string(group_id)
    })?;
    encoder.write_bool(include_authorized_operations);
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescribeResponseV0 {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedConsumerGroup>,
}

impl ConsumerGroupDescribeResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let groups = decoder
            .read_compact_array("consumer group descriptions", |decoder| {
                DescribedConsumerGroup::decode(decoder, false)
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
pub struct ConsumerGroupDescribeResponseV1 {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedConsumerGroup>,
}

impl ConsumerGroupDescribeResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let groups = decoder
            .read_compact_array("consumer group descriptions", |decoder| {
                DescribedConsumerGroup::decode(decoder, true)
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
pub struct DescribedConsumerGroup {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub group_id: String,
    pub group_state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor_name: String,
    pub members: Vec<DescribedConsumerGroupMember>,
    pub authorized_operations: i32,
}

impl DescribedConsumerGroup {
    fn decode(decoder: &mut Decoder<'_>, has_member_type: bool) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let group_id = decoder.read_compact_string()?;
        let group_state = decoder.read_compact_string()?;
        let group_epoch = decoder.read_i32()?;
        let assignment_epoch = decoder.read_i32()?;
        let assignor_name = decoder.read_compact_string()?;
        let members = decoder
            .read_compact_array("consumer group members", |decoder| {
                DescribedConsumerGroupMember::decode(decoder, has_member_type)
            })?
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
            assignor_name,
            members,
            authorized_operations,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedConsumerGroupMember {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub subscribed_topic_regex: Option<String>,
    pub assignment: ConsumerGroupDescribeAssignment,
    pub target_assignment: ConsumerGroupDescribeAssignment,
    /// -1 is unknown, 0 is classic, and 1 is a consumer-protocol member.
    pub member_type: i8,
}

impl DescribedConsumerGroupMember {
    fn decode(decoder: &mut Decoder<'_>, has_member_type: bool) -> Result<Self> {
        let member_id = decoder.read_compact_string()?;
        let instance_id = decoder.read_compact_nullable_string()?;
        let rack_id = decoder.read_compact_nullable_string()?;
        let member_epoch = decoder.read_i32()?;
        let client_id = decoder.read_compact_string()?;
        let client_host = decoder.read_compact_string()?;
        let subscribed_topic_names = decoder
            .read_compact_array("subscribed topic names", |decoder| {
                decoder.read_compact_string()
            })?
            .unwrap_or_default();
        let subscribed_topic_regex = decoder.read_compact_nullable_string()?;
        let assignment = ConsumerGroupDescribeAssignment::decode(decoder)?;
        let target_assignment = ConsumerGroupDescribeAssignment::decode(decoder)?;
        let member_type = if has_member_type {
            decoder.read_i8()?
        } else {
            -1
        };
        decoder.read_tagged_fields()?;
        Ok(Self {
            member_id,
            instance_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            assignment,
            target_assignment,
            member_type,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescribeAssignment {
    pub topic_partitions: Vec<ConsumerGroupDescribeTopicPartitions>,
}

impl ConsumerGroupDescribeAssignment {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_partitions = decoder
            .read_compact_array("consumer group assignment topics", |decoder| {
                ConsumerGroupDescribeTopicPartitions::decode(decoder)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self { topic_partitions })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescribeTopicPartitions {
    pub topic_id: [u8; 16],
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

impl ConsumerGroupDescribeTopicPartitions {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_id = decoder.read_uuid()?;
        let topic_name = decoder.read_compact_string()?;
        let partitions = decoder
            .read_compact_array("consumer group assignment partitions", |decoder| {
                decoder.read_i32()
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            topic_id,
            topic_name,
            partitions,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{ConsumerGroupDescribeRequestV1, ConsumerGroupDescribeResponseV1, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_consumer_group_describe_v1_request() {
        let request = ConsumerGroupDescribeRequestV1 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_ids: vec!["orders".to_owned(), "payments".to_owned()],
            include_authorized_operations: true,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..4], &[0, 69, 0, 1]);
        assert_eq!(API_KEY, 69);
    }

    #[test]
    fn decodes_consumer_group_describe_v1_response() -> crate::error::Result<()> {
        let mut bytes = Encoder::new();
        bytes.write_i32(9);
        bytes.write_compact_array(Some(&[1_i8]), |encoder, _| {
            encoder.write_i16(0);
            encoder.write_compact_nullable_string(Some("ok"))?;
            encoder.write_compact_string("orders")?;
            encoder.write_compact_string("Stable")?;
            encoder.write_i32(4);
            encoder.write_i32(5);
            encoder.write_compact_string("uniform")?;
            encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                encoder.write_compact_string("member-1")?;
                encoder.write_compact_nullable_string(None)?;
                encoder.write_compact_nullable_string(Some("rack-a"))?;
                encoder.write_i32(7);
                encoder.write_compact_string("client-a")?;
                encoder.write_compact_string("/127.0.0.1")?;
                encoder.write_compact_array(Some(&["orders".to_owned()]), |encoder, topic| {
                    encoder.write_compact_string(topic)
                })?;
                encoder.write_compact_nullable_string(None)?;
                for _ in 0..2 {
                    encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                        encoder.write_uuid(&[7; 16]);
                        encoder.write_compact_string("orders")?;
                        encoder.write_compact_array(Some(&[0_i32, 2]), |encoder, partition| {
                            encoder.write_i32(*partition);
                            Ok(())
                        })?;
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                }
                encoder.write_i8(1);
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_i32(-2147483648);
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ConsumerGroupDescribeResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 9);
        assert_eq!(response.groups[0].group_id, "orders");
        assert_eq!(response.groups[0].group_epoch, 4);
        assert_eq!(response.groups[0].members[0].member_type, 1);
        assert_eq!(
            response.groups[0].members[0].assignment.topic_partitions[0].partitions,
            [0, 2]
        );
        assert_eq!(response.groups[0].authorized_operations, -2147483648);
        assert!(decoder.is_empty());
        Ok(())
    }
}
