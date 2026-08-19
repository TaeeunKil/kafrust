use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka ShareGroupDescribe API key.
pub const API_KEY: i16 = 77;

/// ShareGroupDescribe v1 request.
///
/// Version 1 is the stable KIP-932 wire shape. Kafka 4.0's early-access v0
/// was removed in Kafka 4.1, so the public protocol surface starts at v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescribeRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_ids: Vec<String>,
    pub include_authorized_operations: bool,
}

impl ShareGroupDescribeRequestV1 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
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

/// ShareGroupDescribe v1 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescribeResponseV1 {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedShareGroup>,
}

impl ShareGroupDescribeResponseV1 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let groups = decoder
            .read_compact_array("share group descriptions", DescribedShareGroup::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

/// One share group returned by ShareGroupDescribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroup {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub group_id: String,
    pub group_state: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignor_name: String,
    pub members: Vec<DescribedShareGroupMember>,
    pub authorized_operations: i32,
}

impl DescribedShareGroup {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let group_id = decoder.read_compact_string()?;
        let group_state = decoder.read_compact_string()?;
        let group_epoch = decoder.read_i32()?;
        let assignment_epoch = decoder.read_i32()?;
        let assignor_name = decoder.read_compact_string()?;
        let members = decoder
            .read_compact_array("share group members", DescribedShareGroupMember::decode)?
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

/// One member returned by ShareGroupDescribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedShareGroupMember {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assignment: ShareGroupDescribeAssignment,
}

impl DescribedShareGroupMember {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let member_id = decoder.read_compact_string()?;
        let rack_id = decoder.read_compact_nullable_string()?;
        let member_epoch = decoder.read_i32()?;
        let client_id = decoder.read_compact_string()?;
        let client_host = decoder.read_compact_string()?;
        let subscribed_topic_names = decoder
            .read_compact_array("share group subscribed topics", |decoder| {
                decoder.read_compact_string()
            })?
            .unwrap_or_default();
        let assignment = ShareGroupDescribeAssignment::decode(decoder)?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            member_id,
            rack_id,
            member_epoch,
            client_id,
            client_host,
            subscribed_topic_names,
            assignment,
        })
    }
}

/// Assignment returned for one share-group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescribeAssignment {
    pub topic_partitions: Vec<ShareGroupDescribeTopicPartitions>,
}

impl ShareGroupDescribeAssignment {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_partitions = decoder
            .read_compact_array("share group assignment topics", |decoder| {
                ShareGroupDescribeTopicPartitions::decode(decoder)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self { topic_partitions })
    }
}

/// Topic partitions assigned to one share-group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescribeTopicPartitions {
    pub topic_id: [u8; 16],
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

impl ShareGroupDescribeTopicPartitions {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_id = decoder.read_uuid()?;
        let topic_name = decoder.read_compact_string()?;
        let partitions = decoder
            .read_compact_array("share group assignment partitions", |decoder| {
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
    use super::{ShareGroupDescribeRequestV1, ShareGroupDescribeResponseV1, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_share_group_describe_v1_request() {
        let request = ShareGroupDescribeRequestV1 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_ids: vec!["share-orders".to_owned()],
            include_authorized_operations: true,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..4], &[0, 77, 0, 1]);
        assert_eq!(API_KEY, 77);
    }

    #[test]
    fn decodes_share_group_describe_v1_response() -> crate::error::Result<()> {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes.write_compact_array(Some(&[1_i8]), |encoder, _| {
            encoder.write_i16(0);
            encoder.write_compact_nullable_string(Some("ok"))?;
            encoder.write_compact_string("share-orders")?;
            encoder.write_compact_string("Stable")?;
            encoder.write_i32(4);
            encoder.write_i32(5);
            encoder.write_compact_string("uniform")?;
            encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                encoder.write_compact_string("member-1")?;
                encoder.write_compact_nullable_string(Some("rack-a"))?;
                encoder.write_i32(7);
                encoder.write_compact_string("client-a")?;
                encoder.write_compact_string("/127.0.0.1")?;
                encoder.write_compact_array(Some(&["orders".to_owned()]), |encoder, topic| {
                    encoder.write_compact_string(topic)
                })?;
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
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_i32(-2147483648);
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = ShareGroupDescribeResponseV1::decode_body(&mut decoder)?;

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.groups[0].group_id, "share-orders");
        assert_eq!(response.groups[0].group_epoch, 4);
        assert_eq!(response.groups[0].members[0].member_epoch, 7);
        assert_eq!(
            response.groups[0].members[0].assignment.topic_partitions[0].partitions,
            [0, 2]
        );
        assert_eq!(response.groups[0].authorized_operations, -2147483648);
        assert!(decoder.is_empty());
        Ok(())
    }
}
