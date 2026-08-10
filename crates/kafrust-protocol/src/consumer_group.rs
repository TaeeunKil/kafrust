use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerProtocolSubscriptionV0 {
    pub topics: Vec<String>,
    pub user_data: Option<Vec<u8>>,
}

/// Consumer protocol subscription version 1 with previously owned partitions.
///
/// Kafka added `OwnedPartitions` in version 1 so cooperative assignors can
/// stage ownership transfers across rebalances.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerProtocolSubscriptionV1 {
    pub topics: Vec<String>,
    pub user_data: Option<Vec<u8>>,
    pub owned_partitions: Vec<ConsumerProtocolTopicAssignment>,
}

impl ConsumerProtocolSubscriptionV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        encoder.write_i16(1);
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            encoder.write_string(topic)
        })?;
        encoder.write_nullable_bytes(self.user_data.as_deref())?;
        encoder.write_array(
            Some(self.owned_partitions.as_slice()),
            |encoder, assignment| assignment.encode(encoder),
        )?;
        Ok(encoder.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        expect_version("consumer protocol subscription", decoder.read_i16()?, 1)?;
        Ok(Self {
            topics: decoder
                .read_array(
                    "consumer protocol subscription topics",
                    Decoder::read_string,
                )?
                .unwrap_or_default(),
            user_data: decoder.read_nullable_bytes()?,
            owned_partitions: decoder
                .read_array(
                    "consumer protocol subscription owned partitions",
                    ConsumerProtocolTopicAssignment::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

impl ConsumerProtocolSubscriptionV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        encoder.write_i16(0);
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            encoder.write_string(topic)
        })?;
        encoder.write_nullable_bytes(self.user_data.as_deref())?;
        Ok(encoder.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        expect_version_v0("consumer protocol subscription", decoder.read_i16()?)?;
        let subscription = Self {
            topics: decoder
                .read_array(
                    "consumer protocol subscription topics",
                    Decoder::read_string,
                )?
                .unwrap_or_default(),
            user_data: decoder.read_nullable_bytes()?,
        };
        Ok(subscription)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerProtocolAssignmentV0 {
    pub assignments: Vec<ConsumerProtocolTopicAssignment>,
    pub user_data: Option<Vec<u8>>,
}

impl ConsumerProtocolAssignmentV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        encoder.write_i16(0);
        encoder.write_array(Some(self.assignments.as_slice()), |encoder, assignment| {
            assignment.encode(encoder)
        })?;
        encoder.write_nullable_bytes(self.user_data.as_deref())?;
        Ok(encoder.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        expect_version_v0("consumer protocol assignment", decoder.read_i16()?)?;
        let assignment = Self {
            assignments: decoder
                .read_array(
                    "consumer protocol assignment topics",
                    ConsumerProtocolTopicAssignment::decode,
                )?
                .unwrap_or_default(),
            user_data: decoder.read_nullable_bytes()?,
        };
        Ok(assignment)
    }
}

fn expect_version_v0(kind: &'static str, version: i16) -> Result<()> {
    expect_version(kind, version, 0)
}

fn expect_version(kind: &'static str, version: i16, expected: i16) -> Result<()> {
    if version == expected {
        return Ok(());
    }

    Err(Error::UnsupportedVersion { kind, version })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerProtocolTopicAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl ConsumerProtocolTopicAssignment {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.topic)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            topic: decoder.read_string()?,
            partitions: decoder
                .read_array("consumer protocol assignment partitions", |decoder| {
                    decoder.read_i32()
                })?
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0,
        ConsumerProtocolSubscriptionV1, ConsumerProtocolTopicAssignment,
    };

    #[test]
    fn encodes_consumer_protocol_subscription_v0() {
        let subscription = ConsumerProtocolSubscriptionV0 {
            topics: vec!["orders".to_owned(), "payments".to_owned()],
            user_data: None,
        };

        assert_eq!(
            subscription.encode().unwrap(),
            [
                0, 0, // version
                0, 0, 0, 2, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // orders
                0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // payments
                0xff, 0xff, 0xff, 0xff, // null user data
            ]
        );
    }

    #[test]
    fn decodes_consumer_protocol_subscription_v0() {
        let encoded = ConsumerProtocolSubscriptionV0 {
            topics: vec!["orders".to_owned()],
            user_data: Some(vec![1, 2, 3]),
        }
        .encode()
        .unwrap();

        let decoded = ConsumerProtocolSubscriptionV0::decode(&encoded).unwrap();

        assert_eq!(
            decoded,
            ConsumerProtocolSubscriptionV0 {
                topics: vec!["orders".to_owned()],
                user_data: Some(vec![1, 2, 3]),
            }
        );
    }

    #[test]
    fn rejects_unsupported_subscription_version() {
        let error = ConsumerProtocolSubscriptionV0::decode(&[0, 1]).unwrap_err();

        assert_eq!(
            error,
            crate::Error::UnsupportedVersion {
                kind: "consumer protocol subscription",
                version: 1,
            }
        );
    }

    #[test]
    fn encodes_and_decodes_consumer_protocol_subscription_v1_with_owned_partitions() {
        let subscription = ConsumerProtocolSubscriptionV1 {
            topics: vec!["orders".to_owned()],
            user_data: Some(vec![9]),
            owned_partitions: vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![1, 3],
            }],
        };

        let encoded = subscription.encode().unwrap();
        let decoded = ConsumerProtocolSubscriptionV1::decode(&encoded).unwrap();

        assert_eq!(decoded, subscription);
    }

    #[test]
    fn encodes_consumer_protocol_assignment_v0() {
        let assignment = ConsumerProtocolAssignmentV0 {
            assignments: vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![0, 2],
            }],
            user_data: Some(vec![9]),
        };

        assert_eq!(
            assignment.encode().unwrap(),
            [
                0, 0, // version
                0, 0, 0, 1, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
                0, 0, 0, 2, // partition count
                0, 0, 0, 0, // partition 0
                0, 0, 0, 2, // partition 2
                0, 0, 0, 1, 9, // user data
            ]
        );
    }

    #[test]
    fn decodes_consumer_protocol_assignment_v0() {
        let encoded = ConsumerProtocolAssignmentV0 {
            assignments: vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![0, 2],
            }],
            user_data: None,
        }
        .encode()
        .unwrap();

        let decoded = ConsumerProtocolAssignmentV0::decode(&encoded).unwrap();

        assert_eq!(
            decoded,
            ConsumerProtocolAssignmentV0 {
                assignments: vec![ConsumerProtocolTopicAssignment {
                    topic: "orders".to_owned(),
                    partitions: vec![0, 2],
                }],
                user_data: None,
            }
        );
    }
}
