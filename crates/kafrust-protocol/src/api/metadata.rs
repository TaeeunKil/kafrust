use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Option<Vec<String>>,
}

impl MetadataRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(self.topics.as_deref(), |encoder, topic| {
            encoder.write_string(topic)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

impl BrokerMetadata {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            node_id: decoder.read_i32()?,
            host: decoder.read_string()?,
            port: decoder.read_i32()?,
            rack: decoder.read_nullable_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

impl PartitionMetadata {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            partition_index: decoder.read_i32()?,
            leader_id: decoder.read_i32()?,
            replica_nodes: decoder
                .read_array("replica nodes", |decoder| decoder.read_i32())?
                .unwrap_or_default(),
            isr_nodes: decoder
                .read_array("isr nodes", |decoder| decoder.read_i32())?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            name: decoder.read_string()?,
            is_internal: decoder.read_bool()?,
            partitions: decoder
                .read_array("partitions", PartitionMetadata::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponseV1 {
    pub brokers: Vec<BrokerMetadata>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let response = Self {
            brokers: decoder
                .read_array("brokers", BrokerMetadata::decode)?
                .unwrap_or_default(),
            controller_id: decoder.read_i32()?,
            topics: decoder
                .read_array("topics", TopicMetadata::decode)?
                .unwrap_or_default(),
        };
        decoder.finish()?;
        Ok(response)
    }
}

/// Topic selector used by Metadata v12.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestTopicV12 {
    pub topic_id: [u8; 16],
    pub name: Option<String>,
}

impl MetadataRequestTopicV12 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_uuid(&self.topic_id);
        encoder.write_compact_nullable_string(self.name.as_deref())?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

/// Metadata v12 request with topic UUID support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestV12 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Option<Vec<MetadataRequestTopicV12>>,
    pub allow_auto_topic_creation: bool,
    pub include_topic_authorized_operations: bool,
}

impl MetadataRequestV12 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 12,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(self.topics.as_deref(), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_bool(self.allow_auto_topic_creation);
        encoder.write_bool(self.include_topic_authorized_operations);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// Broker endpoint returned by Metadata v12.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBrokerV12 {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

impl MetadataBrokerV12 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let broker = Self {
            node_id: decoder.read_i32()?,
            host: decoder.read_compact_string()?,
            port: decoder.read_i32()?,
            rack: decoder.read_compact_nullable_string()?,
        };
        decoder.read_tagged_fields()?;
        Ok(broker)
    }
}

/// Partition metadata returned by Metadata v12.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPartitionV12 {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl MetadataPartitionV12 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let partition = Self {
            error_code: decoder.read_i16()?,
            partition_index: decoder.read_i32()?,
            leader_id: decoder.read_i32()?,
            leader_epoch: decoder.read_i32()?,
            replica_nodes: decoder
                .read_compact_array("metadata v12 replica nodes", |decoder| decoder.read_i32())?
                .unwrap_or_default(),
            isr_nodes: decoder
                .read_compact_array("metadata v12 isr nodes", |decoder| decoder.read_i32())?
                .unwrap_or_default(),
            offline_replicas: decoder
                .read_compact_array("metadata v12 offline replicas", |decoder| {
                    decoder.read_i32()
                })?
                .unwrap_or_default(),
        };
        decoder.read_tagged_fields()?;
        Ok(partition)
    }
}

/// Topic metadata returned by Metadata v12, including the stable topic UUID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataTopicV12 {
    pub error_code: i16,
    pub name: Option<String>,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<MetadataPartitionV12>,
    pub topic_authorized_operations: i32,
}

impl MetadataTopicV12 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic = Self {
            error_code: decoder.read_i16()?,
            name: decoder.read_compact_nullable_string()?,
            topic_id: decoder.read_uuid()?,
            is_internal: decoder.read_bool()?,
            partitions: decoder
                .read_compact_array("metadata v12 partitions", MetadataPartitionV12::decode)?
                .unwrap_or_default(),
            topic_authorized_operations: decoder.read_i32()?,
        };
        decoder.read_tagged_fields()?;
        Ok(topic)
    }
}

/// Metadata v12 response with topic UUIDs and flexible encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponseV12 {
    pub throttle_time_ms: i32,
    pub brokers: Vec<MetadataBrokerV12>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<MetadataTopicV12>,
}

impl MetadataResponseV12 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let response = Self {
            throttle_time_ms: decoder.read_i32()?,
            brokers: decoder
                .read_compact_array("metadata v12 brokers", MetadataBrokerV12::decode)?
                .unwrap_or_default(),
            cluster_id: decoder.read_compact_nullable_string()?,
            controller_id: decoder.read_i32()?,
            topics: decoder
                .read_compact_array("metadata v12 topics", MetadataTopicV12::decode)?
                .unwrap_or_default(),
        };
        decoder.read_tagged_fields()?;
        decoder.finish()?;
        Ok(response)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        MetadataRequestTopicV12, MetadataRequestV1, MetadataRequestV12, MetadataResponseV1, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_metadata_request_v1_for_topics() {
        let request = MetadataRequestV1 {
            correlation_id: 9,
            client_id: Some("kafrust".to_owned()),
            topics: Some(vec!["orders".to_owned()]),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 3, // api key
                0, 1, // api version
                0, 0, 0, 9, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 0, 0, 1, // topics count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            ]
        );
        assert_eq!(API_KEY, 3);
    }

    #[test]
    fn encodes_metadata_request_v1_for_all_topics() {
        let request = MetadataRequestV1 {
            correlation_id: 9,
            client_id: None,
            topics: None,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 3, // api key
                0, 1, // api version
                0, 0, 0, 9, // correlation id
                0xff, 0xff, // null client id
                0xff, 0xff, 0xff, 0xff, // null topics array
            ]
        );
    }

    #[test]
    fn encodes_metadata_request_v12_with_topic_name_selector() {
        let request = MetadataRequestV12 {
            correlation_id: 11,
            client_id: Some("kafrust".to_owned()),
            topics: Some(vec![MetadataRequestTopicV12 {
                topic_id: [0; 16],
                name: Some("orders".to_owned()),
            }]),
            allow_auto_topic_creation: false,
            include_topic_authorized_operations: false,
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 3, 0, 12]);
        assert_eq!(&encoded[4..8], &[0, 0, 0, 11]);
        assert!(encoded.windows(6).any(|bytes| bytes == b"orders"));
        assert!(encoded.ends_with(&[0, 0, 0]));
    }

    #[test]
    fn decodes_metadata_response_v12_with_topic_uuid() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes
            .write_compact_array(
                Some(&[super::MetadataBrokerV12 {
                    node_id: 1,
                    host: "localhost".to_owned(),
                    port: 9092,
                    rack: None,
                }]),
                |encoder, broker| {
                    encoder.write_i32(broker.node_id);
                    encoder.write_compact_string(&broker.host)?;
                    encoder.write_i32(broker.port);
                    encoder.write_compact_nullable_string(broker.rack.as_deref())?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                },
            )
            .unwrap();
        bytes
            .write_compact_nullable_string(Some("cluster"))
            .unwrap();
        bytes.write_i32(1);
        bytes
            .write_compact_array(
                Some(&[super::MetadataTopicV12 {
                    error_code: 0,
                    name: Some("orders".to_owned()),
                    topic_id: [7; 16],
                    is_internal: false,
                    partitions: vec![super::MetadataPartitionV12 {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 3,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: Vec::new(),
                    }],
                    topic_authorized_operations: -2147483648,
                }]),
                |encoder, topic| {
                    encoder.write_i16(topic.error_code);
                    encoder.write_compact_nullable_string(topic.name.as_deref())?;
                    encoder.write_uuid(&topic.topic_id);
                    encoder.write_bool(topic.is_internal);
                    encoder.write_compact_array(
                        Some(&topic.partitions),
                        |encoder, partition| {
                            encoder.write_i16(partition.error_code);
                            encoder.write_i32(partition.partition_index);
                            encoder.write_i32(partition.leader_id);
                            encoder.write_i32(partition.leader_epoch);
                            encoder.write_compact_array(
                                Some(&partition.replica_nodes),
                                |encoder, value| {
                                    encoder.write_i32(*value);
                                    Ok(())
                                },
                            )?;
                            encoder.write_compact_array(
                                Some(&partition.isr_nodes),
                                |encoder, value| {
                                    encoder.write_i32(*value);
                                    Ok(())
                                },
                            )?;
                            encoder.write_compact_array(
                                Some(&partition.offline_replicas),
                                |encoder, value| {
                                    encoder.write_i32(*value);
                                    Ok(())
                                },
                            )?;
                            encoder.write_empty_tagged_fields();
                            Ok(())
                        },
                    )?;
                    encoder.write_i32(topic.topic_authorized_operations);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                },
            )
            .unwrap();
        bytes.write_empty_tagged_fields();

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = super::MetadataResponseV12::decode_body(&mut decoder).unwrap();

        assert_eq!(response.cluster_id.as_deref(), Some("cluster"));
        assert_eq!(response.topics[0].name.as_deref(), Some("orders"));
        assert_eq!(response.topics[0].topic_id, [7; 16]);
        assert_eq!(response.topics[0].partitions[0].leader_epoch, 3);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_metadata_response_v1() {
        let bytes = [
            0, 0, 0, 1, // brokers count
            0, 0, 0, 1, // node id
            0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // host
            0, 0, 35, 132, // port 9092
            0xff, 0xff, // null rack
            0, 0, 0, 1, // controller id
            0, 0, 0, 1, // topics count
            0, 0, // topic error code
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0,    // is internal false
            0, 0, 0, 1, // partition count
            0, 0, // partition error code
            0, 0, 0, 0, // partition index
            0, 0, 0, 1, // leader id
            0, 0, 0, 1, // replica count
            0, 0, 0, 1, // replica node
            0, 0, 0, 1, // isr count
            0, 0, 0, 1, // isr node
        ];

        let mut decoder = Decoder::new(&bytes);
        let response = MetadataResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.controller_id, 1);
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.brokers[0].host, "localhost");
        assert_eq!(response.brokers[0].port, 9092);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].partitions[0].leader_id, 1);
        assert_eq!(response.topics[0].partitions[0].replica_nodes, vec![1]);
        assert_eq!(response.topics[0].partitions[0].isr_nodes, vec![1]);
        assert!(decoder.is_empty());
    }
}
