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
        Ok(Self {
            brokers: decoder
                .read_array("brokers", BrokerMetadata::decode)?
                .unwrap_or_default(),
            controller_id: decoder.read_i32()?,
            topics: decoder
                .read_array("topics", TopicMetadata::decode)?
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{MetadataRequestV1, MetadataResponseV1, API_KEY};
    use crate::codec::Decoder;

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
