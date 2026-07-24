use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 47;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<OffsetDeleteRequestTopicV0>,
}

impl OffsetDeleteRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.group_id)?;
        encoder.write_array(Some(&self.topics), |encoder, topic| {
            encoder.write_string(&topic.name)?;
            encoder.write_array(Some(&topic.partitions), |encoder, partition| {
                encoder.write_i32(partition.partition_index);
                Ok(())
            })
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteRequestTopicV0 {
    pub name: String,
    pub partitions: Vec<OffsetDeleteRequestPartitionV0>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetDeleteRequestPartitionV0 {
    pub partition_index: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteResponseV0 {
    pub error_code: i16,
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetDeleteResponseTopicV0>,
}

impl OffsetDeleteResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            error_code: decoder.read_i16()?,
            throttle_time_ms: decoder.read_i32()?,
            topics: decoder
                .read_array("offset delete topics", OffsetDeleteResponseTopicV0::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteResponseTopicV0 {
    pub name: String,
    pub partitions: Vec<OffsetDeleteResponsePartitionV0>,
}

impl OffsetDeleteResponseTopicV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "offset delete partitions",
                    OffsetDeleteResponsePartitionV0::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetDeleteResponsePartitionV0 {
    pub partition_index: i32,
    pub error_code: i16,
}

impl OffsetDeleteResponsePartitionV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        OffsetDeleteRequestPartitionV0, OffsetDeleteRequestTopicV0, OffsetDeleteRequestV0,
        OffsetDeleteResponseV0, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_offset_delete_v0_request() {
        let request = OffsetDeleteRequestV0 {
            correlation_id: 12,
            client_id: Some("kafrust".to_owned()),
            group_id: "orders-group".to_owned(),
            topics: vec![OffsetDeleteRequestTopicV0 {
                name: "orders".to_owned(),
                partitions: vec![
                    OffsetDeleteRequestPartitionV0 { partition_index: 0 },
                    OffsetDeleteRequestPartitionV0 { partition_index: 2 },
                ],
            }],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 47, // API key
                0, 0, // API version
                0, 0, 0, 12, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u', b'p', 0,
                0, 0, 1, // topic count
                0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
                0, 0, 0, 2, // partition count
                0, 0, 0, 0, // partition 0
                0, 0, 0, 2, // partition 2
            ]
        );
        assert_eq!(API_KEY, 47);
    }

    #[test]
    fn decodes_offset_delete_v0_response_in_schema_order() {
        let bytes = [
            0, 69, // group ID not found
            0, 0, 0, 5, // throttle time
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 0, 0, 2, // partition count
            0, 0, 0, 0, // partition 0
            0, 0, // success
            0, 0, 0, 2, // partition 2
            0, 86, // group subscribed to topic
        ];
        let mut decoder = Decoder::new(&bytes);

        let response = OffsetDeleteResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.error_code, 69);
        assert_eq!(response.throttle_time_ms, 5);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].partitions.len(), 2);
        assert_eq!(response.topics[0].partitions[0].partition_index, 0);
        assert_eq!(response.topics[0].partitions[0].error_code, 0);
        assert_eq!(response.topics[0].partitions[1].partition_index, 2);
        assert_eq!(response.topics[0].partitions[1].error_code, 86);
        assert!(decoder.is_empty());
    }
}
