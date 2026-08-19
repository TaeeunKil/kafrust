use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka AlterShareGroupOffsets API key.
pub const ALTER_API_KEY: i16 = 91;
/// Kafka DeleteShareGroupOffsets API key.
pub const DELETE_API_KEY: i16 = 92;

/// One share-group partition offset to set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsPartitionV0 {
    pub partition_index: i32,
    pub start_offset: i64,
}

/// Offsets to set for one share-group topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsTopicV0 {
    pub topic_name: String,
    pub partitions: Vec<AlterShareGroupOffsetsPartitionV0>,
}

/// AlterShareGroupOffsets v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<AlterShareGroupOffsetsTopicV0>,
}

impl AlterShareGroupOffsetsRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: ALTER_API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.group_id)?;
        encoder.write_compact_array(Some(&self.topics), |encoder, topic| {
            encoder.write_compact_string(&topic.topic_name)?;
            encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                encoder.write_i32(partition.partition_index);
                encoder.write_i64(partition.start_offset);
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// One partition result returned by AlterShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsPartitionResultV0 {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl AlterShareGroupOffsetsPartitionResultV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let partition_index = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            partition_index,
            error_code,
            error_message,
        })
    }
}

/// One topic result returned by AlterShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsTopicResultV0 {
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub partitions: Vec<AlterShareGroupOffsetsPartitionResultV0>,
}

impl AlterShareGroupOffsetsTopicResultV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_name = decoder.read_compact_string()?;
        let topic_id = decoder.read_uuid()?;
        let partitions = decoder
            .read_compact_array("alter share group offset partitions", |decoder| {
                AlterShareGroupOffsetsPartitionResultV0::decode(decoder)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            topic_name,
            topic_id,
            partitions,
        })
    }
}

/// AlterShareGroupOffsets v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub responses: Vec<AlterShareGroupOffsetsTopicResultV0>,
}

impl AlterShareGroupOffsetsResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let responses = decoder
            .read_compact_array("alter share group offset responses", |decoder| {
                AlterShareGroupOffsetsTopicResultV0::decode(decoder)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            responses,
        })
    }
}

/// One topic whose share-group offsets should be deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsTopicV0 {
    pub topic_name: String,
}

/// DeleteShareGroupOffsets v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_id: String,
    pub topics: Vec<DeleteShareGroupOffsetsTopicV0>,
}

impl DeleteShareGroupOffsetsRequestV0 {
    /// Encodes the flexible request, including its request header.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: DELETE_API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_string(&self.group_id)?;
        encoder.write_compact_array(Some(&self.topics), |encoder, topic| {
            encoder.write_compact_string(&topic.topic_name)?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// One topic result returned by DeleteShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsTopicResultV0 {
    pub topic_name: String,
    pub topic_id: [u8; 16],
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl DeleteShareGroupOffsetsTopicResultV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let topic_name = decoder.read_compact_string()?;
        let topic_id = decoder.read_uuid()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            topic_name,
            topic_id,
            error_code,
            error_message,
        })
    }
}

/// DeleteShareGroupOffsets v0 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub responses: Vec<DeleteShareGroupOffsetsTopicResultV0>,
}

impl DeleteShareGroupOffsetsResponseV0 {
    /// Decodes the flexible response body after the response header.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let responses = decoder
            .read_compact_array("delete share group offset responses", |decoder| {
                DeleteShareGroupOffsetsTopicResultV0::decode(decoder)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            responses,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AlterShareGroupOffsetsPartitionV0, AlterShareGroupOffsetsRequestV0,
        AlterShareGroupOffsetsResponseV0, AlterShareGroupOffsetsTopicV0,
        DeleteShareGroupOffsetsRequestV0, DeleteShareGroupOffsetsTopicV0, ALTER_API_KEY,
        DELETE_API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_alter_share_group_offsets_v0_request() {
        let request = AlterShareGroupOffsetsRequestV0 {
            correlation_id: 23,
            client_id: Some("kafrust".to_owned()),
            group_id: "share-orders".to_owned(),
            topics: vec![AlterShareGroupOffsetsTopicV0 {
                topic_name: "orders".to_owned(),
                partitions: vec![AlterShareGroupOffsetsPartitionV0 {
                    partition_index: 2,
                    start_offset: 42,
                }],
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..4], &[0, 91, 0, 0]);
        assert_eq!(ALTER_API_KEY, 91);
        assert_eq!(encoded.last(), Some(&0));
    }

    #[test]
    fn encodes_delete_share_group_offsets_v0_request() {
        let request = DeleteShareGroupOffsetsRequestV0 {
            correlation_id: 24,
            client_id: Some("kafrust".to_owned()),
            group_id: "share-orders".to_owned(),
            topics: vec![DeleteShareGroupOffsetsTopicV0 {
                topic_name: "orders".to_owned(),
            }],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..4], &[0, 92, 0, 0]);
        assert_eq!(DELETE_API_KEY, 92);
        assert_eq!(encoded.last(), Some(&0));
    }

    #[test]
    fn decodes_alter_share_group_offsets_v0_response() -> crate::error::Result<()> {
        let mut bytes = Encoder::new();
        bytes.write_i32(8);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(Some("ok"))?;
        bytes.write_compact_array(Some(&[1_i8]), |encoder, _| {
            encoder.write_compact_string("orders")?;
            encoder.write_uuid(&[7; 16]);
            encoder.write_compact_array(Some(&[1_i8]), |encoder, _| {
                encoder.write_i32(2);
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })?;
        bytes.write_empty_tagged_fields();

        let encoded = bytes.into_bytes();
        let mut decoder = Decoder::new(&encoded);
        let response = AlterShareGroupOffsetsResponseV0::decode_body(&mut decoder)?;
        assert_eq!(response.throttle_time_ms, 8);
        assert_eq!(response.responses[0].topic_name, "orders");
        assert_eq!(response.responses[0].partitions[0].partition_index, 2);
        assert!(decoder.is_empty());
        Ok(())
    }
}
