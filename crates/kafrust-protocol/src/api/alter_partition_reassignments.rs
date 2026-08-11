use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 45;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub timeout_ms: i32,
    pub topics: Vec<AlterPartitionReassignmentsTopicV0>,
}

impl AlterPartitionReassignmentsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i32(self.timeout_ms);
        encoder.write_compact_array(Some(&self.topics), |encoder, topic| {
            encoder.write_compact_string(&topic.name)?;
            encoder.write_compact_array(Some(&topic.partitions), |encoder, partition| {
                encoder.write_i32(partition.partition_index);
                encoder.write_compact_array(
                    partition.replicas.as_deref(),
                    |encoder, replica| {
                        encoder.write_i32(*replica);
                        Ok(())
                    },
                )?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsTopicV0 {
    pub name: String,
    pub partitions: Vec<AlterPartitionReassignmentsPartitionV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsPartitionV0 {
    pub partition_index: i32,
    pub replicas: Option<Vec<i32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub responses: Vec<AlterPartitionReassignmentsTopicResponseV0>,
}

impl AlterPartitionReassignmentsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let error_message = decoder.read_compact_nullable_string()?;
        let responses = decoder
            .read_compact_array("alter partition reassignment responses", |decoder| {
                let name = decoder.read_compact_string()?;
                let partitions = decoder
                    .read_compact_array(
                        "alter partition reassignment partition responses",
                        |decoder| {
                            let partition_index = decoder.read_i32()?;
                            let error_code = decoder.read_i16()?;
                            let error_message = decoder.read_compact_nullable_string()?;
                            decoder.read_tagged_fields()?;
                            Ok(AlterPartitionReassignmentsPartitionResponseV0 {
                                partition_index,
                                error_code,
                                error_message,
                            })
                        },
                    )?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok(AlterPartitionReassignmentsTopicResponseV0 { name, partitions })
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsTopicResponseV0 {
    pub name: String,
    pub partitions: Vec<AlterPartitionReassignmentsPartitionResponseV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsPartitionResponseV0 {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AlterPartitionReassignmentsPartitionV0, AlterPartitionReassignmentsRequestV0,
        AlterPartitionReassignmentsResponseV0, AlterPartitionReassignmentsTopicV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_alter_partition_reassignments_v0_request() {
        let request = AlterPartitionReassignmentsRequestV0 {
            correlation_id: 12,
            client_id: None,
            timeout_ms: 30_000,
            topics: vec![AlterPartitionReassignmentsTopicV0 {
                name: "orders".to_owned(),
                partitions: vec![
                    AlterPartitionReassignmentsPartitionV0 {
                        partition_index: 0,
                        replicas: Some(vec![3, 1, 2]),
                    },
                    AlterPartitionReassignmentsPartitionV0 {
                        partition_index: 1,
                        replicas: None,
                    },
                ],
            }],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 12]);
        assert!(bytes.windows(4).any(|window| window == [0, 0, 0, 3]));
        assert_eq!(bytes[bytes.len() - 1], 0);
    }

    #[test]
    fn decodes_alter_partition_reassignments_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(7);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("orders").unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = AlterPartitionReassignmentsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(response.responses[0].name, "orders");
        assert_eq!(response.responses[0].partitions[0].partition_index, 0);
        assert_eq!(response.responses[0].partitions[0].error_code, 0);
        assert!(decoder.is_empty());
    }
}
