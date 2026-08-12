use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 61;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub topics: Vec<DescribeProducersTopicV0>,
}

impl DescribeProducersRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&self.topics), |encoder, topic| {
            encoder.write_compact_string(&topic.name)?;
            encoder.write_array(Some(&topic.partition_indexes), |encoder, partition| {
                encoder.write_i32(*partition);
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
pub struct DescribeProducersTopicV0 {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersResponseV0 {
    pub throttle_time_ms: i32,
    pub topics: Vec<DescribeProducersTopicResponseV0>,
}

impl DescribeProducersResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let topics = decoder
            .read_compact_array("describe producers topics", |decoder| {
                let name = decoder.read_compact_string()?;
                let partitions = decoder
                    .read_compact_array("describe producers partitions", |decoder| {
                        let partition_index = decoder.read_i32()?;
                        let error_code = decoder.read_i16()?;
                        let error_message = decoder.read_compact_nullable_string()?;
                        let active_producers = decoder
                            .read_compact_array(
                                "describe producers active producers",
                                DescribeProducersActiveProducerV0::decode,
                            )?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok(DescribeProducersPartitionResponseV0 {
                            partition_index,
                            error_code,
                            error_message,
                            active_producers,
                        })
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok(DescribeProducersTopicResponseV0 { name, partitions })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersTopicResponseV0 {
    pub name: String,
    pub partitions: Vec<DescribeProducersPartitionResponseV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersPartitionResponseV0 {
    pub partition_index: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub active_producers: Vec<DescribeProducersActiveProducerV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersActiveProducerV0 {
    pub producer_id: i64,
    pub producer_epoch: i32,
    pub last_sequence: i32,
    pub last_timestamp: i64,
    pub coordinator_epoch: i32,
    pub current_txn_start_offset: i64,
}

impl DescribeProducersActiveProducerV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let result = Self {
            producer_id: decoder.read_i64()?,
            producer_epoch: decoder.read_i32()?,
            last_sequence: decoder.read_i32()?,
            last_timestamp: decoder.read_i64()?,
            coordinator_epoch: decoder.read_i32()?,
            current_txn_start_offset: decoder.read_i64()?,
        };
        decoder.read_tagged_fields()?;
        Ok(result)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        DescribeProducersRequestV0, DescribeProducersResponseV0, DescribeProducersTopicV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_producers_v0_request() {
        let request = DescribeProducersRequestV0 {
            correlation_id: 61,
            client_id: Some("kafrust".to_owned()),
            topics: vec![DescribeProducersTopicV0 {
                name: "orders".to_owned(),
                partition_indexes: vec![0, 2],
            }],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 61]);
        assert_eq!(&bytes[8..10], &[0, 7]); // fixed nullable client ID length
        assert_eq!(bytes[18], 2); // one topic
        assert_eq!(bytes.last(), Some(&0));
    }

    #[test]
    fn decodes_describe_producers_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(12);
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("orders").unwrap();
        bytes.write_unsigned_varint(3);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_compact_nullable_string(None).unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_i64(42);
        bytes.write_i32(3);
        bytes.write_i32(17);
        bytes.write_i64(1_700_000_000_000);
        bytes.write_i32(9);
        bytes.write_i64(-1);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_i32(1);
        bytes.write_i16(29);
        bytes.write_compact_nullable_string(Some("denied")).unwrap();
        bytes.write_unsigned_varint(1);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = DescribeProducersResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.topics[0].name, "orders");
        assert_eq!(response.topics[0].partitions[0].partition_index, 0);
        assert_eq!(
            response.topics[0].partitions[0].active_producers[0].producer_id,
            42
        );
        assert_eq!(response.topics[0].partitions[1].error_code, 29);
        assert_eq!(
            response.topics[0].partitions[1].error_message.as_deref(),
            Some("denied")
        );
        assert!(decoder.is_empty());
    }
}
