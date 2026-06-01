use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::header::RequestHeader;

pub const API_KEY: i16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicV2>,
}

impl ProduceRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_i16(self.acks);
        encoder.write_i32(self.timeout_ms);
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceTopicV2 {
    pub name: String,
    pub partitions: Vec<ProducePartitionV2>,
}

impl ProduceTopicV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionV2 {
    pub partition_index: i32,
    pub records: Vec<MessageSetMessage>,
}

impl ProducePartitionV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        let record_set = encode_message_set(&self.records)?;
        encoder.write_bytes(&record_set)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSetMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: i64,
}

impl MessageSetMessage {
    pub fn new(key: Option<Vec<u8>>, value: Option<Vec<u8>>, timestamp_ms: i64) -> Self {
        Self {
            key,
            value,
            timestamp_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponseV2 {
    pub responses: Vec<ProduceTopicResponseV2>,
    pub throttle_time_ms: i32,
}

impl ProduceResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            responses: decoder
                .read_array("produce responses", ProduceTopicResponseV2::decode)?
                .unwrap_or_default(),
            throttle_time_ms: decoder.read_i32()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceTopicResponseV2 {
    pub name: String,
    pub partitions: Vec<ProducePartitionResponseV2>,
}

impl ProduceTopicResponseV2 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "produce partition responses",
                    ProducePartitionResponseV2::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponseV2 {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
}

impl ProducePartitionResponseV2 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            base_offset: decoder.read_i64()?,
            log_append_time_ms: decoder.read_i64()?,
        })
    }
}

fn encode_message_set(records: &[MessageSetMessage]) -> Result<Vec<u8>> {
    let mut set = Encoder::new();
    for record in records {
        let message = encode_message(record)?;
        set.write_i64(0);
        set.write_i32(i32::try_from(message.len()).map_err(|_| Error::LengthOverflow("message"))?);
        set.write_raw(&message);
    }
    Ok(set.into_bytes())
}

fn encode_message(record: &MessageSetMessage) -> Result<Vec<u8>> {
    let mut body = Encoder::new();
    body.write_i8(1);
    body.write_i8(0);
    body.write_i64(record.timestamp_ms);
    body.write_nullable_bytes(record.key.as_deref())?;
    body.write_nullable_bytes(record.value.as_deref())?;
    let body = body.into_bytes();

    let mut message = Encoder::new();
    message.write_i32(crc32_ieee(&body) as i32);
    message.write_raw(&body);
    Ok(message.into_bytes())
}

fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        MessageSetMessage, ProducePartitionV2, ProduceRequestV2, ProduceResponseV2, ProduceTopicV2,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_produce_request_v2() {
        let request = ProduceRequestV2 {
            correlation_id: 5,
            client_id: Some("kafrust".to_owned()),
            acks: 1,
            timeout_ms: 30_000,
            topics: vec![ProduceTopicV2 {
                name: "orders".to_owned(),
                partitions: vec![ProducePartitionV2 {
                    partition_index: 0,
                    records: vec![MessageSetMessage::new(
                        Some(b"order-1".to_vec()),
                        Some(b"created".to_vec()),
                        0,
                    )],
                }],
            }],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(
            &bytes[0..17],
            &[0, 0, 0, 2, 0, 0, 0, 5, 0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't',]
        );
        assert!(bytes.len() > 60);
    }

    #[test]
    fn decodes_produce_response_v2() {
        let bytes = [
            0, 0, 0, 1, // topic response count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, 0, 1, // partition response count
            0, 0, 0, 0, // partition
            0, 0, // error code
            0, 0, 0, 0, 0, 0, 0, 42, // base offset
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log append time -1
            0, 0, 0, 0, // throttle time
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = ProduceResponseV2::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses[0].name, "orders");
        assert_eq!(response.responses[0].partitions[0].partition_index, 0);
        assert_eq!(response.responses[0].partitions[0].error_code, 0);
        assert_eq!(response.responses[0].partitions[0].base_offset, 42);
        assert!(decoder.is_empty());
    }
}
