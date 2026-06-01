use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::header::RequestHeader;

pub const API_KEY: i16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub topics: Vec<FetchTopicV2>,
}

impl FetchRequestV2 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 2,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_i32(self.replica_id);
        encoder.write_i32(self.max_wait_ms);
        encoder.write_i32(self.min_bytes);
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicV2 {
    pub name: String,
    pub partitions: Vec<FetchPartitionV2>,
}

impl FetchTopicV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionV2 {
    pub partition_index: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
}

impl FetchPartitionV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i64(self.fetch_offset);
        encoder.write_i32(self.max_bytes);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponseV2 {
    pub throttle_time_ms: i32,
    pub responses: Vec<FetchTopicResponseV2>,
}

impl FetchResponseV2 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            responses: decoder
                .read_array("fetch responses", FetchTopicResponseV2::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicResponseV2 {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponseV2>,
}

impl FetchTopicResponseV2 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "fetch partition responses",
                    FetchPartitionResponseV2::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponseV2 {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub records: Vec<MessageSetRecord>,
}

impl FetchPartitionResponseV2 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            high_watermark: decoder.read_i64()?,
            records: decode_message_set(&decoder.read_bytes()?)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSetRecord {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

fn decode_message_set(bytes: &[u8]) -> Result<Vec<MessageSetRecord>> {
    let mut decoder = Decoder::new(bytes);
    let mut records = Vec::new();

    while !decoder.is_empty() {
        let offset = decoder.read_i64()?;
        let message_size = decoder.read_i32()?;
        if message_size < 0 {
            return Err(Error::NegativeLength {
                kind: "message",
                length: message_size,
            });
        }
        let message_size =
            usize::try_from(message_size).map_err(|_| Error::LengthOverflow("message"))?;
        let message = decoder.read_exact(message_size)?;
        records.push(decode_message(offset, message)?);
    }

    Ok(records)
}

fn decode_message(offset: i64, bytes: &[u8]) -> Result<MessageSetRecord> {
    let mut decoder = Decoder::new(bytes);
    let _crc = decoder.read_i32()?;
    let magic = decoder.read_i8()?;
    let _attributes = decoder.read_i8()?;
    let timestamp_ms = match magic {
        0 => -1,
        1 => decoder.read_i64()?,
        _ => {
            return Err(Error::UnsupportedVersion {
                kind: "message magic",
                version: i16::from(magic),
            })
        }
    };
    let key = decoder.read_nullable_bytes()?;
    let value = decoder.read_nullable_bytes()?;

    Ok(MessageSetRecord {
        offset,
        timestamp_ms,
        key,
        value,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        FetchPartitionV2, FetchRequestV2, FetchResponseV2, FetchTopicV2, MessageSetRecord,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_fetch_request_v2() {
        let request = FetchRequestV2 {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            topics: vec![FetchTopicV2 {
                name: "orders".to_owned(),
                partitions: vec![FetchPartitionV2 {
                    partition_index: 0,
                    fetch_offset: 42,
                    max_bytes: 1_048_576,
                }],
            }],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, 1, 0, 2]);
        assert!(bytes.len() > 40);
    }

    #[test]
    fn decodes_fetch_response_v2_with_message_set() {
        let mut message = Encoder::new();
        message.write_i32(0);
        message.write_i8(1);
        message.write_i8(0);
        message.write_i64(123);
        message.write_nullable_bytes(Some(b"order-1")).unwrap();
        message.write_nullable_bytes(Some(b"created")).unwrap();
        let message = message.into_bytes();

        let mut set = Encoder::new();
        set.write_i64(42);
        set.write_i32(i32::try_from(message.len()).unwrap());
        set.write_raw(&message);
        let set = set.into_bytes();

        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_bytes(&set).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV2::decode_body(&mut decoder).unwrap();
        let record = MessageSetRecord {
            offset: 42,
            timestamp_ms: 123,
            key: Some(b"order-1".to_vec()),
            value: Some(b"created".to_vec()),
        };

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses[0].partitions[0].high_watermark, 43);
        assert_eq!(response.responses[0].partitions[0].records, vec![record]);
        assert!(decoder.is_empty());
    }
}
