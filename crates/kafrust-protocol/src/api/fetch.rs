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

    while decoder.remaining() >= 12 {
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
        // Fetch responses may end with a partial trailing message set entry.
        if decoder.remaining() < message_size {
            break;
        }
        let message = decoder.read_exact(message_size)?;
        records.extend(decode_message_or_batch(offset, message)?);
    }

    Ok(records)
}

fn decode_message_or_batch(offset: i64, bytes: &[u8]) -> Result<Vec<MessageSetRecord>> {
    match bytes.get(4).copied() {
        Some(2) => decode_record_batch(offset, bytes),
        _ => Ok(vec![decode_message(offset, bytes)?]),
    }
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

fn decode_record_batch(base_offset: i64, bytes: &[u8]) -> Result<Vec<MessageSetRecord>> {
    let mut decoder = Decoder::new(bytes);
    let _partition_leader_epoch = decoder.read_i32()?;
    let magic = decoder.read_i8()?;
    if magic != 2 {
        return Err(Error::UnsupportedVersion {
            kind: "record batch magic",
            version: i16::from(magic),
        });
    }
    let _crc = decoder.read_i32()?;
    let _attributes = decoder.read_i16()?;
    let _last_offset_delta = decoder.read_i32()?;
    let base_timestamp = decoder.read_i64()?;
    let _max_timestamp = decoder.read_i64()?;
    let _producer_id = decoder.read_i64()?;
    let _producer_epoch = decoder.read_i16()?;
    let _base_sequence = decoder.read_i32()?;
    let record_count = decoder.read_i32()?;
    if record_count < 0 {
        return Err(Error::NegativeLength {
            kind: "record batch records",
            length: record_count,
        });
    }

    let record_count =
        usize::try_from(record_count).map_err(|_| Error::LengthOverflow("record batch records"))?;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let record_length = decoder.read_varint()?;
        if record_length < 0 {
            return Err(Error::NegativeLength {
                kind: "record",
                length: record_length,
            });
        }
        let record_length =
            usize::try_from(record_length).map_err(|_| Error::LengthOverflow("record"))?;
        let record_bytes = decoder.read_exact(record_length)?;
        records.push(decode_record(base_offset, base_timestamp, record_bytes)?);
    }

    Ok(records)
}

fn decode_record(base_offset: i64, base_timestamp: i64, bytes: &[u8]) -> Result<MessageSetRecord> {
    let mut decoder = Decoder::new(bytes);
    let _attributes = decoder.read_i8()?;
    let timestamp_delta = decoder.read_varlong()?;
    let offset_delta = decoder.read_varint()?;
    let key = decoder.read_varint_nullable_bytes()?;
    let value = decoder.read_varint_nullable_bytes()?;
    let header_count = decoder.read_varint()?;
    if header_count < 0 {
        return Err(Error::NegativeLength {
            kind: "record headers",
            length: header_count,
        });
    }
    for _ in 0..header_count {
        let _header_key = decoder.read_varint_bytes()?;
        let _header_value = decoder.read_varint_nullable_bytes()?;
    }

    Ok(MessageSetRecord {
        offset: base_offset.saturating_add(i64::from(offset_delta)),
        timestamp_ms: base_timestamp.saturating_add(timestamp_delta),
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

    #[test]
    fn decodes_fetch_response_v2_ignores_partial_trailing_message_set_entry() {
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
        set.write_i64(-1);
        set.write_i32(61);
        set.write_raw(&[0; 22]);
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

        assert_eq!(response.responses[0].partitions[0].records.len(), 1);
        assert_eq!(response.responses[0].partitions[0].records[0].offset, 42);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_fetch_response_v2_with_record_batch() {
        let mut record = Vec::new();
        record.push(0);
        write_varlong(&mut record, 5);
        write_varint(&mut record, 0);
        write_varint(&mut record, 7);
        record.extend_from_slice(b"order-1");
        write_varint(&mut record, 7);
        record.extend_from_slice(b"created");
        write_varint(&mut record, 0);

        let mut batch = Encoder::new();
        batch.write_i32(0);
        batch.write_i8(2);
        batch.write_i32(0);
        batch.write_i16(0);
        batch.write_i32(0);
        batch.write_i64(1_000);
        batch.write_i64(1_005);
        batch.write_i64(-1);
        batch.write_i16(-1);
        batch.write_i32(-1);
        batch.write_i32(1);
        let mut encoded_record = Vec::new();
        write_varint(&mut encoded_record, i32::try_from(record.len()).unwrap());
        encoded_record.extend_from_slice(&record);
        batch.write_raw(&encoded_record);
        let batch = batch.into_bytes();

        let mut set = Encoder::new();
        set.write_i64(42);
        set.write_i32(i32::try_from(batch.len()).unwrap());
        set.write_raw(&batch);
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
            timestamp_ms: 1_005,
            key: Some(b"order-1".to_vec()),
            value: Some(b"created".to_vec()),
        };

        assert_eq!(response.responses[0].partitions[0].records, vec![record]);
        assert!(decoder.is_empty());
    }

    fn write_varint(output: &mut Vec<u8>, value: i32) {
        write_unsigned_varint(output, u64::from(((value << 1) ^ (value >> 31)) as u32));
    }

    fn write_varlong(output: &mut Vec<u8>, value: i64) {
        write_unsigned_varint(output, ((value << 1) ^ (value >> 63)) as u64);
    }

    fn write_unsigned_varint(output: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }
}
