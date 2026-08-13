use crate::api::produce::RecordBatchHeader;
use crate::codec::{DecodeLimits, Decoder, Encoder};
use crate::error::{Error, Result};
use crate::header::RequestHeader;
use crate::record_batch::{decompress_record_batch_records_with_limit, RecordBatchCompression};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequestV4 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topics: Vec<FetchTopicV2>,
}

/// Fetch request version 11 with rack-aware read selection.
///
/// Version 11 keeps the non-flexible wire format used by the existing direct
/// consumer while adding fetch-session fields and the consumer rack ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequestV11 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<FetchTopicV11>,
    pub forgotten_topics: Vec<FetchForgottenTopicV11>,
    pub rack_id: String,
}

/// Fetch request version 12 with flexible encoding and rack-aware read selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequestV12 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<FetchTopicV12>,
    pub forgotten_topics: Vec<FetchForgottenTopicV12>,
    pub rack_id: String,
}

impl FetchRequestV12 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 12,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i32(self.replica_id);
        encoder.write_i32(self.max_wait_ms);
        encoder.write_i32(self.min_bytes);
        encoder.write_i32(self.max_bytes);
        encoder.write_i8(self.isolation_level);
        encoder.write_i32(self.session_id);
        encoder.write_i32(self.session_epoch);
        encoder.write_compact_array(Some(self.topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_compact_array(Some(self.forgotten_topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_compact_string(&self.rack_id)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicV12 {
    pub name: String,
    pub partitions: Vec<FetchPartitionV12>,
}

impl FetchTopicV12 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.name)?;
        encoder.write_compact_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode(encoder)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionV12 {
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub log_start_offset: i64,
    pub max_bytes: i32,
}

impl FetchPartitionV12 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i32(self.current_leader_epoch);
        encoder.write_i64(self.fetch_offset);
        encoder.write_i32(self.last_fetched_epoch);
        encoder.write_i64(self.log_start_offset);
        encoder.write_i32(self.max_bytes);
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchForgottenTopicV12 {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl FetchForgottenTopicV12 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_compact_string(&self.name)?;
        encoder.write_compact_array(Some(self.partitions.as_slice()), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

impl FetchRequestV11 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 11,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_i32(self.replica_id);
        encoder.write_i32(self.max_wait_ms);
        encoder.write_i32(self.min_bytes);
        encoder.write_i32(self.max_bytes);
        encoder.write_i8(self.isolation_level);
        encoder.write_i32(self.session_id);
        encoder.write_i32(self.session_epoch);
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_array(Some(self.forgotten_topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        encoder.write_string(&self.rack_id)?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicV11 {
    pub name: String,
    pub partitions: Vec<FetchPartitionV11>,
}

impl FetchTopicV11 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode(encoder)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionV11 {
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub log_start_offset: i64,
    pub max_bytes: i32,
}

impl FetchPartitionV11 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        encoder.write_i32(self.current_leader_epoch);
        encoder.write_i64(self.fetch_offset);
        encoder.write_i64(self.log_start_offset);
        encoder.write_i32(self.max_bytes);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchForgottenTopicV11 {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl FetchForgottenTopicV11 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })
    }
}

impl FetchRequestV4 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 4,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_i32(self.replica_id);
        encoder.write_i32(self.max_wait_ms);
        encoder.write_i32(self.min_bytes);
        encoder.write_i32(self.max_bytes);
        encoder.write_i8(self.isolation_level);
        encoder.write_array(Some(self.topics.as_slice()), |encoder, topic| {
            topic.encode(encoder)
        })?;
        Ok(encoder.into_bytes())
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponseV4 {
    pub throttle_time_ms: i32,
    pub responses: Vec<FetchTopicResponseV4>,
}

/// Fetch response version 11 with broker-selected read replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponseV11 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub session_id: i32,
    pub responses: Vec<FetchTopicResponseV11>,
}

/// Fetch response version 12 with flexible encoding and broker-selected reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponseV12 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub session_id: i32,
    pub responses: Vec<FetchTopicResponseV12>,
}

impl FetchResponseV12 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            session_id: decoder.read_i32()?,
            responses: decoder
                .read_compact_array("fetch responses", FetchTopicResponseV12::decode)?
                .unwrap_or_default(),
        })
        .and_then(|response| {
            decoder.read_tagged_fields()?;
            Ok(response)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicResponseV12 {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponseV12>,
}

impl FetchTopicResponseV12 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let name = decoder.read_compact_string()?;
        let partitions = decoder
            .read_compact_array(
                "fetch partition responses",
                FetchPartitionResponseV12::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self { name, partitions })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponseV12 {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<AbortedTransactionV12>,
    pub preferred_read_replica: i32,
    pub records: Vec<MessageSetRecord>,
}

impl FetchPartitionResponseV12 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let limits = decoder.limits();
        let partition_index = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let high_watermark = decoder.read_i64()?;
        let last_stable_offset = decoder.read_i64()?;
        let log_start_offset = decoder.read_i64()?;
        let aborted_transactions = decoder
            .read_compact_array("aborted transactions", AbortedTransactionV12::decode)?
            .unwrap_or_default();
        let preferred_read_replica = decoder.read_i32()?;
        let records = decoder
            .read_compact_nullable_bytes()?
            .map(|bytes| decode_message_set(&bytes, limits))
            .transpose()?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            aborted_transactions,
            preferred_read_replica,
            records,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortedTransactionV12 {
    pub producer_id: i64,
    pub first_offset: i64,
}

impl AbortedTransactionV12 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let producer_id = decoder.read_i64()?;
        let first_offset = decoder.read_i64()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            producer_id,
            first_offset,
        })
    }
}

impl FetchResponseV11 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            session_id: decoder.read_i32()?,
            responses: decoder
                .read_array("fetch responses", FetchTopicResponseV11::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicResponseV11 {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponseV11>,
}

impl FetchTopicResponseV11 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "fetch partition responses",
                    FetchPartitionResponseV11::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponseV11 {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<AbortedTransactionV4>,
    pub preferred_read_replica: i32,
    pub records: Vec<MessageSetRecord>,
}

impl FetchPartitionResponseV11 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let limits = decoder.limits();
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            high_watermark: decoder.read_i64()?,
            last_stable_offset: decoder.read_i64()?,
            log_start_offset: decoder.read_i64()?,
            aborted_transactions: decoder
                .read_array("aborted transactions", AbortedTransactionV4::decode)?
                .unwrap_or_default(),
            preferred_read_replica: decoder.read_i32()?,
            records: decode_message_set(&decoder.read_bytes()?, limits)?,
        })
    }
}

impl FetchResponseV4 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            responses: decoder
                .read_array("fetch responses", FetchTopicResponseV4::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchTopicResponseV4 {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponseV4>,
}

impl FetchTopicResponseV4 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "fetch partition responses",
                    FetchPartitionResponseV4::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponseV4 {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub aborted_transactions: Vec<AbortedTransactionV4>,
    pub records: Vec<MessageSetRecord>,
}

impl FetchPartitionResponseV4 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let limits = decoder.limits();
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            high_watermark: decoder.read_i64()?,
            last_stable_offset: decoder.read_i64()?,
            aborted_transactions: decoder
                .read_array("aborted transactions", AbortedTransactionV4::decode)?
                .unwrap_or_default(),
            records: decode_message_set(&decoder.read_bytes()?, limits)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortedTransactionV4 {
    pub producer_id: i64,
    pub first_offset: i64,
}

impl AbortedTransactionV4 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            producer_id: decoder.read_i64()?,
            first_offset: decoder.read_i64()?,
        })
    }
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
        let limits = decoder.limits();
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            high_watermark: decoder.read_i64()?,
            records: decode_message_set(&decoder.read_bytes()?, limits)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSetRecord {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordBatchHeader>,
    pub producer_id: Option<i64>,
    pub transactional: bool,
    pub control: bool,
}

fn decode_message_set(bytes: &[u8], limits: DecodeLimits) -> Result<Vec<MessageSetRecord>> {
    let mut decoder = Decoder::with_limits(bytes, limits);
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
        let decoded = decode_message_or_batch(offset, message, limits)?;
        let total = records
            .len()
            .checked_add(decoded.len())
            .ok_or(Error::LengthOverflow("fetch records"))?;
        decoder.ensure_collection_length("fetch records", total)?;
        records.extend(decoded);
    }

    Ok(records)
}

fn decode_message_or_batch(
    offset: i64,
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<Vec<MessageSetRecord>> {
    match bytes.get(4).copied() {
        Some(2) => decode_record_batch(offset, bytes, limits),
        _ => Ok(vec![decode_message(offset, bytes, limits)?]),
    }
}

fn decode_message(offset: i64, bytes: &[u8], limits: DecodeLimits) -> Result<MessageSetRecord> {
    let mut decoder = Decoder::with_limits(bytes, limits);
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
        headers: Vec::new(),
        producer_id: None,
        transactional: false,
        control: false,
    })
}

fn decode_record_batch(
    base_offset: i64,
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<Vec<MessageSetRecord>> {
    let mut decoder = Decoder::with_limits(bytes, limits);
    let _partition_leader_epoch = decoder.read_i32()?;
    let magic = decoder.read_i8()?;
    if magic != 2 {
        return Err(Error::UnsupportedVersion {
            kind: "record batch magic",
            version: i16::from(magic),
        });
    }
    let _crc = decoder.read_i32()?;
    let attributes = decoder.read_i16()?;
    let compression = RecordBatchCompression::from_attributes(attributes)?;
    let _last_offset_delta = decoder.read_i32()?;
    let base_timestamp = decoder.read_i64()?;
    let _max_timestamp = decoder.read_i64()?;
    let producer_id = decoder.read_i64()?;
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
    decoder.ensure_collection_length("record batch records", record_count)?;
    let record_bytes = if compression.is_compressed() {
        let compressed = decoder.read_exact(decoder.remaining())?;
        decompress_record_batch_records_with_limit(
            compression,
            compressed,
            limits.max_decompressed_record_bytes(),
        )?
    } else {
        if decoder.remaining() > limits.max_decompressed_record_bytes() {
            return Err(Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                actual: decoder.remaining(),
                max: limits.max_decompressed_record_bytes(),
            });
        }
        decoder.read_exact(decoder.remaining())?.to_vec()
    };
    let mut record_decoder = Decoder::with_limits(&record_bytes, limits);
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let record_length = record_decoder.read_varint()?;
        if record_length < 0 {
            return Err(Error::NegativeLength {
                kind: "record",
                length: record_length,
            });
        }
        let record_length =
            usize::try_from(record_length).map_err(|_| Error::LengthOverflow("record"))?;
        let record_bytes = record_decoder.read_exact(record_length)?;
        records.push(decode_record(
            base_offset,
            base_timestamp,
            producer_id,
            attributes,
            record_bytes,
            limits,
        )?);
    }

    Ok(records)
}

fn decode_record(
    base_offset: i64,
    base_timestamp: i64,
    producer_id: i64,
    batch_attributes: i16,
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<MessageSetRecord> {
    let mut decoder = Decoder::with_limits(bytes, limits);
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
    let header_count =
        usize::try_from(header_count).map_err(|_| Error::LengthOverflow("record headers"))?;
    decoder.ensure_collection_length("record headers", header_count)?;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let header_key =
            String::from_utf8(decoder.read_varint_bytes()?).map_err(|_| Error::InvalidUtf8)?;
        let header_value = decoder.read_varint_nullable_bytes()?;
        headers.push(RecordBatchHeader::new(header_key, header_value));
    }

    Ok(MessageSetRecord {
        offset: base_offset.saturating_add(i64::from(offset_delta)),
        timestamp_ms: base_timestamp.saturating_add(timestamp_delta),
        key,
        value,
        headers,
        producer_id: (producer_id >= 0).then_some(producer_id),
        transactional: batch_attributes & 0x10 != 0,
        control: batch_attributes & 0x20 != 0,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        FetchPartitionV11, FetchPartitionV12, FetchPartitionV2, FetchRequestV11, FetchRequestV12,
        FetchRequestV2, FetchRequestV4, FetchResponseV11, FetchResponseV12, FetchResponseV2,
        FetchResponseV4, FetchTopicV11, FetchTopicV12, FetchTopicV2, MessageSetRecord,
        RecordBatchHeader,
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
    fn encodes_fetch_request_v4() {
        let request = FetchRequestV4 {
            correlation_id: 8,
            client_id: Some("kafrust".to_owned()),
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
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
        assert_eq!(&bytes[0..4], &[0, 1, 0, 4]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 8]);
        assert!(bytes.len() > 45);
    }

    #[test]
    fn encodes_fetch_request_v11_with_rack_and_fetch_session_fields() {
        let request = FetchRequestV11 {
            correlation_id: 9,
            client_id: Some("kafrust".to_owned()),
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 1,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicV11 {
                name: "orders".to_owned(),
                partitions: vec![FetchPartitionV11 {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 42,
                    log_start_offset: -1,
                    max_bytes: 1_048_576,
                }],
            }],
            forgotten_topics: Vec::new(),
            rack_id: "rack-a".to_owned(),
        };

        let bytes = request.encode().unwrap();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.read_i16().unwrap(), 1);
        assert_eq!(decoder.read_i16().unwrap(), 11);
        assert_eq!(decoder.read_i32().unwrap(), 9);
        assert_eq!(
            decoder.read_nullable_string().unwrap().as_deref(),
            Some("kafrust")
        );
        assert_eq!(decoder.read_i32().unwrap(), -1);
        assert_eq!(decoder.read_i32().unwrap(), 500);
        assert_eq!(decoder.read_i32().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 1_048_576);
        assert_eq!(decoder.read_i8().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 1);
        assert_eq!(decoder.read_string().unwrap(), "orders");
        assert_eq!(decoder.read_i32().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), -1);
        assert_eq!(decoder.read_i64().unwrap(), 42);
        assert_eq!(decoder.read_i64().unwrap(), -1);
        assert_eq!(decoder.read_i32().unwrap(), 1_048_576);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_string().unwrap(), "rack-a");
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_fetch_response_v11_with_preferred_read_replica() {
        let mut bytes = Encoder::new();
        bytes.write_i32(3);
        bytes.write_i16(0);
        bytes.write_i32(17);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_i64(42);
        bytes.write_i64(40);
        bytes.write_i32(0);
        bytes.write_i32(2);
        bytes.write_bytes(&[]).unwrap();

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV11::decode_body(&mut decoder).unwrap();
        let partition = &response.responses[0].partitions[0];

        assert_eq!(response.throttle_time_ms, 3);
        assert_eq!(response.session_id, 17);
        assert_eq!(partition.log_start_offset, 40);
        assert_eq!(partition.preferred_read_replica, 2);
        assert!(partition.records.is_empty());
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_fetch_request_v12_with_flexible_rack_fields() {
        let request = FetchRequestV12 {
            correlation_id: 10,
            client_id: Some("kafrust".to_owned()),
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 1,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicV12 {
                name: "orders".to_owned(),
                partitions: vec![FetchPartitionV12 {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 42,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    max_bytes: 1_048_576,
                }],
            }],
            forgotten_topics: Vec::new(),
            rack_id: "rack-a".to_owned(),
        };

        let bytes = request.encode().unwrap();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.read_i16().unwrap(), 1);
        assert_eq!(decoder.read_i16().unwrap(), 12);
        assert_eq!(decoder.read_i32().unwrap(), 10);
        assert_eq!(
            decoder.read_nullable_string().unwrap().as_deref(),
            Some("kafrust")
        );
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), -1);
        assert_eq!(decoder.read_i32().unwrap(), 500);
        assert_eq!(decoder.read_i32().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 1_048_576);
        assert_eq!(decoder.read_i8().unwrap(), 1);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_compact_string().unwrap(), "orders");
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 2);
        assert_eq!(decoder.read_i32().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), -1);
        assert_eq!(decoder.read_i64().unwrap(), 42);
        assert_eq!(decoder.read_i32().unwrap(), -1);
        assert_eq!(decoder.read_i64().unwrap(), -1);
        assert_eq!(decoder.read_i32().unwrap(), 1_048_576);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 1);
        assert_eq!(decoder.read_compact_string().unwrap(), "rack-a");
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_fetch_response_v12_with_preferred_read_replica() {
        let mut bytes = Encoder::new();
        bytes.write_i32(3);
        bytes.write_i16(0);
        bytes.write_i32(17);
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("orders").unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_i64(42);
        bytes.write_i64(40);
        bytes.write_unsigned_varint(2);
        bytes.write_i64(7);
        bytes.write_i64(40);
        bytes.write_unsigned_varint(0);
        bytes.write_i32(2);
        bytes.write_compact_nullable_bytes(Some(&[])).unwrap();
        bytes.write_unsigned_varint(0);
        bytes.write_unsigned_varint(0);
        bytes.write_unsigned_varint(0);

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV12::decode_body(&mut decoder).unwrap();
        let partition = &response.responses[0].partitions[0];

        assert_eq!(response.throttle_time_ms, 3);
        assert_eq!(response.session_id, 17);
        assert_eq!(partition.log_start_offset, 40);
        assert_eq!(partition.preferred_read_replica, 2);
        assert_eq!(partition.aborted_transactions[0].producer_id, 7);
        assert!(partition.records.is_empty());
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_fetch_response_v4_with_aborted_transaction() {
        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_i64(42);
        bytes.write_i32(1);
        bytes.write_i64(7);
        bytes.write_i64(40);
        bytes.write_bytes(&[]).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV4::decode_body(&mut decoder).unwrap();
        let partition = &response.responses[0].partitions[0];

        assert_eq!(partition.high_watermark, 43);
        assert_eq!(partition.last_stable_offset, 42);
        assert_eq!(partition.aborted_transactions[0].producer_id, 7);
        assert_eq!(partition.aborted_transactions[0].first_offset, 40);
        assert!(partition.records.is_empty());
        assert!(decoder.is_empty());
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
            headers: Vec::new(),
            producer_id: None,
            transactional: false,
            control: false,
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
        write_varint(&mut record, 2);
        write_varint(&mut record, 6);
        record.extend_from_slice(b"source");
        write_varint(&mut record, 8);
        record.extend_from_slice(b"checkout");
        write_varint(&mut record, 9);
        record.extend_from_slice(b"tombstone");
        write_varint(&mut record, -1);

        let mut batch = Encoder::new();
        batch.write_i32(0);
        batch.write_i8(2);
        batch.write_i32(0);
        batch.write_i16(0x10);
        batch.write_i32(0);
        batch.write_i64(1_000);
        batch.write_i64(1_005);
        batch.write_i64(7);
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
            headers: vec![
                RecordBatchHeader::new("source", Some(b"checkout".to_vec())),
                RecordBatchHeader::new("tombstone", None),
            ],
            producer_id: Some(7),
            transactional: true,
            control: false,
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
