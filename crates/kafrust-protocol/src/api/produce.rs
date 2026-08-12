use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::header::RequestHeader;
use crate::record_batch::{compress_record_batch_records, RecordBatchCompression};

pub const API_KEY: i16 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV2 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicV2>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV3 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicV3>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV7 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicV3>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV9 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicV3>,
}

/// Flexible Produce request used by Kafka API versions 11 and newer.
///
/// Kafka keeps the v9 flexible RecordBatch schema for these versions; the
/// request header carries the negotiated API version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequestV11 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicV3>,
}

impl ProduceRequestV3 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_record_batch_request(
            3,
            self.correlation_id,
            self.client_id.clone(),
            self.transactional_id.as_deref(),
            self.acks,
            self.timeout_ms,
            &self.topics,
        )
    }
}

impl ProduceRequestV7 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_record_batch_request(
            7,
            self.correlation_id,
            self.client_id.clone(),
            self.transactional_id.as_deref(),
            self.acks,
            self.timeout_ms,
            &self.topics,
        )
    }
}

impl ProduceRequestV9 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_flexible_produce_request(
            9,
            self.correlation_id,
            self.client_id.clone(),
            self.transactional_id.as_deref(),
            self.acks,
            self.timeout_ms,
            &self.topics,
        )
    }
}

impl ProduceRequestV11 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_flexible_produce_request(
            11,
            self.correlation_id,
            self.client_id.clone(),
            self.transactional_id.as_deref(),
            self.acks,
            self.timeout_ms,
            &self.topics,
        )
    }
}

fn encode_flexible_produce_request(
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    transactional_id: Option<&str>,
    acks: i16,
    timeout_ms: i32,
    topics: &[ProduceTopicV3],
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: API_KEY,
        api_version,
        correlation_id,
        client_id,
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_nullable_string(transactional_id)?;
    encoder.write_i16(acks);
    encoder.write_i32(timeout_ms);
    encoder.write_compact_array(Some(topics), |encoder, topic| {
        topic.encode_v9(encoder, transactional_id.is_some())
    })?;
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

fn encode_record_batch_request(
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    transactional_id: Option<&str>,
    acks: i16,
    timeout_ms: i32,
    topics: &[ProduceTopicV3],
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: API_KEY,
        api_version,
        correlation_id,
        client_id,
    }
    .encode_v1(&mut encoder)?;
    encoder.write_nullable_string(transactional_id)?;
    encoder.write_i16(acks);
    encoder.write_i32(timeout_ms);
    encoder.write_array(Some(topics), |encoder, topic| {
        topic.encode(encoder, transactional_id.is_some())
    })?;
    Ok(encoder.into_bytes())
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceTopicV3 {
    pub name: String,
    pub partitions: Vec<ProducePartitionV3>,
}

impl ProduceTopicV3 {
    fn encode(&self, encoder: &mut Encoder, transactional: bool) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode(encoder, transactional)
        })
    }

    fn encode_v9(&self, encoder: &mut Encoder, transactional: bool) -> Result<()> {
        encoder.write_compact_string(&self.name)?;
        encoder.write_compact_array(Some(self.partitions.as_slice()), |encoder, partition| {
            partition.encode_v9(encoder, transactional)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionV3 {
    pub partition_index: i32,
    pub records: Vec<RecordBatchMessage>,
    pub compression: RecordBatchCompression,
    pub identity: RecordBatchIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordBatchIdentity {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

impl RecordBatchIdentity {
    pub const NON_IDEMPOTENT: Self = Self {
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
    };
}

impl ProducePartitionV3 {
    fn encode(&self, encoder: &mut Encoder, transactional: bool) -> Result<()> {
        encoder.write_i32(self.partition_index);
        let record_set = encode_record_batch_set_with_compression_identity_and_transaction(
            &self.records,
            self.compression,
            self.identity,
            transactional,
        )?;
        encoder.write_bytes(&record_set)
    }

    fn encode_v9(&self, encoder: &mut Encoder, transactional: bool) -> Result<()> {
        encoder.write_i32(self.partition_index);
        let record_set = encode_record_batch_set_with_compression_identity_and_transaction(
            &self.records,
            self.compression,
            self.identity,
            transactional,
        )?;
        encoder.write_compact_nullable_bytes(Some(&record_set))?;
        encoder.write_empty_tagged_fields();
        Ok(())
    }
}

impl ProducePartitionV2 {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_i32(self.partition_index);
        let record_set = encode_message_set(&self.records)?;
        encoder.write_bytes(&record_set)
    }
}

/// Returns the encoded byte length of a Produce v2 message set.
pub fn encoded_message_set_len(records: &[MessageSetMessage]) -> Result<usize> {
    Ok(encode_message_set(records)?.len())
}

/// Returns the encoded byte length of a Produce v3 record batch set.
pub fn encoded_record_batch_set_len(records: &[RecordBatchMessage]) -> Result<usize> {
    Ok(encode_record_batch_set(records)?.len())
}

/// Returns the encoded byte length of a Produce v3 record batch set.
pub fn encoded_record_batch_set_len_with_compression(
    records: &[RecordBatchMessage],
    compression: RecordBatchCompression,
) -> Result<usize> {
    encoded_record_batch_set_len_with_compression_and_identity(
        records,
        compression,
        RecordBatchIdentity::NON_IDEMPOTENT,
    )
}

pub fn encoded_record_batch_set_len_with_compression_and_identity(
    records: &[RecordBatchMessage],
    compression: RecordBatchCompression,
    identity: RecordBatchIdentity,
) -> Result<usize> {
    Ok(
        encode_record_batch_set_with_compression_and_identity(records, compression, identity)?
            .len(),
    )
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
pub struct RecordBatchHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

impl RecordBatchHeader {
    pub fn new(key: impl Into<String>, value: Option<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatchMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: i64,
    pub headers: Vec<RecordBatchHeader>,
}

impl RecordBatchMessage {
    pub fn new(key: Option<Vec<u8>>, value: Option<Vec<u8>>, timestamp_ms: i64) -> Self {
        Self {
            key,
            value,
            timestamp_ms,
            headers: Vec::new(),
        }
    }

    pub fn header(mut self, key: impl Into<String>, value: Option<Vec<u8>>) -> Self {
        self.headers.push(RecordBatchHeader::new(key, value));
        self
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponseV7 {
    pub responses: Vec<ProduceTopicResponseV7>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponseV9 {
    pub responses: Vec<ProduceTopicResponseV9>,
    pub throttle_time_ms: i32,
}

/// Flexible Produce response used by Kafka API versions 11 and newer.
pub type ProduceResponseV11 = ProduceResponseV9;

impl ProduceResponseV9 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let responses = decoder
            .read_compact_array("produce responses", ProduceTopicResponseV9::decode)?
            .unwrap_or_default();
        let throttle_time_ms = decoder.read_i32()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceTopicResponseV9 {
    pub name: String,
    pub partitions: Vec<ProducePartitionResponseV9>,
}

impl ProduceTopicResponseV9 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let name = decoder.read_compact_string()?;
        let partitions = decoder
            .read_compact_array(
                "produce partition responses",
                ProducePartitionResponseV9::decode,
            )?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self { name, partitions })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponseV9 {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
    pub record_errors: Vec<ProduceRecordErrorV9>,
    pub error_message: Option<String>,
}

impl ProducePartitionResponseV9 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let partition_index = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let base_offset = decoder.read_i64()?;
        let log_append_time_ms = decoder.read_i64()?;
        let log_start_offset = decoder.read_i64()?;
        let record_errors = decoder
            .read_compact_array("produce record errors", ProduceRecordErrorV9::decode)?
            .unwrap_or_default();
        let error_message = decoder.read_compact_nullable_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            partition_index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRecordErrorV9 {
    pub batch_index: i32,
    pub error_message: Option<String>,
}

impl ProduceRecordErrorV9 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let batch_index = decoder.read_i32()?;
        let error_message = decoder.read_compact_nullable_string()?;
        decoder.read_tagged_fields()?;
        Ok(Self {
            batch_index,
            error_message,
        })
    }
}

impl ProduceResponseV7 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            responses: decoder
                .read_array("produce responses", ProduceTopicResponseV7::decode)?
                .unwrap_or_default(),
            throttle_time_ms: decoder.read_i32()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceTopicResponseV7 {
    pub name: String,
    pub partitions: Vec<ProducePartitionResponseV7>,
}

impl ProduceTopicResponseV7 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "produce partition responses",
                    ProducePartitionResponseV7::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponseV7 {
    pub partition_index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
    pub log_start_offset: i64,
}

impl ProducePartitionResponseV7 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            base_offset: decoder.read_i64()?,
            log_append_time_ms: decoder.read_i64()?,
            log_start_offset: decoder.read_i64()?,
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

fn encode_record_batch_set(records: &[RecordBatchMessage]) -> Result<Vec<u8>> {
    encode_record_batch_set_with_compression(records, RecordBatchCompression::None)
}

fn encode_record_batch_set_with_compression(
    records: &[RecordBatchMessage],
    compression: RecordBatchCompression,
) -> Result<Vec<u8>> {
    encode_record_batch_set_with_compression_and_identity(
        records,
        compression,
        RecordBatchIdentity::NON_IDEMPOTENT,
    )
}

fn encode_record_batch_set_with_compression_and_identity(
    records: &[RecordBatchMessage],
    compression: RecordBatchCompression,
    identity: RecordBatchIdentity,
) -> Result<Vec<u8>> {
    encode_record_batch_set_with_compression_identity_and_transaction(
        records,
        compression,
        identity,
        false,
    )
}

fn encode_record_batch_set_with_compression_identity_and_transaction(
    records: &[RecordBatchMessage],
    compression: RecordBatchCompression,
    identity: RecordBatchIdentity,
    transactional: bool,
) -> Result<Vec<u8>> {
    let base_timestamp = records
        .first()
        .map(|record| record.timestamp_ms)
        .unwrap_or_default();
    let max_timestamp = records
        .iter()
        .map(|record| record.timestamp_ms)
        .max()
        .unwrap_or(base_timestamp);
    let last_offset_delta = records
        .len()
        .checked_sub(1)
        .map(|delta| i32::try_from(delta).map_err(|_| Error::LengthOverflow("record batch")))
        .transpose()?
        .unwrap_or_default();

    let record_count =
        i32::try_from(records.len()).map_err(|_| Error::LengthOverflow("record batch records"))?;
    let mut record_bytes = Encoder::new();
    for (offset_delta, record) in records.iter().enumerate() {
        let encoded = encode_record(record, base_timestamp, offset_delta)?;
        record_bytes.write_varint(
            i32::try_from(encoded.len()).map_err(|_| Error::LengthOverflow("record"))?,
        );
        record_bytes.write_raw(&encoded);
    }
    let record_bytes = compress_record_batch_records(compression, &record_bytes.into_bytes())?;

    let mut crc_payload = Encoder::new();
    let attributes = compression.attributes() | if transactional { 0x10 } else { 0 };
    crc_payload.write_i16(attributes);
    crc_payload.write_i32(last_offset_delta);
    crc_payload.write_i64(base_timestamp);
    crc_payload.write_i64(max_timestamp);
    crc_payload.write_i64(identity.producer_id);
    crc_payload.write_i16(identity.producer_epoch);
    crc_payload.write_i32(identity.base_sequence);
    crc_payload.write_i32(record_count);
    crc_payload.write_raw(&record_bytes);
    let crc_payload = crc_payload.into_bytes();

    let mut batch = Encoder::new();
    batch.write_i32(0);
    batch.write_i8(2);
    batch.write_i32(crc32c(&crc_payload) as i32);
    batch.write_raw(&crc_payload);
    let batch = batch.into_bytes();

    let mut set = Encoder::new();
    set.write_i64(0);
    set.write_i32(i32::try_from(batch.len()).map_err(|_| Error::LengthOverflow("record batch"))?);
    set.write_raw(&batch);
    Ok(set.into_bytes())
}

fn encode_record(
    record: &RecordBatchMessage,
    base_timestamp: i64,
    offset_delta: usize,
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    encoder.write_i8(0);
    encoder.write_varlong(record.timestamp_ms.saturating_sub(base_timestamp));
    encoder.write_varint(
        i32::try_from(offset_delta).map_err(|_| Error::LengthOverflow("record offset delta"))?,
    );
    encoder.write_varint_nullable_bytes(record.key.as_deref())?;
    encoder.write_varint_nullable_bytes(record.value.as_deref())?;
    encoder.write_varint(
        i32::try_from(record.headers.len()).map_err(|_| Error::LengthOverflow("record headers"))?,
    );
    for header in &record.headers {
        encoder.write_varint_bytes(header.key.as_bytes())?;
        encoder.write_varint_nullable_bytes(header.value.as_deref())?;
    }
    Ok(encoder.into_bytes())
}

fn crc32_ieee(bytes: &[u8]) -> u32 {
    crc32_with_table(bytes, &CRC32_IEEE_TABLE)
}

fn crc32c(bytes: &[u8]) -> u32 {
    crc32_with_table(bytes, &CRC32C_TABLE)
}

const CRC32_IEEE_TABLE: [u32; 256] = crc32_table(0xedb8_8320);
const CRC32C_TABLE: [u32; 256] = crc32_table(0x82f6_3b78);

const fn crc32_table(polynomial: u32) -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = 0u32.wrapping_sub(value & 1);
            value = (value >> 1) ^ (polynomial & mask);
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

fn crc32_with_table(bytes: &[u8], table: &[u32; 256]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        let index = usize::from((crc as u8) ^ byte);
        crc = (crc >> 8) ^ table[index];
    }
    !crc
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        crc32_ieee, crc32c, encode_message_set, encode_record_batch_set,
        encode_record_batch_set_with_compression,
        encode_record_batch_set_with_compression_and_identity,
        encode_record_batch_set_with_compression_identity_and_transaction, encoded_message_set_len,
        encoded_record_batch_set_len, MessageSetMessage, ProducePartitionV2, ProducePartitionV3,
        ProduceRequestV11, ProduceRequestV2, ProduceRequestV3, ProduceRequestV7, ProduceRequestV9,
        ProduceResponseV2, ProduceResponseV7, ProduceResponseV9, ProduceTopicV2, ProduceTopicV3,
        RecordBatchIdentity, RecordBatchMessage,
    };
    use crate::codec::{DecodeLimits, Decoder};
    use crate::record_batch::RecordBatchCompression;
    use crate::{api::fetch::FetchResponseV2, codec::Encoder};

    #[test]
    fn crc_implementations_match_standard_check_vectors() {
        assert_eq!(crc32_ieee(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

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
    fn encodes_idempotent_record_batch_identity() {
        let set = encode_record_batch_set_with_compression_and_identity(
            &[RecordBatchMessage::new(
                Some(b"order-1".to_vec()),
                Some(b"created".to_vec()),
                1_000,
            )],
            RecordBatchCompression::None,
            RecordBatchIdentity {
                producer_id: 42,
                producer_epoch: 3,
                base_sequence: 7,
            },
        )
        .unwrap();

        assert_eq!(&set[43..51], &42_i64.to_be_bytes());
        assert_eq!(&set[51..53], &3_i16.to_be_bytes());
        assert_eq!(&set[53..57], &7_i32.to_be_bytes());
    }

    #[test]
    fn encodes_transactional_record_batch_attribute() {
        let set = encode_record_batch_set_with_compression_identity_and_transaction(
            &[RecordBatchMessage::new(
                None,
                Some(b"created".to_vec()),
                1_000,
            )],
            RecordBatchCompression::None,
            RecordBatchIdentity {
                producer_id: 42,
                producer_epoch: 3,
                base_sequence: 0,
            },
            true,
        )
        .unwrap();

        assert_eq!(&set[21..23], &0x10_i16.to_be_bytes());
    }

    #[test]
    fn encodes_produce_request_v3_with_record_batch() {
        let request = ProduceRequestV3 {
            correlation_id: 5,
            client_id: Some("kafrust".to_owned()),
            transactional_id: None,
            acks: 1,
            timeout_ms: 30_000,
            topics: vec![ProduceTopicV3 {
                name: "orders".to_owned(),
                partitions: vec![ProducePartitionV3 {
                    partition_index: 0,
                    compression: RecordBatchCompression::None,
                    identity: RecordBatchIdentity::NON_IDEMPOTENT,
                    records: vec![RecordBatchMessage::new(
                        Some(b"order-1".to_vec()),
                        Some(b"created".to_vec()),
                        1_000,
                    )
                    .header("source", Some(b"checkout".to_vec()))],
                }],
            }],
        };

        let bytes = request.encode().unwrap();

        assert_eq!(&bytes[0..4], &[0, 0, 0, 3]);
        assert!(bytes.len() > 80);
    }

    #[test]
    fn record_batch_encoding_roundtrips_through_fetch_decoder() {
        let record_set = encode_record_batch_set(&[RecordBatchMessage::new(
            Some(b"order-1".to_vec()),
            Some(b"created".to_vec()),
            1_000,
        )
        .header("source", Some(b"checkout".to_vec()))])
        .unwrap();

        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_bytes(&record_set).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV2::decode_body(&mut decoder).unwrap();
        let record = &response.responses[0].partitions[0].records[0];

        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp_ms, 1_000);
        assert_eq!(record.key.as_deref(), Some(&b"order-1"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"created"[..]));
        assert!(decoder.is_empty());
    }

    #[test]
    fn gzip_record_batch_encoding_roundtrips_through_fetch_decoder() {
        let record_set =
            encode_record_batch_set_with_compression(
                &[RecordBatchMessage::new(
                    Some(b"order-1".to_vec()),
                    Some(b"created".to_vec()),
                    1_000,
                )
                .header("source", Some(b"checkout".to_vec()))],
                RecordBatchCompression::Gzip,
            )
            .unwrap();

        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_bytes(&record_set).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV2::decode_body(&mut decoder).unwrap();
        let record = &response.responses[0].partitions[0].records[0];

        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp_ms, 1_000);
        assert_eq!(record.key.as_deref(), Some(&b"order-1"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"created"[..]));
        assert!(decoder.is_empty());
    }

    #[test]
    fn snappy_record_batch_encoding_roundtrips_through_fetch_decoder() {
        let record_set =
            encode_record_batch_set_with_compression(
                &[RecordBatchMessage::new(
                    Some(b"order-1".to_vec()),
                    Some(b"created".to_vec()),
                    1_000,
                )
                .header("source", Some(b"checkout".to_vec()))],
                RecordBatchCompression::Snappy,
            )
            .unwrap();

        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_bytes(&record_set).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV2::decode_body(&mut decoder).unwrap();
        let record = &response.responses[0].partitions[0].records[0];

        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp_ms, 1_000);
        assert_eq!(record.key.as_deref(), Some(&b"order-1"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"created"[..]));
        assert!(decoder.is_empty());
    }

    #[test]
    fn lz4_record_batch_encoding_roundtrips_through_fetch_decoder() {
        let record_set =
            encode_record_batch_set_with_compression(
                &[RecordBatchMessage::new(
                    Some(b"order-1".to_vec()),
                    Some(b"created".to_vec()),
                    1_000,
                )
                .header("source", Some(b"checkout".to_vec()))],
                RecordBatchCompression::Lz4,
            )
            .unwrap();

        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_bytes(&record_set).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV2::decode_body(&mut decoder).unwrap();
        let record = &response.responses[0].partitions[0].records[0];

        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp_ms, 1_000);
        assert_eq!(record.key.as_deref(), Some(&b"order-1"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"created"[..]));
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_produce_request_v7_with_record_batch() {
        let request = ProduceRequestV7 {
            correlation_id: 5,
            client_id: Some("kafrust".to_owned()),
            transactional_id: None,
            acks: 1,
            timeout_ms: 30_000,
            topics: vec![ProduceTopicV3 {
                name: "orders".to_owned(),
                partitions: vec![ProducePartitionV3 {
                    partition_index: 0,
                    compression: RecordBatchCompression::Zstd,
                    identity: RecordBatchIdentity::NON_IDEMPOTENT,
                    records: vec![RecordBatchMessage::new(
                        Some(b"order-1".to_vec()),
                        Some(b"created".to_vec()),
                        1_000,
                    )],
                }],
            }],
        };

        let bytes = request.encode().unwrap();

        assert_eq!(&bytes[0..4], &[0, 0, 0, 7]);
        assert!(bytes.len() > 70);
    }

    #[test]
    fn encodes_produce_request_v9_with_flexible_record_batch() {
        let request = ProduceRequestV9 {
            correlation_id: 5,
            client_id: Some("kafrust".to_owned()),
            transactional_id: Some("orders-tx".to_owned()),
            acks: -1,
            timeout_ms: 30_000,
            topics: vec![ProduceTopicV3 {
                name: "orders".to_owned(),
                partitions: vec![ProducePartitionV3 {
                    partition_index: 0,
                    compression: RecordBatchCompression::None,
                    identity: RecordBatchIdentity {
                        producer_id: 42,
                        producer_epoch: 3,
                        base_sequence: 7,
                    },
                    records: vec![RecordBatchMessage::new(
                        Some(b"order-1".to_vec()),
                        Some(b"created".to_vec()),
                        1_000,
                    )],
                }],
            }],
        };

        let bytes = request.encode().unwrap();

        assert_eq!(&bytes[0..4], &[0, 0, 0, 9]);
        assert!(bytes
            .windows(7)
            .any(|window| window == [10, b'o', b'r', b'd', b'e', b'r', b's']));
        assert!(bytes.len() > 80);
    }

    #[test]
    fn encodes_produce_request_v11_with_flexible_record_batch_schema() {
        let request = ProduceRequestV11 {
            correlation_id: 5,
            client_id: Some("kafrust".to_owned()),
            transactional_id: None,
            acks: 1,
            timeout_ms: 30_000,
            topics: Vec::new(),
        };

        let bytes = request.encode().unwrap();

        assert_eq!(&bytes[0..4], &[0, 0, 0, 11]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 5]);
        assert!(bytes.len() > 20);
    }

    #[test]
    fn zstd_record_batch_encoding_roundtrips_through_fetch_decoder() {
        let record_set =
            encode_record_batch_set_with_compression(
                &[RecordBatchMessage::new(
                    Some(b"order-1".to_vec()),
                    Some(b"created".to_vec()),
                    1_000,
                )
                .header("source", Some(b"checkout".to_vec()))],
                RecordBatchCompression::Zstd,
            )
            .unwrap();

        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_bytes(&record_set).unwrap();
        let bytes = bytes.into_bytes();

        let mut decoder = Decoder::new(&bytes);
        let response = FetchResponseV2::decode_body(&mut decoder).unwrap();
        let record = &response.responses[0].partitions[0].records[0];

        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp_ms, 1_000);
        assert_eq!(record.key.as_deref(), Some(&b"order-1"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"created"[..]));
        assert!(decoder.is_empty());
    }

    #[test]
    fn fetch_decoder_applies_custom_decompression_limit_to_record_batch() {
        let record_set = encode_record_batch_set_with_compression(
            &[RecordBatchMessage::new(
                Some(b"order-1".to_vec()),
                Some(vec![b'x'; 1024]),
                1_000,
            )],
            RecordBatchCompression::Zstd,
        )
        .unwrap();

        let mut bytes = Encoder::new();
        bytes.write_i32(0);
        bytes.write_i32(1);
        bytes.write_string("orders").unwrap();
        bytes.write_i32(1);
        bytes.write_i32(0);
        bytes.write_i16(0);
        bytes.write_i64(43);
        bytes.write_bytes(&record_set).unwrap();
        let bytes = bytes.into_bytes();
        let limits = DecodeLimits::new().with_max_decompressed_record_bytes(64);
        let mut decoder = Decoder::with_limits(&bytes, limits);

        assert!(matches!(
            FetchResponseV2::decode_body(&mut decoder),
            Err(crate::Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                max: 64,
                ..
            })
        ));
    }

    #[test]
    fn reports_message_set_encoded_len() {
        let records = [MessageSetMessage::new(
            Some(b"order-1".to_vec()),
            Some(b"created".to_vec()),
            1_000,
        )];

        assert_eq!(
            encoded_message_set_len(&records).unwrap(),
            encode_message_set(&records).unwrap().len()
        );
    }

    #[test]
    fn reports_record_batch_set_encoded_len() {
        let records =
            [
                RecordBatchMessage::new(
                    Some(b"order-1".to_vec()),
                    Some(b"created".to_vec()),
                    1_000,
                )
                .header("source", Some(b"checkout".to_vec())),
            ];

        assert_eq!(
            encoded_record_batch_set_len(&records).unwrap(),
            encode_record_batch_set(&records).unwrap().len()
        );
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

    #[test]
    fn decodes_produce_response_v7() {
        let bytes = [
            0, 0, 0, 1, // topic response count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, 0, 1, // partition response count
            0, 0, 0, 0, // partition
            0, 0, // error code
            0, 0, 0, 0, 0, 0, 0, 42, // base offset
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // log append time -1
            0, 0, 0, 0, 0, 0, 0, 7, // log start offset
            0, 0, 0, 0, // throttle time
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = ProduceResponseV7::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses[0].name, "orders");
        assert_eq!(response.responses[0].partitions[0].base_offset, 42);
        assert_eq!(response.responses[0].partitions[0].log_start_offset, 7);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_produce_response_v9_with_record_error_fields() {
        let mut bytes = Encoder::new();
        bytes
            .write_compact_array(Some(&[()]), |encoder, _| {
                encoder.write_compact_string("orders")?;
                encoder.write_compact_array(Some(&[()]), |encoder, _| {
                    encoder.write_i32(0);
                    encoder.write_i16(0);
                    encoder.write_i64(42);
                    encoder.write_i64(-1);
                    encoder.write_i64(7);
                    encoder.write_compact_array(Some(&[()]), |encoder, _| {
                        encoder.write_i32(3);
                        encoder.write_compact_nullable_string(Some("bad record"))?;
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_compact_nullable_string(Some("batch rejected"))?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        bytes.write_i32(0);
        bytes.write_empty_tagged_fields();

        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = ProduceResponseV9::decode_body(&mut decoder).unwrap();

        assert_eq!(response.responses[0].name, "orders");
        let partition = &response.responses[0].partitions[0];
        assert_eq!(partition.base_offset, 42);
        assert_eq!(partition.log_start_offset, 7);
        assert_eq!(partition.record_errors[0].batch_index, 3);
        assert_eq!(
            partition.record_errors[0].error_message.as_deref(),
            Some("bad record")
        );
        assert_eq!(partition.error_message.as_deref(), Some("batch rejected"));
        assert!(decoder.is_empty());
    }
}
