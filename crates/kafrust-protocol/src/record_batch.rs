use std::io::{Read, Write};

use crate::error::{Error, Result};

const COMPRESSION_CODEC_MASK: i16 = 0x07;
#[cfg(test)]
const MAX_DECOMPRESSED_RECORD_BYTES: usize = 64 * 1024 * 1024;
const XERIAL_SNAPPY_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0, 0, 0, 0, 1, 0, 0, 0, 1,
];
const XERIAL_SNAPPY_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];
const XERIAL_SNAPPY_BLOCK_BYTES: usize = 32 * 1024;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordBatchCompression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl RecordBatchCompression {
    pub fn from_attributes(attributes: i16) -> Result<Self> {
        match attributes & COMPRESSION_CODEC_MASK {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            code => Err(Error::UnsupportedVersion {
                kind: "record batch compression codec",
                version: code,
            }),
        }
    }

    pub fn attributes(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Snappy => 2,
            Self::Lz4 => 3,
            Self::Zstd => 4,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    pub fn is_compressed(self) -> bool {
        self != Self::None
    }
}

pub(crate) fn compress_record_batch_records(
    compression: RecordBatchCompression,
    records: &[u8],
) -> Result<Vec<u8>> {
    match compression {
        RecordBatchCompression::None => Ok(records.to_vec()),
        RecordBatchCompression::Gzip => gzip_compress(records),
        RecordBatchCompression::Snappy => snappy_compress(records),
        RecordBatchCompression::Lz4 => lz4_compress(records),
        RecordBatchCompression::Zstd => zstd_compress(records),
    }
}

#[cfg(test)]
pub(crate) fn decompress_record_batch_records(
    compression: RecordBatchCompression,
    records: &[u8],
) -> Result<Vec<u8>> {
    decompress_record_batch_records_with_limit(compression, records, MAX_DECOMPRESSED_RECORD_BYTES)
}

pub(crate) fn decompress_record_batch_records_with_limit(
    compression: RecordBatchCompression,
    records: &[u8],
    max_decompressed_bytes: usize,
) -> Result<Vec<u8>> {
    match compression {
        RecordBatchCompression::None => {
            ensure_decompressed_output_limit(records.len(), max_decompressed_bytes)?;
            Ok(records.to_vec())
        }
        RecordBatchCompression::Gzip => gzip_decompress(records, max_decompressed_bytes),
        RecordBatchCompression::Snappy => snappy_decompress(records, max_decompressed_bytes),
        RecordBatchCompression::Lz4 => lz4_decompress_with_limit(records, max_decompressed_bytes),
        RecordBatchCompression::Zstd => zstd_decompress_with_limit(records, max_decompressed_bytes),
    }
}

fn gzip_compress(records: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(records)
        .map_err(|error| compression_error("gzip", error))?;
    encoder
        .finish()
        .map_err(|error| compression_error("gzip", error))
}

fn gzip_decompress(records: &[u8], max_decompressed_bytes: usize) -> Result<Vec<u8>> {
    let decoder = flate2::read::GzDecoder::new(records);
    let read_limit = u64::try_from(max_decompressed_bytes)
        .map_err(|_| Error::LengthOverflow("decompressed record batch"))?
        .checked_add(1)
        .ok_or(Error::LengthOverflow("decompressed record batch"))?;
    let mut limited = decoder.take(read_limit);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|error| compression_error("gzip", error))?;
    ensure_decompressed_output_limit(output.len(), max_decompressed_bytes)?;
    Ok(output)
}

fn snappy_compress(records: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(
        XERIAL_SNAPPY_HEADER
            .len()
            .checked_add(records.len())
            .ok_or(Error::LengthOverflow("snappy record batch"))?,
    );
    output.extend_from_slice(&XERIAL_SNAPPY_HEADER);

    let mut encoder = snap::raw::Encoder::new();
    for block in records.chunks(XERIAL_SNAPPY_BLOCK_BYTES) {
        let compressed = encoder
            .compress_vec(block)
            .map_err(|error| snappy_error(error.to_string()))?;
        let compressed_len = u32::try_from(compressed.len())
            .map_err(|_| Error::LengthOverflow("snappy record batch block"))?;
        output.extend_from_slice(&compressed_len.to_be_bytes());
        output.extend_from_slice(&compressed);
    }
    Ok(output)
}

fn snappy_decompress(records: &[u8], max_decompressed_bytes: usize) -> Result<Vec<u8>> {
    if records.starts_with(&XERIAL_SNAPPY_MAGIC) {
        return snappy_xerial_decompress(records, max_decompressed_bytes);
    }

    ensure_snappy_output_limit(records, 0, max_decompressed_bytes)?;
    snap::raw::Decoder::new()
        .decompress_vec(records)
        .map_err(|error| snappy_error(error.to_string()))
}

fn snappy_xerial_decompress(records: &[u8], max_decompressed_bytes: usize) -> Result<Vec<u8>> {
    if !records.starts_with(&XERIAL_SNAPPY_HEADER) {
        return Err(snappy_error(
            "invalid or unsupported xerial framing header".to_owned(),
        ));
    }

    let mut output = Vec::new();
    let mut position = XERIAL_SNAPPY_HEADER.len();
    let mut decoder = snap::raw::Decoder::new();
    while position < records.len() {
        let length_end = position
            .checked_add(4)
            .ok_or(Error::LengthOverflow("snappy record batch block"))?;
        let length_bytes = records
            .get(position..length_end)
            .ok_or_else(|| snappy_error("truncated xerial block length".to_owned()))?;
        let block_len = u32::from_be_bytes([
            length_bytes[0],
            length_bytes[1],
            length_bytes[2],
            length_bytes[3],
        ]) as usize;
        position = length_end;

        let block_end = position
            .checked_add(block_len)
            .ok_or(Error::LengthOverflow("snappy record batch block"))?;
        let block = records
            .get(position..block_end)
            .ok_or_else(|| snappy_error("xerial block extends past input".to_owned()))?;
        ensure_snappy_output_limit(block, output.len(), max_decompressed_bytes)?;
        let decompressed = decoder
            .decompress_vec(block)
            .map_err(|error| snappy_error(error.to_string()))?;
        output.extend_from_slice(&decompressed);
        position = block_end;
    }
    Ok(output)
}

fn ensure_snappy_output_limit(
    block: &[u8],
    already_decompressed: usize,
    max_decompressed_bytes: usize,
) -> Result<()> {
    let block_len =
        snap::raw::decompress_len(block).map_err(|error| snappy_error(error.to_string()))?;
    let total_len = already_decompressed
        .checked_add(block_len)
        .ok_or(Error::LengthOverflow("decompressed record batch"))?;
    ensure_decompressed_output_limit(total_len, max_decompressed_bytes)
}

fn lz4_compress(records: &[u8]) -> Result<Vec<u8>> {
    let mut settings = lz_fear::CompressionSettings::default();
    settings
        .independent_blocks(true)
        .block_checksums(false)
        .content_checksum(false)
        .block_size(64 * 1024);
    let mut output = Vec::new();
    settings
        .compress(records, &mut output)
        .map_err(|error| compression_reason("lz4", error.to_string()))?;
    Ok(output)
}

fn lz4_decompress_with_limit(records: &[u8], max_decompressed_bytes: usize) -> Result<Vec<u8>> {
    let decoder = lz_fear::LZ4FrameReader::new(records)
        .map_err(|error| compression_reason("lz4", error.to_string()))?
        .into_read();
    let read_limit = u64::try_from(max_decompressed_bytes)
        .map_err(|_| Error::LengthOverflow("decompressed record batch"))?
        .checked_add(1)
        .ok_or(Error::LengthOverflow("decompressed record batch"))?;
    let mut limited = decoder.take(read_limit);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|error| compression_error("lz4", error))?;
    ensure_decompressed_output_limit(output.len(), max_decompressed_bytes)?;
    Ok(output)
}

fn zstd_compress(records: &[u8]) -> Result<Vec<u8>> {
    std::panic::catch_unwind(|| {
        ruzstd::encoding::compress_to_vec(records, ruzstd::encoding::CompressionLevel::Fastest)
    })
    .map_err(|_| compression_reason("zstd", "encoder panicked".to_owned()))
}

fn zstd_decompress_with_limit(records: &[u8], max_decompressed_bytes: usize) -> Result<Vec<u8>> {
    ensure_zstd_frame_limits(records, max_decompressed_bytes)?;
    std::panic::catch_unwind(|| {
        let decoder = ruzstd::decoding::StreamingDecoder::new(records)
            .map_err(|error| compression_reason("zstd", error.to_string()))?;
        let read_limit = u64::try_from(max_decompressed_bytes)
            .map_err(|_| Error::LengthOverflow("decompressed record batch"))?
            .checked_add(1)
            .ok_or(Error::LengthOverflow("decompressed record batch"))?;
        let mut limited = decoder.take(read_limit);
        let mut output = Vec::new();
        limited
            .read_to_end(&mut output)
            .map_err(|error| compression_error("zstd", error))?;
        ensure_decompressed_output_limit(output.len(), max_decompressed_bytes)?;
        Ok(output)
    })
    .map_err(|_| compression_reason("zstd", "decoder panicked".to_owned()))?
}

fn ensure_zstd_frame_limits(records: &[u8], max_decompressed_bytes: usize) -> Result<()> {
    if !records.starts_with(&ZSTD_MAGIC) {
        return Err(compression_reason("zstd", "invalid frame magic".to_owned()));
    }

    let descriptor = *records
        .get(ZSTD_MAGIC.len())
        .ok_or_else(|| compression_reason("zstd", "truncated frame header".to_owned()))?;
    let single_segment = descriptor & 0x20 != 0;
    let mut position = ZSTD_MAGIC.len() + 1;

    let window_size = if single_segment {
        None
    } else {
        let window_descriptor = *records
            .get(position)
            .ok_or_else(|| compression_reason("zstd", "truncated window descriptor".to_owned()))?;
        position += 1;
        let exponent = u64::from(window_descriptor >> 3);
        let mantissa = u64::from(window_descriptor & 0x07);
        let window_base = 1u64 << (10 + exponent);
        Some(window_base + (window_base / 8) * mantissa)
    };

    let dictionary_id_bytes = match descriptor & 0x03 {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 4,
        _ => unreachable!(),
    };
    position = position
        .checked_add(dictionary_id_bytes)
        .ok_or(Error::LengthOverflow("zstd frame header"))?;

    let content_size_bytes = match descriptor >> 6 {
        0 if single_segment => 1,
        0 => 0,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => unreachable!(),
    };
    let content_size = read_zstd_little_endian(records, position, content_size_bytes)?;
    let content_size = if content_size_bytes == 2 {
        content_size + 256
    } else {
        content_size
    };
    let required_window = window_size.unwrap_or(content_size);
    let actual = required_window.max(content_size);
    let max = u64::try_from(max_decompressed_bytes)
        .map_err(|_| Error::LengthOverflow("decompressed record batch"))?;
    if actual > max {
        return Err(Error::LimitExceeded {
            kind: "decompressed record batch bytes",
            actual: usize::try_from(actual).unwrap_or(usize::MAX),
            max: max_decompressed_bytes,
        });
    }
    Ok(())
}

fn ensure_decompressed_output_limit(actual: usize, max: usize) -> Result<()> {
    if actual > max {
        return Err(Error::LimitExceeded {
            kind: "decompressed record batch bytes",
            actual,
            max,
        });
    }
    Ok(())
}

fn read_zstd_little_endian(records: &[u8], position: usize, length: usize) -> Result<u64> {
    let end = position
        .checked_add(length)
        .ok_or(Error::LengthOverflow("zstd frame header"))?;
    let bytes = records
        .get(position..end)
        .ok_or_else(|| compression_reason("zstd", "truncated frame header".to_owned()))?;
    Ok(bytes.iter().enumerate().fold(0u64, |value, (index, byte)| {
        value | (u64::from(*byte) << (index * 8))
    }))
}

fn compression_error(codec: &'static str, error: std::io::Error) -> Error {
    compression_reason(codec, error.to_string())
}

fn compression_reason(codec: &'static str, reason: String) -> Error {
    Error::Compression { codec, reason }
}

fn snappy_error(reason: String) -> Error {
    compression_reason("snappy", reason)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        compress_record_batch_records, decompress_record_batch_records,
        decompress_record_batch_records_with_limit, lz4_decompress_with_limit,
        zstd_decompress_with_limit, RecordBatchCompression, MAX_DECOMPRESSED_RECORD_BYTES,
        XERIAL_SNAPPY_HEADER, ZSTD_MAGIC,
    };
    use crate::error::Error;

    #[test]
    fn snappy_xerial_roundtrip_spans_multiple_blocks() {
        let records = vec![b'x'; 70 * 1024];

        let compressed =
            compress_record_batch_records(RecordBatchCompression::Snappy, &records).unwrap();
        let decompressed =
            decompress_record_batch_records(RecordBatchCompression::Snappy, &compressed).unwrap();

        assert!(compressed.starts_with(&XERIAL_SNAPPY_HEADER));
        assert_eq!(decompressed, records);
    }

    #[test]
    fn snappy_decoder_accepts_raw_blocks() {
        let records = b"kafka record batch";
        let compressed = snap::raw::Encoder::new().compress_vec(records).unwrap();

        assert_eq!(
            decompress_record_batch_records(RecordBatchCompression::Snappy, &compressed).unwrap(),
            records
        );
    }

    #[test]
    fn snappy_decoder_rejects_declared_output_over_limit() {
        let mut declared_len = MAX_DECOMPRESSED_RECORD_BYTES + 1;
        let mut hostile_block = Vec::new();
        while declared_len >= 0x80 {
            hostile_block.push((declared_len as u8) | 0x80);
            declared_len >>= 7;
        }
        hostile_block.push(declared_len as u8);

        assert_eq!(
            decompress_record_batch_records(RecordBatchCompression::Snappy, &hostile_block)
                .unwrap_err(),
            Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                actual: MAX_DECOMPRESSED_RECORD_BYTES + 1,
                max: MAX_DECOMPRESSED_RECORD_BYTES,
            }
        );
    }

    #[test]
    fn gzip_decoder_honors_custom_output_limit() {
        let records = vec![b'x'; 1024];
        let compressed =
            compress_record_batch_records(RecordBatchCompression::Gzip, &records).unwrap();

        assert_eq!(
            decompress_record_batch_records_with_limit(
                RecordBatchCompression::Gzip,
                &compressed,
                64,
            )
            .unwrap_err(),
            Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                actual: 65,
                max: 64,
            }
        );
    }

    #[test]
    fn snappy_decoder_honors_custom_output_limit() {
        let records = vec![b'x'; 1024];
        let compressed =
            compress_record_batch_records(RecordBatchCompression::Snappy, &records).unwrap();

        assert_eq!(
            decompress_record_batch_records_with_limit(
                RecordBatchCompression::Snappy,
                &compressed,
                64,
            )
            .unwrap_err(),
            Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                actual: 1024,
                max: 64,
            }
        );
    }

    #[test]
    fn snappy_decoder_rejects_truncated_xerial_block() {
        let mut compressed = XERIAL_SNAPPY_HEADER.to_vec();
        compressed.extend_from_slice(&10u32.to_be_bytes());
        compressed.extend_from_slice(&[1, 2, 3]);

        assert!(matches!(
            decompress_record_batch_records(RecordBatchCompression::Snappy, &compressed),
            Err(Error::Compression {
                codec: "snappy",
                ..
            })
        ));
    }

    #[test]
    fn lz4_frame_roundtrips_with_kafka_magic() {
        let records = vec![b'x'; 70 * 1024];

        let compressed =
            compress_record_batch_records(RecordBatchCompression::Lz4, &records).unwrap();
        let decompressed =
            decompress_record_batch_records(RecordBatchCompression::Lz4, &compressed).unwrap();

        assert_eq!(&compressed[..4], &[0x04, 0x22, 0x4d, 0x18]);
        assert_eq!(decompressed, records);
    }

    #[test]
    fn lz4_decoder_rejects_output_over_limit() {
        let records = vec![b'x'; 1024];
        let compressed =
            compress_record_batch_records(RecordBatchCompression::Lz4, &records).unwrap();

        assert_eq!(
            lz4_decompress_with_limit(&compressed, 64).unwrap_err(),
            Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                actual: 65,
                max: 64,
            }
        );
    }

    #[test]
    fn lz4_decoder_rejects_malformed_frame() {
        assert!(matches!(
            decompress_record_batch_records(RecordBatchCompression::Lz4, b"not an lz4 frame"),
            Err(Error::Compression { codec: "lz4", .. })
        ));
    }

    #[test]
    fn zstd_frame_roundtrips_with_kafka_magic() {
        let records = vec![b'x'; 140 * 1024];

        let compressed =
            compress_record_batch_records(RecordBatchCompression::Zstd, &records).unwrap();
        let decompressed =
            decompress_record_batch_records(RecordBatchCompression::Zstd, &compressed).unwrap();

        assert_eq!(&compressed[..4], &ZSTD_MAGIC);
        assert_eq!(decompressed, records);
    }

    #[test]
    fn zstd_decoder_rejects_output_over_limit() {
        let records = vec![b'x'; 1024];
        let compressed =
            compress_record_batch_records(RecordBatchCompression::Zstd, &records).unwrap();

        assert!(matches!(
            zstd_decompress_with_limit(&compressed, 64),
            Err(Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                max: 64,
                ..
            })
        ));
    }

    #[test]
    fn zstd_decoder_rejects_declared_window_over_limit() {
        let frame = [ZSTD_MAGIC.as_slice(), &[0, 0]].concat();

        assert!(matches!(
            zstd_decompress_with_limit(&frame, 64),
            Err(Error::LimitExceeded {
                kind: "decompressed record batch bytes",
                max: 64,
                ..
            })
        ));
    }

    #[test]
    fn zstd_decoder_rejects_malformed_frame() {
        assert!(matches!(
            decompress_record_batch_records(RecordBatchCompression::Zstd, b"not a zstd frame"),
            Err(Error::Compression { codec: "zstd", .. })
        ));
    }
}
