use std::io::{Read, Write};

use crate::error::{Error, Result};

const COMPRESSION_CODEC_MASK: i16 = 0x07;
const MAX_DECOMPRESSED_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const XERIAL_SNAPPY_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0, 0, 0, 0, 1, 0, 0, 0, 1,
];
const XERIAL_SNAPPY_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];
const XERIAL_SNAPPY_BLOCK_BYTES: usize = 32 * 1024;

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
        RecordBatchCompression::Zstd => Err(Error::UnsupportedCompression {
            codec: compression.name(),
        }),
    }
}

pub(crate) fn decompress_record_batch_records(
    compression: RecordBatchCompression,
    records: &[u8],
) -> Result<Vec<u8>> {
    match compression {
        RecordBatchCompression::None => Ok(records.to_vec()),
        RecordBatchCompression::Gzip => gzip_decompress(records),
        RecordBatchCompression::Snappy => snappy_decompress(records),
        RecordBatchCompression::Lz4 => lz4_decompress(records),
        RecordBatchCompression::Zstd => Err(Error::UnsupportedCompression {
            codec: compression.name(),
        }),
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

fn gzip_decompress(records: &[u8]) -> Result<Vec<u8>> {
    let decoder = flate2::read::GzDecoder::new(records);
    let mut limited = decoder.take(MAX_DECOMPRESSED_RECORD_BYTES + 1);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|error| compression_error("gzip", error))?;
    if output.len() as u64 > MAX_DECOMPRESSED_RECORD_BYTES {
        return Err(Error::LengthOverflow("decompressed record batch"));
    }
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

fn snappy_decompress(records: &[u8]) -> Result<Vec<u8>> {
    if records.starts_with(&XERIAL_SNAPPY_MAGIC) {
        return snappy_xerial_decompress(records);
    }

    ensure_snappy_output_limit(records, 0)?;
    snap::raw::Decoder::new()
        .decompress_vec(records)
        .map_err(|error| snappy_error(error.to_string()))
}

fn snappy_xerial_decompress(records: &[u8]) -> Result<Vec<u8>> {
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
        ensure_snappy_output_limit(block, output.len())?;
        let decompressed = decoder
            .decompress_vec(block)
            .map_err(|error| snappy_error(error.to_string()))?;
        output.extend_from_slice(&decompressed);
        position = block_end;
    }
    Ok(output)
}

fn ensure_snappy_output_limit(block: &[u8], already_decompressed: usize) -> Result<()> {
    let block_len =
        snap::raw::decompress_len(block).map_err(|error| snappy_error(error.to_string()))?;
    let total_len = already_decompressed
        .checked_add(block_len)
        .ok_or(Error::LengthOverflow("decompressed record batch"))?;
    if total_len as u64 > MAX_DECOMPRESSED_RECORD_BYTES {
        return Err(Error::LengthOverflow("decompressed record batch"));
    }
    Ok(())
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

fn lz4_decompress(records: &[u8]) -> Result<Vec<u8>> {
    lz4_decompress_with_limit(records, MAX_DECOMPRESSED_RECORD_BYTES)
}

fn lz4_decompress_with_limit(records: &[u8], max_decompressed_bytes: u64) -> Result<Vec<u8>> {
    let decoder = lz_fear::LZ4FrameReader::new(records)
        .map_err(|error| compression_reason("lz4", error.to_string()))?
        .into_read();
    let read_limit = max_decompressed_bytes
        .checked_add(1)
        .ok_or(Error::LengthOverflow("decompressed record batch"))?;
    let mut limited = decoder.take(read_limit);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(|error| compression_error("lz4", error))?;
    if output.len() as u64 > max_decompressed_bytes {
        return Err(Error::LengthOverflow("decompressed record batch"));
    }
    Ok(output)
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
        compress_record_batch_records, decompress_record_batch_records, lz4_decompress_with_limit,
        RecordBatchCompression, MAX_DECOMPRESSED_RECORD_BYTES, XERIAL_SNAPPY_HEADER,
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
            Error::LengthOverflow("decompressed record batch")
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
            Error::LengthOverflow("decompressed record batch")
        );
    }

    #[test]
    fn lz4_decoder_rejects_malformed_frame() {
        assert!(matches!(
            decompress_record_batch_records(RecordBatchCompression::Lz4, b"not an lz4 frame"),
            Err(Error::Compression { codec: "lz4", .. })
        ));
    }
}
