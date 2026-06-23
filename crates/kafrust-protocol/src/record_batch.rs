use std::io::{Read, Write};

use crate::error::{Error, Result};

const COMPRESSION_CODEC_MASK: i16 = 0x07;
const MAX_DECOMPRESSED_RECORD_BYTES: u64 = 64 * 1024 * 1024;

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
        RecordBatchCompression::Snappy
        | RecordBatchCompression::Lz4
        | RecordBatchCompression::Zstd => Err(Error::UnsupportedCompression {
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
        RecordBatchCompression::Snappy
        | RecordBatchCompression::Lz4
        | RecordBatchCompression::Zstd => Err(Error::UnsupportedCompression {
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

fn compression_error(codec: &'static str, error: std::io::Error) -> Error {
    Error::Compression {
        codec,
        reason: error.to_string(),
    }
}
