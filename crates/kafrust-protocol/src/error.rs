use core::fmt;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    InvalidBool(i8),
    InvalidNullableStruct(i8),
    NegativeLength {
        kind: &'static str,
        length: i32,
    },
    LengthOverflow(&'static str),
    LimitExceeded {
        kind: &'static str,
        actual: usize,
        max: usize,
    },
    InvalidUtf8,
    VarintTooLong,
    UnsupportedVersion {
        kind: &'static str,
        version: i16,
    },
    UnsupportedCompression {
        codec: &'static str,
    },
    Compression {
        codec: &'static str,
        reason: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                f,
                "unexpected end of input: needed {needed} bytes, had {remaining}"
            ),
            Self::InvalidBool(value) => write!(f, "invalid boolean value {value}"),
            Self::InvalidNullableStruct(value) => {
                write!(f, "invalid nullable struct marker {value}")
            }
            Self::NegativeLength { kind, length } => {
                write!(f, "negative {kind} length {length}")
            }
            Self::LengthOverflow(kind) => write!(f, "{kind} length does not fit Kafka encoding"),
            Self::LimitExceeded { kind, actual, max } => {
                write!(f, "{kind} limit exceeded: {actual} is greater than {max}")
            }
            Self::InvalidUtf8 => f.write_str("invalid UTF-8 string"),
            Self::VarintTooLong => f.write_str("unsigned varint is too long"),
            Self::UnsupportedVersion { kind, version } => {
                write!(f, "unsupported {kind} version {version}")
            }
            Self::UnsupportedCompression { codec } => {
                write!(f, "unsupported record batch compression codec {codec}")
            }
            Self::Compression { codec, reason } => {
                write!(f, "{codec} record batch compression error: {reason}")
            }
        }
    }
}

impl std::error::Error for Error {}
