use core::fmt;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnexpectedEof { needed: usize, remaining: usize },
    InvalidBool(i8),
    NegativeLength { kind: &'static str, length: i32 },
    LengthOverflow(&'static str),
    InvalidUtf8,
    VarintTooLong,
    UnsupportedVersion { kind: &'static str, version: i16 },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                f,
                "unexpected end of input: needed {needed} bytes, had {remaining}"
            ),
            Self::InvalidBool(value) => write!(f, "invalid boolean value {value}"),
            Self::NegativeLength { kind, length } => {
                write!(f, "negative {kind} length {length}")
            }
            Self::LengthOverflow(kind) => write!(f, "{kind} length does not fit Kafka encoding"),
            Self::InvalidUtf8 => f.write_str("invalid UTF-8 string"),
            Self::VarintTooLong => f.write_str("unsigned varint is too long"),
            Self::UnsupportedVersion { kind, version } => {
                write!(f, "unsupported {kind} version {version}")
            }
        }
    }
}

impl std::error::Error for Error {}
