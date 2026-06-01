mod decode;
mod encode;

pub use decode::{Decoder, TaggedField};
pub use encode::Encoder;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{Decoder, Encoder};
    use crate::Error;

    #[test]
    fn encodes_and_decodes_fixed_width_primitives() {
        let mut encoder = Encoder::new();
        encoder.write_bool(true);
        encoder.write_i16(0x1234);
        encoder.write_i32(0x1234_5678);
        encoder.write_i64(0x0102_0304_0506_0708);

        let bytes = encoder.into_bytes();
        assert_eq!(
            bytes,
            [
                1, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                0x08,
            ]
        );

        let mut decoder = Decoder::new(&bytes);
        assert!(decoder.read_bool().unwrap());
        assert_eq!(decoder.read_i16().unwrap(), 0x1234);
        assert_eq!(decoder.read_i32().unwrap(), 0x1234_5678);
        assert_eq!(decoder.read_i64().unwrap(), 0x0102_0304_0506_0708);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_and_decodes_nullable_strings_and_bytes() {
        let mut encoder = Encoder::new();
        encoder.write_nullable_string(Some("topic")).unwrap();
        encoder.write_nullable_string(None).unwrap();
        encoder.write_nullable_bytes(Some(&[1, 2, 3])).unwrap();
        encoder.write_nullable_bytes(None).unwrap();

        let bytes = encoder.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("topic".to_owned())
        );
        assert_eq!(decoder.read_nullable_string().unwrap(), None);
        assert_eq!(decoder.read_nullable_bytes().unwrap(), Some(vec![1, 2, 3]));
        assert_eq!(decoder.read_nullable_bytes().unwrap(), None);
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_and_decodes_compact_types_and_empty_tags() {
        let mut encoder = Encoder::new();
        encoder.write_unsigned_varint(300);
        encoder.write_compact_string("kafka").unwrap();
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_compact_bytes(&[9, 8]).unwrap();
        encoder.write_empty_tagged_fields();

        let bytes = encoder.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.read_unsigned_varint().unwrap(), 300);
        assert_eq!(decoder.read_compact_string().unwrap(), "kafka");
        assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
        assert_eq!(decoder.read_compact_bytes().unwrap(), vec![9, 8]);
        assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
        assert!(decoder.is_empty());
    }

    #[test]
    fn rejects_invalid_bool_and_short_input() {
        let mut decoder = Decoder::new(&[2]);
        assert_eq!(decoder.read_bool(), Err(Error::InvalidBool(2)));

        let mut decoder = Decoder::new(&[0]);
        assert!(matches!(
            decoder.read_i16(),
            Err(Error::UnexpectedEof {
                needed: 2,
                remaining: 1
            })
        ));
    }
}
