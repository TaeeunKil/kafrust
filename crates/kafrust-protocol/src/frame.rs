use crate::codec::{Decoder, Encoder};
use crate::error::{Error, Result};

pub fn encode_frame(payload: &[u8]) -> Result<Vec<u8>> {
    let length = i32::try_from(payload.len()).map_err(|_| Error::LengthOverflow("frame"))?;
    let mut encoder = Encoder::new();
    encoder.write_i32(length);
    let mut frame = encoder.into_bytes();
    frame.extend_from_slice(payload);
    Ok(frame)
}

pub fn decode_frame(input: &[u8]) -> Result<&[u8]> {
    let mut decoder = Decoder::new(input);
    let length = decoder.read_i32()?;
    if length < 0 {
        return Err(Error::NegativeLength {
            kind: "frame",
            length,
        });
    }
    let length = usize::try_from(length).map_err(|_| Error::LengthOverflow("frame"))?;
    decoder.read_exact(length)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{decode_frame, encode_frame};

    #[test]
    fn encodes_and_decodes_frame() {
        let frame = encode_frame(&[0xde, 0xad, 0xbe, 0xef]).unwrap();
        assert_eq!(frame, [0, 0, 0, 4, 0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(decode_frame(&frame).unwrap(), &[0xde, 0xad, 0xbe, 0xef]);
    }
}
