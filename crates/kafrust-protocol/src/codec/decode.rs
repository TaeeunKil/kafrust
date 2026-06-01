use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedField {
    pub tag: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, position: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.position)
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_exact(1)?[0] as i8)
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        match self.read_i8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidBool(value)),
        }
    }

    pub fn read_i16(&mut self) -> Result<i16> {
        let bytes = self.read_exact(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_exact(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn read_string(&mut self) -> Result<String> {
        let length = self.read_i16()?;
        if length < 0 {
            return Err(Error::NegativeLength {
                kind: "string",
                length: i32::from(length),
            });
        }
        let length = usize::try_from(length).map_err(|_| Error::LengthOverflow("string"))?;
        self.read_utf8(length)
    }

    pub fn read_nullable_string(&mut self) -> Result<Option<String>> {
        let length = self.read_i16()?;
        if length == -1 {
            return Ok(None);
        }
        if length < -1 {
            return Err(Error::NegativeLength {
                kind: "nullable string",
                length: i32::from(length),
            });
        }
        let length =
            usize::try_from(length).map_err(|_| Error::LengthOverflow("nullable string"))?;
        Ok(Some(self.read_utf8(length)?))
    }

    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.read_i32()?;
        if length < 0 {
            return Err(Error::NegativeLength {
                kind: "bytes",
                length,
            });
        }
        let length = usize::try_from(length).map_err(|_| Error::LengthOverflow("bytes"))?;
        Ok(self.read_exact(length)?.to_vec())
    }

    pub fn read_nullable_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let length = self.read_i32()?;
        if length == -1 {
            return Ok(None);
        }
        if length < -1 {
            return Err(Error::NegativeLength {
                kind: "nullable bytes",
                length,
            });
        }
        let length =
            usize::try_from(length).map_err(|_| Error::LengthOverflow("nullable bytes"))?;
        Ok(Some(self.read_exact(length)?.to_vec()))
    }

    pub fn read_unsigned_varint(&mut self) -> Result<u32> {
        let mut value = 0u32;
        for shift in (0..=28).step_by(7) {
            let byte = self.read_exact(1)?[0];
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Error::VarintTooLong)
    }

    pub fn read_varint(&mut self) -> Result<i32> {
        let value = self.read_unsigned_varint()?;
        Ok(((value >> 1) as i32) ^ -((value & 1) as i32))
    }

    pub fn read_varlong(&mut self) -> Result<i64> {
        let mut value = 0u64;
        for shift in (0..=63).step_by(7) {
            let byte = self.read_exact(1)?[0];
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(((value >> 1) as i64) ^ -((value & 1) as i64));
            }
        }
        Err(Error::VarintTooLong)
    }

    pub fn read_varint_bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.read_varint()?;
        if length < 0 {
            return Err(Error::NegativeLength {
                kind: "varint bytes",
                length,
            });
        }
        let length = usize::try_from(length).map_err(|_| Error::LengthOverflow("varint bytes"))?;
        Ok(self.read_exact(length)?.to_vec())
    }

    pub fn read_varint_nullable_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let length = self.read_varint()?;
        if length == -1 {
            return Ok(None);
        }
        if length < -1 {
            return Err(Error::NegativeLength {
                kind: "varint nullable bytes",
                length,
            });
        }
        let length =
            usize::try_from(length).map_err(|_| Error::LengthOverflow("varint nullable bytes"))?;
        Ok(Some(self.read_exact(length)?.to_vec()))
    }

    pub fn read_compact_string(&mut self) -> Result<String> {
        let encoded_length = self.read_unsigned_varint()?;
        let length = encoded_length.checked_sub(1).ok_or(Error::NegativeLength {
            kind: "compact string",
            length: -1,
        })?;
        let length =
            usize::try_from(length).map_err(|_| Error::LengthOverflow("compact string"))?;
        self.read_utf8(length)
    }

    pub fn read_compact_nullable_string(&mut self) -> Result<Option<String>> {
        let encoded_length = self.read_unsigned_varint()?;
        if encoded_length == 0 {
            return Ok(None);
        }
        let length = usize::try_from(encoded_length - 1)
            .map_err(|_| Error::LengthOverflow("compact nullable string"))?;
        Ok(Some(self.read_utf8(length)?))
    }

    pub fn read_compact_bytes(&mut self) -> Result<Vec<u8>> {
        let encoded_length = self.read_unsigned_varint()?;
        let length = encoded_length.checked_sub(1).ok_or(Error::NegativeLength {
            kind: "compact bytes",
            length: -1,
        })?;
        let length = usize::try_from(length).map_err(|_| Error::LengthOverflow("compact bytes"))?;
        Ok(self.read_exact(length)?.to_vec())
    }

    pub fn read_compact_nullable_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let encoded_length = self.read_unsigned_varint()?;
        if encoded_length == 0 {
            return Ok(None);
        }
        let length = usize::try_from(encoded_length - 1)
            .map_err(|_| Error::LengthOverflow("compact nullable bytes"))?;
        Ok(Some(self.read_exact(length)?.to_vec()))
    }

    pub fn read_array<T>(
        &mut self,
        kind: &'static str,
        mut read_item: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Option<Vec<T>>> {
        let length = self.read_i32()?;
        if length == -1 {
            return Ok(None);
        }
        if length < -1 {
            return Err(Error::NegativeLength { kind, length });
        }
        let length = usize::try_from(length).map_err(|_| Error::LengthOverflow(kind))?;
        let mut values = Vec::with_capacity(length);
        for _ in 0..length {
            values.push(read_item(self)?);
        }
        Ok(Some(values))
    }

    pub fn read_compact_array<T>(
        &mut self,
        kind: &'static str,
        mut read_item: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Option<Vec<T>>> {
        let encoded_length = self.read_unsigned_varint()?;
        if encoded_length == 0 {
            return Ok(None);
        }
        let length =
            usize::try_from(encoded_length - 1).map_err(|_| Error::LengthOverflow(kind))?;
        let mut values = Vec::with_capacity(length);
        for _ in 0..length {
            values.push(read_item(self)?);
        }
        Ok(Some(values))
    }

    pub fn read_tagged_fields(&mut self) -> Result<Vec<TaggedField>> {
        let count = self.read_unsigned_varint()?;
        let count = usize::try_from(count).map_err(|_| Error::LengthOverflow("tagged fields"))?;
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = self.read_unsigned_varint()?;
            let length = self.read_unsigned_varint()?;
            let length =
                usize::try_from(length).map_err(|_| Error::LengthOverflow("tagged field data"))?;
            let data = self.read_exact(length)?.to_vec();
            fields.push(TaggedField { tag, data });
        }
        Ok(fields)
    }

    pub fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        if self.remaining() < length {
            return Err(Error::UnexpectedEof {
                needed: length,
                remaining: self.remaining(),
            });
        }
        let start = self.position;
        self.position += length;
        Ok(&self.input[start..self.position])
    }

    fn read_utf8(&mut self, length: usize) -> Result<String> {
        let bytes = self.read_exact(length)?;
        let value = core::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
        Ok(value.to_owned())
    }
}
