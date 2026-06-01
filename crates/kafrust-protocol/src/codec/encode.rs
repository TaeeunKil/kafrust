use crate::error::{Error, Result};

#[derive(Debug, Clone, Default)]
pub struct Encoder {
    output: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.output
    }

    pub fn write_i8(&mut self, value: i8) {
        self.output.push(value as u8);
    }

    pub fn write_bool(&mut self, value: bool) {
        self.write_i8(if value { 1 } else { 0 });
    }

    pub fn write_i16(&mut self, value: i16) {
        self.output.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i32(&mut self, value: i32) {
        self.output.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i64(&mut self, value: i64) {
        self.output.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_string(&mut self, value: &str) -> Result<()> {
        let length = i16::try_from(value.len()).map_err(|_| Error::LengthOverflow("string"))?;
        self.write_i16(length);
        self.output.extend_from_slice(value.as_bytes());
        Ok(())
    }

    pub fn write_nullable_string(&mut self, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => self.write_string(value),
            None => {
                self.write_i16(-1);
                Ok(())
            }
        }
    }

    pub fn write_bytes(&mut self, value: &[u8]) -> Result<()> {
        let length = i32::try_from(value.len()).map_err(|_| Error::LengthOverflow("bytes"))?;
        self.write_i32(length);
        self.output.extend_from_slice(value);
        Ok(())
    }

    pub fn write_nullable_bytes(&mut self, value: Option<&[u8]>) -> Result<()> {
        match value {
            Some(value) => self.write_bytes(value),
            None => {
                self.write_i32(-1);
                Ok(())
            }
        }
    }

    pub fn write_unsigned_varint(&mut self, mut value: u32) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    pub fn write_compact_string(&mut self, value: &str) -> Result<()> {
        let length =
            u32::try_from(value.len()).map_err(|_| Error::LengthOverflow("compact string"))?;
        let encoded_length = length
            .checked_add(1)
            .ok_or(Error::LengthOverflow("compact string"))?;
        self.write_unsigned_varint(encoded_length);
        self.output.extend_from_slice(value.as_bytes());
        Ok(())
    }

    pub fn write_compact_nullable_string(&mut self, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => self.write_compact_string(value),
            None => {
                self.write_unsigned_varint(0);
                Ok(())
            }
        }
    }

    pub fn write_compact_bytes(&mut self, value: &[u8]) -> Result<()> {
        let length =
            u32::try_from(value.len()).map_err(|_| Error::LengthOverflow("compact bytes"))?;
        let encoded_length = length
            .checked_add(1)
            .ok_or(Error::LengthOverflow("compact bytes"))?;
        self.write_unsigned_varint(encoded_length);
        self.output.extend_from_slice(value);
        Ok(())
    }

    pub fn write_compact_nullable_bytes(&mut self, value: Option<&[u8]>) -> Result<()> {
        match value {
            Some(value) => self.write_compact_bytes(value),
            None => {
                self.write_unsigned_varint(0);
                Ok(())
            }
        }
    }

    pub fn write_array<T>(
        &mut self,
        values: Option<&[T]>,
        mut write_item: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        match values {
            Some(values) => {
                let length =
                    i32::try_from(values.len()).map_err(|_| Error::LengthOverflow("array"))?;
                self.write_i32(length);
                for value in values {
                    write_item(self, value)?;
                }
            }
            None => self.write_i32(-1),
        }
        Ok(())
    }

    pub fn write_compact_array<T>(
        &mut self,
        values: Option<&[T]>,
        mut write_item: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        match values {
            Some(values) => {
                let length =
                    u32::try_from(values.len()).map_err(|_| Error::LengthOverflow("array"))?;
                let encoded_length = length
                    .checked_add(1)
                    .ok_or(Error::LengthOverflow("compact array"))?;
                self.write_unsigned_varint(encoded_length);
                for value in values {
                    write_item(self, value)?;
                }
            }
            None => self.write_unsigned_varint(0),
        }
        Ok(())
    }

    pub fn write_empty_tagged_fields(&mut self) {
        self.write_unsigned_varint(0);
    }
}
