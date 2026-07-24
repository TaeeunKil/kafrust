use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 42;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub group_ids: Vec<String>,
}

impl DeleteGroupsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_array(Some(&self.group_ids), |encoder, group_id| {
            encoder.write_string(group_id)
        })?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupsResponseV1 {
    pub throttle_time_ms: i32,
    pub results: Vec<DeleteGroupResultV1>,
}

impl DeleteGroupsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            results: decoder
                .read_array("delete group results", DeleteGroupResultV1::decode)?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupResultV1 {
    pub group_id: String,
    pub error_code: i16,
}

impl DeleteGroupResultV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            group_id: decoder.read_string()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DeleteGroupsRequestV1, DeleteGroupsResponseV1, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_delete_groups_v1_request() {
        let request = DeleteGroupsRequestV1 {
            correlation_id: 19,
            client_id: Some("kafrust".to_owned()),
            group_ids: vec!["orders".to_owned()],
        };

        assert_eq!(&request.encode().unwrap()[0..4], &[0, 42, 0, 1]);
        assert_eq!(API_KEY, 42);
    }

    #[test]
    fn decodes_delete_groups_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(6);
        bytes.write_i32(2);
        bytes.write_string("orders").unwrap();
        bytes.write_i16(0);
        bytes.write_string("active").unwrap();
        bytes.write_i16(68);
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = DeleteGroupsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 6);
        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].group_id, "orders");
        assert_eq!(response.results[0].error_code, 0);
        assert_eq!(response.results[1].group_id, "active");
        assert_eq!(response.results[1].error_code, 68);
        assert!(decoder.is_empty());
    }
}
