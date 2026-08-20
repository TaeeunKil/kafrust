use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl ListGroupsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        Ok(encoder.into_bytes())
    }
}

/// Kafka ListGroups v4 request.
///
/// Version 4 adds a broker-side group-state filter and uses the flexible
/// request header/body encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsRequestV4 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub states_filter: Vec<String>,
}

impl ListGroupsRequestV4 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_flexible_request(
            4,
            self.correlation_id,
            self.client_id.as_deref(),
            &self.states_filter,
            &[],
        )
    }
}

/// Kafka ListGroups v5 request.
///
/// Version 5 adds a group-type filter to the v4 state filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsRequestV5 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub states_filter: Vec<String>,
    pub types_filter: Vec<String>,
}

impl ListGroupsRequestV5 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode_flexible_request(
            5,
            self.correlation_id,
            self.client_id.as_deref(),
            &self.states_filter,
            &self.types_filter,
        )
    }
}

fn encode_flexible_request(
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    states_filter: &[String],
    types_filter: &[String],
) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    RequestHeader {
        api_key: API_KEY,
        api_version,
        correlation_id,
        client_id: client_id.map(str::to_owned),
    }
    .encode_v2(&mut encoder)?;
    encoder.write_compact_array(Some(states_filter), |encoder, state| {
        encoder.write_compact_string(state)
    })?;
    if api_version >= 5 {
        encoder.write_compact_array(Some(types_filter), |encoder, group_type| {
            encoder.write_compact_string(group_type)
        })?;
    }
    encoder.write_empty_tagged_fields();
    Ok(encoder.into_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<ListedGroupV1>,
}

impl ListGroupsResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            groups: decoder
                .read_array("listed groups", ListedGroupV1::decode)?
                .unwrap_or_default(),
        })
    }
}

/// Kafka ListGroups v4 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponseV4 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<ListedGroupV4>,
}

impl ListGroupsResponseV4 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let groups = decoder
            .read_compact_array("listed groups", ListedGroupV4::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }
}

/// Kafka ListGroups v5 response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListGroupsResponseV5 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<ListedGroupV5>,
}

impl ListGroupsResponseV5 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let groups = decoder
            .read_compact_array("listed groups", ListedGroupV5::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroupV1 {
    pub group_id: String,
    pub protocol_type: String,
}

impl ListedGroupV1 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            group_id: decoder.read_string()?,
            protocol_type: decoder.read_string()?,
        })
    }
}

/// One group returned by ListGroups v4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroupV4 {
    pub group_id: String,
    pub protocol_type: String,
    pub group_state: String,
}

impl ListedGroupV4 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let group = Self {
            group_id: decoder.read_compact_string()?,
            protocol_type: decoder.read_compact_string()?,
            group_state: decoder.read_compact_string()?,
        };
        decoder.read_tagged_fields()?;
        Ok(group)
    }
}

/// One group returned by ListGroups v5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroupV5 {
    pub group_id: String,
    pub protocol_type: String,
    pub group_state: String,
    pub group_type: String,
}

impl ListedGroupV5 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let group = Self {
            group_id: decoder.read_compact_string()?,
            protocol_type: decoder.read_compact_string()?,
            group_state: decoder.read_compact_string()?,
            group_type: decoder.read_compact_string()?,
        };
        decoder.read_tagged_fields()?;
        Ok(group)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ListGroupsRequestV1, ListGroupsRequestV4, ListGroupsRequestV5, ListGroupsResponseV1,
        ListGroupsResponseV4, ListGroupsResponseV5, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_list_groups_v1_request() {
        let request = ListGroupsRequestV1 {
            correlation_id: 17,
            client_id: Some("kafrust".to_owned()),
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 16, // API key
                0, 1, // API version
                0, 0, 0, 17, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
            ]
        );
        assert_eq!(API_KEY, 16);
    }

    #[test]
    fn decodes_list_groups_v1_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i16(0);
        bytes.write_i32(2);
        bytes.write_string("orders").unwrap();
        bytes.write_string("consumer").unwrap();
        bytes.write_string("connect-cluster").unwrap();
        bytes.write_string("connect").unwrap();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = ListGroupsResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.groups.len(), 2);
        assert_eq!(response.groups[0].group_id, "orders");
        assert_eq!(response.groups[0].protocol_type, "consumer");
        assert_eq!(response.groups[1].protocol_type, "connect");
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_list_groups_v4_request_with_state_filter() {
        let request = ListGroupsRequestV4 {
            correlation_id: 17,
            client_id: Some("kafrust".to_owned()),
            states_filter: vec!["Stable".to_owned(), "Empty".to_owned()],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 16, // API key
                0, 4, // API version
                0, 0, 0, 17, // correlation ID
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client ID
                0,    // request header tagged fields
                3,    // compact array length + 1
                7, b'S', b't', b'a', b'b', b'l', b'e', 6, b'E', b'm', b'p', b't', b'y',
                0, // request tagged fields
            ]
        );
    }

    #[test]
    fn encodes_list_groups_v5_request_with_state_and_type_filters() {
        let request = ListGroupsRequestV5 {
            correlation_id: 17,
            client_id: None,
            states_filter: vec!["Stable".to_owned()],
            types_filter: vec!["consumer".to_owned()],
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 16, // API key
                0, 5, // API version
                0, 0, 0, 17, // correlation ID
                255, 255, // nullable client ID
                0,   // request header tagged fields
                2,   // states compact array length + 1
                7, b'S', b't', b'a', b'b', b'l', b'e', 2, // types compact array length + 1
                9, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', 0, // request tagged fields
            ]
        );
    }

    #[test]
    fn decodes_list_groups_v4_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i16(0);
        bytes
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_compact_string("consumer")?;
                encoder.write_compact_string("Stable")?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = ListGroupsResponseV4::decode_body(&mut decoder).unwrap();

        assert_eq!(response.groups[0].group_state, "Stable");
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_list_groups_v5_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(4);
        bytes.write_i16(0);
        bytes
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_compact_string("consumer")?;
                encoder.write_compact_string("Stable")?;
                encoder.write_compact_string("consumer")?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = ListGroupsResponseV5::decode_body(&mut decoder).unwrap();

        assert_eq!(response.groups[0].group_type, "consumer");
        assert!(decoder.is_empty());
    }
}
