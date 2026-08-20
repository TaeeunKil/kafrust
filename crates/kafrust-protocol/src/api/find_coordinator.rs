use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorType {
    Group,
    Transaction,
    Share,
}

impl CoordinatorType {
    fn as_i8(self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
            Self::Share => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub coordinator_key: String,
    pub coordinator_type: CoordinatorType,
}

impl FindCoordinatorRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.coordinator_key)?;
        encoder.write_i8(self.coordinator_type.as_i8());
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponseV1 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

/// FindCoordinator v6 request for share-partition coordinators.
///
/// Kafka 4.x uses this flexible request for the KIP-932 share coordinator
/// lookup. Each key is a `group:topic-id:partition` resource identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorRequestV6 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub coordinator_type: CoordinatorType,
    pub coordinator_keys: Vec<String>,
}

impl FindCoordinatorRequestV6 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 6,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_i8(self.coordinator_type.as_i8());
        encoder.write_compact_array(Some(&self.coordinator_keys), |encoder, key| {
            encoder.write_compact_string(key)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// One coordinator result returned by FindCoordinator v6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResultV6 {
    pub coordinator_key: String,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

/// FindCoordinator v6 response for one or more coordinator keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponseV6 {
    pub throttle_time_ms: i32,
    pub coordinators: Vec<FindCoordinatorResultV6>,
}

impl FindCoordinatorResponseV6 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let coordinators = decoder
            .read_compact_array("find coordinator results", |decoder| {
                let result = FindCoordinatorResultV6 {
                    coordinator_key: decoder.read_compact_string()?,
                    node_id: decoder.read_i32()?,
                    host: decoder.read_compact_string()?,
                    port: decoder.read_i32()?,
                    error_code: decoder.read_i16()?,
                    error_message: decoder.read_compact_nullable_string()?,
                };
                decoder.read_tagged_fields()?;
                Ok(result)
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            coordinators,
        })
    }
}

impl FindCoordinatorResponseV1 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_nullable_string()?,
            node_id: decoder.read_i32()?,
            host: decoder.read_string()?,
            port: decoder.read_i32()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        CoordinatorType, FindCoordinatorRequestV1, FindCoordinatorRequestV6,
        FindCoordinatorResponseV1, FindCoordinatorResponseV6, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_find_coordinator_v1_for_group() {
        let request = FindCoordinatorRequestV1 {
            correlation_id: 11,
            client_id: Some("kafrust".to_owned()),
            coordinator_key: "orders-group".to_owned(),
            coordinator_type: CoordinatorType::Group,
        };

        assert_eq!(
            request.encode().unwrap(),
            [
                0, 10, // api key
                0, 1, // api version
                0, 0, 0, 11, // correlation id
                0, 7, b'k', b'a', b'f', b'r', b'u', b's', b't', // client id
                0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u',
                b'p', // coordinator key
                0,    // group coordinator type
            ]
        );
        assert_eq!(API_KEY, 10);
    }

    #[test]
    fn encodes_find_coordinator_v1_for_transaction() {
        let request = FindCoordinatorRequestV1 {
            correlation_id: 12,
            client_id: None,
            coordinator_key: "orders-tx".to_owned(),
            coordinator_type: CoordinatorType::Transaction,
        };

        assert_eq!(request.encode().unwrap().last(), Some(&1));
    }

    #[test]
    fn encodes_find_coordinator_v1_for_share_group() {
        let request = FindCoordinatorRequestV1 {
            correlation_id: 13,
            client_id: Some("kafrust".to_owned()),
            coordinator_key: "share-group".to_owned(),
            coordinator_type: CoordinatorType::Share,
        };

        assert_eq!(request.encode().unwrap().last(), Some(&2));
    }

    #[test]
    fn encodes_find_coordinator_v6_for_share_partition() {
        let request = FindCoordinatorRequestV6 {
            correlation_id: 14,
            client_id: Some("kafrust".to_owned()),
            coordinator_type: CoordinatorType::Share,
            coordinator_keys: vec!["share-orders:00000000-0000-0000-0000-000000000007:0".to_owned()],
        };

        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[0..4], &[0, 10, 0, 6]);
        assert_eq!(encoded[17], 0); // request header tagged fields
        assert_eq!(encoded[18], 2); // Share coordinator type
        assert_eq!(encoded.last(), Some(&0)); // request body tagged fields
    }

    #[test]
    fn decodes_find_coordinator_v1_response() {
        let bytes = [
            0, 0, 0, 0, // throttle time
            0, 0, // error code
            0xff, 0xff, // null error message
            0, 0, 0, 2, // node id
            0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // host
            0, 0, 35, 132, // port 9092
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = FindCoordinatorResponseV1::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message, None);
        assert_eq!(response.node_id, 2);
        assert_eq!(response.host, "localhost");
        assert_eq!(response.port, 9092);
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_find_coordinator_v6_response() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0_i32.to_be_bytes());
        bytes.push(2); // one coordinator
        bytes.push(7); // compact string length + 1
        bytes.extend_from_slice(b"orders");
        bytes.extend_from_slice(&3_i32.to_be_bytes());
        bytes.push(7); // compact string length + 1
        bytes.extend_from_slice(b"broker");
        bytes.extend_from_slice(&9092_i32.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.push(0); // null compact error message
        bytes.push(0); // coordinator tagged fields
        bytes.push(0); // response tagged fields

        let mut decoder = Decoder::new(&bytes);
        let response = FindCoordinatorResponseV6::decode_body(&mut decoder).unwrap();
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.coordinators.len(), 1);
        assert_eq!(response.coordinators[0].coordinator_key, "orders");
        assert_eq!(response.coordinators[0].node_id, 3);
        assert_eq!(response.coordinators[0].host, "broker");
        assert_eq!(response.coordinators[0].port, 9092);
        assert_eq!(response.coordinators[0].error_code, 0);
        assert_eq!(response.coordinators[0].error_message, None);
        assert!(decoder.is_empty());
    }
}
