use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 66;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub state_filters: Vec<String>,
    pub producer_id_filters: Vec<i64>,
}

impl ListTransactionsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encode_filters(&mut encoder, &self.state_filters, &self.producer_id_filters)?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub state_filters: Vec<String>,
    pub producer_id_filters: Vec<i64>,
    pub duration_filter_ms: i64,
}

impl ListTransactionsRequestV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encode_filters(&mut encoder, &self.state_filters, &self.producer_id_filters)?;
        encoder.write_i64(self.duration_filter_ms);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

fn encode_filters(encoder: &mut Encoder, states: &[String], producer_ids: &[i64]) -> Result<()> {
    encoder.write_compact_array(Some(states), |encoder, state| {
        encoder.write_compact_string(state)
    })?;
    encoder.write_compact_array(Some(producer_ids), |encoder, producer_id| {
        encoder.write_i64(*producer_id);
        Ok(())
    })?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsResponseV0 {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub unknown_state_filters: Vec<String>,
    pub transaction_states: Vec<ListedTransactionV0>,
}

impl ListTransactionsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let error_code = decoder.read_i16()?;
        let unknown_state_filters = decoder
            .read_compact_array("list transactions unknown state filters", |decoder| {
                decoder.read_compact_string()
            })?
            .unwrap_or_default();
        let transaction_states = decoder
            .read_compact_array("listed transactions", ListedTransactionV0::decode)?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            unknown_state_filters,
            transaction_states,
        })
    }
}

pub type ListTransactionsResponseV1 = ListTransactionsResponseV0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedTransactionV0 {
    pub transactional_id: String,
    pub producer_id: i64,
    pub transaction_state: String,
}

impl ListedTransactionV0 {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        let result = Self {
            transactional_id: decoder.read_compact_string()?,
            producer_id: decoder.read_i64()?,
            transaction_state: decoder.read_compact_string()?,
        };
        decoder.read_tagged_fields()?;
        Ok(result)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        ListTransactionsRequestV0, ListTransactionsRequestV1, ListTransactionsResponseV0, API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_list_transactions_v0_request() {
        let request = ListTransactionsRequestV0 {
            correlation_id: 66,
            client_id: Some("kafrust".to_owned()),
            state_filters: vec!["Ongoing".to_owned()],
            producer_id_filters: vec![42],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 66]);
        assert!(bytes.ends_with(&[0]));
    }

    #[test]
    fn encodes_list_transactions_v1_duration_filter() {
        let request = ListTransactionsRequestV1 {
            correlation_id: 67,
            client_id: None,
            state_filters: Vec::new(),
            producer_id_filters: Vec::new(),
            duration_filter_ms: 30_000,
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 1]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 67]);
        assert_eq!(&bytes[8..10], &[255, 255]);
        assert_eq!(bytes[10], 0); // request-header tagged fields
        assert_eq!(bytes[11], 1); // empty state filter array
        assert_eq!(bytes[12], 1); // empty producer ID filter array
        assert_eq!(&bytes[13..21], &30_000_i64.to_be_bytes());
        assert!(bytes.ends_with(&[0]));
    }

    #[test]
    fn decodes_list_transactions_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(8);
        bytes.write_i16(0);
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("UnknownState").unwrap();
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("payments-tx").unwrap();
        bytes.write_i64(42);
        bytes.write_compact_string("Ongoing").unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = ListTransactionsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 8);
        assert_eq!(response.unknown_state_filters, ["UnknownState"]);
        assert_eq!(
            response.transaction_states[0].transactional_id,
            "payments-tx"
        );
        assert_eq!(response.transaction_states[0].producer_id, 42);
        assert_eq!(response.transaction_states[0].transaction_state, "Ongoing");
        assert!(decoder.is_empty());
    }
}
