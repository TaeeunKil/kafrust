use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 65;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTransactionsRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_ids: Vec<String>,
}

impl DescribeTransactionsRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_array(Some(&self.transactional_ids), |encoder, id| {
            encoder.write_compact_string(id)
        })?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTransactionsResponseV0 {
    pub throttle_time_ms: i32,
    pub transaction_states: Vec<DescribeTransactionsStateV0>,
}

impl DescribeTransactionsResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let throttle_time_ms = decoder.read_i32()?;
        let transaction_states = decoder
            .read_compact_array("describe transactions states", |decoder| {
                let error_code = decoder.read_i16()?;
                let transactional_id = decoder.read_compact_string()?;
                let transaction_state = decoder.read_compact_string()?;
                let transaction_timeout_ms = decoder.read_i32()?;
                let transaction_start_time_ms = decoder.read_i64()?;
                let producer_id = decoder.read_i64()?;
                let producer_epoch = decoder.read_i16()?;
                let topics = decoder
                    .read_compact_array("describe transaction topics", |decoder| {
                        let topic = decoder.read_compact_string()?;
                        let partitions = decoder
                            .read_array("describe transaction partitions", |decoder| {
                                decoder.read_i32()
                            })?
                            .unwrap_or_default();
                        decoder.read_tagged_fields()?;
                        Ok(DescribeTransactionsTopicV0 { topic, partitions })
                    })?
                    .unwrap_or_default();
                decoder.read_tagged_fields()?;
                Ok(DescribeTransactionsStateV0 {
                    error_code,
                    transactional_id,
                    transaction_state,
                    transaction_timeout_ms,
                    transaction_start_time_ms,
                    producer_id,
                    producer_epoch,
                    topics,
                })
            })?
            .unwrap_or_default();
        decoder.read_tagged_fields()?;
        Ok(Self {
            throttle_time_ms,
            transaction_states,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTransactionsStateV0 {
    pub error_code: i16,
    pub transactional_id: String,
    pub transaction_state: String,
    pub transaction_timeout_ms: i32,
    pub transaction_start_time_ms: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<DescribeTransactionsTopicV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTransactionsTopicV0 {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DescribeTransactionsRequestV0, DescribeTransactionsResponseV0, API_KEY};
    use crate::codec::{Decoder, Encoder};

    #[test]
    fn encodes_describe_transactions_v0_request() {
        let request = DescribeTransactionsRequestV0 {
            correlation_id: 65,
            client_id: None,
            transactional_ids: vec!["payments-tx".to_owned()],
        };

        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[0..4], &[0, API_KEY as u8, 0, 0]);
        assert_eq!(&bytes[4..8], &[0, 0, 0, 65]);
        assert_eq!(bytes.last(), Some(&0));
    }

    #[test]
    fn decodes_describe_transactions_v0_response() {
        let mut bytes = Encoder::new();
        bytes.write_i32(7);
        bytes.write_unsigned_varint(3);
        bytes.write_i16(0);
        bytes.write_compact_string("payments-tx").unwrap();
        bytes.write_compact_string("Ongoing").unwrap();
        bytes.write_i32(60_000);
        bytes.write_i64(1_700_000_000_000);
        bytes.write_i64(99);
        bytes.write_i16(4);
        bytes.write_unsigned_varint(2);
        bytes.write_compact_string("orders").unwrap();
        bytes
            .write_array(Some(&[0, 2]), |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            })
            .unwrap();
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        bytes.write_i16(15);
        bytes.write_compact_string("missing-tx").unwrap();
        bytes.write_compact_string("Empty").unwrap();
        bytes.write_i32(60_000);
        bytes.write_i64(-1);
        bytes.write_i64(-1);
        bytes.write_i16(-1);
        bytes.write_unsigned_varint(1);
        bytes.write_empty_tagged_fields();
        bytes.write_empty_tagged_fields();
        let bytes = bytes.into_bytes();
        let mut decoder = Decoder::new(&bytes);

        let response = DescribeTransactionsResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(
            response.transaction_states[0].transactional_id,
            "payments-tx"
        );
        assert_eq!(response.transaction_states[0].producer_id, 99);
        assert_eq!(response.transaction_states[0].topics[0].partitions, [0, 2]);
        assert_eq!(response.transaction_states[1].error_code, 15);
        assert!(decoder.is_empty());
    }
}
