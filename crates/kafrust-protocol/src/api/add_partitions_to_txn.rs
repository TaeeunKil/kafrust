use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

pub const API_KEY: i16 = 24;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

impl AddPartitionsToTxnRequestV0 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v1(&mut encoder)?;
        encoder.write_string(&self.transactional_id)?;
        encoder.write_i64(self.producer_id);
        encoder.write_i16(self.producer_epoch);
        encoder.write_array(Some(&self.topics), |encoder, topic| topic.encode(encoder))?;
        Ok(encoder.into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

impl AddPartitionsToTxnTopic {
    fn encode(&self, encoder: &mut Encoder) -> Result<()> {
        encoder.write_string(&self.name)?;
        encoder.write_array(Some(&self.partitions), |encoder, partition| {
            encoder.write_i32(*partition);
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnResponseV0 {
    pub throttle_time_ms: i32,
    pub errors: Vec<AddPartitionsToTxnTopicResult>,
}

impl AddPartitionsToTxnResponseV0 {
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: decoder.read_i32()?,
            errors: decoder
                .read_array(
                    "add partitions to transaction topic results",
                    AddPartitionsToTxnTopicResult::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnTopicResult {
    pub name: String,
    pub partitions: Vec<AddPartitionsToTxnPartitionResult>,
}

impl AddPartitionsToTxnTopicResult {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            name: decoder.read_string()?,
            partitions: decoder
                .read_array(
                    "add partitions to transaction partition results",
                    AddPartitionsToTxnPartitionResult::decode,
                )?
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnPartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
}

impl AddPartitionsToTxnPartitionResult {
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self {
            partition_index: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AddPartitionsToTxnRequestV0, AddPartitionsToTxnResponseV0, AddPartitionsToTxnTopic, API_KEY,
    };
    use crate::codec::Decoder;

    #[test]
    fn encodes_add_partitions_to_txn_v0_request() {
        let request = AddPartitionsToTxnRequestV0 {
            correlation_id: 41,
            client_id: Some("kafrust".to_owned()),
            transactional_id: "orders-tx".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            topics: vec![AddPartitionsToTxnTopic {
                name: "orders".to_owned(),
                partitions: vec![0, 2],
            }],
        };
        let encoded = request.encode().unwrap();

        assert_eq!(&encoded[0..8], &[0, 24, 0, 0, 0, 0, 0, 41]);
        assert_eq!(
            &encoded[encoded.len() - 12..],
            &[0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2]
        );
        assert_eq!(API_KEY, 24);
    }

    #[test]
    fn decodes_add_partitions_to_txn_v0_response() {
        let bytes = [
            0, 0, 0, 7, // throttle time
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, 0, 2, // partition count
            0, 0, 0, 0, 0, 0, // partition 0, success
            0, 0, 0, 2, 0, 47, // partition 2, invalid producer epoch
        ];
        let mut decoder = Decoder::new(&bytes);
        let response = AddPartitionsToTxnResponseV0::decode_body(&mut decoder).unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(response.errors[0].name, "orders");
        assert_eq!(response.errors[0].partitions[0].error_code, 0);
        assert_eq!(response.errors[0].partitions[1].partition_index, 2);
        assert_eq!(response.errors[0].partitions[1].error_code, 47);
        assert!(decoder.is_empty());
    }
}
