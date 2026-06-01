use std::time::SystemTime;

use crate::config::ClientConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    None,
    Leader,
    All,
}

impl Acks {
    pub fn as_i16(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    key: String,
    value: Vec<u8>,
}

impl Header {
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerRecord {
    topic: String,
    partition: Option<i32>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<Header>,
    timestamp: Option<SystemTime>,
}

impl ProducerRecord {
    pub fn to(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: None,
            headers: Vec::new(),
            timestamp: None,
        }
    }

    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn value(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.value = Some(value.into());
        self
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push(Header::new(key, value));
        self
    }

    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition_ref(&self) -> Option<i32> {
        self.partition
    }

    pub fn key_ref(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn value_ref(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    pub fn timestamp_ref(&self) -> Option<SystemTime> {
        self.timestamp
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: Option<SystemTime>,
}

impl RecordMetadata {
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        timestamp: Option<SystemTime>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            timestamp,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn timestamp(&self) -> Option<SystemTime> {
        self.timestamp
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerConfig {
    client: ClientConfig,
    acks: Acks,
}

impl ProducerConfig {
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            acks: Acks::Leader,
        }
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    pub fn acks_ref(&self) -> Acks {
        self.acks
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{Acks, ProducerConfig, ProducerRecord, RecordMetadata};

    #[test]
    fn maps_acks_to_kafka_values() {
        assert_eq!(Acks::None.as_i16(), 0);
        assert_eq!(Acks::Leader.as_i16(), 1);
        assert_eq!(Acks::All.as_i16(), -1);
    }

    #[test]
    fn builds_producer_record_with_kafka_concepts() {
        let record = ProducerRecord::to("orders")
            .partition(2)
            .key("order-123")
            .value("created")
            .header("source", "checkout");

        assert_eq!(record.topic(), "orders");
        assert_eq!(record.partition_ref(), Some(2));
        assert_eq!(record.key_ref().unwrap(), b"order-123");
        assert_eq!(record.value_ref().unwrap(), b"created");
        assert_eq!(record.headers()[0].key(), "source");
        assert_eq!(record.headers()[0].value(), b"checkout");
    }

    #[test]
    fn builds_producer_config() {
        let config = ProducerConfig::new(["localhost:9092"])
            .client_id("orders-api")
            .acks(Acks::All);

        assert_eq!(config.acks_ref(), Acks::All);
        assert_eq!(config.client_config().client_id_ref(), Some("orders-api"));
    }

    #[test]
    fn exposes_record_metadata() {
        let metadata = RecordMetadata::new("orders", 1, 42, None);

        assert_eq!(metadata.topic(), "orders");
        assert_eq!(metadata.partition(), 1);
        assert_eq!(metadata.offset(), 42);
        assert_eq!(metadata.timestamp(), None);
    }
}
