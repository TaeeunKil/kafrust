#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// Low-level Kafka request client.
pub mod client;
/// Shared connection configuration.
pub mod config;
/// Direct topic/partition consumer API.
pub mod consumer;
/// Error and result types.
pub mod error;
/// Classic consumer group alpha API.
pub mod group;
/// Producer API.
pub mod producer;

mod scram;

pub use client::Client;
pub use config::{ClientConfig, SaslCredentials, SaslMechanism, SecurityProtocol};
pub use consumer::{Consumer, ConsumerAssignment, ConsumerConfig, ConsumerRecord};
pub use error::{BrokerErrorKind, Error, Result};
pub use group::{ConsumerGroup, ConsumerGroupConfig, ConsumerGroupHeartbeat};
pub use kafrust_protocol as protocol;
pub use producer::{
    Acks, BufferedProducer, Compression, Header, ProducerBatchFailure, ProducerBatchRecordOutcome,
    ProducerBatchReport, ProducerConfig, ProducerDelivery, ProducerRecord, RecordMetadata,
};

/// Returns the crate version compiled into this build.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_crate_version() {
        assert_eq!(crate::version(), env!("CARGO_PKG_VERSION"));
    }
}
