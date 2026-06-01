#![doc = include_str!("../../../README.md")]
#![forbid(unsafe_code)]

pub mod client;
pub mod config;
pub mod consumer;
pub mod error;
pub mod producer;

pub use client::Client;
pub use config::ClientConfig;
pub use consumer::{Consumer, ConsumerConfig, ConsumerRecord};
pub use error::{Error, Result};
pub use kafrust_protocol as protocol;
pub use producer::{Acks, Header, ProducerConfig, ProducerRecord, RecordMetadata};

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
