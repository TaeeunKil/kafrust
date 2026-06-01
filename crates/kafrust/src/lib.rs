#![doc = include_str!("../../../README.md")]
#![forbid(unsafe_code)]

pub mod client;
pub mod config;
pub mod error;

pub use client::Client;
pub use config::ClientConfig;
pub use error::{Error, Result};
pub use kafrust_protocol as protocol;

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
