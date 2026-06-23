#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub mod api;
pub mod codec;
pub mod consumer_group;
pub mod error;
pub mod frame;
pub mod header;
pub mod record_batch;

pub use error::{Error, Result};

/// Returns the protocol crate version compiled into this build.
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
