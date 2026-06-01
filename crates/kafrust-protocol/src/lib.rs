#![forbid(unsafe_code)]

//! Kafka wire protocol support for kafrust.
//!
//! This crate is intentionally runtime-free. It owns Kafka request/response
//! encoding and decoding so the public client can stay focused on user-facing
//! producer and consumer behavior.

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
