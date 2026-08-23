# Published `0.3.6` Boundary Evidence

- date_utc: 2026-08-23
- source_commit: `8feeb1a0c8a6f1356ee2ad3bd5e375666cd0e6d0`
- candidate: `kafrust 0.3.6` and `kafrust-protocol 0.3.6`
- protocol_published_at: `2026-08-23T02:42:56.878585Z`
- client_published_at: `2026-08-23T02:45:02.515315Z`
- protocol_checksum: `731e80f6e2588f6c3c460896d0521c6582bae51c42a386ac05f26ec37e1279bd`
- client_checksum: `4fe2758d0093ef4b2a236090cca4dc7511b9e865f5e18ce42823e428a6be71d2`
- registry_api: [protocol](https://crates.io/api/v1/crates/kafrust-protocol/0.3.6), [client](https://crates.io/api/v1/crates/kafrust/0.3.6)

The protocol was uploaded first. After crates.io exposed the exact protocol
version, `cargo publish --dry-run -p kafrust` downloaded
`kafrust-protocol = 0.3.6` from the registry and passed package verification;
the client was then published. A fresh external project with an exact
`kafrust = "=0.3.6"` dependency generated a lockfile containing both
`kafrust 0.3.6` and `kafrust-protocol 0.3.6` with no path or patch source. Its
`cargo check --locked` passed on stable Rust and Rust 1.81.0.

At the time of this record both docs.rs pages returned HTTP 404 while indexing:
docs.rs availability is pending and is not being claimed as passed. The full
V1-20 broker/security/workload matrix is also still open; this evidence closes
only the ordered published-package boundary slice and does not authorize
`1.0.0`.
