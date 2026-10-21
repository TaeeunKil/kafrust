# Rust 1.81 to 1.87 Migration

The published `0.5.0` kafrust release requires Rust 1.87 or newer. The prior
`0.4.0` release supports Rust 1.81. The minimum changed because the patched
Zstandard decoder requires the newer compiler.

## Why the minimum changed

The protocol crate directly uses `ruzstd` to decompress Kafka Zstandard record
batches. `ruzstd 0.8.1` is affected by
[RUSTSEC-2026-0344](https://github.com/RustSec/advisory-db/blob/main/crates/ruzstd/RUSTSEC-2026-0344.md):
a crafted, small compressed frame can make the decoder allocate several
gigabytes before kafrust's outer decompressed-output limit can reject the
result. `ruzstd 0.9.1` includes the upstream fix and declares Rust 1.87 as its
minimum supported compiler.

kafrust `0.5.0` uses that released fix rather than
maintaining a local fork of the decompressor. The published `kafrust 0.4.0`
and `kafrust-protocol 0.4.0` artifacts are unchanged and do not contain this
fix. Upgrade to `0.5.0` before processing untrusted Zstandard records.

## What to change

Install Rust 1.87 or newer, then rebuild your application. No Kafka client API
or configuration migration is required for this compiler upgrade.

```sh
rustup toolchain install 1.87.0
cargo +1.87.0 check
```

If your project must remain on Rust 1.81, pinning Tokio below 1.54 may avoid
the separate compiler-resolution failure, but it does not fix the Zstandard
vulnerability. There is no safe Rust 1.81 workaround for decoding untrusted
Zstandard records with `ruzstd 0.8.1`. Upgrade the compiler and use the
`kafrust 0.5.0` release, which contains the `ruzstd 0.9.1` fix, before
processing untrusted compressed Kafka data.
