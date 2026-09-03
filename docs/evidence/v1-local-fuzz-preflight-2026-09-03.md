# Local fuzz preflight (2026-09-03)

## Scope

The source checkout at `fb1abf7bafd1ea6d82013070d1a9c4c26c1b98ef` was checked
for a bounded local fuzz smoke on the company computer. No fuzz campaign credit
is claimed from this preflight.

## Results

The Windows nightly target build compiled the fuzz crates but could not link
the libFuzzer binaries with MSVC: `link.exe` returned `LNK1561` (entry point
must be defined). This is a runner/toolchain limitation, not a Rust protocol
test result.

Ubuntu-T9 WSL2 has only the pinned Rust 1.81.0 toolchain. A `cargo +nightly`
preflight attempted to refresh the missing Linux nightly toolchain but failed
before compilation because `static.rust-lang.org` DNS resolution was
temporarily unavailable. `cargo-fuzz` was not reached or installed.

No fuzz target executed, no corpus was changed, and no crash/OOM disposition
was produced. The two retained GitHub qualification sets remain the only
campaign evidence; two weekly sets and the resource-limit gates remain open.
