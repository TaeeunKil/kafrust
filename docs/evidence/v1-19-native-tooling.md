# V1-19 Native-Tooling Evidence

- date_utc: 2026-08-23
- source_commit: `83864c1058347dd753608307bdd5ab1d7eb68be3`
- checker: `scripts/check_v1_native_tooling.py`
- platform: `x86_64-unknown-linux-gnu`
- report_sha256: `d6b8c46bab42422369fa819be3b8337c87a807d001480db1b9804946a71ea122`

## Results

The checker audits the default, `tls`, `blocking`, `otlp`, and all-feature
normal dependency trees against Cargo metadata for the same target. The
resolved package counts were 55, 66, 55, 71, and 82 respectively. The default,
`blocking`, and `otlp` profiles reported no native-tooling candidates. The
`tls` and all-feature profiles reported `ring` with a custom build script and
`ring_core_0_17_14_` link name.

The default package was also checked with `CC`, `CXX`, `AR`, and `PKG_CONFIG`
set to nonexistent tools:

```text
cargo check -p kafrust --no-default-features --lib
```

That no-C default build passed. This supports the default pure-Rust build
promise; it does not claim that optional TLS needs no native compiler or that
transitive dependencies contain no unsafe code.
