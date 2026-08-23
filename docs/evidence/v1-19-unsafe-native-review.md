# V1-19 Unsafe/Native Owner Review

- date_utc: 2026-08-23
- scope: 62 entries from the all-feature x86_64-unknown-linux-gnu inventory
- checker: [`scripts/check_v1_unsafe_native_review.py`](../../scripts/check_v1_unsafe_native_review.py)
- inventory: [`v1-19-unsafe-native-inventory.json`](v1-19-unsafe-native-inventory.json)
- review_matrix: [`v1-19-unsafe-native-review.json`](v1-19-unsafe-native-review.json)
- review_matrix_sha256: `64aabcb52b79f58668068f3562b720f86ad71acda6bfe06a1e42f2cfc8ae470c`

The review matrix covers every inventory identity with an owner, a boundary
classification, a re-review action, and a candidate-only risk disposition. It
contains 46 upstream unsafe implementation entries, 10 build/code-generation
entries, and six named native/platform boundaries: `getrandom`, `libc`, `mio`,
`ring`, `rustls-platform-verifier`, and `socket2`.

The six native/platform paths were manually traced with the all-feature Cargo
tree. `ring` is reachable through optional TLS/rustls;
`rustls-platform-verifier` is the explicit platform certificate-verifier path;
`getrandom` is used by rand and ring; and `libc`, `mio`, and `socket2` are
Tokio/OS networking paths. The default no-C check remains passing, and
optional TLS/ring tooling remains documented rather than hidden.

This review accepts the current boundaries for the `0.3.6` pre-1.0 candidate
only. It is not a source audit of every upstream unsafe block, a vulnerability
or maintainer-trust guarantee, or final `1.0.0` risk acceptance. Any package,
target, feature, advisory, or dependency change requires the inventory and this
matrix to be rerun before release.
