# Producer delivery phase expiry (2026-09-04)

## Scope

At source commit `eeb675e`, immediate and batch producer delivery deadlines
track the operation phase through metadata lookup, broker capability
negotiation, retries, and Produce. A deadline that expires before Produce now
reports the actual `DeliveryPhase` and `possibly_transmitted=false`; a prior
Produce attempt remains represented by a separate transmission flag if a
later retry exhausts the same total budget.

## Verification

```text
cargo test -p kafrust --test fault_injection
33 passed; 0 failed; finished in 0.14s

wsl.exe -d Ubuntu-T9 -e bash -lc \
  'cd "/mnt/c/Users/user/Documents/New project 4" && \
   cargo test -p kafrust --test fault_injection'
33 passed; 0 failed; finished in 0.61s on Rust 1.81.0 x86_64

cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

The four new regressions cover both immediate `send` and `send_batch`:

- metadata response withheld: `DeliveryPhase::Metadata`, no Produce frame;
- broker `ApiVersions` response withheld: `DeliveryPhase::Capability`, no
  Produce frame.

Each case returns the typed `DeliveryDeadlineExceeded` error with
`possibly_transmitted=false` and a 20 ms total budget. The scripted broker
observed one Metadata request in the metadata cases, and one Metadata plus one
ApiVersions request in the capability cases; it observed zero Produce
requests in all four cases. The company WSL2 replay produced the same 33-test
result using the x86_64 Rust 1.81 toolchain.

## Boundary

This closes deterministic pre-Produce metadata/capability phase classification
for immediate and batch sends. It does not qualify long campaigns, published
mixed-outcome reconciliation, accepted-floor or multi-broker coverage,
service canaries, or release authorization.
