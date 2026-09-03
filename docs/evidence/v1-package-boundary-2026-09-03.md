# 0.3.6 staged package boundary (2026-09-03)

## Scope

At source commit `b4505903e9b15b3e7452b9e6f8e9cbf3f6ea679b`,
`scripts/verify_package_boundary.py --staged` created the 0.3.6 package pair
from the checkout and checked consumer fixtures against the staged tarballs.
The check is package-boundary evidence; it does not publish either crate.

## Artifacts

```text
kafrust-protocol-0.3.6.crate
sha256=74e7db2905e88fd41b8ebb10669a4f712ba0459fade2a288fb8834ed6a0415bb

kafrust-0.3.6.crate
sha256=890c585914d2e44af2d72cd10da698eea5a732f778854f280ff818193dac859e
```

## Verification

The temporary consumer fixtures all passed `cargo check --locked` and a
dependency-tree inspection for these feature profiles:

- default
- tls
- blocking
- otlp
- all

The protocol package was verified from its staged tarball before the client
package was built with the staged protocol dependency. The check did not
create or modify Docker resources, and the repository remained clean.

## Boundary

This establishes a packaged-candidate boundary for the 0.3.6 source checkout.
It is not a crates.io publication, live broker compatibility matrix,
multi-platform qualification, long campaign, service canary, or release
authorization.
