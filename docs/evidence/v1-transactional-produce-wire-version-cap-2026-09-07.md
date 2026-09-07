# Transactional Produce wire-version cap (2026-09-07)

## Scope

Source commit `652afb1d8aa81f5db768e43e15f9f6b4a6da244f` adds a scripted-broker
regression for the direct transactional `Producer::send` path. The broker
advertises Produce maximum version 13, while the producer is in an active
legacy TV0/TV1 transaction with a registered partition. The observed request
header is Produce API v11, proving that the wire path does not silently switch
to the TV2-only v12/v13 shapes.

## Verification

```text
cargo fmt --all
cargo test -p kafrust transactional_send_caps_produce_version_when_tv2_is_advertised --all-features -- --nocapture
1 passed; 0 failed
git diff --check
```

The scripted broker withholds the Produce response after observing the v11
frame. The canceled operation consequently makes the transaction defunct,
which also exercises the existing post-transmission safety boundary. No
external broker, Docker resource, or published artifact was used.

## Boundary

This closes the direct request-path proof for the legacy transactional Produce
cap. It does not qualify the Kafka `transaction.version=2` state machine,
read-committed reconciliation, accepted-floor or pinned-current live profiles,
three-broker movement, long campaigns, published artifacts, or release
authorization. The complete V1-06 transaction and V1-03 live exit criteria
remain open.
