# V1 classic rebalance callback ordering (2026-09-07)

## Scope

This record covers the deterministic callback lifecycle for a classic
`ConsumerGroup` rejoin. It is local scripted-broker evidence only; it does not
qualify published artifacts, secured broker profiles, long campaigns, or the
V1 release.

## Defect and correction

Source commit `42169a3` found and fixed a duplicate `After` notification during
classic and KIP-848 rejoin. The internal join helpers already constructed a
fully joined group and notified the listener. `ConsumerGroup::rejoin` then
notified `After` a second time after restoring assignment state. The helpers
now accept an explicit notification flag: initial joins notify from the helper,
while rejoin helpers remain silent and `rejoin` emits the single final callback
after assignment, paused state, and commit-worker membership have been
restored.

## Deterministic regression

`consumer_group_restores_assignment_and_fetches_after_rejoin` uses scripted
coordinator and leader sockets. It records the real public listener callbacks
and asserts the exact sequence:

```text
After generation 1: one assignment
Before generation 1: one assignment
After generation 2: one assignment
```

The same test then fetches offset `42`, advances the position to `43`, and
confirms the rejoin completed after coordinator loss. The prior implementation
failed because it produced a fourth event (`After generation 2`) immediately
after the expected final callback.

The KIP-848 counterpart
`consumer_protocol_rejoins_and_fetches_after_rebalance_error` records the same
three-event sequence while recovering the member epoch and fetching offset
`42`, proving the helper flag is symmetric across both group protocols.

## Verification

- `cargo fmt --all`
- `cargo test -p kafrust --test fault_injection consumer_group_ -- --nocapture` — 4 passed
- `cargo test -p kafrust --test fault_injection consumer_protocol_rejoins_and_fetches_after_rebalance_error -- --exact --nocapture` — 1 passed
- `cargo test -p kafrust --lib group::tests -- --nocapture` — 73 passed
- Source commit: `42169a3`

## Boundary

This closes the deterministic duplicate-callback boundary for the exercised
classic coordinator-loss/rejoin path. Callback panic policy, background
heartbeat matrices, published security profiles, 100-cycle qualification, and
long-campaign evidence remain separate V1-08 gates.
