# V1-20 Published Smoke Readiness Rerun (2026-08-23)

Run [32627054021](https://github.com/TaeeunKil/kafrust/actions/runs/32627054021)
validated the bounded topic/group metadata retry in source
`af3dbfc04deb00873c7cd5b1cdbc16ac4c1444a2`. The exact published
`kafrust 0.3.6` / `kafrust-protocol 0.3.6` pair ran in twelve fresh external
Cargo projects against the published-crate-smoke matrix.

All twelve jobs passed, including Kafka 3.9.1 classic, which had exposed the
previous KRaft metadata propagation flake. The run uploaded 24 retained files:
12 exact external `Cargo.lock` files and 12 captured fixture outputs, for
286,576 bytes total and 90-day retention. The retry is bounded at ten seconds;
if topic or active-group metadata remains invisible, the fixture still fails.

This is published-artifact smoke and fixture-readiness evidence only. It does
not provide the full V1-20 matrix, latency/RSS/retry SLOs, long fault campaign,
service canary, API freeze, RC, or stable-release evidence.
