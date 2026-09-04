# Published self-hosted foreground-lifetime smoke (2026-09-04)

## Scope

This record covers one bounded, non-qualification diagnostic used to verify
that the Ubuntu-T9 WSL runner remains alive when a foreground WSL process holds
the distribution open for the duration of the job. It is not a V1-21 six-hour
campaign, a V1-22 performance sample, or release evidence.

## Immutable run identity

- workflow: [Published Multi-Broker Soak Smoke run 33825722908](https://github.com/TaeeunKil/kafrust/actions/runs/33825722908)
- source commit: `17a9f8a15da22053934b9a71c82d64051c8cbdd7`
- published client: `kafrust 0.3.6`
- broker: `apache/kafka:4.3.1`
- broker image digest: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- runner: `wsl-ubuntu-t9`, Ubuntu-T9 WSL2, Linux x86_64, labels `self-hosted,Linux,X64,docker,wsl2`
- campaign identity: `foreground-lifetime-smoke-2026-09-04`, segment `0/1`
- fault mode: single-broker restart with leader, coordinator, and combined events
- fault schedule: `leader@25,coordinator@50,combined@75`
- configured soak/outage: 120 seconds / 10 seconds
- workflow wall time: 3 minutes 33 seconds (`2026-09-04T01:25:41Z` to `2026-09-04T01:29:14Z`)
- uploaded artifact: `kafrust-published-fault-segment-foreground-lifetime-smoke-2026-09-04-0-33825722908` (artifact id `9919934750`)

The operator held a foreground WSL process for the bounded run. This is a
diagnostic lifetime guard only; it is not a permanent service or unattended
runner guarantee.

## Result

The runner selection, checkout, capacity guard, Rust installation, three-broker
startup, and published external build all passed. The soak ran for 120.663
seconds and completed its three scheduled fault events:

- attempted/acknowledged/consumed: `3,624,900` / `3,624,900` / `3,624,900`
- payload: 1 KiB; partitions: 3
- operation errors: `1`; failed requests: `17`; retries: `31`
- recovery: `true`; unknown outcomes: `0`
- loss/duplicates: `0` / `0`
- reconciliation digest:
  `2057f20dc345f24d335fd94282612a582f5c4b874a6b67c2d691a2347f7f7c6c`
- final in-flight/buffered gauges: `0` / `0`
- maximum in-flight/buffered gauges: `1` / `0`
- secret scan count: `0`

The immutable descriptor records `continuity_claim: qualified` for this
single bounded segment and has Cargo.lock digest
`2f81a33ed05baf0321bb7a643355bec49a5e3a8904d7cd2354e54a86314f976b`.
Workflow cleanup completed successfully and was prefix-scoped; existing Docker
containers, networks, and volumes were not pruned or modified.

## Interpretation and non-claims

Keeping a foreground WSL process alive prevented the service interruption seen
in [run 33824960369](https://github.com/TaeeunKil/kafrust/actions/runs/33824960369),
so the short published path is reproducible under an explicit lifetime guard.
This does not identify or fix the Windows-side WSL shutdown initiator, prove
resolver persistence across a full restart, or establish unattended lifetime
stability. It does not count toward V1-21's six-hour campaigns, 100-cycle or
ambiguity families, V1-22's five-repetition eight-hour SLO, a service canary,
or any `0.3.7`/`1.0.0` release decision.
