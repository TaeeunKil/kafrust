# Company workstation consumer-controls short smoke (2026-09-04)

## Scope

- source commit: `a981a35a32db0ca61b5aa1a391a58a0ccf9f184c`
- host: company Windows x64 workstation
- runtime: Ubuntu-T9 WSL2, Linux `x86_64`, Rust 1.81.0
- Docker: 29.5.3
- broker: Kafka 4.3.1 single-node KRaft
- image digest: `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- isolated container: `kafrust-company-consumer-controls-20260904`
- isolated topics: `kafrust-company-offset-reset-20260904` and
  `kafrust-company-position-control-20260904`

## Checks

The pushed `consumer_group_offset_reset` example passed earliest and latest
reset policies, committed out-of-range recovery after DeleteRecords, and
returned the expected recovered record. On a separate topic,
`consumer_position_control` passed direct and group watermarks, pause/resume,
seek, and position advancement after polling a produced record.

The uniquely named container and topics were removed on exit. No existing
company Docker container, network, volume, or image was pruned or modified.

## Boundary

This is local deterministic/current-source single-node diagnostic evidence for
offset reset and position controls. It does not close published artifact
qualification, accepted-floor/security coverage, retention or leader-failure
matrices beyond this bounded run, queue saturation, long campaigns, service
canary, or release authorization.
