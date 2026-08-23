# V1-20 Published `0.3.6` Smoke Evidence

- client/protocol: `0.3.6`
- source_commit: `8feeb1a0c8a6f1356ee2ad3bd5e375666cd0e6d0`
- published crate smoke: [run 32613844625](https://github.com/TaeeunKil/kafrust/actions/runs/32613844625)
- published multi-broker 3.7.2 classic: [run 32613851826](https://github.com/TaeeunKil/kafrust/actions/runs/32613851826)
- published multi-broker 4.3.1 KIP-848: [run 32613855210](https://github.com/TaeeunKil/kafrust/actions/runs/32613855210)
- published mTLS: [run 32614025832](https://github.com/TaeeunKil/kafrust/actions/runs/32614025832)
- published signed OAUTHBEARER: [run 32614029249](https://github.com/TaeeunKil/kafrust/actions/runs/32614029249)
- published secure multi-broker SCRAM: [run 32614033627](https://github.com/TaeeunKil/kafrust/actions/runs/32614033627)
- published secure transaction failover: [run 32614039766](https://github.com/TaeeunKil/kafrust/actions/runs/32614039766)

The seven-job published smoke matrix passed all profiles: Kafka 3.7.2
classic, Kafka 4.3.1 KIP-848, Kafka 3.7.2 SASL_SSL/SCRAM-SHA-256, and gzip,
snappy, lz4, and zstd. The two multi-broker jobs also passed published
pre/post-failover phases with exact registry dependency verification.

The follow-up published security/fault slices also passed: mutual TLS, signed
OAUTHBEARER, secured three-broker SCRAM failover, and secured transaction
failover. These verify the named published profiles only; they do not turn the
remaining matrix or long-duration gates into claims.

This is the first published-artifact smoke slice for `0.3.6`. It does not
close every accepted broker/security/workload row, long fault or SLO campaign,
migration canary, API freeze, or stable release. Those remain separate V1-20+
gates.
