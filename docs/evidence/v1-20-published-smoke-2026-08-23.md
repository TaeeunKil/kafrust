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

## Additional published surface runs

The same exact `0.3.6` registry pair was then exercised from source commit
`fca0e22133bedd57c61050ad32c94bc572a84794`:

- API 74 configuration on Kafka 4.3.1: [run 32614292940](https://github.com/TaeeunKil/kafrust/actions/runs/32614292940)
- DescribeCluster on Kafka 4.3.1: [run 32614294346](https://github.com/TaeeunKil/kafrust/actions/runs/32614294346)
- ConsumerGroupDescribe: [run 32614295845](https://github.com/TaeeunKil/kafrust/actions/runs/32614295845)
- KIP-848 consumer churn (ten drop cycles): [run 32614297414](https://github.com/TaeeunKil/kafrust/actions/runs/32614297414)
- member-aware OffsetFetch/OffsetCommit v10: [run 32614298827](https://github.com/TaeeunKil/kafrust/actions/runs/32614298827)
- KIP-848 regex/dynamic assignment: [run 32614300424](https://github.com/TaeeunKil/kafrust/actions/runs/32614300424)
- ShareConsumer runtime: [run 32614302188](https://github.com/TaeeunKil/kafrust/actions/runs/32614302188)
- Streams group runtime: [run 32614251288](https://github.com/TaeeunKil/kafrust/actions/runs/32614251288)
- Streams API surface on stable and Rust 1.81: [run 32614303856](https://github.com/TaeeunKil/kafrust/actions/runs/32614303856)
- ShareConsumer multi-member ownership: [run 32614372643](https://github.com/TaeeunKil/kafrust/actions/runs/32614372643), rerun from corrective workflow commit `546a3a1`
- Share Group State replication/coordinator failover: [run 32614253491](https://github.com/TaeeunKil/kafrust/actions/runs/32614253491)

All listed runs passed and assert exact published dependency versions in fresh
external lockfiles. An earlier multi-member attempt, [run
32614249505](https://github.com/TaeeunKil/kafrust/actions/runs/32614249505),
stopped in the verification shell because `log_base` was not scoped in that
step; the ownership assertions before that reference failure completed. The
workflow was corrected in `546a3a1` and the rerun above passed. This correction
is workflow evidence, not a product-failure waiver.

These rows expand the published smoke slice; they still do not close the full
broker/security/workload matrix, repeated fault campaigns, SLO, migration,
API-freeze, or release gates.

## Additional Share, transaction, and secure Admin rows

From the roadmap commit `8daf75d36e27117c9837a1ed51efbc98b4ee18d6`, the exact
published pair also passed:

- Share heartbeat coordinator failover, three cycles: [run 32614751284](https://github.com/TaeeunKil/kafrust/actions/runs/32614751284)
- Share multi-broker failover: [run 32614756542](https://github.com/TaeeunKil/kafrust/actions/runs/32614756542)
- secure Share multi-member ownership (SASL_SSL/SCRAM): [run 32614758515](https://github.com/TaeeunKil/kafrust/actions/runs/32614758515)
- transaction coordinator failover: [run 32614759961](https://github.com/TaeeunKil/kafrust/actions/runs/32614759961)
- secure classic group rebalance (SASL_SSL/SCRAM): [run 32614761625](https://github.com/TaeeunKil/kafrust/actions/runs/32614761625)
- secure Admin authorization: [run 32614763007](https://github.com/TaeeunKil/kafrust/actions/runs/32614763007)
- secure Admin mutation/offset paths: [run 32614764570](https://github.com/TaeeunKil/kafrust/actions/runs/32614764570)
- Share member-loss rebalancing with the default 180-second member lifetime: [run 32614875041](https://github.com/TaeeunKil/kafrust/actions/runs/32614875041)
- repeated Share member-loss ownership recovery across eight cycles with the default 120-second lifetime: [run 32615024395](https://github.com/TaeeunKil/kafrust/actions/runs/32615024395)
- pinned-current secure Kafka 4.3.1 KIP-848 leader failover: [run 32615403411](https://github.com/TaeeunKil/kafrust/actions/runs/32615403411)

The two short-parameter Share member-loss attempts (30 seconds) are retained as
diagnostics only: both reached the initial member's partial assignment but
ended before the broker session timeout could complete reassignment. They are
not counted as product failures or passed rows. The default-timing rerun above
is the qualification result.

All listed passing jobs assert the exact `0.3.6` client/protocol lockfile pair;
the secure rows use fresh external projects and Rust 1.81. These are named
published profiles, not the full accepted matrix, long-duration fault/SLO,
migration canary, API freeze, or stable-release gates.

## Published continuity and codec matrix

The published crate smoke matrix was expanded in commit
`69e12726847f082d69213348110c6f4e8fc9cdeb` and passed all ten jobs in [run
32615530030](https://github.com/TaeeunKil/kafrust/actions/runs/32615530030):
Kafka 3.7.2 classic, 3.8.1 classic, 3.9.1 classic, 4.0.0 classic, 4.3.1
KIP-848, 3.7.2 SASL_SSL/SCRAM-SHA-256, and the gzip, snappy, lz4, and zstd
codec profiles. Each job built a fresh external project and checked the exact
`0.3.6` client/protocol lockfile pair. This closes the draft matrix's
continuity/package-codec smoke slice, but not every security/workload
combination or the downstream fault/SLO/release gates.
