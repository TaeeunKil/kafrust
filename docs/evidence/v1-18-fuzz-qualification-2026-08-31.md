# V1-18 Fuzz Qualification Campaign — 2026-08-31

- workflow: [Fuzz Qualification 33380868376](https://github.com/TaeeunKil/kafrust/actions/runs/33380868376)
- source commit: `5de0ba22f82a3df4278c602d482ff7e912007f3b`
- event: scheduled weekly campaign
- result: passed
- targets: 10
- shards: 40 (four per target)
- declared budget: 900 seconds per shard, 3,600 cumulative seconds per target
- toolchain/resource limits: nightly, 2,048 MiB RSS, 10-second input timeout
- artifact inventory: 40 retained qualification records, 9,956,751 bytes total
- artifact verification: `scripts/check_v1_fuzz_qualification_artifacts.py` passed
- corpus verification: every target/shard corpus SHA-256 matched its record
- crash/OOM disposition: no crash, hang, or OOM artifact file was retained

This is one successful historical qualification campaign set. It is not by
itself a V1-18 completion claim: later source commits changed stable runtime
code, so current-head qualification must be re-established before the milestone
can close.
