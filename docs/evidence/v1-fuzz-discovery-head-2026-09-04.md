# Fuzz Discovery From Pushed Head — 2026-09-04

The short `Fuzz Check` workflow passed from source commit
`2beaa719fa1a65b39ee8b7fe51f73c6c49cdc730` in
[run 33842200320](https://github.com/TaeeunKil/kafrust/actions/runs/33842200320).
All ten checked-in libFuzzer targets compiled with nightly Rust and ran for
the workflow's 30-second-per-target discovery window. The uploaded artifact
[`kafrust-fuzz-33842200320`](https://github.com/TaeeunKil/kafrust/actions/runs/33842200320/artifacts/9925409831)
contains 5,078 files, is 1,585,378 bytes, and has zip digest
`a578d2b23e74ed6d11ed6e9852f1afdad27be49cd4951b4c6cbb4747bcb694a3`.

## Target results

| Target | Executions | Peak RSS |
| --- | ---: | ---: |
| `codec` | 4,368,996 | 459 MiB |
| `frame` | 26,072,179 | 605 MiB |
| `api_versions_response` | 3,543,334 | 488 MiB |
| `group_describe_response` | 841,073 | 384 MiB |
| `share_group_offsets_response` | 882,041 | 467 MiB |
| `streams_groups_response` | 1,121,993 | 374 MiB |
| `offset_commit_response` | 917,304 | 581 MiB |
| `offset_fetch_response` | 1,304,280 | 377 MiB |
| `compression` | 170,191 | 433 MiB |
| `list_groups_response` | 926,132 | 526 MiB |
| **Total** | **40,147,523** | **605 MiB maximum** |

No target failed and no crash/OOM artifact was emitted by the successful
workflow. This is discovery evidence only: it is not one of the required
3,600-second target qualification sets, a weekly campaign pass, or proof that
the fuzz targets are free of crashes, hangs, or OOMs. V1-18 remains
`In progress`.
