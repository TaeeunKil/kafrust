# Hosted fuzz discovery rerun (2026-09-04)

## Scope

The pushed source commit `a8199d66b75cae90db4de33b3f7db629a6b0eacc` passed
[Fuzz Check run 33821955157](https://github.com/TaeeunKil/kafrust/actions/runs/33821955157)
on a hosted Linux x64 runner. The workflow compiled all ten checked-in
libFuzzer targets and ran each target against its checked-in corpus for the
bounded 30-second discovery window.

| Target | Executions |
| --- | ---: |
| `codec` | 4,644,940 |
| `frame` | 25,607,837 |
| `api_versions_response` | 3,436,316 |
| `group_describe_response` | 849,480 |
| `share_group_offsets_response` | 1,057,052 |
| `streams_groups_response` | 1,384,776 |
| `offset_commit_response` | 1,117,294 |
| `offset_fetch_response` | 1,031,446 |
| `compression` | 190,473 |
| `list_groups_response` | 897,330 |
| **Total** | **40,216,944** |

The job completed successfully in approximately 6 minutes 40 seconds. The
uploaded artifact is
[`kafrust-fuzz-33821955157`](https://github.com/TaeeunKil/kafrust/actions/runs/33821955157/artifacts/9918716093)
(artifact ID `9918716093`, 5,131 files, 1,603,798 bytes, ZIP SHA-256
`1327fa6f43e556e62ec2d04cf60731435fe77f176c517c73b62d103360eadecb`). No
crash or OOM artifact file was emitted by the bounded run.

## Boundary

This is a successful discovery smoke and corpus-retention check. It is not a
3,600-second-per-target qualification set, not one of the remaining weekly
campaign passes, and not proof that crashes, hangs, or OOMs cannot occur. The
remaining V1-18 weekly campaign and retained crash/OOM disposition gates stay
open; this run does not authorize a version bump or release.
