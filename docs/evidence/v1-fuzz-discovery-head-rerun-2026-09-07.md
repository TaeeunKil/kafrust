# Current-head fuzz discovery rerun — 2026-09-07

- workflow: [Fuzz Check 34075853513](https://github.com/TaeeunKil/kafrust/actions/runs/34075853513)
- source commit: `18ee34d9c12c9c16c19764e27322130c717ee15a`
- result: passed
- targets: all ten checked-in libFuzzer targets
- duration: 30 seconds per target
- limits: 2,048 MiB RSS, 10-second input timeout
- observed: every target compiled and completed its corpus-backed run; no
  target failed and the discovery artifact uploaded successfully
- artifact: `kafrust-fuzz-34075853513` (artifact ID `10002123213`, 1,595,787 bytes)

The checkout differs from the previously recorded `2b0ece7` runtime only by
documentation commits, but this run is retained against the exact pushed head
for provenance. It is discovery evidence only: it is not a 3,600-second target
qualification set, a weekly campaign pass, proof of absence of bugs, or V1-18
completion evidence.

