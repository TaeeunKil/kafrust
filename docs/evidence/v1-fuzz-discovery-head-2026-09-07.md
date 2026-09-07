# Current-head fuzz discovery — 2026-09-07

- workflow: [Fuzz Check 34073530866](https://github.com/TaeeunKil/kafrust/actions/runs/34073530866)
- source commit: `2b0ece76974a68a56f6f28799110a46695345b5f`
- result: passed
- targets: all ten checked-in libFuzzer targets
- duration: 30 seconds per target
- limits: 2,048 MiB RSS, 10-second input timeout
- observed: every target compiled and completed its corpus-backed run; the
  workflow uploaded the discovery artifact without a failed target
- artifact: `kafrust-fuzz-34073530866` (artifact ID `10001374410`, 1,563,904 bytes)

This is current-head discovery evidence only. It is not a 3,600-second target
qualification set, a weekly qualification campaign, proof of absence of bugs,
or V1-18 completion evidence.
