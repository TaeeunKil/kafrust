# Fuzz corpus discovery check (2026-09-04)

- source_commit: `ce4719b17dc1f62cc8d5ee46a56a1d7b61493e6f`
- workflow: [Fuzz Check run 33799057829](https://github.com/TaeeunKil/kafrust/actions/runs/33799057829)
- evidence level: CI
- toolchain: nightly Rust with cargo-fuzz on `ubuntu-latest`
- targets: ten checked-in libFuzzer targets
- mode: corpus-backed discovery check, 30 seconds per target
- result: all compile and run steps passed; corpus and crash-artifact upload
  completed

This is one discovery smoke run. It is not one of the required 3,600-second
per-target qualification campaign sets, does not count as a weekly campaign
set, and does not by itself establish absence of crashes, hangs, or OOMs.
The two remaining weekly qualification passes and retained crash/OOM
disposition remain open in V1-18.
