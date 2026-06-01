# Agent Instructions

This repository is developed with coding agents. Treat these instructions as project policy, not as private scratch notes.

## Project Rules

- Keep kafrust a pure Rust Kafka client.
- Do not introduce librdkafka, C client bindings, or a required C toolchain.
- Prefer small, reviewable changes over broad rewrites.
- Keep public APIs minimal until protocol and runtime behavior are stable.
- Ground protocol behavior in Kafka API/version details when making assumptions.
- Add focused tests for protocol encoding, decoding, and observable client behavior.
- Update README or docs when changing public behavior, project direction, or development workflow.
- Use Conventional Commits for all commits.

## Working Loop

1. Read the relevant docs and nearby code before editing.
2. State the intended change in concrete terms.
3. Make the smallest useful change.
4. Run the narrowest relevant verification.
5. Summarize what changed, what was verified, and what remains uncertain.

## Rust Expectations

- Prefer explicit error types over stringly typed failures.
- Keep async runtime assumptions visible and deliberate.
- Avoid `unsafe` unless there is a clear, documented reason.
- Keep wire-format code easy to audit.
- Separate protocol mechanics from high-level client ergonomics.
