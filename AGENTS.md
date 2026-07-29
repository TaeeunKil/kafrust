# Agent Instructions

This repository is developed with coding agents. Treat these instructions as project policy, not as private scratch notes.

## Project Rules

- Keep kafrust a pure Rust Kafka client.
- Do not introduce librdkafka, C client bindings, or a required C toolchain.
- Preserve Kafka user-facing concepts and operational behavior in public APIs.
- Prefer Rust ergonomics that clarify Kafka concepts over abstractions that hide them.
- Prefer small, reviewable changes over broad rewrites.
- Keep public APIs minimal until protocol and runtime behavior are stable.
- Ground protocol behavior in Kafka API/version details when making assumptions.
- Add focused tests for protocol encoding, decoding, and observable client behavior.
- Update README or docs when changing public behavior, project direction, or development workflow.
- Use Conventional Commits for all commits.

## Working Loop

1. Read the relevant docs and nearby code before editing.
2. Identify the root cause and describe the relevant ownership, lifetime,
   protocol, or concurrency flow.
3. State the intended change in concrete terms.
4. Make the smallest coherent change.
5. Run the narrowest relevant verification while iterating.
6. Run the required final validation when Rust code changes.
7. Summarize behavior changes, verification, and remaining risks.

## Required Validation

After modifying Rust code, run:

1. `cargo fmt --all`
2. `cargo check --workspace --all-targets`
3. `cargo test --workspace --all-features`
4. `cargo clippy --workspace --all-targets --all-features -- -D warnings`
5. `cargo doc --workspace --all-features --no-deps`
6. `git diff --check`

Use focused tests during development, but do not claim completion while a
required command is failing. Report commands that could not be executed and
why. CI must remain green on the repository MSRV and stable Rust.

## Rust Expectations

- Prefer explicit error types over stringly typed failures.
- Keep async runtime assumptions visible and deliberate.
- Avoid `unsafe` unless there is a clear, documented reason.
- Keep wire-format code easy to audit.
- Separate protocol mechanics from high-level client ergonomics.

## Ownership And Allocation

- Do not add `.clone()` merely to bypass a borrow-checker error.
- Before cloning, consider borrowing, moving ownership, or narrowing scopes.
- Cloning cheap shared handles such as `Arc` is acceptable when intentional.
- Prefer `&str`, `&[T]`, and other borrowed inputs when ownership is not needed.
- Avoid unnecessary `String`, `Vec`, and collection allocations.
- Do not introduce a `'static` bound solely to silence a lifetime error.

## Error Handling

- Do not add `unwrap`, `expect`, `panic!`, `todo!`, or `unimplemented!` to
  production paths.
- Propagate or handle recoverable errors explicitly and add useful context at
  I/O, network, protocol, and public API boundaries.
- Do not silently discard meaningful errors. `let _ = ...` is allowed only for
  intentional best-effort cleanup, notification to a possibly closed receiver,
  or another result that is genuinely non-actionable.
- Keep stable library and domain boundaries typed. Do not replace typed logic
  with strings or untyped maps.
- Test fixtures may use `unwrap` or `expect` when failure should abort the test
  and the assertion context remains clear.

## Async And Concurrency

- Do not hold mutex, read/write lock, or similar guards across `.await` unless
  the lock is explicitly designed for it and the lifetime is justified.
- Keep critical sections small and prefer ownership transfer or message passing.
- Do not introduce `Arc<Mutex<_>>` without a concrete shared-mutation need.
- Use bounded channels by default.
- Define queue saturation, cancellation, timeout, retry, and shutdown behavior.
- Do not detach spawned tasks without lifecycle and error handling.
- Check whether spawned futures must be `Send`.

## Unsafe Code

- Do not introduce `unsafe` unless it is explicitly required.
- Every unsafe block must document its safety invariants.
- Isolate unsafe implementation details behind a safe abstraction.
- Unsafe changes require focused tests and explicit manual review.

## API And Architecture

- Preserve Kafka concepts and observable operational behavior in public APIs.
- Do not change a public API merely to avoid an implementation difficulty.
- Intentional public API changes require tests, documentation, and migration
  notes. This project is pre-1.0, so justified API correction remains possible.
- Model finite states with enums rather than combinations of booleans when that
  prevents invalid states.
- Keep domain and protocol logic separate from transport and runtime mechanics.
- Prefer simple, explicit code over premature generic abstractions.
- Do not add a dependency when the standard library or an existing dependency
  reasonably solves the problem.

## Testing

- Add or update tests for every observable behavior change.
- Cover failure paths, boundary conditions, malformed input, and invalid state.
- Bug fixes require a regression test that fails before the fix.
- Do not weaken, delete, or ignore tests merely to make the suite pass.
- Prefer deterministic synchronization over arbitrary sleeps.
- Keep protocol fixtures byte-auditable and assert the relevant API version.

## Kafka Protocol And Runtime

- Ground protocol behavior in Kafka API keys, versions, and schema details.
- Make nullable, compact, flexible-version, header-version, byte-order, and
  framing assumptions explicit.
- Treat broker responses and record payloads as untrusted input.
- Enforce frame, array, batch, and decompression limits before allocation.
- Distinguish transport success from broker or partition-level success.
- Classify broker errors as retryable, fatal, fencing, duplicate, or terminal
  where the workflow requires that distinction.
- Preserve producer ID, epoch, and sequence identity across idempotent retries.
- Make retries idempotent and account for duplicate, delayed, out-of-order, and
  ambiguous responses.
- Do not claim broker or security compatibility without recorded live evidence.
- Never log credentials, tokens, SCRAM material, or unredacted secrets.

## Performance

- Do not assume Rust code is fast without measurement.
- Avoid speculative optimization, but inspect allocations, cloning, lock
  contention, task count, batching, and serialization in hot paths.
- Performance-sensitive changes require a benchmark, profile, or comparable
  before/after evidence.

## Forbidden Shortcuts

- Arbitrary clones, broad `Arc<Mutex<_>>`, or unnecessary `'static` bounds.
- New production `unwrap`, `expect`, panic, or placeholder macros.
- Suppressing Clippy warnings without a documented reason.
- Deleting or weakening tests to obtain a green build.
- Changing public APIs merely to avoid ownership or compiler errors.
- Broad rewrites when a small, reviewable change solves the problem.
