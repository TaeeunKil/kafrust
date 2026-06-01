# Contributing

## Branch Strategy

This project uses solo trunk-based development while it is early and maintained by a single primary author.

`main` should stay buildable and reviewable. Small, verified changes may land directly on `main`.

Use a short-lived branch when a change is likely to be risky, experimental, or spread across multiple commits.

Create a branch for changes that:

- modify public APIs
- change crate or module boundaries
- require multiple commits
- are experimental or likely to be reverted
- may temporarily break tests or CI

Branch names should use a short type prefix:

```text
feat/protocol-primitives
feat/api-versions
feat/metadata-request
test/protocol-fixtures
docs/architecture
chore/ci
fix/response-decoding
```

Avoid long-lived milestone branches unless the project has multiple concurrent maintainers or release trains.

## Commit Convention

This project uses Conventional Commits.

Format:

```text
<type>(<scope>): <summary>
```

The scope is optional. Keep the summary imperative, lowercase where natural, and under 72 characters.

Common types:

- `feat`: user-facing functionality
- `fix`: bug fixes
- `docs`: documentation-only changes
- `test`: tests
- `refactor`: code changes without behavior changes
- `perf`: performance improvements
- `build`: build system or dependency changes
- `ci`: CI configuration
- `chore`: maintenance that does not affect source or tests

Examples:

```text
feat(producer): add record batching
fix(protocol): handle flexible version tags
docs: define project goals
test(consumer): cover group rebalance timeout
```

Use `!` for breaking changes:

```text
feat(api)!: replace producer config builder
```
