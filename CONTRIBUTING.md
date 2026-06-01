# Contributing

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

