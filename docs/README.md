# Documentation

The root [README](../README.md) is the short product and quick-start guide.
This page is the map for the detailed material. It keeps the README from
becoming a release ledger while making the project status and qualification
evidence easy to find.

## Start here

- [Project strategy](project-strategy.md) — product direction and support boundary
- [Roadmap](roadmap.md) — current sequencing and open work
- [Pre-1.0 release track](milestones/pre-1.0/README.md) — scoped 0.x milestones
- [Compatibility](compatibility.md) — broker, protocol, security, and runtime claims
- [Release preparation](release.md) — version, publication, and release gates
- [Migration from rust-rdkafka](migration-from-rust-rdkafka.md) — migration notes
- [v1.0 milestone program](milestones/v1.0/README.md) — conditional 1.0 qualification gates and milestone status

## Strategy and comparison

- [Competitive source audit](competitor-source-audit-2026-08-20.md) — dated competitor snapshot
- [Milestone planning handoff](milestone-planning-handoff-2026-08-21.md) — planning history and decisions

## API and usage

- [Producer API](producer-api.md)
- [Producer buffering](producer-buffering.md)
- [Consumer API](consumer-api.md)
- [Consumer groups](consumer-groups.md)
- [Share consumer](share-consumer.md)
- [Streams group](streams-group.md)
- [Admin API](admin-api.md)
- [Telemetry](telemetry.md)
- [Broker roundtrip](broker-roundtrip.md)
- [API stability](api-stability.md)
- [Public API audit](public-api-audit.md)

## Performance and operations

- [Performance](performance.md) — benchmark method, profiles, and limits
- [v1.0 qualification ledger](evidence/qualification-ledger.md) — gate-by-gate evidence index
- [Evidence archive](evidence/README.md) — dated live runs, audits, manifests, and diagnostic records
- [Latest bounded campaign record](evidence/v1-local-bounded-followup-2026-09-08.md) — active workstation diagnostic and explicit non-claims

Evidence files are historical records. A newer dated record can supersede an
older observation, but old records remain useful for understanding regressions
and decisions.

## Development

- [Contributing](../CONTRIBUTING.md) — contribution workflow
- [Agentic development](agentic-development.md) — repository automation and agent workflow
- [Repository policy](../AGENTS.md) — required validation and project rules

## How to read the records

Use the root README for the current public positioning and a first run. Use the
topic documents for API behavior and examples. Use Compatibility for claims
that can be made about brokers and features. Use the pre-1.0 release track for
scoped 0.x gates and the v1.0 milestone pages for the full 1.0 qualification
gates and ownership. Use `evidence/` for what was actually observed in a dated
environment and workload.

The source checkout and the published crate are separate artifacts. A workflow,
local source change, or passing unit test does not by itself prove that the
published package has the same behavior; the relevant evidence record must say
so explicitly.
