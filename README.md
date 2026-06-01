# kafrust

kafrust is a pure Rust Kafka client with no librdkafka or C toolchain dependency.

The project is starting with a production-oriented focus:

- native Rust implementation
- async-first client design
- protocol correctness
- clear operational behavior

## Design Principle

kafrust should feel familiar to existing Kafka users while being native Rust internally.

The public API should preserve Kafka concepts such as topics, partitions, offsets, consumer groups, client IDs, acknowledgements, retries, metadata refresh, and rebalancing. Rust ergonomics should improve how these concepts are expressed, not hide or rename the operational model Kafka users already understand.

## Development

kafrust is developed with an agent-assisted workflow that keeps agent work reviewable and grounded in explicit project rules.

- [Contributing](CONTRIBUTING.md)
- [Agent instructions](AGENTS.md)
- [Agentic development workflow](docs/agentic-development.md)
- [Roadmap](docs/roadmap.md)

## License

kafrust is licensed under either of:

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
