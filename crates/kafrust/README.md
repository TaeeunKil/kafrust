# kafrust

kafrust is a pure Rust Kafka client with no librdkafka or C toolchain dependency.

The public alpha exposes:

- low-level Kafka request roundtrips through `Client`
- metadata-routed producer sends through `ProducerConfig`
- direct topic/partition fetch through `ConsumerConfig`
- classic consumer group join, poll, heartbeat, and commit through `ConsumerGroupConfig`

The client keeps Kafka concepts visible: bootstrap servers, client IDs, topics,
partitions, offsets, acknowledgements, metadata refresh, consumer groups,
heartbeats, and commits are represented directly in the API.

See the repository README and `docs/` directory for the roadmap, alpha limits,
compatibility notes, and broker smoke test workflow.
