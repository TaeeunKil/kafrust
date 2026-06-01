# Producer API Direction

The producer API should preserve Kafka concepts while using Rust builders and typed options.

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

let config = ProducerConfig::new(["localhost:9092"])
    .client_id("orders-api")
    .acks(Acks::All);

let record = ProducerRecord::to("orders")
    .key("order-123")
    .value("created")
    .header("source", "checkout");
```

The public model intentionally keeps Kafka terms visible:

- topic
- partition
- key
- value
- headers
- timestamp
- acknowledgements
- record metadata

The first producer implementation should stay byte-first. Serialization adapters can be added later without forcing serde or another encoding choice into the core client.
