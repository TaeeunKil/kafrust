# kafrust

[English](README.md) | [한국어](README.ko.md)

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

`librdkafka`나 C 툴체인 의존성이 없는 순수 Rust Kafka 클라이언트입니다.

kafrust는 `librdkafka` 래핑 없이 Kafka 프로토콜 호환성이 필요한 Rust
애플리케이션을 위한 알파 Kafka 클라이언트입니다. 공개 API에서는 bootstrap
server, client ID, topic, partition, offset, acknowledgement, metadata refresh,
consumer group, heartbeat, commit 같은 Kafka 개념을 그대로 드러냅니다.

현재 릴리즈: `0.2.1`.

지금 kafrust는 실험, 로컬 브로커 확인, 단순 내부 도구, API 평가에 적합합니다.
넓은 범위의 성숙한 production Kafka 기능이 즉시 필요하다면 Rust에서는 여전히
`rust-rdkafka`가 현실적인 기본 선택입니다.

## 목차

- [배경](#배경)
- [설치](#설치)
  - [요구 사항](#요구-사항)
- [사용법](#사용법)
  - [프로듀서](#프로듀서)
  - [버퍼드 프로듀서](#버퍼드-프로듀서)
  - [직접 컨슈머](#직접-컨슈머)
  - [컨슈머 그룹](#컨슈머-그룹)
- [호환성](#호환성)
- [현재 한계](#현재-한계)
- [API](#api)
- [문서](#문서)
- [번역](#번역)
- [기여](#기여)
- [라이선스](#라이선스)

## 배경

kafrust는 Kafka 클라이언트입니다. Kafka 브로커나 Kafka 호환 서버 프로젝트가
아닙니다.

전략적 목표는 순수 Rust 구현, 감사 가능한 프로토콜 코드, 필수 C 툴체인 제거를
중요하게 보는 애플리케이션에 실용적인 Rust-native 클라이언트를 제공하는
것입니다. 이 프로젝트는 의도적으로 `librdkafka`를 래핑하지 않으며, Kafka의
운영 모델을 일반 큐 추상화 뒤에 숨기지 않습니다.

가까운 로드맵의 우선순위는 다음과 같습니다.

- TLS와 SASL을 통한 보안 클라이언트 연결
- multi-broker metadata, leader, failover 동작
- consumer group 안정화
- compression 지원
- observability, limit, benchmark, compatibility evidence

대체 목표, non-goal, 기존 대안, completion tier는
[Project Strategy](docs/project-strategy.md)를 참고하세요.

## 설치

Rust 프로젝트에 kafrust를 추가합니다.

```toml
[dependencies]
kafrust = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Cargo 명령으로도 추가할 수 있습니다.

```sh
cargo add kafrust@0.2
```

### 요구 사항

- Rust `1.75` 이상.
- 애플리케이션의 Tokio runtime.
- 런타임 클라이언트 호출에 사용할 Kafka broker.
- `librdkafka`, C client binding, 필수 C toolchain은 필요하지 않습니다.

## 사용법

예제를 실행할 때 `KAFRUST_BOOTSTRAP_SERVERS`로 broker 주소를 지정합니다. 변수를
생략하면 예제는 `localhost:9092`를 사용합니다.

Smoke 예제는 secured broker 확인을 위해 `KAFRUST_SECURITY_PROTOCOL`,
`KAFRUST_SASL_USERNAME`, `KAFRUST_SASL_PASSWORD`도 읽습니다.

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

### 프로듀서

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-producer")
        .acks(Acks::Leader)
        .build()
        .await?;

    let metadata = producer
        .send(
            ProducerRecord::to("kafrust-smoke")
                .key("example-key")
                .value("hello from kafrust"),
        )
        .await?;

    println!(
        "produced {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );

    Ok(())
}
```

### 버퍼드 프로듀서

record를 큐에 넣고 linger time, record count, byte count, `flush`, `close`로
flush해야 할 때 `ProducerConfig::build_buffered`를 사용합니다.

```rust
use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut producer = ProducerConfig::new(["localhost:9092"])
        .client_id("example-buffered-producer")
        .acks(Acks::Leader)
        .linger_ms(10)
        .max_records_per_batch(100)
        .build_buffered()
        .await?;

    let delivery = producer
        .send(ProducerRecord::to("kafrust-smoke").value("buffered value"))
        .await?;

    let metadata = delivery.await?;
    producer.close().await?;

    println!("buffered record offset {}", metadata.offset());

    Ok(())
}
```

### 직접 컨슈머

직접 컨슈머는 명시적인 topic partition과 offset에서 fetch합니다. consumer group
동작이 필요하기 전의 단순 경로에 유용합니다.

```rust
use kafrust::ConsumerConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ConsumerConfig::new(["localhost:9092"])
        .client_id("example-consumer")
        .build()
        .await?;

    consumer.assign("kafrust-smoke", 0, 0);
    for record in consumer.poll().await? {
        println!(
            "fetched {}-{}@{} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.value().map(String::from_utf8_lossy)
        );
    }

    Ok(())
}
```

### 컨슈머 그룹

현재 consumer group API는 join, sync, heartbeat, poll, offset commit을 지원하는
알파 classic consumer group 경로입니다.

```rust
use kafrust::ConsumerGroupConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut group = ConsumerGroupConfig::new(["localhost:9092"], "example-group")
        .client_id("example-consumer-group")
        .subscribe("kafrust-smoke")
        .join()
        .await?;

    let records = group.poll().await?;
    group.commit_offsets().await?;

    println!("processed {} records", records.len());

    Ok(())
}
```

## 호환성

kafrust의 호환성 주장은 실제 broker로 검증된 동작으로 제한합니다.

| kafrust | Broker | Mode | Security | Status |
| --- | --- | --- | --- | --- |
| `0.2.x` | Apache Kafka `3.7.2` | single-node KRaft | `PLAINTEXT` | Passing live smoke |

현재 검증된 경로는 다음과 같습니다.

- `ApiVersions v0`와 `Metadata v1` roundtrip.
- high-level producer single-record, batch, buffered send.
- Fetch v2 response decoding을 사용하는 direct topic-partition fetch.
- classic consumer group join, sync, heartbeat, poll, offset commit.

현재 근거는 [Compatibility](docs/compatibility.md)와
[Broker Roundtrip](docs/broker-roundtrip.md)를 참고하세요.

## 현재 한계

- 공개 API는 pre-`1.0`이며 minor release 사이에 변경될 수 있습니다.
- plaintext TCP는 기본 networking path로 유지됩니다.
- TLS transport는 non-default `tls` crate feature 뒤에서 사용할 수 있으며,
  Kafka `3.7.2` broker roundtrip, producer, direct consumer, consumer group
  smoke path에 대해 검증되었습니다.
- SASL/PLAIN 인증은 Kafka `3.7.2` `SaslPlaintext`에서 broker roundtrip,
  producer, direct consumer, consumer group smoke path로 검증되었습니다.
  `SaslTls`와 더 넓은 SASL workflow는 아직 지원 범위로 주장하지 않습니다.
- broker compatibility는 Kafka `3.7.2`에 대해서만 검증되었습니다.
- multi-broker cluster, leader failover, rack awareness, partition expansion은 아직
  지원 범위로 주장하지 않습니다.
- idempotent producer, transaction, compression, admin API, 넓은 observability는
  아직 구현되지 않았습니다.
- 현재 request loop는 broker response를 기대하므로 `acks=0`은 아직 지원하지
  않습니다.

## API

주요 공개 진입점:

- low-level Kafka request roundtrip용 `Client`.
- `ProducerConfig`, `Producer`, `BufferedProducer`, `ProducerRecord`.
- `ConsumerConfig`, `Consumer`, `ConsumerAssignment`, `ConsumerRecord`.
- `ConsumerGroupConfig`, `ConsumerGroup`, `ConsumerGroupHeartbeat`.
- plaintext, TLS, SASL connection mode를 표현하는 `SecurityProtocol`.
- companion crate인 `kafrust-protocol`을 위한 `kafrust::protocol`.

생성된 API 문서:

- [`kafrust`](https://docs.rs/kafrust/0.2.1/kafrust/)
- [`kafrust-protocol`](https://docs.rs/kafrust-protocol/0.2.1/kafrust_protocol/)

## 문서

- [Contributing](CONTRIBUTING.md)
- [Agent instructions](AGENTS.md)
- [Agentic development workflow](docs/agentic-development.md)
- [Project strategy](docs/project-strategy.md)
- [Roadmap](docs/roadmap.md)
- [Broker roundtrip](docs/broker-roundtrip.md)
- [Compatibility](docs/compatibility.md)
- [API stability](docs/api-stability.md)
- [Producer API direction](docs/producer-api.md)
- [Producer buffering and linger design](docs/producer-buffering.md)
- [Consumer API direction](docs/consumer-api.md)
- [Consumer group direction](docs/consumer-groups.md)
- [Release preparation](docs/release.md)

## 번역

`README.md`는 canonical English README입니다.

현재 번역:

- [English](README.md)
- [한국어](README.ko.md)

추가 번역 README를 만들 때는 `README.ja.md`처럼 BCP 47 language tag를 사용하고,
release fact, compatibility claim, limit을 `README.md`와 맞춰야 합니다.

## 기여

GitHub issue와 pull request를 받습니다.

기여하기 전에 다음을 확인하세요.

- [Contributing](CONTRIBUTING.md)과 [Agent instructions](AGENTS.md)를 읽습니다.
- kafrust를 순수 Rust Kafka client로 유지합니다.
- `librdkafka`, C client binding, 필수 C toolchain을 도입하지 않습니다.
- 공개 API에서 Kafka user-facing concept을 보존합니다.
- Conventional Commits를 사용합니다.
- protocol behavior 또는 관측 가능한 client behavior에 집중한 test를 추가합니다.
- public behavior, project direction, workflow가 바뀌면 docs를 갱신합니다.

## 라이선스

MIT OR Apache-2.0 (c) kafrust contributors.

[MIT License](LICENSE-MIT)와
[Apache License, Version 2.0](LICENSE-APACHE)를 참고하세요.
