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

현재 릴리즈: `0.3.4`.

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
  - [Share 그룹](#share-그룹)
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

현재 구현과 검증 범위는 다음과 같습니다.

- TLS, SASL/PLAIN, SASL/SCRAM, OAUTHBEARER 연결 경로
- Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 compatibility matrix
- multi-broker metadata, leader, coordinator failover
- compression, idempotent producer, transactions, `read_committed`
- typed Admin API, consumer-group 운영, metrics, limits, benchmark, soak 검증

대체 목표, non-goal, 기존 대안, completion tier는
[Project Strategy](docs/project-strategy.md)를 참고하세요.

## 설치

Rust 프로젝트에 kafrust를 추가합니다.

```toml
[dependencies]
kafrust = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Cargo 명령으로도 추가할 수 있습니다.

```sh
cargo add kafrust@0.3
```

### 요구 사항

- Rust `1.81` 이상.
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
알파 classic consumer group 경로입니다. range, round-robin, eager sticky,
cooperative-sticky assignor와 KIP-848 consumer protocol을 제공합니다.

OffsetCommit이 Kafka에 도달했을 수 있지만 응답을 잃으면 group API는 group
ID, member ID, generation/member epoch, 정확한 topic-partition next offset을
담은 `Error::ConsumerGroupCommitOutcomeUnknown`을 반환합니다. 모호한
요청은 자동 재전송하지 않으므로 새 commit 전에 해당 offset을 조정해야
합니다. 이 규칙은 직접 commit과 bounded background worker에 동일하게
적용됩니다.

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

### Share 그룹

현재 개발 브랜치에는 안정화된 KIP-932 v1 API와 선택적인 KIP-1206 ShareFetch v2
획득 모드, KIP-1222 renewal acknowledgement를 사용하는 알파
`ShareConsumer`도 들어 있습니다. Share 그룹은
partition position을 commit하는
모델이 아니라, 획득한 각 record를 `Accept`, `Release`, `Reject`, `Gap`으로
개별 acknowledgement하는 작업 큐 모델입니다.

```rust
use kafrust::{ShareAcknowledgementType, ShareConsumerConfig};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let mut consumer = ShareConsumerConfig::new(["localhost:9092"], "orders")
        .subscribe("orders")
        .build()
        .await?;

    loop {
        let records = consumer.poll().await?;
        for record in &records {
            process(record.value())?;
            consumer.acknowledge(record, ShareAcknowledgementType::Accept)?;
        }
        consumer.commit().await?;
    }
}
```

이 API는 아직 pre-`1.0`이지만 공개된 `0.3.3` artifact에 포함되어 있습니다.
protocol test와 injected-broker wire 왕복 테스트를 통과했고, 취소 가능한
background heartbeat task도 추가했습니다. Kafka 4.3.1에서 KIP-1222 renewal,
expiry/redelivery, 최종 Accept, broker failover, coordinator 복구가 검증됐고,
두 published member가 6개 partition을 중복 없이 나눠 처리하는 bounded gate도
[run 32388813780](https://github.com/TaeeunKil/kafrust/actions/runs/32388813780)에서
통과했습니다. 응답이 유실된 acknowledgement는 typed
unknown-outcome 오류로 노출하며 자동 재전송하지 않습니다. 정확한 알파 계약은
같은 published workflow에서 60초 동안 384개 record를 처리하는 확장도
[run 32389641275](https://github.com/TaeeunKil/kafrust/actions/runs/32389641275)에서
통과했으며, 두 member가 각각 192개를 처리하고 모든 partition/offset 조합이
중복되지 않았습니다.
두 member가 heartbeat를 시작한 뒤 한 member를 강제 종료하고 다른 member가
6개 partition을 모두 다시 할당받는 member-loss 검증도
[run 32390219711](https://github.com/TaeeunKil/kafrust/actions/runs/32390219711)에서
통과했습니다.
같은 group에서 두 번의 forced-loss를 수행하는 churn 검증도
[run 32391027028](https://github.com/TaeeunKil/kafrust/actions/runs/32391027028)에서
통과했습니다. 첫 번째에는 member 1이, 두 번째에는 재join한 member 2가
6개 partition을 모두 takeover했고 12개 offset이 중복되지 않았습니다.
`BatchOptimized`가 기본 모드이며, `RecordLimit`은 ShareFetch v2를 광고하는
broker가 필요합니다. `Renew`는 ShareAcknowledge v2에서 record를 완료하지
않고 lock을 연장한 뒤 다음 poll에서 재처리할 수 있게 합니다. 정확한 알파
계약은 [Share Consumer](docs/share-consumer.md)를 참고하세요.

## Client Telemetry

KIP-714 client telemetry는 low-level `Client` method와 high-level
`TelemetryClient`로 사용할 수 있습니다. broker capability 협상, subscription
state 유지, payload 제한, 오래된 subscription 재조회, push interval jitter,
shutdown 시 terminating push를 포함합니다. 애플리케이션이 OTLP MetricsData
provider를 제공해야 하며, 내장 OTLP 직렬화와 non-zero compression, 실제 broker
plugin live 검증은 아직 진행 중입니다. 자세한 내용은
[Client Telemetry](docs/telemetry.md)를 참고하세요.

## 호환성

kafrust의 호환성 주장은 실제 broker로 검증된 동작으로 제한합니다.

v1 qualification 목표의 broker floor는 Kafka `3.7.2`이며 `3.8.1`, `3.9.1`,
`4.0.0`, `4.3.1`을 continuity/pinned profile로 사용합니다. 배포는 KRaft만
대상으로 하며 single-node baseline, three-broker failover, controller
listener를 통한 Admin routing을 포함하고 ZooKeeper와 managed-service 동등성은
미청구 상태로 둡니다. Tokio async가 필수이고 `blocking`은 자체 runtime을
소유하는 adapter이며, alternate runtime과 일반 synchronous API는 범위에서
제외합니다. 정확한 보안/워크로드 경계와 immutable 결과는
[v1.0 support contract](docs/compatibility.md#v10-support-contract)와
[qualification ledger](docs/evidence/qualification-ledger.md)에 기록합니다.

| kafrust | Broker | Mode | Security | Status |
| --- | --- | --- | --- | --- |
| `0.3.x` | Apache Kafka `3.7.2` | single-node KRaft | `PLAINTEXT` | Passing live smoke |

현재 검증된 경로는 다음과 같습니다.

- Kafka 3.7.2, 3.8.1, 3.9.1, 4.3.1 single-node plaintext smoke.
- Kafka 3.7.2 multi-broker, TLS, SASL_PLAINTEXT, SASL_SSL/SCRAM 프로필.
- producer, batch/buffered delivery, 네 가지 compression, idempotence,
  transactions, `read_committed` consumer.
- classic consumer group, sticky/cooperative rebalance, KIP-848 consumer protocol,
  explicit/automatic commit, position and queue controls.
- typed topic/config/group/ACL/quota/SCRAM/reassignment/offset Admin API.

현재 근거는 [Compatibility](docs/compatibility.md)와
[Broker Roundtrip](docs/broker-roundtrip.md)를 참고하세요.

## 현재 한계

- 공개 API는 pre-`1.0`이며 minor release 사이에 변경될 수 있습니다.
- 공개 API는 pre-`1.0`이며 minor release 사이에 변경될 수 있습니다.
- 기본 networking path는 plaintext이고 TLS는 non-default `tls` feature입니다.
  TLS와 SASL 경로의 지원 범위는 [Compatibility](docs/compatibility.md)의
  실제 broker 증거를 기준으로 판단해야 합니다.
- OAUTHBEARER는 Kafka 내장 validator와 서명된 로컬 OIDC/JWKS fixture로
  검증되었지만, 외부 provider별 정책과 운영 credential rotation은 별도
  qualification이 필요합니다.
- transactions의 broker 응답을 관찰할 수 없는 경우 결과는 의도적으로
  `TransactionOutcomeUnknown`으로 보고되며 producer를 폐기해야 합니다.
- ShareConsumer의 higher-cycle churn, long-running ownership, backpressure,
  그리고 broader published-artifact qualification은 아직 1.0 release
  gate입니다.
- KIP-848 member-aware offset semantics, 더 넓은 fault-injection, target
  broker 권한/정책, 실제 서비스 canary는 아직 1.0 release gate입니다.
- `rust-rdkafka`의 전체 config passthrough와 non-Tokio/synchronous API는
  현재 설계 범위에 포함하지 않습니다.

## API

주요 공개 진입점:

- low-level Kafka request roundtrip용 `Client`.
- cluster, topic, config, consumer-group, ACL, quota, SCRAM, reassignment,
  producer/transaction inspection, committed offset 관리를 위한 `AdminClient`.
- `ProducerConfig`, `Producer`, `BufferedProducer`, `ProducerRecord`.
- `ConsumerConfig`, `Consumer`, `ConsumerAssignment`, `ConsumerRecord`, 그리고
  bounded per-partition delivery를 위한 `ConsumerPartitionQueue`.
- `ConsumerGroupConfig`, `ConsumerGroup`, `ConsumerGroupHeartbeat`.
- plaintext, TLS, SASL connection mode를 표현하는 `SecurityProtocol`.
- companion crate인 `kafrust-protocol`을 위한 `kafrust::protocol`.

생성된 API 문서:

- [`kafrust`](https://docs.rs/kafrust/0.3.4/kafrust/)
- [`kafrust-protocol`](https://docs.rs/kafrust-protocol/0.3.4/kafrust_protocol/)

## 문서

- [Contributing](CONTRIBUTING.md)
- [Agent instructions](AGENTS.md)
- [Agentic development workflow](docs/agentic-development.md)
- [Project strategy](docs/project-strategy.md)
- [Roadmap](docs/roadmap.md)
- [v1.0 milestone program](docs/milestones/v1.0/README.md)
- [Broker roundtrip](docs/broker-roundtrip.md)
- [Compatibility](docs/compatibility.md)
- [API stability](docs/api-stability.md)
- [Public API audit](docs/public-api-audit.md)
- [Producer API direction](docs/producer-api.md)
- [Producer buffering and linger design](docs/producer-buffering.md)
- [Consumer API direction](docs/consumer-api.md)
- [Consumer group direction](docs/consumer-groups.md)
- [Share Consumer](docs/share-consumer.md)
- [Client telemetry](docs/telemetry.md)
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
