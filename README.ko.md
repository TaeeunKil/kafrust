# kafrust

[English](README.md) | [한국어](README.ko.md)

[![Crates.io](https://img.shields.io/crates/v/kafrust.svg)](https://crates.io/crates/kafrust)
[![Docs.rs](https://docs.rs/kafrust/badge.svg)](https://docs.rs/kafrust)
[![CI](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml/badge.svg)](https://github.com/TaeeunKil/kafrust/actions/workflows/ci.yml)

`librdkafka`나 필수 C 클라이언트 의존성 없이 Kafka 프로토콜을 사용하는
순수 Rust Kafka 클라이언트입니다.

현재 공개 버전은 **`0.3.6`**입니다. 아직 `1.0` 이전이므로 마이너 버전에서
공개 API가 바뀔 수 있습니다. 작업 트리에는 공개 패키지에 포함되지 않은 수정이
있을 수 있으므로, 소스 체크아웃을 릴리스 산출물로 판단하기 전
[릴리스 문서](docs/release.md)와 [증거 기록](docs/evidence/)을 확인하세요.

실험, 로컬 브로커 점검, 내부 도구, API 평가에 적합합니다. 지금 당장 성숙한
범용 프로덕션 클라이언트가 필요하다면
[`rust-rdkafka`](https://github.com/fede1024/rust-rdkafka)가 실용적인 Rust 기본
선택지입니다.

## 목차

- [프로젝트 성격](#프로젝트-성격)
- [설치](#설치)
- [빠른 시작](#빠른-시작)
- [지원 범위](#지원-범위)
- [호환성 경계](#호환성-경계)
- [현재 한계](#현재-한계)
- [문서](#문서)
- [개발](#개발)
- [라이선스](#라이선스)

## 프로젝트 성격

kafrust는 Kafka 호환 브로커가 아니라 Kafka 클라이언트입니다. 공개 API에서
bootstrap 서버, 토픽, 파티션, 오프셋, acknowledgement, 메타데이터, consumer
group, heartbeat, commit 같은 Kafka 개념을 직접 드러냅니다.

클라이언트 경계는 순수 Rust로 유지합니다. 기본 빌드는 `librdkafka`, C 바인딩,
필수 C 툴체인을 사용하지 않습니다. 선택 기능인 `tls`는 `rustls`를 사용하며,
플랫폼에 따라 암호화 provider의 네이티브 도구가 필요할 수 있습니다.

## 설치

```toml
[dependencies]
kafrust = "0.3.6"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

또는:

```sh
cargo add kafrust@0.3.6
```

필요 환경:

- Rust `1.81` 이상
- 비동기 API용 Tokio 런타임
- 런타임 호출을 수행할 Kafka 브로커
- 소유 런타임 동기 어댑터에는 선택 `blocking` 기능

## 빠른 시작

bootstrap 주소와 토픽을 정한 뒤 포함된 producer 예제를 실행합니다.

```sh
KAFRUST_BOOTSTRAP_SERVERS=localhost:9092 \
KAFRUST_TOPIC=kafrust-smoke \
cargo run -p kafrust --example producer_send
```

최소 producer 예제:

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
        .send(ProducerRecord::to("kafrust-smoke").value("hello from kafrust"))
        .await?;
    println!("{}-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());
    Ok(())
}
```

consumer, group, transaction, buffering, Share, Streams, Admin API는
[문서 인덱스](docs/README.md#api-and-usage)에서 시작하세요.

## 지원 범위

공개 클라이언트의 주요 영역은 다음과 같습니다.

- `Client`를 통한 저수준 Kafka 요청/응답 왕복
- 즉시, batch, buffered, idempotent, transactional producer
- direct consumer와 classic/KIP-848 consumer group
- 문서화된 안정성 한계가 있는 Share 및 Streams 관련 API
- topic, group, config, ACL, quota, SCRAM, reassignment, offset,
  transaction, controller 경로용 타입 기반 Admin 작업
- plaintext, TLS, SASL/PLAIN, SCRAM, OAUTHBEARER 연결 경로
- compression, 제한된 response/decode 크기, metrics, client telemetry

세부 기능과 예제는 주제별 문서에 있습니다. README에는 전체 API와 모든
프로토콜 버전 주장을 반복해서 적지 않습니다.

## 호환성 경계

문서화된 검증 목표는 KRaft 기반 Apache Kafka이며, 기준 버전은 `3.7.2`이고
`3.8.1`, `3.9.1`, `4.0.0`, `4.3.1` 고정 프로필을 추가로 포함합니다. 정확한
브로커·보안·기능·워크로드 주장은 [호환성 문서](docs/compatibility.md)에
관리합니다.

현재 지원 경계는 범용 `rust-rdkafka` 호환보다 좁습니다.

- KRaft만 범위에 포함하며 ZooKeeper와 managed service 동등성은 주장하지 않습니다.
- Tokio 비동기가 기본 런타임이며 다른 런타임 지원은 목표가 아닙니다.
- workflow 통과나 소스 구현만으로는 공개 패키지 검증을 의미하지 않습니다.
  해당 증거 행이 있어야 합니다.
- 상세 실시간 결과와 과거 실행 기록은 README가 아니라
  [docs/evidence](docs/evidence/)에 둡니다.

## 현재 한계

- 공개 API는 `1.0` 이전이며 마이너 버전 사이에도 바뀔 수 있습니다.
- 일부 고급 Admin, Share, Streams, 보안, 브로커 내부 경로는 구현되어 있지만
  별도 검증 대상이거나 아직 불안정합니다.
- 장시간 fault/soak 및 성능 게이트는 짧은 smoke 테스트와 별개입니다.
- `1.0` 프로그램에는 명명된 서비스 canary, forward cutover, credential
  rotation, rollback이 필요하며 로컬 테스트만으로 충족되지 않습니다.
- 범용 프로덕션 준비 완료, managed service 지원, `rust-rdkafka` drop-in
  호환을 주장하지 않습니다.

[v1.0 마일스톤](docs/milestones/v1.0/README.md)에서 넓은 프로덕션 검증
게이트를 확인하세요. 실제 0.x 순서는
[pre-1.0 릴리스 트랙](docs/milestones/pre-1.0/README.md)에 정리했고,
[릴리스 준비 문서](docs/release.md)에서 버전 정책을 설명합니다.

## 문서

주제별 문서는 [문서 인덱스](docs/README.md)에서 찾을 수 있습니다.

생성된 API 문서:

- [`kafrust`](https://docs.rs/kafrust/0.3.6/kafrust/)
- [`kafrust-protocol`](https://docs.rs/kafrust-protocol/0.3.6/kafrust_protocol/)

## 개발

```sh
cargo fmt --all
cargo check --workspace --all-targets
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

변경 전 [Contributing](CONTRIBUTING.md)과 [AGENTS.md](AGENTS.md)를 읽으세요.
클라이언트는 순수 Rust로 유지하고, 공개 API에서 Kafka 개념을 보존하며,
Conventional Commits와 관찰 가능한 동작에 대한 집중 테스트를 사용합니다.

## 라이선스

MIT OR Apache-2.0. [LICENSE-MIT](LICENSE-MIT)와
[LICENSE-APACHE](LICENSE-APACHE)를 확인하세요.
