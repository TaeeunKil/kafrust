#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

use kafrust::protocol::api::metadata::MetadataResponseV12;
use kafrust::protocol::api::offset_fetch::OffsetFetchResponseV10;
use kafrust::protocol::codec::{Decoder, Encoder};
use kafrust::protocol::consumer_group::{
    ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0, ConsumerProtocolTopicAssignment,
};
use kafrust::{
    AdminClient, ClientConfig, ClientMetrics, ConsumerConfig, ConsumerGroupConfig,
    ConsumerGroupOffset, ConsumerGroupOffsetQuery, ConsumerGroupProtocol, ProducerConfig,
    ProducerRecord, ShareAcknowledgementType, ShareConsumerConfig,
};
use support::{ScriptedBroker, ScriptedResponse};

#[test]
fn offset_fetch_v10_fixture_decodes_with_response_header() {
    let body = offset_fetch_v10_response_body_at([8; 16], 42);
    let mut decoder = Decoder::new(&body);
    decoder.read_tagged_fields().unwrap();
    let response = OffsetFetchResponseV10::decode_body(&mut decoder).unwrap();

    assert_eq!(response.groups[0].group_id, "orders-consumer-recovery");
    assert_eq!(response.groups[0].topics[0].topic_id, [8; 16]);
}

#[tokio::test]
async fn admin_read_reconnects_after_scripted_metadata_response_loss() {
    let broker = ScriptedBroker::start(vec![
        ScriptedResponse::Drop,
        ScriptedResponse::RespondAndClose(metadata_response_body("orders", 0)),
    ])
    .await
    .expect("scripted broker should bind");
    let address = broker.address();
    let metrics = ClientMetrics::new();
    let admin = AdminClient::new(
        ClientConfig::new([address.to_string()])
            .metrics(metrics.clone())
            .request_timeout_ms(1_000),
    )
    .max_retries(1);

    let topics = admin
        .list_topics()
        .await
        .expect("AdminClient should retry the dropped metadata response");

    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name(), "orders");
    assert_eq!(topics[0].partition_count(), 1);
    assert!(metrics.snapshot().retries >= 1);

    let observations = broker
        .finish()
        .await
        .expect("scripted broker should complete both steps");
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].api_key, 3);
    assert_eq!(observations[0].api_version, 1);
    assert_eq!(observations[1].api_key, 3);
    assert_eq!(observations[1].api_version, 1);
}

#[tokio::test]
async fn admin_member_aware_offset_commit_response_loss_is_unknown() {
    let coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_member_offset_v10_response_body()),
        ScriptedResponse::Respond(metadata_v12_response_body_for_topics(
            "127.0.0.1:9092".parse().unwrap(),
            &[("orders", [6; 16])],
        )),
        ScriptedResponse::Drop,
    ])
    .await
    .expect("member-aware coordinator broker should bind");
    let bootstrap = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        find_coordinator_response_for_address(coordinator.address()),
    )])
    .await
    .expect("member-aware bootstrap broker should bind");

    let metrics = ClientMetrics::new();
    let admin = AdminClient::new(
        ClientConfig::new([bootstrap.address().to_string()])
            .metrics(metrics.clone())
            .request_timeout_ms(1_000),
    );

    let error = admin
        .alter_consumer_group_offsets_with_member(
            "orders-group",
            "member-1",
            7,
            None,
            &[ConsumerGroupOffset::new("orders", 0, 42).leader_epoch(5)],
        )
        .await
        .expect_err("lost member-aware OffsetCommit response must remain ambiguous");
    assert!(matches!(
        error,
        kafrust::Error::AdminMutationOutcomeUnknown {
            operation: "OffsetCommit"
        }
    ));
    assert_eq!(metrics.snapshot().retries, 0);

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("member-aware bootstrap broker should observe coordinator lookup");
    let coordinator_observations = coordinator
        .finish()
        .await
        .expect("member-aware coordinator should observe OffsetCommit v10");
    assert_eq!(
        bootstrap_observations
            .iter()
            .map(|request| (request.api_key, request.api_version))
            .collect::<Vec<_>>(),
        [(10, 1)]
    );
    assert_eq!(
        coordinator_observations
            .iter()
            .map(|request| (request.api_key, request.api_version))
            .collect::<Vec<_>>(),
        [(18, 3), (3, 12), (8, 10)]
    );
}

#[tokio::test]
async fn admin_member_aware_offsets_negotiate_v10_with_topic_ids() {
    const TOPIC_ID: [u8; 16] = [6; 16];
    let fetch_coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_member_offset_v10_response_body()),
        ScriptedResponse::Respond(offset_fetch_v10_response_body_for_group(
            "orders-consumer-recovery",
            TOPIC_ID,
            42,
        )),
    ])
    .await
    .expect("member-aware v10 fetch coordinator should bind");
    let fetch_bootstrap = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        find_coordinator_response_for_address(fetch_coordinator.address()),
    )])
    .await
    .expect("member-aware v10 fetch bootstrap should bind");

    let admin = AdminClient::new(
        ClientConfig::new([fetch_bootstrap.address().to_string()]).request_timeout_ms(1_000),
    );
    let listed = admin
        .list_consumer_group_offsets_with_member(
            "orders-consumer-recovery",
            Some("member-1"),
            7,
            Some(&[ConsumerGroupOffsetQuery::new("orders", [0]).topic_id(TOPIC_ID)]),
            true,
        )
        .await
        .expect("Admin OffsetFetch should negotiate v10");
    assert!(listed.is_success());
    assert_eq!(listed.topics()[0].topic(), "orders");
    assert_eq!(listed.topics()[0].partitions()[0].committed_offset(), 42);

    let fetch_observations = fetch_coordinator
        .finish()
        .await
        .expect("member-aware v10 fetch coordinator should finish");
    assert_eq!(
        fetch_observations
            .iter()
            .map(|request| (request.api_key, request.api_version))
            .collect::<Vec<_>>(),
        [(18, 3), (9, 10)]
    );
    fetch_bootstrap
        .finish()
        .await
        .expect("member-aware v10 fetch bootstrap should finish");

    let commit_coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_member_offset_v10_response_body()),
        ScriptedResponse::Respond(offset_commit_v10_response_body(TOPIC_ID)),
    ])
    .await
    .expect("member-aware v10 commit coordinator should bind");
    let commit_bootstrap = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        find_coordinator_response_for_address(commit_coordinator.address()),
    )])
    .await
    .expect("member-aware v10 commit bootstrap should bind");
    let admin = AdminClient::new(
        ClientConfig::new([commit_bootstrap.address().to_string()]).request_timeout_ms(1_000),
    );
    let altered = admin
        .alter_consumer_group_offsets_with_member(
            "orders-consumer-recovery",
            "member-1",
            7,
            None,
            &[ConsumerGroupOffset::new("orders", 0, 43).topic_id(TOPIC_ID)],
        )
        .await
        .expect("Admin OffsetCommit should negotiate v10");
    assert!(altered.is_success());
    assert_eq!(altered.topics()[0].topic(), "orders");

    let commit_observations = commit_coordinator
        .finish()
        .await
        .expect("member-aware v10 commit coordinator should finish");
    assert_eq!(
        commit_observations
            .iter()
            .map(|request| (request.api_key, request.api_version))
            .collect::<Vec<_>>(),
        [(18, 3), (8, 10)]
    );
    commit_bootstrap
        .finish()
        .await
        .expect("member-aware v10 commit bootstrap should finish");
}

#[tokio::test]
async fn admin_member_aware_offsets_resolve_topic_ids_for_v10() {
    const TOPIC_ID: [u8; 16] = [7; 16];
    let fetch_coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_member_offset_v10_response_body()),
        ScriptedResponse::Respond(metadata_v12_response_body_for_topics(
            "127.0.0.1:9092".parse().unwrap(),
            &[("orders", TOPIC_ID)],
        )),
        ScriptedResponse::Respond(offset_fetch_v10_response_body_for_group(
            "orders-consumer-recovery",
            TOPIC_ID,
            42,
        )),
    ])
    .await
    .expect("member-aware v10 fetch coordinator should bind");
    let fetch_bootstrap = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        find_coordinator_response_for_address(fetch_coordinator.address()),
    )])
    .await
    .expect("member-aware v10 fetch bootstrap should bind");

    let admin = AdminClient::new(
        ClientConfig::new([fetch_bootstrap.address().to_string()]).request_timeout_ms(1_000),
    );
    let listed = admin
        .list_consumer_group_offsets_with_member(
            "orders-consumer-recovery",
            Some("member-1"),
            7,
            Some(&[ConsumerGroupOffsetQuery::new("orders", [0])]),
            true,
        )
        .await
        .expect("Admin OffsetFetch should resolve topic UUID and negotiate v10");
    assert!(listed.is_success());
    assert_eq!(listed.topics()[0].topic(), "orders");
    assert_eq!(listed.topics()[0].partitions()[0].committed_offset(), 42);

    let fetch_observations = fetch_coordinator
        .finish()
        .await
        .expect("member-aware v10 fetch coordinator should finish");
    assert_eq!(
        fetch_observations
            .iter()
            .map(|request| (request.api_key, request.api_version))
            .collect::<Vec<_>>(),
        [(18, 3), (3, 12), (9, 10)]
    );
    fetch_bootstrap
        .finish()
        .await
        .expect("member-aware v10 fetch bootstrap should finish");

    let commit_coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_member_offset_v10_response_body()),
        ScriptedResponse::Respond(metadata_v12_response_body_for_topics(
            "127.0.0.1:9092".parse().unwrap(),
            &[("orders", TOPIC_ID)],
        )),
        ScriptedResponse::Respond(offset_commit_v10_response_body(TOPIC_ID)),
    ])
    .await
    .expect("member-aware v10 commit coordinator should bind");
    let commit_bootstrap = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        find_coordinator_response_for_address(commit_coordinator.address()),
    )])
    .await
    .expect("member-aware v10 commit bootstrap should bind");
    let admin = AdminClient::new(
        ClientConfig::new([commit_bootstrap.address().to_string()]).request_timeout_ms(1_000),
    );
    let altered = admin
        .alter_consumer_group_offsets_with_member(
            "orders-consumer-recovery",
            "member-1",
            7,
            None,
            &[ConsumerGroupOffset::new("orders", 0, 43)],
        )
        .await
        .expect("Admin OffsetCommit should resolve topic UUID and negotiate v10");
    assert!(altered.is_success());
    assert_eq!(altered.topics()[0].topic(), "orders");

    let commit_observations = commit_coordinator
        .finish()
        .await
        .expect("member-aware v10 commit coordinator should finish");
    assert_eq!(
        commit_observations
            .iter()
            .map(|request| (request.api_key, request.api_version))
            .collect::<Vec<_>>(),
        [(18, 3), (3, 12), (8, 10)]
    );
    commit_bootstrap
        .finish()
        .await
        .expect("member-aware v10 commit bootstrap should finish");
}

#[tokio::test]
async fn admin_member_aware_offsets_fall_back_to_v9_without_metadata_v12() {
    let coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_member_offset_v10_without_metadata_response_body()),
        ScriptedResponse::Respond(offset_fetch_v9_response_body_at(42)),
    ])
    .await
    .expect("member-aware fallback coordinator should bind");
    let bootstrap = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        find_coordinator_response_for_address(coordinator.address()),
    )])
    .await
    .expect("member-aware fallback bootstrap should bind");

    let admin = AdminClient::new(
        ClientConfig::new([bootstrap.address().to_string()]).request_timeout_ms(1_000),
    );
    let listed = admin
        .list_consumer_group_offsets_with_member(
            "orders-consumer-recovery",
            Some("member-1"),
            7,
            Some(&[ConsumerGroupOffsetQuery::new("orders", [0])]),
            true,
        )
        .await
        .expect("Admin OffsetFetch should fall back to v9");
    assert!(listed.is_success());
    assert_eq!(listed.topics()[0].partitions()[0].committed_offset(), 42);

    let observations = coordinator
        .finish()
        .await
        .expect("member-aware fallback coordinator should finish");
    assert_eq!(
        observations
            .iter()
            .map(|request| (request.api_key, request.api_version))
            .collect::<Vec<_>>(),
        [(18, 3), (9, 9)]
    );
    bootstrap
        .finish()
        .await
        .expect("member-aware fallback bootstrap should finish");
}

#[tokio::test]
async fn idempotent_producer_retries_dropped_response_with_same_batch_sequence() {
    let broker = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(init_producer_id_response_body()),
        ScriptedResponse::RespondWithAddressAndClose(metadata_response_for_address),
        ScriptedResponse::Respond(api_versions_response_body(3)),
        ScriptedResponse::Drop,
        ScriptedResponse::RespondWithAddressAndClose(metadata_response_for_address),
        ScriptedResponse::Respond(api_versions_response_body(3)),
        ScriptedResponse::Respond(produce_v3_response_body(46, -1)),
    ])
    .await
    .expect("scripted broker should bind");
    let address = broker.address();
    let metrics = ClientMetrics::new();
    let mut producer = ProducerConfig::new([address.to_string()])
        .metrics(metrics.clone())
        .request_timeout_ms(1_000)
        .enable_idempotence(true)
        .max_retries(1)
        .build()
        .await
        .expect("idempotent producer should initialize");

    let metadata = producer
        .send_batch([ProducerRecord::to("orders").partition(0).value("value")])
        .await
        .expect("duplicate produce response should be treated as success");

    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].offset(), -1);
    assert!(metrics.snapshot().retries >= 1);
    assert_eq!(metrics.snapshot().broker_errors, 1);

    let observations = broker
        .finish()
        .await
        .expect("scripted broker should complete all producer steps");
    assert_eq!(observations.len(), 7);
    assert_eq!(observations[0].api_key, 22);
    assert_eq!(observations[1].api_key, 3);
    assert_eq!(observations[2].api_key, 18);
    assert_eq!(observations[3].api_key, 0);
    assert_eq!(observations[4].api_key, 3);
    assert_eq!(observations[5].api_key, 18);
    assert_eq!(observations[6].api_key, 0);
    assert_eq!(observations[3].frame, observations[6].frame);
}

#[tokio::test]
async fn idempotent_producer_fatal_sequence_errors_are_terminal() {
    for error_code in [45, 47, 90] {
        let broker = ScriptedBroker::start(vec![
            ScriptedResponse::Respond(init_producer_id_response_body()),
            ScriptedResponse::RespondWithAddressAndClose(metadata_response_for_address),
            ScriptedResponse::Respond(api_versions_response_body(3)),
            ScriptedResponse::Respond(produce_v3_response_body(error_code, -1)),
        ])
        .await
        .expect("scripted broker should bind");
        let address = broker.address();
        let metrics = ClientMetrics::new();
        let mut producer = ProducerConfig::new([address.to_string()])
            .metrics(metrics.clone())
            .request_timeout_ms(1_000)
            .enable_idempotence(true)
            .max_retries(1)
            .build()
            .await
            .expect("idempotent producer should initialize");

        let error = producer
            .send_batch([ProducerRecord::to("orders").partition(0).value("value")])
            .await
            .expect_err("fatal sequence errors must fail the batch");
        assert!(matches!(
            error,
            kafrust::Error::Broker { code, .. } if code == error_code
        ));

        let second_error = producer
            .send(ProducerRecord::to("orders").partition(0).value("value-2"))
            .await
            .expect_err("a producer with a fatal sequence error must stay defunct");
        assert!(matches!(
            second_error,
            kafrust::Error::Broker { code, .. } if code == error_code
        ));
        assert_eq!(metrics.snapshot().retries, 0);
        assert_eq!(metrics.snapshot().broker_errors, 1);

        let observations = broker
            .finish()
            .await
            .expect("scripted broker should complete the fatal sequence path");
        assert_eq!(observations.len(), 4);
        assert_eq!(
            observations
                .iter()
                .map(|request| request.api_key)
                .collect::<Vec<_>>(),
            [22, 3, 18, 0]
        );
    }
}

#[tokio::test]
async fn transactional_commit_response_loss_marks_outcome_unknown_and_defunct() {
    let coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::RespondAndClose(init_producer_id_response_body()),
        ScriptedResponse::Drop,
    ])
    .await
    .expect("coordinator broker should bind");
    let coordinator_address = coordinator.address();
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(find_coordinator_response_for_address(coordinator_address)),
        ScriptedResponse::RespondAndClose(find_coordinator_response_for_address(
            coordinator_address,
        )),
    ])
    .await
    .expect("bootstrap broker should bind");
    let address = bootstrap.address();
    let metrics = ClientMetrics::new();
    let mut producer = ProducerConfig::new([address.to_string()])
        .metrics(metrics.clone())
        .request_timeout_ms(1_000)
        .transactional_id("orders-tx")
        .max_retries(2)
        .build()
        .await
        .expect("transactional producer should initialize");

    producer
        .begin_transaction()
        .expect("transaction should begin");
    let error = producer
        .commit_transaction()
        .await
        .expect_err("lost EndTxn response must not be reported as committed");

    assert!(matches!(
        error,
        kafrust::Error::TransactionOutcomeUnknown {
            operation: "commit"
        }
    ));
    assert_eq!(
        producer.transaction_status(),
        Some(kafrust::TransactionStatus::Defunct)
    );
    assert!(matches!(
        producer.begin_transaction(),
        Err(kafrust::Error::TransactionProducerDefunct)
    ));
    assert_eq!(metrics.snapshot().retries, 0);

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("bootstrap broker should complete coordinator discovery");
    let coordinator_observations = coordinator
        .finish()
        .await
        .expect("coordinator broker should complete init and EndTxn");
    assert_eq!(bootstrap_observations.len(), 2);
    assert_eq!(bootstrap_observations[0].api_key, 10);
    assert_eq!(bootstrap_observations[1].api_key, 10);
    assert_eq!(coordinator_observations.len(), 2);
    assert_eq!(coordinator_observations[0].api_key, 22);
    assert_eq!(coordinator_observations[1].api_key, 26);
    assert!(coordinator_observations[1]
        .frame
        .windows(b"orders-tx".len())
        .any(|window| { window == b"orders-tx" }));
}

#[tokio::test]
async fn direct_consumer_reconnects_after_fetch_response_loss() {
    let broker = ScriptedBroker::start(vec![
        ScriptedResponse::RespondWithAddressAndClose(metadata_response_for_address),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Drop,
        ScriptedResponse::RespondWithAddressAndClose(metadata_response_for_address),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(fetch_v12_response_body_with_record(42)),
    ])
    .await
    .expect("scripted broker should bind");
    let address = broker.address();
    let metrics = ClientMetrics::new();
    let mut consumer = ConsumerConfig::new([address.to_string()])
        .metrics(metrics.clone())
        .request_timeout_ms(1_000)
        .max_retries(1)
        .build()
        .await
        .expect("consumer should connect");

    let records = consumer
        .fetch("orders", 0, 42)
        .await
        .expect("consumer should recover the dropped fetch response");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset(), 42);
    assert_eq!(records[0].key(), Some(&b"order-1"[..]));
    assert_eq!(records[0].value(), Some(&b"created"[..]));
    assert!(metrics.snapshot().retries >= 1);

    let observations = broker
        .finish()
        .await
        .expect("scripted broker should complete all consumer steps");
    assert_eq!(observations.len(), 6);
    assert_eq!(observations[0].api_key, 3);
    assert_eq!(observations[1].api_key, 18);
    assert_eq!(observations[2].api_key, 1);
    assert_eq!(observations[3].api_key, 3);
    assert_eq!(observations[4].api_key, 18);
    assert_eq!(observations[5].api_key, 1);
}

#[tokio::test]
async fn consumer_group_retries_transient_coordinator_lookup_failure() {
    let coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(join_group_response_body(1)),
        ScriptedResponse::Respond(sync_group_response_body()),
        ScriptedResponse::Respond(offset_fetch_response_body()),
    ])
    .await
    .expect("coordinator broker should bind");
    let coordinator_address = coordinator.address();
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(find_coordinator_error_response_body()),
        ScriptedResponse::Respond(find_coordinator_response_for_address(coordinator_address)),
        ScriptedResponse::Respond(metadata_response_body("orders", 0)),
        ScriptedResponse::RespondAndKeepAlive(api_versions_response_body(0)),
    ])
    .await
    .expect("bootstrap broker should bind");
    let address = bootstrap.address();
    let metrics = ClientMetrics::new();
    let group = ConsumerGroupConfig::new([address.to_string()], "orders-group")
        .with_client_config(
            ClientConfig::new([address.to_string()])
                .metrics(metrics.clone())
                .request_timeout_ms(1_000),
        )
        .subscribe("orders")
        .max_retries(1)
        .join()
        .await
        .expect("consumer group should recover coordinator lookup");

    assert_eq!(group.group_id(), "orders-group");
    assert_eq!(group.member_id(), "member-1");
    assert_eq!(group.generation_id(), 1);
    assert_eq!(group.position("orders", 0), Some(0));
    assert!(metrics.snapshot().retries >= 1);

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("bootstrap broker should complete coordinator retry and join setup");
    let coordinator_observations = coordinator
        .finish()
        .await
        .expect("coordinator broker should complete group setup");
    assert_eq!(bootstrap_observations.len(), 4);
    assert_eq!(bootstrap_observations[0].api_key, 10);
    assert_eq!(bootstrap_observations[1].api_key, 10);
    assert_eq!(bootstrap_observations[2].api_key, 3);
    assert_eq!(bootstrap_observations[3].api_key, 18);
    assert_eq!(coordinator_observations.len(), 3);
    assert_eq!(coordinator_observations[0].api_key, 11);
    assert_eq!(coordinator_observations[1].api_key, 14);
    assert_eq!(coordinator_observations[2].api_key, 9);
}

#[tokio::test]
async fn consumer_group_rejoins_after_coordinator_connection_loss() {
    let coordinator_a = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(join_group_response_body(1)),
        ScriptedResponse::Respond(sync_group_empty_response_body()),
        ScriptedResponse::Respond(offset_fetch_empty_response_body()),
        ScriptedResponse::Drop,
    ])
    .await
    .expect("initial coordinator broker should bind");
    let coordinator_b = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(join_group_response_body(2)),
        ScriptedResponse::Respond(sync_group_empty_response_body()),
        ScriptedResponse::Respond(offset_fetch_empty_response_body()),
    ])
    .await
    .expect("replacement coordinator broker should bind");
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_a.address(),
        )),
        ScriptedResponse::Respond(metadata_response_body("orders", 0)),
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_b.address(),
        )),
        ScriptedResponse::RespondAndKeepAlive(metadata_response_body("orders", 0)),
    ])
    .await
    .expect("bootstrap broker should bind");
    let address = bootstrap.address();
    let metrics = ClientMetrics::new();
    let mut group = ConsumerGroupConfig::new([address.to_string()], "orders-group")
        .with_client_config(
            ClientConfig::new([address.to_string()])
                .metrics(metrics.clone())
                .request_timeout_ms(1_000),
        )
        .subscribe("orders")
        .max_retries(1)
        .join()
        .await
        .expect("consumer group should join before coordinator loss");

    let records = group
        .poll()
        .await
        .expect("consumer group should rejoin after coordinator connection loss");

    assert!(records.is_empty());
    assert_eq!(group.group_id(), "orders-group");
    assert_eq!(group.member_id(), "member-1");
    assert_eq!(group.generation_id(), 2);
    assert!(metrics.snapshot().retries >= 1);

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("bootstrap broker should complete initial join and rejoin");
    let initial_observations = coordinator_a
        .finish()
        .await
        .expect("initial coordinator should observe the dropped heartbeat");
    let replacement_observations = coordinator_b
        .finish()
        .await
        .expect("replacement coordinator should complete the rejoin");
    assert_eq!(bootstrap_observations.len(), 4);
    assert_eq!(
        bootstrap_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [10, 3, 10, 3]
    );
    assert_eq!(initial_observations.len(), 4);
    assert_eq!(
        initial_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [11, 14, 9, 12]
    );
    assert_eq!(replacement_observations.len(), 3);
    assert_eq!(
        replacement_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [11, 14, 9]
    );
}

#[tokio::test]
async fn consumer_group_restores_assignment_and_fetches_after_rejoin() {
    let leader = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(fetch_v12_response_body_with_record(42)),
    ])
    .await
    .expect("partition leader broker should bind");
    let coordinator_a = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(join_group_response_body(1)),
        ScriptedResponse::Respond(sync_group_response_body()),
        ScriptedResponse::Respond(offset_fetch_response_body_at(42)),
        ScriptedResponse::Drop,
    ])
    .await
    .expect("initial coordinator broker should bind");
    let coordinator_b = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(join_group_response_body(2)),
        ScriptedResponse::Respond(sync_group_response_body()),
        ScriptedResponse::Respond(offset_fetch_response_body_at(42)),
    ])
    .await
    .expect("replacement coordinator broker should bind");
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_a.address(),
        )),
        ScriptedResponse::Respond(metadata_response_body("orders", leader.address().port())),
        ScriptedResponse::Respond(api_versions_response_body(0)),
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_b.address(),
        )),
        ScriptedResponse::Respond(metadata_response_body("orders", leader.address().port())),
        ScriptedResponse::Respond(api_versions_response_body(0)),
        ScriptedResponse::RespondAndKeepAlive(metadata_response_body(
            "orders",
            leader.address().port(),
        )),
    ])
    .await
    .expect("bootstrap broker should bind");

    let metrics = ClientMetrics::new();
    let mut group = ConsumerGroupConfig::new(
        [bootstrap.address().to_string()],
        "orders-assignment-recovery",
    )
    .with_client_config(
        ClientConfig::new([bootstrap.address().to_string()])
            .metrics(metrics.clone())
            .request_timeout_ms(1_000),
    )
    .subscribe("orders")
    .max_retries(1)
    .join()
    .await
    .expect("consumer group should join with an assignment");

    let records = group
        .poll()
        .await
        .expect("consumer group should rejoin and fetch after coordinator loss");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset(), 42);
    assert_eq!(group.generation_id(), 2);
    assert_eq!(group.position("orders", 0), Some(43));
    assert!(metrics.snapshot().retries >= 1);

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("bootstrap broker should complete both joins and fetch metadata");
    let initial_observations = coordinator_a
        .finish()
        .await
        .expect("initial coordinator should observe the dropped heartbeat");
    let replacement_observations = coordinator_b
        .finish()
        .await
        .expect("replacement coordinator should restore the assignment");
    let leader_observations = leader
        .finish()
        .await
        .expect("partition leader should serve the recovered fetch");
    assert_eq!(
        bootstrap_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [10, 3, 18, 10, 3, 18, 3]
    );
    assert_eq!(
        initial_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [11, 14, 9, 12]
    );
    assert_eq!(
        replacement_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [11, 14, 9]
    );
    assert_eq!(
        leader_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [18, 1]
    );
    assert_eq!(leader_observations[1].api_version, 12);
}

#[tokio::test]
async fn consumer_protocol_rejoins_and_fetches_after_rebalance_error() {
    const TOPIC_ID: [u8; 16] = [8; 16];

    let leader = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(fetch_v12_response_body_with_record(42)),
    ])
    .await
    .expect("partition leader broker should bind");
    let coordinator_a = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(consumer_group_heartbeat_response_body(TOPIC_ID, 1)),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body_with_offset_fetch(
            12, 9,
        )),
        ScriptedResponse::Respond(offset_fetch_v9_response_body_at(42)),
        ScriptedResponse::Drop,
    ])
    .await
    .expect("initial KIP-848 coordinator broker should bind");
    let coordinator_b = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(consumer_group_heartbeat_error_response_body(27)),
        ScriptedResponse::Respond(consumer_group_heartbeat_error_response_body(27)),
        ScriptedResponse::Respond(consumer_group_heartbeat_response_body(TOPIC_ID, 2)),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(offset_fetch_v10_response_body_at(TOPIC_ID, 42)),
        ScriptedResponse::Respond(offset_commit_v10_response_body(TOPIC_ID)),
    ])
    .await
    .expect("replacement KIP-848 coordinator broker should bind");
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_a.address(),
        )),
        ScriptedResponse::Respond(metadata_v12_share_response_body(leader.address(), TOPIC_ID)),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(metadata_v12_share_response_body(leader.address(), TOPIC_ID)),
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_b.address(),
        )),
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_b.address(),
        )),
        ScriptedResponse::Respond(metadata_v12_share_response_body(leader.address(), TOPIC_ID)),
        ScriptedResponse::Respond(find_coordinator_response_for_address(
            coordinator_b.address(),
        )),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(metadata_v12_share_response_body(leader.address(), TOPIC_ID)),
        ScriptedResponse::RespondAndKeepAlive(metadata_response_body(
            "orders",
            leader.address().port(),
        )),
    ])
    .await
    .expect("KIP-848 bootstrap broker should bind");

    let metrics = ClientMetrics::new();
    let mut group = ConsumerGroupConfig::new(
        [bootstrap.address().to_string()],
        "orders-consumer-recovery",
    )
    .with_client_config(
        ClientConfig::new([bootstrap.address().to_string()])
            .metrics(metrics.clone())
            .request_timeout_ms(1_000),
    )
    .subscribe("orders")
    .group_protocol(ConsumerGroupProtocol::Consumer)
    .max_retries(1)
    .join()
    .await
    .expect("KIP-848 consumer group should join with an assignment");

    let records = group
        .poll()
        .await
        .expect("KIP-848 consumer group should rejoin and fetch after rebalance error");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset(), 42);
    assert_eq!(group.generation_id(), 2);
    assert_eq!(group.position("orders", 0), Some(43));
    assert!(metrics.snapshot().retries >= 1);
    group
        .commit_offsets()
        .await
        .expect("KIP-848 consumer group should commit with OffsetCommit v10");

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("KIP-848 bootstrap broker should complete retries, rejoin, and fetch metadata");
    let initial_observations = coordinator_a
        .finish()
        .await
        .expect("initial KIP-848 coordinator should observe heartbeat loss");
    let replacement_observations = coordinator_b
        .finish()
        .await
        .expect("replacement KIP-848 coordinator should complete retry and rejoin assignment");
    let leader_observations = leader
        .finish()
        .await
        .expect("partition leader should serve the KIP-848 recovered fetch");
    assert_eq!(
        bootstrap_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [10, 3, 18, 3, 10, 10, 3, 10, 18, 3, 3]
    );
    assert_eq!(
        initial_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [68, 18, 9, 68]
    );
    assert_eq!(initial_observations[2].api_version, 9);
    assert_eq!(
        replacement_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [68, 68, 68, 18, 9, 8]
    );
    assert_eq!(replacement_observations[4].api_version, 10);
    assert_eq!(replacement_observations[5].api_version, 10);
    assert_eq!(
        leader_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [18, 1]
    );
    assert_eq!(leader_observations[1].api_version, 12);
}

#[tokio::test]
async fn consumer_protocol_regex_refreshes_unknown_topic_uuid_assignment() {
    const INITIAL_TOPIC_ID: [u8; 16] = [8; 16];
    const NEW_TOPIC_ID: [u8; 16] = [9; 16];

    let metadata_body = metadata_v12_response_body_for_topics(
        "127.0.0.1:9092".parse().unwrap(),
        &[("orders", INITIAL_TOPIC_ID), ("orders-new", NEW_TOPIC_ID)],
    );
    let mut metadata_decoder = Decoder::new(&metadata_body);
    metadata_decoder.read_tagged_fields().unwrap();
    let metadata = MetadataResponseV12::decode_body(&mut metadata_decoder).unwrap();
    assert_eq!(
        metadata
            .topics
            .iter()
            .map(|topic| (topic.name.as_deref(), topic.topic_id))
            .collect::<Vec<_>>(),
        vec![
            (Some("orders"), INITIAL_TOPIC_ID),
            (Some("orders-new"), NEW_TOPIC_ID),
        ]
    );

    let leader = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(fetch_v12_response_body_for_topic("orders-new", 42)),
    ])
    .await
    .expect("regex assignment leader broker should bind");
    let coordinator = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(consumer_group_heartbeat_response_body(INITIAL_TOPIC_ID, 1)),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(offset_fetch_v10_response_body_at(INITIAL_TOPIC_ID, -1)),
        ScriptedResponse::Respond(consumer_group_heartbeat_response_body(NEW_TOPIC_ID, 2)),
        ScriptedResponse::Respond(offset_fetch_v10_response_body_at(NEW_TOPIC_ID, -1)),
    ])
    .await
    .expect("regex assignment coordinator broker should bind");
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(metadata_response_body("orders", leader.address().port())),
        ScriptedResponse::Respond(find_coordinator_response_for_address(coordinator.address())),
        ScriptedResponse::Respond(metadata_v12_response_body_for_topics(
            leader.address(),
            &[("orders", INITIAL_TOPIC_ID)],
        )),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(metadata_v12_response_body_for_topics(
            leader.address(),
            &[("orders", INITIAL_TOPIC_ID)],
        )),
        ScriptedResponse::Respond(metadata_response_body_for_topics(
            &["orders", "orders-new"],
            leader.address().port(),
        )),
        ScriptedResponse::Respond(metadata_v12_response_body_for_topics(
            leader.address(),
            &[("orders", INITIAL_TOPIC_ID), ("orders-new", NEW_TOPIC_ID)],
        )),
        ScriptedResponse::Respond(api_versions_metadata_fetch_response_body(12)),
        ScriptedResponse::Respond(metadata_v12_response_body_for_topics(
            leader.address(),
            &[("orders", INITIAL_TOPIC_ID), ("orders-new", NEW_TOPIC_ID)],
        )),
        ScriptedResponse::Respond(metadata_response_body(
            "orders-new",
            leader.address().port(),
        )),
    ])
    .await
    .expect("regex assignment bootstrap broker should bind");

    let mut group = ConsumerGroupConfig::new(
        [bootstrap.address().to_string()],
        "orders-consumer-recovery",
    )
    .with_client_config(
        ClientConfig::new([bootstrap.address().to_string()]).request_timeout_ms(1_000),
    )
    .subscribe_pattern(r"^orders(-new)?$")
    .group_protocol(ConsumerGroupProtocol::Consumer)
    .max_retries(1)
    .join()
    .await
    .expect("regex KIP-848 consumer group should join");

    let records = group
        .poll()
        .await
        .expect("regex KIP-848 consumer group should refresh unknown topic UUID");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].topic(), "orders-new");
    assert_eq!(records[0].offset(), 42);
    assert_eq!(group.position("orders-new", 0), Some(43));

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("regex assignment bootstrap should complete metadata refresh");
    let coordinator_observations = coordinator
        .finish()
        .await
        .expect("regex assignment coordinator should complete heartbeats");
    let leader_observations = leader
        .finish()
        .await
        .expect("regex assignment leader should serve the new topic");

    assert_eq!(
        bootstrap_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [3, 10, 3, 18, 3, 3, 3, 18, 3, 3]
    );
    assert_eq!(
        coordinator_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [68, 18, 9, 68, 9]
    );
    assert_eq!(
        leader_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [18, 1]
    );
    assert_eq!(leader_observations[1].api_version, 12);
}

#[tokio::test]
async fn share_consumer_marks_lost_acknowledgement_response_unknown() {
    const TOPIC_ID: [u8; 16] = [6; 16];

    let coordinator = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        share_heartbeat_response_body(TOPIC_ID),
    )])
    .await
    .expect("share coordinator broker should bind");
    let leader = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(share_fetch_response_body_with_record(TOPIC_ID, 10)),
        ScriptedResponse::Drop,
    ])
    .await
    .expect("share leader broker should bind");
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_share_response_body()),
        ScriptedResponse::Respond(find_coordinator_response_for_address(coordinator.address())),
        ScriptedResponse::Respond(metadata_v12_share_response_body(leader.address(), TOPIC_ID)),
        ScriptedResponse::RespondAndKeepAlive(metadata_v12_share_response_body(
            leader.address(),
            TOPIC_ID,
        )),
    ])
    .await
    .expect("share bootstrap broker should bind");

    let metrics = ClientMetrics::new();
    let mut consumer = ShareConsumerConfig::new([bootstrap.address().to_string()], "orders-share")
        .with_client_config(
            ClientConfig::new([bootstrap.address().to_string()])
                .metrics(metrics.clone())
                .request_timeout_ms(1_000),
        )
        .subscribe("orders")
        .max_retries(1)
        .build()
        .await
        .expect("share consumer should build from negotiated API support");

    let records = consumer
        .poll()
        .await
        .expect("share consumer should fetch the acquired record");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset(), 10);
    assert_eq!(records[0].key(), Some(&b"order-1"[..]));
    assert_eq!(records[0].value(), Some(&b"created"[..]));

    consumer
        .acknowledge(&records[0], ShareAcknowledgementType::Accept)
        .expect("record acknowledgement should be staged locally");
    let error = consumer
        .commit()
        .await
        .expect_err("lost ShareAcknowledge response must remain ambiguous");
    assert!(matches!(
        error,
        kafrust::Error::ShareAcknowledgementOutcomeUnknown { broker_id: 1 }
    ));
    assert_eq!(consumer.pending_acknowledgement_reconciliation_count(), 1);
    assert_eq!(metrics.snapshot().retries, 0);

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("share bootstrap broker should complete setup and metadata refresh");
    let coordinator_observations = coordinator
        .finish()
        .await
        .expect("share coordinator should observe the initial heartbeat");
    let leader_observations = leader
        .finish()
        .await
        .expect("share leader should observe fetch and acknowledgement");
    assert_eq!(
        bootstrap_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [18, 10, 3, 3]
    );
    assert_eq!(
        coordinator_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [76]
    );
    assert_eq!(
        leader_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [78, 79]
    );
}

#[tokio::test]
async fn share_consumer_reconciles_lost_acknowledgement_with_redelivery() {
    const TOPIC_ID: [u8; 16] = [7; 16];

    let coordinator = ScriptedBroker::start(vec![ScriptedResponse::Respond(
        share_heartbeat_response_body(TOPIC_ID),
    )])
    .await
    .expect("share coordinator broker should bind");
    let leader = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(share_fetch_response_body_with_record(TOPIC_ID, 10)),
        ScriptedResponse::Drop,
        ScriptedResponse::Respond(share_fetch_response_body_with_record(TOPIC_ID, 10)),
        ScriptedResponse::Respond(share_acknowledge_success_response_body(TOPIC_ID)),
    ])
    .await
    .expect("share leader broker should bind");
    let bootstrap = ScriptedBroker::start(vec![
        ScriptedResponse::Respond(api_versions_share_response_body()),
        ScriptedResponse::Respond(find_coordinator_response_for_address(coordinator.address())),
        ScriptedResponse::Respond(metadata_v12_share_response_body(leader.address(), TOPIC_ID)),
        ScriptedResponse::RespondAndKeepAlive(metadata_v12_share_response_body(
            leader.address(),
            TOPIC_ID,
        )),
        ScriptedResponse::RespondAndKeepAlive(metadata_v12_share_response_body(
            leader.address(),
            TOPIC_ID,
        )),
    ])
    .await
    .expect("share bootstrap broker should bind");

    let mut consumer =
        ShareConsumerConfig::new([bootstrap.address().to_string()], "orders-share-reconcile")
            .with_client_config(
                ClientConfig::new([bootstrap.address().to_string()]).request_timeout_ms(1_000),
            )
            .subscribe("orders")
            .max_retries(1)
            .build()
            .await
            .expect("share consumer should build");

    let first_delivery = consumer.poll().await.expect("initial fetch should succeed");
    consumer
        .acknowledge(&first_delivery[0], ShareAcknowledgementType::Release)
        .expect("release should be staged");
    let error = consumer
        .commit()
        .await
        .expect_err("lost release response must be ambiguous");
    assert!(matches!(
        error,
        kafrust::Error::ShareAcknowledgementOutcomeUnknown { broker_id: 1 }
    ));
    assert_eq!(consumer.pending_acknowledgement_reconciliation_count(), 1);

    consumer
        .reconcile_acknowledgement_outcomes()
        .await
        .expect("reconciliation should discard the affected share session");
    let redelivered = consumer
        .poll()
        .await
        .expect("reconciliation should allow broker redelivery");
    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].offset(), 10);
    assert_eq!(consumer.pending_acknowledgement_reconciliation_count(), 0);

    consumer
        .acknowledge(&redelivered[0], ShareAcknowledgementType::Accept)
        .expect("redelivered record should accept");
    consumer
        .commit()
        .await
        .expect("replacement acknowledgement should commit");

    let bootstrap_observations = bootstrap
        .finish()
        .await
        .expect("share bootstrap broker should complete metadata refreshes");
    let coordinator_observations = coordinator
        .finish()
        .await
        .expect("share coordinator should observe the initial heartbeat");
    let leader_observations = leader
        .finish()
        .await
        .expect("share leader should observe both fetches and acknowledgements");
    assert_eq!(
        bootstrap_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [18, 10, 3, 3, 3]
    );
    assert_eq!(
        coordinator_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [76]
    );
    assert_eq!(
        leader_observations
            .iter()
            .map(|request| request.api_key)
            .collect::<Vec<_>>(),
        [78, 79, 78, 79]
    );
}

fn metadata_response_body(topic: &str, port: u16) -> Vec<u8> {
    metadata_response_body_for_topics(&[topic], port)
}

fn metadata_response_body_for_topics(topics: &[&str], port: u16) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i32(1);
    encoder.write_i32(1);
    encoder.write_string("127.0.0.1").unwrap();
    encoder.write_i32(i32::from(port));
    encoder.write_nullable_string(None).unwrap();
    encoder.write_i32(1);
    encoder.write_i32(i32::try_from(topics.len()).unwrap());
    for topic in topics {
        encoder.write_i16(0);
        encoder.write_string(topic).unwrap();
        encoder.write_bool(false);
        encoder.write_i32(1);
        encoder.write_i16(0);
        encoder.write_i32(0);
        encoder.write_i32(1);
        encoder
            .write_array(Some(&[1]), |encoder, node| {
                encoder.write_i32(*node);
                Ok(())
            })
            .unwrap();
        encoder
            .write_array(Some(&[1]), |encoder, node| {
                encoder.write_i32(*node);
                Ok(())
            })
            .unwrap();
    }
    encoder.into_bytes()
}

fn metadata_response_for_address(address: std::net::SocketAddr) -> Vec<u8> {
    metadata_response_body("orders", address.port())
}

fn init_producer_id_response_body() -> Vec<u8> {
    vec![
        0, 0, 0, 0, // throttle time
        0, 0, // success
        0, 0, 0, 0, 0, 0, 0, 42, // producer ID
        0, 3, // producer epoch
    ]
}

fn find_coordinator_response_for_address(address: std::net::SocketAddr) -> Vec<u8> {
    let host = address.ip().to_string();
    let mut encoder = Encoder::new();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_nullable_string(None).unwrap();
    encoder.write_i32(1);
    encoder.write_string(&host).unwrap();
    encoder.write_i32(i32::from(address.port()));
    encoder.into_bytes()
}

fn find_coordinator_error_response_body() -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i32(0);
    encoder.write_i16(15); // COORDINATOR_NOT_AVAILABLE
    encoder
        .write_nullable_string(Some("coordinator is temporarily unavailable"))
        .unwrap();
    encoder.write_i32(-1);
    encoder.write_string("127.0.0.1").unwrap();
    encoder.write_i32(0);
    encoder.into_bytes()
}

fn join_group_response_body(generation_id: i32) -> Vec<u8> {
    let subscription = ConsumerProtocolSubscriptionV0 {
        topics: vec!["orders".to_owned()],
        user_data: None,
    }
    .encode()
    .unwrap();
    let mut encoder = Encoder::new();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_i32(generation_id);
    encoder.write_string("range").unwrap();
    encoder.write_string("member-1").unwrap();
    encoder.write_string("member-1").unwrap();
    encoder.write_i32(1);
    encoder.write_string("member-1").unwrap();
    encoder.write_bytes(&subscription).unwrap();
    encoder.into_bytes()
}

fn sync_group_response_body() -> Vec<u8> {
    let assignment = ConsumerProtocolAssignmentV0 {
        assignments: vec![ConsumerProtocolTopicAssignment {
            topic: "orders".to_owned(),
            partitions: vec![0],
        }],
        user_data: None,
    }
    .encode()
    .unwrap();
    let mut encoder = Encoder::new();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_bytes(&assignment).unwrap();
    encoder.into_bytes()
}

fn sync_group_empty_response_body() -> Vec<u8> {
    let assignment = ConsumerProtocolAssignmentV0 {
        assignments: Vec::new(),
        user_data: None,
    }
    .encode()
    .unwrap();
    let mut encoder = Encoder::new();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_bytes(&assignment).unwrap();
    encoder.into_bytes()
}

fn offset_fetch_response_body() -> Vec<u8> {
    offset_fetch_response_body_at(-1)
}

fn offset_fetch_response_body_at(offset: i64) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i32(1);
    encoder.write_string("orders").unwrap();
    encoder.write_i32(1);
    encoder.write_i32(0);
    encoder.write_i64(offset);
    encoder.write_nullable_string(None).unwrap();
    encoder.write_i16(0);
    encoder.write_i16(0);
    encoder.into_bytes()
}

fn offset_fetch_v10_response_body_at(topic_id: [u8; 16], offset: i64) -> Vec<u8> {
    offset_fetch_v10_response_body_for_group("orders-consumer-recovery", topic_id, offset)
}

fn offset_fetch_v10_response_body_for_group(
    group_id: &str,
    topic_id: [u8; 16],
    offset: i64,
) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder
        .write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_compact_string(group_id)?;
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_uuid(&topic_id);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(0);
                    encoder.write_i64(offset);
                    encoder.write_i32(-1);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i16(0);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_i16(0);
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn offset_fetch_v9_response_body_at(offset: i64) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder
        .write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_compact_string("orders-consumer-recovery")?;
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(0);
                    encoder.write_i64(offset);
                    encoder.write_i32(-1);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i16(0);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_i16(0);
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn offset_commit_v10_response_body(topic_id: [u8; 16]) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder
        .write_compact_array(Some(&[()]), |encoder, ()| {
            encoder.write_uuid(&topic_id);
            encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i32(0);
                encoder.write_i16(0);
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn offset_fetch_empty_response_body() -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.into_bytes()
}

fn api_versions_metadata_fetch_response_body(fetch_max_version: i16) -> Vec<u8> {
    api_versions_metadata_fetch_response_body_with_offset_fetch(fetch_max_version, 10)
}

fn api_versions_metadata_fetch_response_body_with_offset_fetch(
    fetch_max_version: i16,
    offset_fetch_max_version: i16,
) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i16(0);
    encoder.write_unsigned_varint(5);
    encoder.write_i16(3);
    encoder.write_i16(0);
    encoder.write_i16(12);
    encoder.write_unsigned_varint(0);
    encoder.write_i16(1);
    encoder.write_i16(0);
    encoder.write_i16(fetch_max_version);
    encoder.write_unsigned_varint(0);
    encoder.write_i16(9);
    encoder.write_i16(0);
    encoder.write_i16(offset_fetch_max_version);
    encoder.write_unsigned_varint(0);
    encoder.write_i16(8);
    encoder.write_i16(0);
    encoder.write_i16(10);
    encoder.write_unsigned_varint(0);
    encoder.write_i32(0);
    encoder.write_unsigned_varint(0);
    encoder.into_bytes()
}

fn fetch_v12_response_body_with_record(offset: i64) -> Vec<u8> {
    fetch_v12_response_body_for_topic("orders", offset)
}

fn fetch_v12_response_body_for_topic(topic: &str, offset: i64) -> Vec<u8> {
    let mut message = Encoder::new();
    message.write_i32(0);
    message.write_i8(1);
    message.write_i8(0);
    message.write_i64(123);
    message.write_nullable_bytes(Some(b"order-1")).unwrap();
    message.write_nullable_bytes(Some(b"created")).unwrap();
    let message = message.into_bytes();

    let mut records = Encoder::new();
    records.write_i64(offset);
    records.write_i32(i32::try_from(message.len()).unwrap());
    records.write_raw(&message);
    let records = records.into_bytes();

    let mut response = Encoder::new();
    response.write_unsigned_varint(0);
    response.write_i32(0);
    response.write_i16(0);
    response.write_i32(0);
    response.write_unsigned_varint(2);
    response.write_compact_string(topic).unwrap();
    response.write_unsigned_varint(2);
    response.write_i32(0);
    response.write_i16(0);
    response.write_i64(43);
    response.write_i64(43);
    response.write_i64(0);
    response.write_unsigned_varint(1);
    response.write_i32(-1);
    response
        .write_compact_nullable_bytes(Some(&records))
        .unwrap();
    response.write_unsigned_varint(0);
    response.write_unsigned_varint(0);
    response.write_unsigned_varint(0);
    response.into_bytes()
}

fn api_versions_response_body(max_produce_version: i16) -> Vec<u8> {
    let mut body = vec![
        0, 0, // success
        2, // compact API key count: one entry
        0, 0, // Produce API key
        0, 0, // minimum version
    ];
    body.extend_from_slice(&max_produce_version.to_be_bytes());
    body.push(0); // API key tagged fields
    body.extend_from_slice(&[0, 0, 0, 0]); // throttle time
    body.push(0); // response tagged fields
    body
}

fn api_versions_member_offset_v10_response_body() -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i16(0);
    encoder.write_unsigned_varint(5);
    for (api_key, max_version) in [(3, 12), (8, 10), (9, 10), (10, 1)] {
        encoder.write_i16(api_key);
        encoder.write_i16(0);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
    }
    encoder.write_i32(0);
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn api_versions_member_offset_v10_without_metadata_response_body() -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i16(0);
    encoder.write_unsigned_varint(4);
    for (api_key, max_version) in [(8, 10), (9, 10), (10, 1)] {
        encoder.write_i16(api_key);
        encoder.write_i16(0);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
    }
    encoder.write_i32(0);
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn consumer_group_heartbeat_response_body(topic_id: [u8; 16], member_epoch: i32) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_compact_nullable_string(None).unwrap();
    encoder
        .write_compact_nullable_string(Some("member-1"))
        .unwrap();
    encoder.write_i32(member_epoch);
    encoder.write_i32(60_000);
    encoder.write_i8(1);
    encoder
        .write_compact_array(Some(&[topic_id]), |encoder, topic_id| {
            encoder.write_uuid(topic_id);
            encoder.write_compact_array(Some(&[0_i32]), |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn api_versions_share_response_body() -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_i16(0);
    encoder.write_unsigned_varint(5);
    for (api_key, max_version) in [(3, 12), (76, 1), (78, 1), (79, 1)] {
        encoder.write_i16(api_key);
        encoder.write_i16(0);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
    }
    encoder.write_i32(0);
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn share_heartbeat_response_body(topic_id: [u8; 16]) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_compact_nullable_string(None).unwrap();
    encoder
        .write_compact_nullable_string(Some("member-1"))
        .unwrap();
    encoder.write_i32(1);
    encoder.write_i32(60_000);
    encoder.write_i8(1);
    encoder
        .write_compact_array(Some(&[topic_id]), |encoder, topic_id| {
            encoder.write_uuid(topic_id);
            encoder.write_compact_array(Some(&[0_i32]), |encoder, partition| {
                encoder.write_i32(*partition);
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn consumer_group_heartbeat_error_response_body(error_code: i16) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder.write_i16(error_code);
    encoder
        .write_compact_nullable_string(Some("consumer group is rebalancing"))
        .unwrap();
    encoder
        .write_compact_nullable_string(Some("member-1"))
        .unwrap();
    encoder.write_i32(1);
    encoder.write_i32(60_000);
    encoder.write_i8(-1);
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn metadata_v12_share_response_body(
    leader_address: std::net::SocketAddr,
    topic_id: [u8; 16],
) -> Vec<u8> {
    metadata_v12_response_body_for_topics(leader_address, &[("orders", topic_id)])
}

fn metadata_v12_response_body_for_topics(
    leader_address: std::net::SocketAddr,
    topics: &[(&str, [u8; 16])],
) -> Vec<u8> {
    let host = leader_address.ip().to_string();
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder
        .write_compact_array(Some(&[1_i32]), |encoder, node_id| {
            encoder.write_i32(*node_id);
            encoder.write_compact_string(&host)?;
            encoder.write_i32(i32::from(leader_address.port()));
            encoder.write_compact_nullable_string(None)?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder
        .write_compact_nullable_string(Some("cluster"))
        .unwrap();
    encoder.write_i32(i32::try_from(topics.len()).unwrap());
    encoder
        .write_compact_array(Some(topics), |encoder, (topic_name, topic_id)| {
            encoder.write_i16(0);
            encoder.write_compact_nullable_string(Some(topic_name))?;
            encoder.write_uuid(topic_id);
            encoder.write_bool(false);
            encoder.write_compact_array(Some(&[0_i32]), |encoder, partition| {
                encoder.write_i16(0);
                encoder.write_i32(*partition);
                encoder.write_i32(1);
                encoder.write_i32(0);
                encoder.write_compact_array(Some(&[1_i32]), |encoder, node_id| {
                    encoder.write_i32(*node_id);
                    Ok(())
                })?;
                encoder.write_compact_array(Some(&[1_i32]), |encoder, node_id| {
                    encoder.write_i32(*node_id);
                    Ok(())
                })?;
                encoder.write_compact_array(Some(&[]), |_encoder, _node_id: &i32| Ok(()))?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_i32(i32::MIN);
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn share_fetch_response_body_with_record(topic_id: [u8; 16], offset: i64) -> Vec<u8> {
    let mut message = Encoder::new();
    message.write_i32(0);
    message.write_i8(1);
    message.write_i8(0);
    message.write_i64(123);
    message.write_nullable_bytes(Some(b"order-1")).unwrap();
    message.write_nullable_bytes(Some(b"created")).unwrap();
    let message = message.into_bytes();

    let mut records = Encoder::new();
    records.write_i64(offset);
    records.write_i32(i32::try_from(message.len()).unwrap());
    records.write_raw(&message);
    let records = records.into_bytes();

    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_compact_nullable_string(None).unwrap();
    encoder.write_i32(30_000);
    encoder
        .write_compact_array(Some(&[topic_id]), |encoder, topic_id| {
            encoder.write_uuid(topic_id);
            encoder.write_compact_array(Some(&[0_i32]), |encoder, partition| {
                encoder.write_i32(*partition);
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_i32(1);
                encoder.write_i32(0);
                encoder.write_empty_tagged_fields();
                encoder.write_compact_nullable_bytes(Some(&records))?;
                encoder.write_compact_array(Some(&[offset]), |encoder, first_offset| {
                    encoder.write_i64(*first_offset);
                    encoder.write_i64(*first_offset);
                    encoder.write_i16(1);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder
        .write_compact_array(Some(&[]), |_encoder, _endpoint: &()| Ok(()))
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn share_acknowledge_success_response_body(topic_id: [u8; 16]) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.write_empty_tagged_fields();
    encoder.write_i32(0);
    encoder.write_i16(0);
    encoder.write_compact_nullable_string(None).unwrap();
    encoder
        .write_compact_array(Some(&[topic_id]), |encoder, topic_id| {
            encoder.write_uuid(topic_id);
            encoder.write_compact_array(Some(&[0_i32]), |encoder, partition| {
                encoder.write_i32(*partition);
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_i32(1);
                encoder.write_i32(0);
                encoder.write_empty_tagged_fields();
                encoder.write_empty_tagged_fields();
                Ok(())
            })?;
            encoder.write_empty_tagged_fields();
            Ok(())
        })
        .unwrap();
    encoder
        .write_compact_array(Some(&[]), |_encoder, _endpoint: &()| Ok(()))
        .unwrap();
    encoder.write_empty_tagged_fields();
    encoder.into_bytes()
}

fn produce_v3_response_body(error_code: i16, base_offset: i64) -> Vec<u8> {
    let mut body = vec![
        0, 0, 0, 1, // topic count
        0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
        0, 0, 0, 1, // partition count
        0, 0, 0, 0, // partition index
    ];
    body.extend_from_slice(&error_code.to_be_bytes());
    body.extend_from_slice(&base_offset.to_be_bytes());
    body.extend_from_slice(&(-1_i64).to_be_bytes()); // log append time
    body.extend_from_slice(&0_i32.to_be_bytes()); // throttle time
    body
}
