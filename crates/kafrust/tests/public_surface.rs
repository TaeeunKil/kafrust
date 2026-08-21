#![allow(clippy::expect_used)]

use kafrust::streams::{StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTopology};
use kafrust::{
    AdminClient, ClientConfig, ConsumerConfig, ConsumerGroupConfig, ListGroupsOptions,
    ProducerConfig, SecurityProtocol, ShareConsumerConfig,
};

fn shared_client_config() -> ClientConfig {
    ClientConfig::new(["broker-a:9092", "broker-b:9092"])
        .client_id("shared-client")
        .client_rack("rack-a")
        .request_timeout_ms(1_250)
        .max_response_bytes(2 * 1024 * 1024)
}

fn assert_shared_policy(config: &ClientConfig) {
    assert_eq!(
        config.bootstrap_servers(),
        &["broker-a:9092".to_owned(), "broker-b:9092".to_owned()]
    );
    assert_eq!(config.client_id_ref(), Some("shared-client"));
    assert_eq!(config.client_rack_ref(), Some("rack-a"));
    assert_eq!(config.request_timeout().as_millis(), 1_250);
    assert_eq!(config.max_response_bytes_ref(), 2 * 1024 * 1024);
}

fn assert_mtls_policy(config: &ClientConfig) {
    assert_eq!(config.security_protocol_ref(), SecurityProtocol::Tls);
    assert_eq!(config.tls_client_certificates_der(), &[vec![1, 2, 3]]);
    assert!(config.has_tls_client_private_key());
}

#[test]
fn high_level_builders_accept_the_shared_client_configuration() {
    let shared = shared_client_config();

    let producer = ProducerConfig::new(["ignored:9092"]).with_client_config(shared.clone());
    assert_shared_policy(producer.client_config());

    let consumer = ConsumerConfig::new(["ignored:9092"]).with_client_config(shared.clone());
    assert_shared_policy(consumer.client_config());

    let group =
        ConsumerGroupConfig::new(["ignored:9092"], "orders").with_client_config(shared.clone());
    assert_shared_policy(group.client_config());

    let share =
        ShareConsumerConfig::new(["ignored:9092"], "orders").with_client_config(shared.clone());
    assert_shared_policy(share.client_config());
}

#[test]
fn exposes_modern_list_groups_options() {
    let options = ListGroupsOptions::new()
        .state("Stable")
        .group_type("consumer");

    assert_eq!(options.states_ref(), &["Stable"]);
    assert_eq!(options.group_types_ref(), &["consumer"]);
}

#[test]
fn high_level_builders_forward_mutual_tls_configuration() {
    let producer = ProducerConfig::new(["ignored:9093"])
        .security_protocol(SecurityProtocol::Tls)
        .tls_client_certificate_der([1, 2, 3])
        .tls_client_private_key_der([4, 5, 6])
        .build_config()
        .expect("producer mTLS configuration should validate without connecting");
    assert_mtls_policy(producer.client_config());

    let consumer = ConsumerConfig::new(["ignored:9093"])
        .security_protocol(SecurityProtocol::Tls)
        .tls_client_certificate_der([1, 2, 3])
        .tls_client_private_key_der([4, 5, 6])
        .build_config()
        .expect("consumer mTLS configuration should validate without connecting");
    assert_mtls_policy(consumer.client_config());

    let group = ConsumerGroupConfig::new(["ignored:9093"], "orders")
        .security_protocol(SecurityProtocol::Tls)
        .tls_client_certificate_der([1, 2, 3])
        .tls_client_private_key_der([4, 5, 6])
        .subscribe("orders")
        .build_config()
        .expect("group mTLS configuration should validate without connecting");
    assert_mtls_policy(group.client_config());

    let share = ShareConsumerConfig::new(["ignored:9093"], "orders")
        .security_protocol(SecurityProtocol::Tls)
        .tls_client_certificate_der([1, 2, 3])
        .tls_client_private_key_der([4, 5, 6])
        .subscribe("orders")
        .build_config()
        .expect("ShareConsumer mTLS configuration should validate without connecting");
    assert_mtls_policy(share.client_config());
}

#[test]
fn high_level_builders_offer_connection_free_build_config_preflight() {
    let shared = shared_client_config();

    let producer = ProducerConfig::new(["ignored:9092"])
        .with_client_config(shared.clone())
        .build_config()
        .expect("producer configuration should validate without connecting");
    assert_shared_policy(producer.client_config());

    let consumer = ConsumerConfig::new(["ignored:9092"])
        .with_client_config(shared.clone())
        .build_config()
        .expect("consumer configuration should validate without connecting");
    assert_shared_policy(consumer.client_config());

    let group = ConsumerGroupConfig::new(["ignored:9092"], "orders")
        .with_client_config(shared.clone())
        .subscribe("orders")
        .build_config()
        .expect("group configuration should validate without connecting");
    assert_shared_policy(group.client_config());

    let share = ShareConsumerConfig::new(["ignored:9092"], "orders")
        .with_client_config(shared.clone())
        .subscribe("orders")
        .build_config()
        .expect("share configuration should validate without connecting");
    assert_shared_policy(share.client_config());

    let streams = kafrust::StreamsGroupConfig::new(
        shared.bootstrap_servers().to_vec(),
        "streams",
        StreamsGroupHeartbeatTopology {
            epoch: 1,
            subtopologies: vec![StreamsGroupHeartbeatSubtopology {
                subtopology_id: "subtopology-0".to_owned(),
                source_topics: vec!["orders".to_owned()],
                source_topic_regex: Vec::new(),
                state_changelog_topics: Vec::new(),
                repartition_sink_topics: Vec::new(),
                repartition_source_topics: Vec::new(),
                copartition_groups: Vec::new(),
            }],
        },
    )
    .security_protocol(SecurityProtocol::Tls)
    .tls_client_certificate_der([1, 2, 3])
    .tls_client_private_key_der([4, 5, 6])
    .build_config()
    .expect("Streams configuration should validate without connecting");
    assert_eq!(streams.group_id(), "streams");
    let assignment = kafrust::StreamsGroupSessionAssignment::default();
    assert!(assignment.active_tasks.is_none());

    let admin_config = shared_client_config();
    let admin_metrics = admin_config.metrics_ref();
    let admin = AdminClient::new(admin_config)
        .build_config()
        .expect("admin configuration should validate without connecting");
    assert_eq!(admin.metrics(), admin_metrics);
}

#[cfg(feature = "blocking")]
#[test]
fn exposes_blocking_adapters_from_the_crate_root() {
    let _ = std::any::type_name::<kafrust::BlockingAdminClient>();
    let _ = std::any::type_name::<kafrust::BlockingBufferedProducer>();
    let _ = std::any::type_name::<kafrust::BlockingBufferedProducerHandle>();
    let _ = std::any::type_name::<kafrust::BlockingConsumer>();
    let _ = std::any::type_name::<kafrust::BlockingConsumerGroup>();
    let _ = std::any::type_name::<kafrust::BlockingProducer>();
    let _ = std::any::type_name::<kafrust::BlockingShareConsumer>();
    let _ = std::any::type_name::<kafrust::BlockingStreamsGroupSession>();
}
