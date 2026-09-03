#![allow(clippy::expect_used)]

use kafrust::protocol::api::api_versions::API_KEY as API_VERSIONS_API_KEY;
use kafrust::protocol::api::fetch::API_KEY as FETCH_API_KEY;
use kafrust::protocol::api::find_coordinator::FindCoordinatorResponseV1;
use kafrust::protocol::api::list_groups::API_KEY as LIST_GROUPS_API_KEY;
use kafrust::protocol::api::list_offsets::{
    ListOffsetsPartitionV1, ListOffsetsTopicV1, API_KEY as LIST_OFFSETS_API_KEY, LATEST_TIMESTAMP,
};
use kafrust::protocol::api::metadata::MetadataRequestTopicV12;
use kafrust::protocol::api::metadata::API_KEY as METADATA_API_KEY;
use kafrust::protocol::api::offset_for_leader_epoch::{
    OffsetForLeaderEpochPartitionV3, OffsetForLeaderEpochTopicV3,
    API_KEY as OFFSET_FOR_LEADER_EPOCH_API_KEY,
};
use kafrust::protocol::api::produce::API_KEY as PRODUCE_API_KEY;
use kafrust::{
    AdminClient, ClientConfig, ConfigResourceType, ConsumerConfig, CreateTopicsOptions,
    DeleteTopicsOptions, DescribeClusterEndpointType, DescribeClusterOptions,
    DescribeConfigsOptions, ListConfigResourcesOptions, ListGroupsOptions, NewTopic,
    ProducerConfig, ProducerRecord, SecurityProtocol, ShareAcknowledgementType, ShareAcquireMode,
    ShareConsumerConfig, ShareGroupOffset, ShareGroupStateBatch, ShareGroupStateDeleteTopic,
    ShareGroupStateInitializePartition, ShareGroupStateInitializeTopic,
    ShareGroupStateReadPartition, ShareGroupStateReadTopic, ShareGroupStateWritePartition,
    ShareGroupStateWriteTopic, TopicConfigResource, UpdateFeaturesOptions,
};
use std::collections::BTreeSet;
use tokio::time::{sleep, Duration, Instant};

#[tokio::test]
async fn api_versions_and_metadata_roundtrip_when_broker_is_configured() {
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping broker roundtrip; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };

    let mut client =
        client_config_from_env(parse_bootstrap_servers(&bootstrap), "kafrust-integration")
            .expect("valid broker test configuration")
            .connect()
            .await
            .expect("connect to Kafka broker");

    let api_versions = client
        .api_versions()
        .await
        .expect("ApiVersions roundtrip should succeed");
    assert!(!api_versions.api_keys.is_empty());

    let api_versions_v3 = client
        .api_versions_v3("kafrust-integration", env!("CARGO_PKG_VERSION"))
        .await
        .expect("flexible ApiVersions roundtrip should succeed");
    assert_eq!(api_versions_v3.error_code, 0);

    let produce_advertised_max = api_versions_v3
        .highest_supported_version(PRODUCE_API_KEY, 13)
        .unwrap_or(-1);
    let produce_high_level_without_topic_id = match produce_advertised_max {
        12.. => 12,
        11 => 11,
        9..=10 => 9,
        7..=8 => 7,
        3..=6 => 3,
        2 => 2,
        _ => -1,
    };
    let produce_high_level_with_topic_id = if produce_advertised_max >= 13 {
        13
    } else {
        produce_high_level_without_topic_id
    };
    let fetch_advertised_max = api_versions_v3
        .highest_supported_version(FETCH_API_KEY, 18)
        .unwrap_or(-1);
    let fetch_high_level_version = match fetch_advertised_max {
        13.. => 13,
        12 => 12,
        11 => 11,
        _ => 4,
    };
    eprintln!(
        "data_plane_version_log produce_advertised_max={} produce_high_level_without_topic_id={} produce_high_level_with_topic_id={} fetch_advertised_max={} fetch_high_level={} metadata_v12={} list_offsets_v1=1 offset_for_leader_epoch_v3=3 api_versions_v3=3",
        produce_advertised_max,
        produce_high_level_without_topic_id,
        produce_high_level_with_topic_id,
        fetch_advertised_max,
        fetch_high_level_version,
        api_versions_v3
            .highest_supported_version(METADATA_API_KEY, 12)
            .unwrap_or(-1),
    );
    assert!(
        api_versions_v3
            .highest_supported_version(LIST_OFFSETS_API_KEY, 1)
            .is_some_and(|version| version >= 1),
        "broker must advertise ListOffsets v1"
    );
    assert!(
        api_versions_v3
            .highest_supported_version(OFFSET_FOR_LEADER_EPOCH_API_KEY, 3)
            .is_some_and(|version| version >= 3),
        "broker must advertise OffsetForLeaderEpoch v3"
    );
    assert!(
        api_versions_v3
            .highest_supported_version(API_VERSIONS_API_KEY, 3)
            .is_some_and(|version| version >= 3),
        "broker must advertise ApiVersions v3"
    );

    let metadata = client
        .metadata(None)
        .await
        .expect("Metadata roundtrip should succeed");
    assert!(!metadata.brokers.is_empty());
    if let Some(expected_brokers) = expected_brokers_from_env() {
        assert!(
            metadata.brokers.len() >= expected_brokers,
            "expected at least {expected_brokers} brokers, got {}",
            metadata.brokers.len()
        );
    }

    let admin = AdminClient::new(
        client_config_from_env(
            parse_bootstrap_servers(&bootstrap),
            "kafrust-admin-features",
        )
        .expect("valid admin feature configuration"),
    );

    if let Some(expected_version) = std::env::var("KAFRUST_EXPECT_LIST_GROUPS_VERSION")
        .ok()
        .map(|value| value.parse::<i16>().expect("valid ListGroups API version"))
    {
        let selected_version = api_versions_v3
            .highest_supported_version(LIST_GROUPS_API_KEY, 5)
            .unwrap_or(1);
        assert_eq!(selected_version, expected_version);

        let mut options = ListGroupsOptions::new();
        if expected_version >= 4 {
            options = options.state("Stable");
        }
        if expected_version >= 5 {
            options = options.group_type("consumer");
        }
        let groups = admin
            .list_groups_with_options(options)
            .await
            .expect("ListGroups negotiation and roundtrip should succeed");
        for group in groups {
            assert_eq!(group.api_version(), expected_version);
        }
    }

    let features = admin
        .describe_features()
        .await
        .expect("Kafka feature metadata should be readable through AdminClient");
    assert!(features.finalized_features_epoch() >= -1);

    if let Ok(topic) = std::env::var("KAFRUST_DATA_PLANE_TOPIC") {
        let create = admin
            .create_topics(&[NewTopic::new(&topic, 1, 1)], CreateTopicsOptions::new())
            .await
            .expect("data-plane probe topic creation should succeed");
        assert!(
            create
                .topics()
                .iter()
                .all(|result| result.is_success() || result.error_code() == 36),
            "data-plane probe topic creation returned an error: {:?}",
            create.topics()
        );

        let leader_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let metadata = client
                .metadata(Some(vec![topic.clone()]))
                .await
                .expect("data-plane probe metadata should succeed");
            let leader_ready = metadata.topics.iter().any(|result| {
                result.name == topic
                    && result.error_code == 0
                    && result
                        .partitions
                        .first()
                        .is_some_and(|partition| partition.leader_id >= 0)
            });
            if leader_ready || Instant::now() >= leader_deadline {
                assert!(
                    leader_ready,
                    "data-plane probe topic leader did not become ready"
                );
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        let list_offsets = client
            .list_offsets_v1(vec![ListOffsetsTopicV1 {
                name: topic.clone(),
                partitions: vec![ListOffsetsPartitionV1 {
                    partition_index: 0,
                    timestamp: LATEST_TIMESTAMP,
                }],
            }])
            .await
            .expect("ListOffsets v1 roundtrip should succeed");
        let list_partition = list_offsets
            .topics
            .first()
            .and_then(|result| result.partitions.first())
            .expect("ListOffsets v1 should return the probe partition");
        assert_eq!(list_partition.error_code, 0);

        let leader_epoch = client
            .offset_for_leader_epoch_v3(vec![OffsetForLeaderEpochTopicV3 {
                name: topic.clone(),
                partitions: vec![OffsetForLeaderEpochPartitionV3 {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    leader_epoch: -1,
                }],
            }])
            .await
            .expect("OffsetForLeaderEpoch v3 roundtrip should succeed");
        let leader_partition = leader_epoch
            .topics
            .first()
            .and_then(|result| result.partitions.first())
            .expect("OffsetForLeaderEpoch v3 should return the probe partition");
        assert_eq!(leader_partition.error_code, 0);
        eprintln!(
            "data_plane_roundtrip_log topic={} list_offsets_error={} list_offsets_offset={} offset_for_leader_epoch_error={} end_offset={}",
            topic,
            list_partition.error_code,
            list_partition.offset,
            leader_partition.error_code,
            leader_partition.end_offset,
        );

        let mut producer = ProducerConfig::new(parse_bootstrap_servers(&bootstrap))
            .client_id("kafrust-data-plane-version-probe-producer")
            .build()
            .await
            .expect("Produce data-plane probe should connect");
        let produced = producer
            .send(
                ProducerRecord::to(topic.clone())
                    .key("data-plane-probe-key")
                    .value("data-plane-probe-value"),
            )
            .await
            .expect("Produce data-plane probe should succeed");
        let mut consumer = ConsumerConfig::new(parse_bootstrap_servers(&bootstrap))
            .client_id("kafrust-data-plane-version-probe-consumer")
            .build()
            .await
            .expect("Fetch data-plane probe should connect");
        let fetched = consumer
            .fetch(topic.clone(), produced.partition(), produced.offset())
            .await
            .expect("Fetch data-plane probe should succeed");
        assert!(
            fetched.iter().any(|record| {
                record.offset() == produced.offset()
                    && record.key() == Some(b"data-plane-probe-key")
                    && record.value() == Some(b"data-plane-probe-value")
            }),
            "Fetch data-plane probe should return the produced record"
        );
        eprintln!(
            "data_plane_high_level_log produce_version={} fetch_version={} produced_offset={} fetched_records={}",
            produce_high_level_with_topic_id,
            fetch_high_level_version,
            produced.offset(),
            fetched.len(),
        );

        let delete = admin
            .delete_topics(&[topic], DeleteTopicsOptions::new())
            .await
            .expect("data-plane probe topic cleanup should succeed");
        assert!(
            delete.topics().iter().all(|result| result.is_success()),
            "data-plane probe topic cleanup returned an error: {:?}",
            delete.topics()
        );
    }

    if let Some(expected_version) = std::env::var("KAFRUST_EXPECT_LIST_CONFIG_RESOURCES_VERSION")
        .ok()
        .map(|value| {
            value
                .parse::<i16>()
                .expect("valid ListConfigResources API version")
        })
    {
        let selected_version = api_versions_v3
            .highest_supported_version(74, 1)
            .filter(|version| *version >= 1)
            .or_else(|| api_versions_v3.highest_supported_version(74, 0))
            .expect("broker should advertise API 74 v0 or v1");
        assert_eq!(selected_version, expected_version);
        let resource_type = std::env::var("KAFRUST_LIST_CONFIG_RESOURCES_RESOURCE_TYPE")
            .ok()
            .map(|value| {
                value
                    .parse::<i8>()
                    .map(ConfigResourceType::from_code)
                    .expect("valid ListConfigResources resource type")
            })
            .unwrap_or(ConfigResourceType::Topic);
        let result = admin
            .list_config_resources(ListConfigResourcesOptions::new().resource_type(resource_type))
            .await
            .expect("ListConfigResources roundtrip should succeed");
        assert_eq!(result.api_version(), expected_version);
        assert!(
            result.is_success(),
            "ListConfigResources returned top-level error {}",
            result.error_code()
        );
    }

    if let Some(expected_version) = std::env::var("KAFRUST_EXPECT_DESCRIBE_CLUSTER_VERSION")
        .ok()
        .map(|value| {
            value
                .parse::<i16>()
                .expect("valid DescribeCluster API version")
        })
    {
        let selected_version = api_versions_v3
            .highest_supported_version(60, 1)
            .expect("broker should advertise DescribeCluster v1");
        assert_eq!(selected_version, expected_version);
        let result = admin
            .describe_cluster_with_options(
                DescribeClusterOptions::new()
                    .include_cluster_authorized_operations(true)
                    .endpoint_type(DescribeClusterEndpointType::Brokers),
            )
            .await
            .expect("DescribeCluster roundtrip should succeed");
        assert!(result.cluster_id().is_some());
        assert_eq!(
            result.endpoint_type(),
            Some(DescribeClusterEndpointType::Brokers)
        );
        assert!(result.cluster_authorized_operations().is_some());
        assert!(!result.brokers().is_empty());
    }

    if let Some(expected_version) = std::env::var("KAFRUST_EXPECT_DESCRIBE_CONFIGS_VERSION")
        .ok()
        .map(|value| {
            value
                .parse::<i16>()
                .expect("valid DescribeConfigs API version")
        })
    {
        let selected_version = api_versions_v3
            .highest_supported_version(32, 4)
            .expect("broker should advertise DescribeConfigs v4");
        assert_eq!(selected_version, expected_version);
        let topic = std::env::var("KAFRUST_CONFIG_TOPIC")
            .expect("KAFRUST_CONFIG_TOPIC is required for DescribeConfigs qualification");
        let result = admin
            .describe_topic_configs(
                &[TopicConfigResource::new(topic)],
                DescribeConfigsOptions::new().include_documentation(true),
            )
            .await
            .expect("DescribeConfigs v4 roundtrip should succeed");
        let resource = result
            .resources()
            .first()
            .expect("DescribeConfigs should return the requested topic");
        assert!(
            resource.is_success(),
            "DescribeConfigs returned resource error {}: {:?}",
            resource.error_code(),
            resource.error_message()
        );
        assert!(
            resource
                .entries()
                .iter()
                .any(|entry| entry.config_type().is_some()),
            "DescribeConfigs v4 should preserve configuration type metadata"
        );
        assert!(
            resource
                .entries()
                .iter()
                .any(|entry| entry.documentation().is_some()),
            "DescribeConfigs v4 should preserve at least one documentation field"
        );
    }

    if let Some(expected_version) = std::env::var("KAFRUST_EXPECT_UPDATE_FEATURES_VERSION")
        .ok()
        .map(|value| {
            value
                .parse::<i16>()
                .expect("valid UpdateFeatures API version")
        })
    {
        let selected_version = api_versions_v3
            .highest_supported_version(57, 1)
            .or_else(|| api_versions_v3.highest_supported_version(57, 0))
            .expect("broker should advertise UpdateFeatures v0 or v1");
        assert_eq!(selected_version, expected_version);
        let validate_only = std::env::var("KAFRUST_UPDATE_FEATURES_VALIDATE_ONLY")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
            .unwrap_or(false);
        let feature_name = std::env::var("KAFRUST_UPDATE_FEATURES_FEATURE").ok();
        let expected_error_code = std::env::var("KAFRUST_UPDATE_FEATURES_EXPECT_ERROR")
            .ok()
            .map(|value| {
                value
                    .parse::<i16>()
                    .expect("valid UpdateFeatures error code")
            })
            .unwrap_or(0);
        let allow_downgrade = std::env::var("KAFRUST_UPDATE_FEATURES_ALLOW_DOWNGRADE")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
            .unwrap_or(false);
        let verify_level = std::env::var("KAFRUST_UPDATE_FEATURES_VERIFY_LEVEL")
            .ok()
            .map(|value| {
                value
                    .parse::<i16>()
                    .expect("valid UpdateFeatures verification level")
            });
        let updates = feature_name
            .as_ref()
            .map(|feature_name| {
                let feature_level = std::env::var("KAFRUST_UPDATE_FEATURES_LEVEL")
                    .ok()
                    .map(|value| {
                        value
                            .parse::<i16>()
                            .expect("valid UpdateFeatures feature level")
                    })
                    .or_else(|| {
                        features
                            .finalized_features()
                            .iter()
                            .find(|feature| feature.name() == feature_name)
                            .map(|feature| feature.max_version_level())
                            // Kafka may omit a supported feature from finalized_features at level 0.
                            .or_else(|| {
                                features
                                    .supported_features()
                                    .iter()
                                    .find(|feature| feature.name() == feature_name)
                                    .map(|_| 0)
                            })
                    })
                    .expect("configured UpdateFeatures feature must be supported or finalized");
                vec![
                    kafrust::FeatureUpdate::new(feature_name.clone(), feature_level)
                        .allow_downgrade(allow_downgrade),
                ]
            })
            .unwrap_or_default();
        let result = admin
            .update_features(
                &updates,
                UpdateFeaturesOptions::default().validate_only(validate_only),
            )
            .await
            .expect("UpdateFeatures request should return a typed broker result");
        assert_eq!(
            result.error_code(),
            expected_error_code,
            "UpdateFeatures returned unexpected top-level error: {:?}",
            result.error_message()
        );
        if let Some(feature_name) = feature_name {
            if expected_error_code == 0 {
                let feature_result = result
                    .results()
                    .iter()
                    .find(|feature| feature.feature() == feature_name)
                    .expect("UpdateFeatures should return the requested feature result");
                assert!(
                    feature_result.is_success(),
                    "UpdateFeatures returned feature error {}: {:?}",
                    feature_result.error_code(),
                    feature_result.error_message()
                );
                if let Some(expected_level) = verify_level {
                    let after = admin
                        .describe_features()
                        .await
                        .expect("feature state should be readable after UpdateFeatures");
                    let actual_level = after
                        .finalized_features()
                        .iter()
                        .find(|feature| feature.name() == feature_name)
                        .map(|feature| feature.max_version_level())
                        .expect("verified UpdateFeatures feature should be finalized");
                    assert_eq!(actual_level, expected_level);
                }
            }
        }
    }
}

#[tokio::test]
async fn find_group_coordinator_roundtrip_when_broker_is_configured() {
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping group coordinator roundtrip; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut client =
        client_config_from_env(parse_bootstrap_servers(&bootstrap), "kafrust-integration")
            .expect("valid broker test configuration")
            .connect()
            .await
            .expect("connect to Kafka broker");

    let coordinator = wait_for_group_coordinator(&mut client, group_id)
        .await
        .expect("FindCoordinator should return a ready coordinator");

    assert!(coordinator.node_id >= 0);
    assert!(!coordinator.host.is_empty());
    assert!(coordinator.port > 0);
}

#[tokio::test]
async fn share_consumer_roundtrip_when_broker_is_configured() {
    let Some(topic) = std::env::var("KAFRUST_SHARE_TOPIC").ok() else {
        eprintln!("skipping share consumer roundtrip; set KAFRUST_SHARE_TOPIC to run it");
        return;
    };
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping share consumer roundtrip; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };
    let group_id = std::env::var("KAFRUST_SHARE_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-share-smoke".to_owned());
    let mut consumer =
        share_consumer_config_from_env(&bootstrap, group_id.clone(), "kafrust-share-consumer")
            .subscribe(topic)
            .max_wait_ms(100)
            .max_retries(10)
            .acquire_mode(ShareAcquireMode::RecordLimit)
            .build()
            .await
            .expect("ShareConsumer should connect to the configured Kafka broker");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = consumer
            .poll()
            .await
            .expect("ShareConsumer poll should succeed");
        if let Some(record) = records.first() {
            let offset = record.offset();
            consumer
                .spawn_heartbeat_task(Duration::from_secs(1))
                .await
                .expect("ShareConsumer heartbeat task should start");
            consumer
                .acknowledge(record, ShareAcknowledgementType::Renew)
                .expect("ShareConsumer renewal should be accepted locally");
            let renewed_records = consumer
                .poll()
                .await
                .expect("ShareConsumer renewal poll should succeed");
            let renewed_record = renewed_records
                .iter()
                .find(|candidate| candidate.offset() == offset)
                .expect("renewed record should be returned by the next poll");
            assert!(consumer.acquisition_lock_timeout_ms().is_some());

            let admin = AdminClient::new(
                client_config_from_env(parse_bootstrap_servers(&bootstrap), "kafrust-share-admin")
                    .expect("valid share admin test configuration"),
            );
            let descriptions = admin
                .describe_share_groups(&[group_id.clone()], true)
                .await
                .expect("ShareGroupDescribe should inspect the active share group");
            assert_eq!(descriptions.len(), 1);
            assert_eq!(descriptions[0].group_id(), group_id);
            assert!(descriptions[0].is_success());
            assert!(
                !descriptions[0].members().is_empty(),
                "ShareGroupDescribe should expose the active member"
            );

            let record_to_complete = if std::env::var_os("KAFRUST_SHARE_TEST_EXPIRY").is_some() {
                let lock_timeout_ms = consumer
                    .acquisition_lock_timeout_ms()
                    .and_then(|timeout| u64::try_from(timeout).ok())
                    .unwrap_or(30_000)
                    .max(1_000);
                sleep(Duration::from_millis(lock_timeout_ms.saturating_add(1_000))).await;
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    let redelivered_records = consumer
                        .poll()
                        .await
                        .expect("ShareConsumer expiry poll should succeed");
                    if let Some(redelivered) = redelivered_records.iter().find(|candidate| {
                        candidate.offset() == offset
                            && candidate.delivery_count() > renewed_record.delivery_count()
                    }) {
                        break redelivered.clone();
                    }
                    assert!(
                        Instant::now() < deadline,
                        "ShareConsumer did not redeliver the expired record"
                    );
                    sleep(Duration::from_millis(100)).await;
                }
            } else {
                renewed_record.clone()
            };
            assert!(record_to_complete.delivery_count() >= renewed_record.delivery_count());
            consumer
                .acknowledge(&record_to_complete, ShareAcknowledgementType::Accept)
                .expect("ShareConsumer completion acknowledgement should be accepted locally");
            consumer
                .commit()
                .await
                .expect("ShareConsumer completion acknowledgement should commit");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ShareConsumer did not receive a record before the smoke deadline"
        );
        sleep(Duration::from_millis(100)).await;
    }

    consumer
        .stop_heartbeat_task()
        .await
        .expect("ShareConsumer heartbeat task should stop cleanly");
    consumer
        .close()
        .await
        .expect("ShareConsumer should leave the share group cleanly");
}

#[tokio::test]
async fn share_consumer_long_acknowledgement_soak_when_broker_is_configured() {
    let Some(cycles) = std::env::var("KAFRUST_SHARE_LONG_CYCLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    else {
        eprintln!("skipping share consumer acknowledgement soak; set KAFRUST_SHARE_LONG_CYCLES");
        return;
    };
    let Some(topic) = std::env::var("KAFRUST_SHARE_TOPIC").ok() else {
        eprintln!("skipping share consumer acknowledgement soak; set KAFRUST_SHARE_TOPIC");
        return;
    };
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping share consumer acknowledgement soak; set KAFRUST_BOOTSTRAP_SERVERS");
        return;
    };
    let prefix = std::env::var("KAFRUST_SHARE_LONG_PREFIX")
        .expect("KAFRUST_SHARE_LONG_PREFIX should be set");
    let group_id = std::env::var("KAFRUST_SHARE_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-share-long-soak".to_owned());
    let mut consumer =
        share_consumer_config_from_env(&bootstrap, group_id, "kafrust-share-acknowledgement-soak")
            .subscribe(topic)
            .max_wait_ms(100)
            .max_records(1)
            .batch_size(1)
            .max_retries(10)
            .acquire_mode(ShareAcquireMode::RecordLimit)
            .build()
            .await
            .expect("ShareConsumer should connect for the acknowledgement soak");
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut accepted_values = BTreeSet::new();
    let mut accepted_offsets = BTreeSet::new();

    while accepted_values.len() < cycles {
        let records = consumer
            .poll()
            .await
            .expect("ShareConsumer poll should succeed during the acknowledgement soak");
        let record_count = records.len();
        for record in records {
            let value = String::from_utf8_lossy(record.value().unwrap_or_default()).into_owned();
            assert!(
                value.starts_with(&prefix),
                "unexpected record in acknowledgement soak: {value}"
            );
            assert!(
                accepted_values.insert(value.clone()),
                "accepted record was redelivered before soak completed: {value}"
            );
            accepted_offsets.insert(record.offset());
            consumer
                .acknowledge(&record, ShareAcknowledgementType::Accept)
                .expect("acknowledgement should be accepted locally");
            consumer
                .commit()
                .await
                .expect("acknowledgement should commit during the soak");
        }
        assert!(
            Instant::now() < deadline,
            "ShareConsumer did not acknowledge {cycles} records before the soak deadline"
        );
        if record_count == 0 {
            sleep(Duration::from_millis(100)).await;
        }
    }

    assert_eq!(accepted_values.len(), cycles);
    assert_eq!(accepted_offsets.len(), cycles);
    consumer
        .close()
        .await
        .expect("ShareConsumer should close after the acknowledgement soak");
}

#[tokio::test]
async fn share_consumer_reconciles_lost_release_response_when_broker_is_configured() {
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!(
            "skipping share consumer acknowledgement ambiguity; set KAFRUST_BOOTSTRAP_SERVERS"
        );
        return;
    };
    let Some(topic) = std::env::var("KAFRUST_SHARE_AMBIGUITY_TOPIC").ok() else {
        eprintln!(
            "skipping share consumer acknowledgement ambiguity; set KAFRUST_SHARE_AMBIGUITY_TOPIC"
        );
        return;
    };
    let expected_value = std::env::var("KAFRUST_SHARE_AMBIGUITY_VALUE")
        .expect("KAFRUST_SHARE_AMBIGUITY_VALUE should be set")
        .into_bytes();
    let group_id = std::env::var("KAFRUST_SHARE_AMBIGUITY_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-share-acknowledgement-ambiguity".to_owned());
    let mut consumer = share_consumer_config_from_env(
        &bootstrap,
        group_id,
        "kafrust-share-acknowledgement-ambiguity",
    )
    .subscribe(topic)
    .max_wait_ms(100)
    .max_records(1)
    .batch_size(1)
    .max_retries(10)
    .acquire_mode(ShareAcquireMode::RecordLimit)
    .build()
    .await
    .expect("ShareConsumer should connect for acknowledgement ambiguity");

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut release_response_was_unknown = false;
    let mut accepted_redelivery = false;
    while !accepted_redelivery {
        let records = consumer
            .poll()
            .await
            .expect("ShareConsumer poll should succeed during acknowledgement reconciliation");
        for record in records {
            assert_eq!(record.value(), Some(expected_value.as_slice()));
            if !release_response_was_unknown {
                consumer
                    .acknowledge(&record, ShareAcknowledgementType::Release)
                    .expect("release acknowledgement should be accepted locally");
                let error = consumer
                    .commit()
                    .await
                    .expect_err("dropped release response must be ambiguous");
                assert!(matches!(
                    error,
                    kafrust::Error::ShareAcknowledgementOutcomeUnknown { .. }
                ));
                consumer
                    .reconcile_acknowledgement_outcomes()
                    .await
                    .expect("acknowledgement reconciliation should discard the affected session");
                let error = consumer
                    .commit()
                    .await
                    .expect_err("unknown acknowledgement must block commit until redelivery");
                assert!(matches!(
                    error,
                    kafrust::Error::ShareAcknowledgementOutcomeUnknown { .. }
                ));
                release_response_was_unknown = true;
            } else {
                consumer
                    .acknowledge(&record, ShareAcknowledgementType::Accept)
                    .expect("redelivered record should accept a replacement acknowledgement");
                consumer
                    .commit()
                    .await
                    .expect("replacement acknowledgement should commit");
                accepted_redelivery = true;
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "ShareConsumer did not reconcile the lost release response before the deadline"
        );
        if !accepted_redelivery {
            sleep(Duration::from_millis(100)).await;
        }
    }

    assert!(release_response_was_unknown);
    consumer
        .close()
        .await
        .expect("ShareConsumer should close after acknowledgement reconciliation");
}

#[tokio::test]
async fn share_group_offset_mutations_when_broker_is_configured() {
    let Some(topic) = std::env::var("KAFRUST_SHARE_TOPIC").ok() else {
        eprintln!("skipping share group offset mutation; set KAFRUST_SHARE_TOPIC to run it");
        return;
    };
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping share group offset mutation; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };
    let group_id = std::env::var("KAFRUST_SHARE_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-share-offset-smoke".to_owned());
    let admin = AdminClient::new(
        client_config_from_env(
            parse_bootstrap_servers(&bootstrap),
            "kafrust-share-offset-admin",
        )
        .expect("valid share offset admin test configuration"),
    );

    let altered = admin
        .alter_share_group_offsets(&group_id, &[ShareGroupOffset::new(topic.clone(), 0, 0)])
        .await
        .expect("AlterShareGroupOffsets should succeed for an empty share group");
    assert!(
        altered.is_success(),
        "share offset alter failed: {altered:?}"
    );

    let listed = admin
        .list_share_group_offsets(&group_id, None)
        .await
        .expect("DescribeShareGroupOffsets should list the altered share offset");
    assert!(
        listed.is_success(),
        "share offset listing failed: {listed:?}"
    );
    let listed_partition = listed
        .topics()
        .iter()
        .find(|topic_result| topic_result.topic_name() == topic)
        .and_then(|topic_result| topic_result.partitions().first())
        .expect("share offset listing should include the altered topic partition");
    assert_eq!(listed_partition.partition(), 0);
    assert_eq!(listed_partition.start_offset(), 0);

    let deleted = admin
        .delete_share_group_offsets(&group_id, std::slice::from_ref(&topic))
        .await
        .expect("DeleteShareGroupOffsets should succeed for an empty share group");
    assert!(
        deleted.is_success(),
        "share offset delete failed: {deleted:?}"
    );

    let deleted_group = admin
        .delete_share_groups(std::slice::from_ref(&group_id))
        .await
        .expect("DeleteGroups should delete the empty share group");
    assert!(
        deleted_group[0].is_success(),
        "share group delete failed: {deleted_group:?}"
    );
}

#[tokio::test]
async fn share_group_state_lifecycle_when_broker_is_configured() {
    let Some(topic_name) = std::env::var("KAFRUST_SHARE_STATE_TOPIC")
        .or_else(|_| std::env::var("KAFRUST_SHARE_TOPIC"))
        .ok()
    else {
        eprintln!(
            "skipping share group state lifecycle; set KAFRUST_SHARE_STATE_TOPIC or KAFRUST_SHARE_TOPIC"
        );
        return;
    };
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping share group state lifecycle; set KAFRUST_BOOTSTRAP_SERVERS");
        return;
    };
    let group_id = std::env::var("KAFRUST_SHARE_STATE_GROUP_ID")
        .unwrap_or_else(|_| format!("kafrust-share-state-{}", std::process::id()));

    let mut metadata_client = client_config_from_env(
        parse_bootstrap_servers(&bootstrap),
        "kafrust-share-state-metadata",
    )
    .expect("valid share state metadata configuration")
    .connect()
    .await
    .expect("connect to Kafka for share state metadata");
    let metadata = metadata_client
        .metadata_v12(Some(vec![MetadataRequestTopicV12 {
            topic_id: [0; 16],
            name: Some(topic_name.clone()),
        }]))
        .await
        .expect("Metadata v12 should return the share state topic UUID");
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(topic_name.as_str()))
        .expect("share state topic should be present in Metadata v12");
    assert_eq!(topic.error_code, 0, "share state topic metadata failed");
    assert_ne!(topic.topic_id, [0; 16], "Kafka should return a topic UUID");
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .expect("share state topic should have partition 0");
    assert_eq!(
        partition.error_code, 0,
        "share state partition metadata failed"
    );

    let admin = AdminClient::new(
        client_config_from_env(
            parse_bootstrap_servers(&bootstrap),
            "kafrust-share-state-admin",
        )
        .expect("valid share state admin configuration"),
    );
    let initialize = admin
        .initialize_share_group_state(
            &group_id,
            &[ShareGroupStateInitializeTopic::new(
                topic.topic_id,
                [ShareGroupStateInitializePartition::new(0, 0, 0)],
            )],
        )
        .await
        .expect("InitializeShareGroupState should succeed");
    assert!(
        initialize.is_success(),
        "share state initialization failed: {initialize:?}"
    );

    let write = admin
        .write_share_group_state(
            &group_id,
            &[ShareGroupStateWriteTopic::new(
                topic.topic_id,
                [ShareGroupStateWritePartition::new(
                    0,
                    0,
                    partition.leader_epoch,
                    0,
                    [ShareGroupStateBatch::new(0, 0, 0, 0)],
                )
                .with_delivery_complete_count(0)],
            )],
        )
        .await
        .expect("WriteShareGroupState v1 should succeed");
    assert!(write.is_success(), "share state write failed: {write:?}");

    let read = admin
        .read_share_group_state(
            &group_id,
            &[ShareGroupStateReadTopic::new(
                topic.topic_id,
                [ShareGroupStateReadPartition::new(0, partition.leader_epoch)],
            )],
        )
        .await
        .expect("ReadShareGroupState should succeed");
    assert!(read.is_success(), "share state read failed: {read:?}");
    let read_partition = read
        .topics()
        .first()
        .and_then(|topic| topic.partitions().first())
        .expect("share state read should return partition 0");
    assert_eq!(read_partition.partition(), 0);
    assert_eq!(read_partition.start_offset(), 0);
    assert_eq!(read_partition.state_batches().len(), 1);
    assert_eq!(read_partition.state_batches()[0].delivery_state(), 0);

    let summary = admin
        .read_share_group_state_summary(
            &group_id,
            &[ShareGroupStateReadTopic::new(
                topic.topic_id,
                [ShareGroupStateReadPartition::new(0, partition.leader_epoch)],
            )],
        )
        .await
        .expect("ReadShareGroupStateSummary should succeed");
    assert!(
        summary.is_success(),
        "share state summary failed: {summary:?}"
    );
    let summary_partition = summary
        .topics()
        .first()
        .and_then(|topic| topic.partitions().first())
        .expect("share state summary should return partition 0");
    assert_eq!(summary_partition.start_offset(), 0);
    assert_eq!(summary_partition.delivery_complete_count(), Some(0));

    let deleted = admin
        .delete_share_group_state(
            &group_id,
            &[ShareGroupStateDeleteTopic::new(topic.topic_id, [0])],
        )
        .await
        .expect("DeleteShareGroupState should succeed");
    assert!(
        deleted.is_success(),
        "share state delete failed: {deleted:?}"
    );
}

#[tokio::test]
async fn share_group_state_replica_failover_when_broker_is_configured() {
    let Some(phase) = std::env::var("KAFRUST_SHARE_STATE_PHASE").ok() else {
        eprintln!("skipping share group state failover; set KAFRUST_SHARE_STATE_PHASE to run it");
        return;
    };
    let Some(topic_name) = std::env::var("KAFRUST_SHARE_STATE_TOPIC")
        .or_else(|_| std::env::var("KAFRUST_SHARE_TOPIC"))
        .ok()
    else {
        eprintln!(
            "skipping share group state failover; set KAFRUST_SHARE_STATE_TOPIC or KAFRUST_SHARE_TOPIC"
        );
        return;
    };
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping share group state failover; set KAFRUST_BOOTSTRAP_SERVERS");
        return;
    };
    let group_id = std::env::var("KAFRUST_SHARE_STATE_GROUP_ID")
        .unwrap_or_else(|_| format!("kafrust-share-state-failover-{}", std::process::id()));
    let partition_index = std::env::var("KAFRUST_SHARE_STATE_PARTITION")
        .unwrap_or_else(|_| "0".to_owned())
        .parse::<i32>()
        .expect("KAFRUST_SHARE_STATE_PARTITION must be a valid partition index");

    let mut metadata_client = client_config_from_env(
        parse_bootstrap_servers(&bootstrap),
        "kafrust-share-state-failover-metadata",
    )
    .expect("valid share state failover metadata configuration")
    .connect()
    .await
    .expect("connect to Kafka for share state failover metadata");
    let metadata = metadata_client
        .metadata_v12(Some(vec![MetadataRequestTopicV12 {
            topic_id: [0; 16],
            name: Some(topic_name.clone()),
        }]))
        .await
        .expect("Metadata v12 should return the replicated share state topic UUID");
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(topic_name.as_str()))
        .expect("replicated share state topic should be present in Metadata v12");
    assert_eq!(topic.error_code, 0, "share state topic metadata failed");
    assert_ne!(topic.topic_id, [0; 16], "Kafka should return a topic UUID");
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == partition_index)
        .expect("replicated share state partition should be present");
    assert_eq!(
        partition.error_code, 0,
        "share state partition metadata failed"
    );

    if phase == "locate" {
        let coordinator = metadata_client
            .find_share_partition_coordinator(&group_id, topic.topic_id, partition_index)
            .await
            .expect("Share coordinator lookup should succeed");
        assert_eq!(
            coordinator.error_code, 0,
            "Share coordinator lookup returned an error"
        );
        println!(
            "KAFRUST_STATE_COORDINATOR_ENDPOINT={}:{}",
            coordinator.host, coordinator.port
        );
        return;
    }

    let admin = AdminClient::new(
        client_config_from_env(
            parse_bootstrap_servers(&bootstrap),
            "kafrust-share-state-failover-admin",
        )
        .expect("valid share state failover admin configuration"),
    );
    let topic_id = topic.topic_id;
    let partition_epoch = partition.leader_epoch;
    let partition_index_for_state = partition_index;

    match phase.as_str() {
        "write" => {
            let initialize = admin
                .initialize_share_group_state(
                    &group_id,
                    &[ShareGroupStateInitializeTopic::new(
                        topic_id,
                        [ShareGroupStateInitializePartition::new(
                            partition_index_for_state,
                            0,
                            0,
                        )],
                    )],
                )
                .await
                .expect("InitializeShareGroupState should succeed before failover");
            assert!(
                initialize.is_success(),
                "share state initialization before failover failed: {initialize:?}"
            );

            let write = admin
                .write_share_group_state(
                    &group_id,
                    &[ShareGroupStateWriteTopic::new(
                        topic_id,
                        [ShareGroupStateWritePartition::new(
                            partition_index_for_state,
                            0,
                            partition_epoch,
                            0,
                            [ShareGroupStateBatch::new(0, 0, 0, 0)],
                        )
                        .with_delivery_complete_count(0)],
                    )],
                )
                .await
                .expect("WriteShareGroupState should succeed before failover");
            assert!(
                write.is_success(),
                "share state write before failover failed: {write:?}"
            );
        }
        "read" => {
            let read = admin
                .read_share_group_state(
                    &group_id,
                    &[ShareGroupStateReadTopic::new(
                        topic_id,
                        [ShareGroupStateReadPartition::new(
                            partition_index_for_state,
                            partition_epoch,
                        )],
                    )],
                )
                .await
                .expect("ReadShareGroupState should recover from a state leader failover");
            assert!(
                read.is_success(),
                "share state read after failover failed: {read:?}"
            );
            let read_partition = read
                .topics()
                .first()
                .and_then(|topic| topic.partitions().first())
                .expect("share state read should return the replicated partition");
            assert_eq!(read_partition.partition(), partition_index_for_state);
            assert_eq!(read_partition.start_offset(), 0);
            assert_eq!(read_partition.state_batches().len(), 1);
            assert_eq!(read_partition.state_batches()[0].delivery_state(), 0);

            let summary = admin
                .read_share_group_state_summary(
                    &group_id,
                    &[ShareGroupStateReadTopic::new(
                        topic_id,
                        [ShareGroupStateReadPartition::new(
                            partition_index_for_state,
                            partition_epoch,
                        )],
                    )],
                )
                .await
                .expect("ReadShareGroupStateSummary should recover from a state leader failover");
            assert!(
                summary.is_success(),
                "share state summary after failover failed: {summary:?}"
            );
            let summary_partition = summary
                .topics()
                .first()
                .and_then(|topic| topic.partitions().first())
                .expect("share state summary should return the replicated partition");
            assert_eq!(summary_partition.start_offset(), 0);
            assert_eq!(summary_partition.delivery_complete_count(), Some(0));

            let deleted = admin
                .delete_share_group_state(
                    &group_id,
                    &[ShareGroupStateDeleteTopic::new(
                        topic_id,
                        [partition_index_for_state],
                    )],
                )
                .await
                .expect("DeleteShareGroupState should recover from a state leader failover");
            assert!(
                deleted.is_success(),
                "share state delete after failover failed: {deleted:?}"
            );
        }
        _ => eprintln!("KAFRUST_SHARE_STATE_PHASE must be write or read"),
    }
}

#[tokio::test]
async fn share_consumer_multi_broker_failover_when_broker_is_configured() {
    let Some(phase) = std::env::var("KAFRUST_SHARE_PHASE").ok() else {
        eprintln!(
            "skipping share consumer multi-broker failover; set KAFRUST_SHARE_PHASE to run it"
        );
        return;
    };
    let Some(topic) = std::env::var("KAFRUST_SHARE_TOPIC").ok() else {
        eprintln!("skipping share consumer multi-broker failover; set KAFRUST_SHARE_TOPIC");
        return;
    };
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping share consumer multi-broker failover; set KAFRUST_BOOTSTRAP_SERVERS");
        return;
    };
    let group_id = std::env::var("KAFRUST_SHARE_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-share-multi-broker-smoke".to_owned());
    let partition = std::env::var("KAFRUST_SHARE_PARTITION")
        .expect("KAFRUST_SHARE_PARTITION should be set")
        .parse::<i32>()
        .expect("KAFRUST_SHARE_PARTITION should be an integer");
    let expected_value = std::env::var("KAFRUST_SHARE_VALUE")
        .expect("KAFRUST_SHARE_VALUE should be set")
        .into_bytes();

    let mut consumer =
        share_consumer_config_from_env(&bootstrap, group_id, "kafrust-share-multi-broker")
            .subscribe(topic.clone())
            .max_wait_ms(100)
            .max_retries(10)
            .acquire_mode(ShareAcquireMode::RecordLimit)
            .build()
            .await
            .expect("ShareConsumer should connect to the configured Kafka cluster");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let records = consumer
            .poll()
            .await
            .expect("ShareConsumer poll should succeed during multi-broker failover");
        if let Some(record) = records.iter().find(|record| {
            record.topic() == topic
                && record.partition() == partition
                && record.value() == Some(expected_value.as_slice())
        }) {
            consumer
                .acknowledge(record, ShareAcknowledgementType::Accept)
                .expect("ShareConsumer should accept the failover record locally");
            consumer
                .commit()
                .await
                .expect("ShareConsumer should commit the failover acknowledgement");
            consumer
                .close()
                .await
                .expect("ShareConsumer should leave the failover share group cleanly");
            println!(
                "share consumer {phase} phase received {}-{}@{}",
                record.topic(),
                record.partition(),
                record.offset()
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "ShareConsumer did not receive the {phase} failover record before the deadline"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn share_consumer_active_heartbeat_failover_when_broker_is_configured() {
    let Some(topic) = std::env::var("KAFRUST_SHARE_TOPIC").ok() else {
        eprintln!("skipping share heartbeat failover; set KAFRUST_SHARE_TOPIC");
        return;
    };
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping share heartbeat failover; set KAFRUST_BOOTSTRAP_SERVERS");
        return;
    };
    let group_id = std::env::var("KAFRUST_SHARE_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-share-heartbeat-failover".to_owned());
    let partition = std::env::var("KAFRUST_SHARE_PARTITION")
        .expect("KAFRUST_SHARE_PARTITION should be set")
        .parse::<i32>()
        .expect("KAFRUST_SHARE_PARTITION should be an integer");
    let pre_value = std::env::var("KAFRUST_SHARE_PRE_VALUE")
        .expect("KAFRUST_SHARE_PRE_VALUE should be set")
        .into_bytes();
    let ready_file = std::env::var("KAFRUST_SHARE_HEARTBEAT_READY_FILE")
        .expect("KAFRUST_SHARE_HEARTBEAT_READY_FILE should be set");
    let cycles = std::env::var("KAFRUST_SHARE_HEARTBEAT_CYCLES")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("KAFRUST_SHARE_HEARTBEAT_CYCLES should be an integer")
        })
        .unwrap_or(1);
    assert!(
        cycles > 0,
        "ShareConsumer heartbeat failover needs one cycle"
    );
    let post_values = if let Ok(prefix) = std::env::var("KAFRUST_SHARE_HEARTBEAT_VALUE_PREFIX") {
        (1..=cycles)
            .map(|cycle| format!("{prefix}{cycle}").into_bytes())
            .collect::<Vec<_>>()
    } else {
        assert_eq!(
            cycles, 1,
            "KAFRUST_SHARE_HEARTBEAT_VALUE_PREFIX is required for repeated cycles"
        );
        vec![std::env::var("KAFRUST_SHARE_VALUE")
            .expect("KAFRUST_SHARE_VALUE should be set")
            .into_bytes()]
    };

    let mut consumer =
        share_consumer_config_from_env(&bootstrap, group_id, "kafrust-share-heartbeat")
            .subscribe(topic.clone())
            .max_wait_ms(100)
            .max_retries(10)
            .acquire_mode(ShareAcquireMode::RecordLimit)
            .build()
            .await
            .expect("ShareConsumer should connect to the configured Kafka cluster");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let records = consumer
            .poll()
            .await
            .expect("ShareConsumer should receive the pre-heartbeat record");
        if let Some(record) = records.iter().find(|record| {
            record.topic() == topic
                && record.partition() == partition
                && record.value() == Some(pre_value.as_slice())
        }) {
            consumer
                .acknowledge(record, ShareAcknowledgementType::Accept)
                .expect("ShareConsumer should accept the pre-heartbeat record locally");
            consumer
                .commit()
                .await
                .expect("ShareConsumer should commit the pre-heartbeat acknowledgement");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ShareConsumer did not receive the pre-heartbeat record before the deadline"
        );
        sleep(Duration::from_millis(100)).await;
    }

    consumer
        .spawn_heartbeat_task(Duration::from_millis(500))
        .await
        .expect("ShareConsumer heartbeat task should start before broker failure");
    sleep(Duration::from_millis(750)).await;

    for cycle in 1..=cycles {
        let cycle_ready_file = format!("{ready_file}-{cycle}-ready");
        let cycle_recovered_file = format!("{ready_file}-{cycle}-recovered");
        std::fs::write(&cycle_ready_file, b"heartbeat-running\n")
            .expect("ShareConsumer heartbeat readiness marker should be writable");
        let expected_value = &post_values[cycle - 1];
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let records = consumer
                .poll()
                .await
                .expect("ShareConsumer should recover its fetch path after coordinator failover");
            if let Some(record) = records.iter().find(|record| {
                record.topic() == topic
                    && record.partition() == partition
                    && record.value() == Some(expected_value.as_slice())
            }) {
                consumer
                    .acknowledge(record, ShareAcknowledgementType::Accept)
                    .expect("ShareConsumer should accept the post-heartbeat record locally");
                consumer
                    .commit()
                    .await
                    .expect("ShareConsumer should commit the post-heartbeat acknowledgement");
                assert!(
                    !consumer.heartbeat_task_is_finished(),
                    "ShareConsumer heartbeat task should remain alive after coordinator failover"
                );
                std::fs::write(&cycle_recovered_file, b"acknowledged\n")
                    .expect("ShareConsumer recovery marker should be writable");
                println!(
                    "share consumer heartbeat cycle {cycle} received {}-{}@{}",
                    record.topic(),
                    record.partition(),
                    record.offset()
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "ShareConsumer did not receive heartbeat cycle {cycle} record before the deadline"
            );
            sleep(Duration::from_millis(100)).await;
        }

        if cycle < cycles {
            let continue_file = format!("{ready_file}-{cycle}-continue");
            while !std::path::Path::new(&continue_file).exists() {
                assert!(
                    Instant::now() < deadline,
                    "workflow did not authorize the next heartbeat failover cycle"
                );
                sleep(Duration::from_millis(100)).await;
            }
        }
    }

    consumer
        .stop_heartbeat_task()
        .await
        .expect("ShareConsumer heartbeat task should stop cleanly after failover");
    consumer
        .close()
        .await
        .expect("ShareConsumer should leave the heartbeat failover group cleanly");
}

fn security_protocol_from_env() -> SecurityProtocol {
    let Ok(value) = std::env::var("KAFRUST_SECURITY_PROTOCOL") else {
        return SecurityProtocol::Plaintext;
    };

    parse_security_protocol(&value).expect("valid KAFRUST_SECURITY_PROTOCOL")
}

fn client_config_from_env(
    bootstrap_servers: Vec<String>,
    client_id: &str,
) -> kafrust::Result<ClientConfig> {
    let mut config = ClientConfig::new(bootstrap_servers)
        .client_id(client_id)
        .security_protocol(security_protocol_from_env());
    if let Some(credentials) = sasl_credentials_from_env()? {
        config = match credentials.mechanism {
            TestSaslMechanism::Plain => {
                config.sasl_plain(credentials.username, credentials.password)
            }
            TestSaslMechanism::ScramSha256 => {
                config.sasl_scram_sha_256(credentials.username, credentials.password)
            }
            TestSaslMechanism::ScramSha512 => {
                config.sasl_scram_sha_512(credentials.username, credentials.password)
            }
            TestSaslMechanism::OAuthBearer => {
                if credentials.username.is_empty() {
                    config.sasl_oauthbearer(credentials.token.unwrap_or_default())
                } else {
                    config.sasl_oauthbearer_with_username(
                        credentials.username,
                        credentials.token.unwrap_or_default(),
                    )
                }
            }
        };
    }
    if let Some(server_name) = tls_server_name_from_env() {
        config = config.tls_server_name(server_name);
    }
    if let Some(certificate) = tls_root_certificate_der_from_env()? {
        config = config.tls_root_certificate_der(certificate);
    }
    if let Some(certificate) = tls_client_certificate_der_from_env()? {
        config = config.tls_client_certificate_der(certificate);
    }
    if let Some(private_key) = tls_client_private_key_der_from_env()? {
        config = config.tls_client_private_key_der(private_key);
    }
    Ok(config)
}

fn share_consumer_config_from_env(
    bootstrap: &str,
    group_id: impl Into<String>,
    client_id: &str,
) -> ShareConsumerConfig {
    let bootstrap_servers = parse_bootstrap_servers(bootstrap);
    let client_config = client_config_from_env(bootstrap_servers.clone(), client_id)
        .expect("valid ShareConsumer client configuration");
    ShareConsumerConfig::new(bootstrap_servers, group_id).with_client_config(client_config)
}

struct TestSaslCredentials {
    mechanism: TestSaslMechanism,
    username: String,
    password: String,
    token: Option<String>,
}

enum TestSaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
}

fn sasl_credentials_from_env() -> kafrust::Result<Option<TestSaslCredentials>> {
    let mechanism = sasl_mechanism_from_env()?;
    if matches!(mechanism, TestSaslMechanism::OAuthBearer) {
        let token = std::env::var("KAFRUST_SASL_TOKEN").map_err(|_| {
            kafrust::Error::Unsupported("KAFRUST_SASL_TOKEN is required for SASL/OAUTHBEARER")
        })?;
        return Ok(Some(TestSaslCredentials {
            mechanism,
            username: std::env::var("KAFRUST_SASL_USERNAME").unwrap_or_default(),
            password: String::new(),
            token: Some(token),
        }));
    }

    let Some(username) = std::env::var("KAFRUST_SASL_USERNAME").ok() else {
        return Ok(None);
    };
    let password = std::env::var("KAFRUST_SASL_PASSWORD").map_err(|_| {
        kafrust::Error::Unsupported(
            "KAFRUST_SASL_PASSWORD is required when KAFRUST_SASL_USERNAME is set",
        )
    })?;
    Ok(Some(TestSaslCredentials {
        mechanism,
        username,
        password,
        token: None,
    }))
}

fn sasl_mechanism_from_env() -> kafrust::Result<TestSaslMechanism> {
    let Ok(value) = std::env::var("KAFRUST_SASL_MECHANISM") else {
        return Ok(TestSaslMechanism::Plain);
    };

    parse_sasl_mechanism(&value)
}

fn parse_sasl_mechanism(value: &str) -> kafrust::Result<TestSaslMechanism> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "" | "plain" => Ok(TestSaslMechanism::Plain),
        "scram-sha-256" => Ok(TestSaslMechanism::ScramSha256),
        "scram-sha-512" => Ok(TestSaslMechanism::ScramSha512),
        "oauthbearer" | "oauth-bearer" => Ok(TestSaslMechanism::OAuthBearer),
        _ => Err(kafrust::Error::Unsupported(
            "unsupported KAFRUST_SASL_MECHANISM; expected plain, scram-sha-256, scram-sha-512, or oauthbearer",
        )),
    }
}

fn tls_server_name_from_env() -> Option<String> {
    std::env::var("KAFRUST_TLS_SERVER_NAME").ok()
}

fn tls_root_certificate_der_from_env() -> kafrust::Result<Option<Vec<u8>>> {
    let Ok(path) = std::env::var("KAFRUST_TLS_ROOT_CERT_DER_PATH") else {
        return Ok(None);
    };

    Ok(Some(std::fs::read(path)?))
}

fn tls_client_certificate_der_from_env() -> kafrust::Result<Option<Vec<u8>>> {
    let Ok(path) = std::env::var("KAFRUST_TLS_CLIENT_CERT_DER_PATH") else {
        return Ok(None);
    };

    Ok(Some(std::fs::read(path)?))
}

fn tls_client_private_key_der_from_env() -> kafrust::Result<Option<Vec<u8>>> {
    let Ok(path) = std::env::var("KAFRUST_TLS_CLIENT_KEY_DER_PATH") else {
        return Ok(None);
    };

    Ok(Some(std::fs::read(path)?))
}

fn parse_security_protocol(value: &str) -> kafrust::Result<SecurityProtocol> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "" | "plaintext" => Ok(SecurityProtocol::Plaintext),
        "ssl" | "tls" => Ok(SecurityProtocol::Tls),
        "sasl_plaintext" => Ok(SecurityProtocol::SaslPlaintext),
        "sasl_ssl" | "sasl_tls" => Ok(SecurityProtocol::SaslTls),
        _ => Err(kafrust::Error::Unsupported(
            "unsupported KAFRUST_SECURITY_PROTOCOL; expected plaintext, tls, ssl, sasl_plaintext, sasl_ssl, or sasl_tls",
        )),
    }
}

fn parse_bootstrap_servers(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn expected_brokers_from_env() -> Option<usize> {
    std::env::var("KAFRUST_EXPECTED_BROKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

#[test]
fn parses_security_protocol_from_environment_value() {
    assert_eq!(
        parse_security_protocol("plaintext").expect("plaintext should parse"),
        SecurityProtocol::Plaintext
    );
    assert_eq!(
        parse_security_protocol("SSL").expect("SSL should parse"),
        SecurityProtocol::Tls
    );
    assert_eq!(
        parse_security_protocol("sasl-ssl").expect("sasl-ssl should parse"),
        SecurityProtocol::SaslTls
    );
}

#[test]
fn parses_sasl_mechanism_from_environment_value() {
    assert!(matches!(
        parse_sasl_mechanism("scram_sha_512").expect("SCRAM mechanism should parse"),
        TestSaslMechanism::ScramSha512
    ));
    assert!(matches!(
        parse_sasl_mechanism("oauthbearer").expect("OAuth mechanism should parse"),
        TestSaslMechanism::OAuthBearer
    ));
}

#[test]
fn parses_bootstrap_server_list_from_environment_value() {
    assert_eq!(
        parse_bootstrap_servers(" localhost:19092,localhost:19093,,localhost:19094 "),
        vec![
            "localhost:19092".to_owned(),
            "localhost:19093".to_owned(),
            "localhost:19094".to_owned(),
        ]
    );
}

async fn wait_for_group_coordinator(
    client: &mut kafrust::Client,
    group_id: String,
) -> kafrust::Result<FindCoordinatorResponseV1> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let coordinator = client.find_group_coordinator(group_id.clone()).await?;
        if coordinator.node_id >= 0 {
            return Ok(coordinator);
        }
        if Instant::now() >= deadline {
            return Ok(coordinator);
        }
        sleep(Duration::from_millis(500)).await;
    }
}
