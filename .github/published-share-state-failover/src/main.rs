use kafrust::protocol::api::metadata::MetadataRequestTopicV12;
use kafrust::{
    AdminClient, ClientConfig, ShareGroupStateBatch, ShareGroupStateDeleteTopic,
    ShareGroupStateInitializePartition, ShareGroupStateInitializeTopic,
    ShareGroupStateReadPartition, ShareGroupStateReadTopic, ShareGroupStateWritePartition,
    ShareGroupStateWriteTopic,
};

fn parse_bootstrap_servers(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let phase = std::env::var("KAFRUST_SHARE_STATE_PHASE")
        .map_err(|_| kafrust::Error::Unsupported("KAFRUST_SHARE_STATE_PHASE is required"))?;
    let topic_name = std::env::var("KAFRUST_SHARE_STATE_TOPIC")
        .map_err(|_| kafrust::Error::Unsupported("KAFRUST_SHARE_STATE_TOPIC is required"))?;
    let bootstrap = std::env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| kafrust::Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS is required"))?;
    let group_id = std::env::var("KAFRUST_SHARE_STATE_GROUP_ID")
        .map_err(|_| kafrust::Error::Unsupported("KAFRUST_SHARE_STATE_GROUP_ID is required"))?;
    let partition_index = std::env::var("KAFRUST_SHARE_STATE_PARTITION")
        .map_err(|_| kafrust::Error::Unsupported("KAFRUST_SHARE_STATE_PARTITION is required"))?
        .parse::<i32>()
        .map_err(|_| {
            kafrust::Error::Unsupported("KAFRUST_SHARE_STATE_PARTITION must be an integer")
        })?;

    let mut metadata_client = ClientConfig::new(parse_bootstrap_servers(&bootstrap))
        .client_id("kafrust-published-share-state-metadata")
        .connect()
        .await?;
    let metadata = metadata_client
        .metadata_v12(Some(vec![MetadataRequestTopicV12 {
            topic_id: [0; 16],
            name: Some(topic_name.clone()),
        }]))
        .await?;
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(topic_name.as_str()))
        .ok_or(kafrust::Error::Unsupported(
            "share state topic metadata is missing",
        ))?;
    if topic.error_code != 0 || topic.topic_id == [0; 16] {
        return Err(kafrust::Error::Unsupported(
            "share state topic metadata is invalid",
        ));
    }
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == partition_index)
        .ok_or(kafrust::Error::Unsupported(
            "share state partition metadata is missing",
        ))?;
    if partition.error_code != 0 {
        return Err(kafrust::Error::Unsupported(
            "share state partition metadata is invalid",
        ));
    }

    if phase == "locate" {
        let coordinator = metadata_client
            .find_share_partition_coordinator(&group_id, topic.topic_id, partition_index)
            .await?;
        if coordinator.error_code != 0 {
            return Err(kafrust::Error::Unsupported(
                "share state coordinator lookup failed",
            ));
        }
        println!(
            "KAFRUST_STATE_COORDINATOR_ENDPOINT={}:{}",
            coordinator.host, coordinator.port
        );
        return Ok(());
    }

    let admin = AdminClient::new(
        ClientConfig::new(parse_bootstrap_servers(&bootstrap))
            .client_id("kafrust-published-share-state-admin"),
    );
    let topic_id = topic.topic_id;
    let partition_epoch = partition.leader_epoch;

    match phase.as_str() {
        "write" => {
            let initialize = admin
                .initialize_share_group_state(
                    &group_id,
                    &[ShareGroupStateInitializeTopic::new(
                        topic_id,
                        [ShareGroupStateInitializePartition::new(
                            partition_index,
                            0,
                            0,
                        )],
                    )],
                )
                .await?;
            if !initialize.is_success() {
                return Err(kafrust::Error::Unsupported(
                    "share state initialization failed",
                ));
            }

            let write = admin
                .write_share_group_state(
                    &group_id,
                    &[ShareGroupStateWriteTopic::new(
                        topic_id,
                        [ShareGroupStateWritePartition::new(
                            partition_index,
                            0,
                            partition_epoch,
                            0,
                            [ShareGroupStateBatch::new(0, 0, 0, 0)],
                        )
                        .with_delivery_complete_count(0)],
                    )],
                )
                .await?;
            if !write.is_success() {
                return Err(kafrust::Error::Unsupported("share state write failed"));
            }
        }
        "read" => {
            let read = admin
                .read_share_group_state(
                    &group_id,
                    &[ShareGroupStateReadTopic::new(
                        topic_id,
                        [ShareGroupStateReadPartition::new(
                            partition_index,
                            partition_epoch,
                        )],
                    )],
                )
                .await?;
            if !read.is_success() {
                return Err(kafrust::Error::Unsupported("share state read failed"));
            }
            let read_partition = read
                .topics()
                .first()
                .and_then(|topic| topic.partitions().first())
                .ok_or(kafrust::Error::Unsupported(
                    "share state read returned no partition",
                ))?;
            if read_partition.partition() != partition_index
                || read_partition.start_offset() != 0
                || read_partition.state_batches().len() != 1
                || read_partition.state_batches()[0].delivery_state() != 0
            {
                return Err(kafrust::Error::Unsupported(
                    "share state read returned unexpected data",
                ));
            }

            let summary = admin
                .read_share_group_state_summary(
                    &group_id,
                    &[ShareGroupStateReadTopic::new(
                        topic_id,
                        [ShareGroupStateReadPartition::new(
                            partition_index,
                            partition_epoch,
                        )],
                    )],
                )
                .await?;
            if !summary.is_success() {
                return Err(kafrust::Error::Unsupported("share state summary failed"));
            }
            let summary_partition = summary
                .topics()
                .first()
                .and_then(|topic| topic.partitions().first())
                .ok_or(kafrust::Error::Unsupported(
                    "share state summary returned no partition",
                ))?;
            if summary_partition.start_offset() != 0
                || summary_partition.delivery_complete_count() != Some(0)
            {
                return Err(kafrust::Error::Unsupported(
                    "share state summary returned unexpected data",
                ));
            }

            let deleted = admin
                .delete_share_group_state(
                    &group_id,
                    &[ShareGroupStateDeleteTopic::new(topic_id, [partition_index])],
                )
                .await?;
            if !deleted.is_success() {
                return Err(kafrust::Error::Unsupported("share state delete failed"));
            }
        }
        _ => {
            return Err(kafrust::Error::Unsupported(
                "KAFRUST_SHARE_STATE_PHASE must be write, locate, or read",
            ));
        }
    }

    Ok(())
}
