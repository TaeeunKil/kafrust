use kafrust::{
    AdminClient, AlterConfigsOptions, ClientConfig, CreatePartitionsOptions, CreateTopicsOptions,
    DeleteTopicsOptions, DescribeConfigsOptions, Error, NewPartitions, NewTopic,
    TopicConfigAlteration, TopicConfigResource,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = std::env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS is required"))?;
    let topic = std::env::var("KAFRUST_ADMIN_TOPIC")
        .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_TOPIC is required"))?;
    let mutation =
        std::env::var("KAFRUST_ADMIN_MUTATION").unwrap_or_else(|_| "create_topics".into());
    let admin = AdminClient::new(
        ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
            .client_id("kafrust-admin-ambiguity-example"),
    );

    match mutation.as_str() {
        "create_topics" => qualify_create_topics(&admin, &topic).await?,
        "create_partitions" => qualify_create_partitions(&admin, &topic).await?,
        "incremental_alter_configs" => qualify_incremental_alter_configs(&admin, &topic).await?,
        "delete_topics" => qualify_delete_topics(&admin, &topic).await?,
        _ => {
            return Err(Error::Unsupported(
                "KAFRUST_ADMIN_MUTATION must be create_topics, create_partitions, incremental_alter_configs, or delete_topics",
            ))
        }
    }
    Ok(())
}

async fn qualify_create_topics(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    let error = match admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await
    {
        Ok(_) => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make CreateTopics ambiguous",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "CreateTopics"
        }
    ) {
        return Err(error);
    }
    println!("CreateTopics response was lost; outcome is explicitly unknown");
    wait_for_topic_state(admin, topic, Some(1)).await
}

async fn qualify_create_partitions(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let partitions = [NewPartitions::new(topic, 2)];
    let error = match admin
        .create_partitions(&partitions, CreatePartitionsOptions::new())
        .await
    {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make CreatePartitions ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "CreatePartitions returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "CreatePartitions"
        }
    ) {
        return Err(error);
    }
    println!("CreatePartitions response was lost; outcome is explicitly unknown");
    wait_for_topic_state(admin, topic, Some(2)).await
}

async fn qualify_delete_topics(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let topic_names = [topic.to_owned()];
    let error = match admin
        .delete_topics(&topic_names, DeleteTopicsOptions::new())
        .await
    {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make DeleteTopics ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "DeleteTopics returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "DeleteTopics"
        }
    ) {
        return Err(error);
    }
    println!("DeleteTopics response was lost; outcome is explicitly unknown");
    wait_for_topic_state(admin, topic, None).await
}

async fn qualify_incremental_alter_configs(
    admin: &AdminClient,
    topic: &str,
) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let alterations = [TopicConfigAlteration::new(topic).set("retention.ms", "120000")];
    let error =
        match admin
            .incremental_alter_topic_configs(&alterations, AlterConfigsOptions::new())
            .await
        {
            Ok(result) if !result.has_errors() => {
                return Err(Error::Unsupported(
                    "the response-drop proxy did not make IncrementalAlterConfigs ambiguous",
                ))
            }
            Ok(_) => return Err(Error::Unsupported(
                "IncrementalAlterConfigs returned a broker error before the response was dropped",
            )),
            Err(error) => error,
        };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "IncrementalAlterConfigs"
        }
    ) {
        return Err(error);
    }
    println!("IncrementalAlterConfigs response was lost; outcome is explicitly unknown");
    wait_for_topic_config_value(admin, topic, "retention.ms", "120000").await
}

async fn wait_for_topic_config_value(
    admin: &AdminClient,
    topic: &str,
    key: &str,
    expected: &str,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let response = admin
            .describe_topic_configs(
                &[TopicConfigResource::with_keys(topic, [key])],
                DescribeConfigsOptions::new(),
            )
            .await?;
        let value = response
            .resources()
            .first()
            .and_then(|resource| resource.entries().iter().find(|entry| entry.name() == key))
            .and_then(|entry| entry.value());
        if value == Some(expected) {
            println!("reconciled applied {key}={expected} for topic {topic}");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous IncrementalAlterConfigs reconciliation did not observe the expected value",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_topic_state(
    admin: &AdminClient,
    topic: &str,
    expected_partitions: Option<usize>,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let listed = admin.list_topics().await?;
        let topic_listing = listed.iter().find(|listing| listing.name() == topic);
        if let Some(expected_partitions) = expected_partitions {
            if let Some(topic_listing) = topic_listing {
                if topic_listing.partition_count() != expected_partitions {
                    return Err(Error::Unsupported(
                        "ambiguous Admin mutation reconciliation returned an unexpected partition count",
                    ));
                }
                println!(
                    "reconciled applied topic {} with {} partition",
                    topic,
                    topic_listing.partition_count()
                );
                return Ok(());
            }
        } else if topic_listing.is_none() {
            println!("reconciled applied deletion of topic {topic}");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            if expected_partitions.is_some() {
                return Err(Error::Unsupported(
                    "ambiguous Admin mutation reconciliation did not observe the expected topic state",
                ));
            }
            return Err(Error::Unsupported(
                "ambiguous Admin mutation reconciliation still observed the deleted topic",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
