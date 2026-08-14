use kafrust::{
    AdminClient, ClientConfig, CreateTopicsOptions, DeleteTopicsOptions, Error, NewTopic,
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
        "delete_topics" => qualify_delete_topics(&admin, &topic).await?,
        _ => {
            return Err(Error::Unsupported(
                "KAFRUST_ADMIN_MUTATION must be create_topics or delete_topics",
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
    wait_for_topic_state(admin, topic, true).await
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
    wait_for_topic_state(admin, topic, false).await
}

async fn wait_for_topic_state(
    admin: &AdminClient,
    topic: &str,
    expected_present: bool,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let listed = admin.list_topics().await?;
        let topic_listing = listed.iter().find(|listing| listing.name() == topic);
        if expected_present {
            if let Some(topic_listing) = topic_listing {
                if topic_listing.partition_count() != 1 {
                    return Err(Error::Unsupported(
                        "ambiguous CreateTopics reconciliation returned an unexpected partition count",
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
            if expected_present {
                return Err(Error::Unsupported(
                    "ambiguous CreateTopics reconciliation did not observe the topic",
                ));
            }
            return Err(Error::Unsupported(
                "ambiguous DeleteTopics reconciliation still observed the topic",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
