use kafrust::{AdminClient, ClientConfig, CreateTopicsOptions, Error, NewTopic};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = std::env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS is required"))?;
    let topic = std::env::var("KAFRUST_ADMIN_TOPIC")
        .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_TOPIC is required"))?;
    let admin = AdminClient::new(
        ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
            .client_id("kafrust-admin-ambiguity-example"),
    );

    let error = match admin
        .create_topics(&[NewTopic::new(&topic, 1, 1)], CreateTopicsOptions::new())
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let listed = admin.list_topics().await?;
        if let Some(topic_listing) = listed.iter().find(|listing| listing.name() == topic) {
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
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous CreateTopics reconciliation did not observe the topic",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
