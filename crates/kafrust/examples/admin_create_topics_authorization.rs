mod common;

use kafrust::{
    AdminClient, ClientConfig, CreateTopicsOptions, DeleteTopicsOptions, Error, NewTopic,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let topic = std::env::var("KAFRUST_ADMIN_TOPIC").map_err(|_| Error::InvalidConfiguration {
        field: "KAFRUST_ADMIN_TOPIC",
        reason: "the authorization example requires a unique topic name",
    })?;
    let expected_error = parse_i16_env("KAFRUST_EXPECT_CREATE_TOPICS_ERROR")?;
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-create-topics-authorization-example"),
    )?;
    let admin = AdminClient::new(config);

    let result = admin
        .create_topics(&[NewTopic::new(&topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let outcome = result
        .topics()
        .first()
        .ok_or(Error::Unsupported("CreateTopics returned no topic outcome"))?;
    if outcome.error_code() != expected_error {
        return Err(Error::Broker {
            code: outcome.error_code(),
            context: format!(
                "CreateTopics returned {}, expected {}",
                outcome.error_code(),
                expected_error
            ),
        });
    }

    if expected_error == 0 {
        if !outcome.is_success() {
            return Err(Error::Broker {
                code: outcome.error_code(),
                context: "CreateTopics success expectation was not successful".to_owned(),
            });
        }
        if !admin
            .list_topics()
            .await?
            .iter()
            .any(|listed| listed.name() == topic)
        {
            return Err(Error::UnknownTopicOrPartition {
                topic,
                partition: -1,
            });
        }
        let deleted = admin
            .delete_topics(&[topic.clone()], DeleteTopicsOptions::new())
            .await?;
        let delete_outcome = deleted
            .topics()
            .first()
            .ok_or(Error::Unsupported("DeleteTopics returned no topic outcome"))?;
        if !delete_outcome.is_success() {
            return Err(Error::Broker {
                code: delete_outcome.error_code(),
                context: "cleanup DeleteTopics failed".to_owned(),
            });
        }
        println!("CreateTopics allowed for {topic}; cleanup completed");
        return Ok(());
    }

    if outcome.is_success() {
        return Err(Error::Broker {
            code: expected_error,
            context: "CreateTopics succeeded despite the expected authorization error".to_owned(),
        });
    }
    if admin
        .list_topics()
        .await?
        .iter()
        .any(|listed| listed.name() == topic)
    {
        return Err(Error::Broker {
            code: expected_error,
            context: "CreateTopics changed topic state despite the expected error".to_owned(),
        });
    }
    println!("CreateTopics denied with expected error {expected_error}; topic state unchanged");
    Ok(())
}

fn parse_i16_env(name: &'static str) -> kafrust::Result<i16> {
    let value = std::env::var(name).map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "the expected broker error code is required",
    })?;
    value.parse().map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "value must be a signed 16-bit integer",
    })
}
