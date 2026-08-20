mod common;

use kafrust::{AdminClient, ClientConfig, DeleteTopicsOptions, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let topic = std::env::var("KAFRUST_ADMIN_TOPIC").map_err(|_| Error::InvalidConfiguration {
        field: "KAFRUST_ADMIN_TOPIC",
        reason: "the authorization example requires a unique topic name",
    })?;
    let expected_error = parse_i16_env("KAFRUST_EXPECT_DELETE_TOPICS_ERROR")?;
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-delete-topics-authorization-example"),
    )?;
    let admin = AdminClient::new(config);

    let result = admin
        .delete_topics(&[topic.clone()], DeleteTopicsOptions::new())
        .await?;
    let outcome = result
        .topics()
        .first()
        .ok_or(Error::Unsupported("DeleteTopics returned no topic outcome"))?;
    if outcome.error_code() != expected_error {
        return Err(Error::Broker {
            code: outcome.error_code(),
            context: format!(
                "DeleteTopics returned {}, expected {}",
                outcome.error_code(),
                expected_error
            ),
        });
    }

    let still_present = admin
        .list_topics()
        .await?
        .iter()
        .any(|listed| listed.name() == topic);
    if expected_error == 0 {
        if !outcome.is_success() {
            return Err(Error::Broker {
                code: outcome.error_code(),
                context: "DeleteTopics success expectation was not successful".to_owned(),
            });
        }
        if still_present {
            return Err(Error::Broker {
                code: expected_error,
                context: "DeleteTopics succeeded but the topic is still listed".to_owned(),
            });
        }
        println!("DeleteTopics allowed for {topic}");
        return Ok(());
    }

    if outcome.is_success() {
        return Err(Error::Broker {
            code: expected_error,
            context: "DeleteTopics succeeded despite the expected authorization error".to_owned(),
        });
    }
    if !still_present {
        return Err(Error::Broker {
            code: expected_error,
            context: "DeleteTopics removed the topic despite the expected error".to_owned(),
        });
    }
    println!("DeleteTopics denied with expected error {expected_error}; topic retained");
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
