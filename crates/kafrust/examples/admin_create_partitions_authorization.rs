mod common;

use kafrust::{
    AdminClient, ClientConfig, CreatePartitionsOptions, DeleteTopicsOptions, Error, NewPartitions,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let topic = std::env::var("KAFRUST_ADMIN_TOPIC").map_err(|_| Error::InvalidConfiguration {
        field: "KAFRUST_ADMIN_TOPIC",
        reason: "the authorization example requires a unique topic name",
    })?;
    let expected_error = parse_i16_env("KAFRUST_EXPECT_CREATE_PARTITIONS_ERROR")?;
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-create-partitions-authorization-example"),
    )?;
    let admin = AdminClient::new(config);

    let result = admin
        .create_partitions(
            &[NewPartitions::new(&topic, 2)],
            CreatePartitionsOptions::new(),
        )
        .await?;
    let outcome = result.topics().first().ok_or(Error::Unsupported(
        "CreatePartitions returned no topic outcome",
    ))?;
    if outcome.error_code() != expected_error {
        return Err(Error::Broker {
            code: outcome.error_code(),
            context: format!(
                "CreatePartitions returned {}, expected {}",
                outcome.error_code(),
                expected_error
            ),
        });
    }

    let partition_count = topic_partition_count(&admin, &topic).await?;
    if expected_error == 0 {
        if !outcome.is_success() {
            return Err(Error::Broker {
                code: outcome.error_code(),
                context: "CreatePartitions success expectation was not successful".to_owned(),
            });
        }
        if partition_count != 2 {
            return Err(Error::Broker {
                code: expected_error,
                context: format!(
                    "CreatePartitions returned success but the topic has {partition_count} partitions"
                ),
            });
        }
        admin
            .delete_topics(&[topic.clone()], DeleteTopicsOptions::new())
            .await?;
        println!("CreatePartitions allowed for {topic}");
        return Ok(());
    }

    if outcome.is_success() {
        return Err(Error::Broker {
            code: expected_error,
            context: "CreatePartitions succeeded despite the expected authorization error"
                .to_owned(),
        });
    }
    if partition_count != 1 {
        return Err(Error::Broker {
            code: expected_error,
            context: format!(
                "CreatePartitions changed the topic to {partition_count} partitions despite the expected authorization error"
            ),
        });
    }
    println!(
        "CreatePartitions denied with expected error {expected_error}; partition count retained"
    );
    Ok(())
}

async fn topic_partition_count(admin: &AdminClient, topic: &str) -> kafrust::Result<usize> {
    admin
        .list_topics()
        .await?
        .into_iter()
        .find(|listed| listed.name() == topic)
        .map(|listed| listed.partition_count())
        .ok_or(Error::Unsupported("topic was not visible in metadata"))
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
