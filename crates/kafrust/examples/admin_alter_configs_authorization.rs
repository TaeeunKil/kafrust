mod common;

use kafrust::{
    AdminClient, AlterConfigsOptions, ClientConfig, DeleteTopicsOptions, DescribeConfigsOptions,
    Error, TopicConfigAlteration, TopicConfigResource, TopicConfigUpdate,
};

const CONFIG_NAME: &str = "retention.ms";
const INITIAL_VALUE: &str = "60000";
const UPDATED_VALUE: &str = "120000";

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let topic = std::env::var("KAFRUST_ADMIN_TOPIC").map_err(|_| Error::InvalidConfiguration {
        field: "KAFRUST_ADMIN_TOPIC",
        reason: "the authorization example requires a unique topic name",
    })?;
    let expected_error = parse_i16_env("KAFRUST_EXPECT_ALTER_CONFIGS_ERROR")?;
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-alter-configs-authorization-example"),
    )?;
    let admin = AdminClient::new(config);
    let incremental = std::env::var_os("KAFRUST_INCREMENTAL_ALTER_CONFIGS").is_some();

    let result = if incremental {
        admin
            .incremental_alter_topic_configs(
                &[TopicConfigAlteration::new(&topic).set(CONFIG_NAME, UPDATED_VALUE)],
                AlterConfigsOptions::new(),
            )
            .await?
    } else {
        admin
            .alter_topic_configs(
                &[TopicConfigUpdate::new(&topic).set(CONFIG_NAME, UPDATED_VALUE)],
                AlterConfigsOptions::new(),
            )
            .await?
    };
    let outcome = result.resources().first().ok_or(Error::Unsupported(
        "AlterConfigs returned no resource outcome",
    ))?;
    if outcome.error_code() != expected_error {
        return Err(Error::Broker {
            code: outcome.error_code(),
            context: format!(
                "AlterConfigs returned {}, expected {}",
                outcome.error_code(),
                expected_error
            ),
        });
    }

    let value = read_config_value(&admin, &topic).await?;
    if expected_error == 0 {
        if !outcome.is_success() {
            return Err(Error::Broker {
                code: outcome.error_code(),
                context: "AlterConfigs success expectation was not successful".to_owned(),
            });
        }
        if value != UPDATED_VALUE {
            return Err(Error::Broker {
                code: expected_error,
                context: format!(
                    "AlterConfigs returned success but {CONFIG_NAME} is {value}, expected {UPDATED_VALUE}"
                ),
            });
        }
        admin
            .delete_topics(&[topic.clone()], DeleteTopicsOptions::new())
            .await?;
        println!("AlterConfigs allowed for {topic}");
        return Ok(());
    }

    if outcome.is_success() {
        return Err(Error::Broker {
            code: expected_error,
            context: "AlterConfigs succeeded despite the expected authorization error".to_owned(),
        });
    }
    if value != INITIAL_VALUE {
        return Err(Error::Broker {
            code: expected_error,
            context: format!(
                "AlterConfigs changed {CONFIG_NAME} to {value} despite the expected authorization error"
            ),
        });
    }
    let operation = if incremental {
        "IncrementalAlterConfigs"
    } else {
        "AlterConfigs"
    };
    println!("{operation} denied with expected error {expected_error}; config retained");
    Ok(())
}

async fn read_config_value(admin: &AdminClient, topic: &str) -> kafrust::Result<String> {
    let result = admin
        .describe_topic_configs(
            &[TopicConfigResource::with_keys(topic, [CONFIG_NAME])],
            DescribeConfigsOptions::new(),
        )
        .await?;
    let resource = result.resources().first().ok_or(Error::Unsupported(
        "DescribeConfigs returned no resource outcome",
    ))?;
    if !resource.is_success() {
        return Err(Error::Broker {
            code: resource.error_code(),
            context: "DescribeConfigs failed while checking AlterConfigs authorization".to_owned(),
        });
    }
    resource
        .entries()
        .iter()
        .find(|entry| entry.name() == CONFIG_NAME)
        .and_then(|entry| entry.value())
        .map(ToOwned::to_owned)
        .ok_or(Error::Unsupported(
            "DescribeConfigs returned no retention.ms value",
        ))
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
