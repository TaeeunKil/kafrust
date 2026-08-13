mod common;

use kafrust::{
    AdminClient, ClientConfig, ElectLeadersOptions, ElectionType, Error, LeaderElection,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_ELECTION_TOPIC")
        .or_else(|_| std::env::var("KAFRUST_TOPIC"))
        .unwrap_or_else(|_| "kafrust-smoke-multi".to_owned());
    let election_type = match std::env::var("KAFRUST_ELECTION_TYPE")
        .unwrap_or_else(|_| "preferred".to_owned())
        .to_ascii_lowercase()
        .as_str()
    {
        "preferred" => ElectionType::Preferred,
        "unclean" => ElectionType::Unclean,
        _ => {
            return Err(Error::Unsupported(
                "KAFRUST_ELECTION_TYPE must be preferred or unclean",
            ))
        }
    };
    let elections = if std::env::var_os("KAFRUST_ELECTION_ALL").is_some() {
        None
    } else {
        let partition = std::env::var("KAFRUST_ELECTION_PARTITION")
            .unwrap_or_else(|_| "0".to_owned())
            .parse()
            .map_err(|_| Error::Unsupported("KAFRUST_ELECTION_PARTITION must be an integer"))?;
        Some(vec![LeaderElection::new(topic.clone()).partition(partition)])
    };

    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-elect-leaders-example"),
    )?;
    let admin = AdminClient::new(config);
    let result = admin
        .elect_leaders(
            elections.as_deref(),
            election_type,
            ElectLeadersOptions::new().timeout(Duration::from_secs(30)),
        )
        .await?;
    if result.error_code() != 0 {
        return Err(Error::Broker {
            code: result.error_code(),
            context: "ElectLeaders top-level response".to_owned(),
        });
    }

    for elected_topic in result.topics() {
        for partition in elected_topic.partitions() {
            println!(
                "leader election topic={} partition={} error_code={} message={:?}",
                elected_topic.name(),
                partition.partition_index(),
                partition.error_code(),
                partition.error_message()
            );
            // Kafka uses ELECTION_NOT_NEEDED (84) for an already preferred
            // leader; that is a successful no-op for a preferred smoke check.
            if partition.error_code() != 0
                && !(election_type == ElectionType::Preferred && partition.error_code() == 84)
            {
                return Err(Error::Broker {
                    code: partition.error_code(),
                    context: format!(
                        "leader election for {}-{}",
                        elected_topic.name(),
                        partition.partition_index()
                    ),
                });
            }
        }
    }
    Ok(())
}
