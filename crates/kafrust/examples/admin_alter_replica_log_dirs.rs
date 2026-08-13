mod common;

use kafrust::{AdminClient, ClientConfig, Error, ReplicaLogDirAssignment};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-alter-replica-log-dirs-example"),
    )?;
    let admin = AdminClient::new(config);
    let broker_id = required_env_i32("KAFRUST_REPLICA_LOG_DIR_BROKER")?;
    let topic = required_env("KAFRUST_REPLICA_LOG_DIR_TOPIC")?;
    let partition = required_env_i32("KAFRUST_REPLICA_LOG_DIR_PARTITION")?;
    let destination = required_env("KAFRUST_REPLICA_LOG_DIR_DESTINATION")?;
    let assignment = ReplicaLogDirAssignment::new(topic, partition, destination);

    let result = admin
        .alter_replica_log_dirs(broker_id, &[assignment])
        .await?;
    println!(
        "broker={} throttle_ms={} success={}",
        result.broker_id(),
        result.throttle_time().as_millis(),
        result.is_success(),
    );
    for topic in result.topics() {
        for partition in topic.partitions() {
            println!(
                "  {}-{} error_code={} success={}",
                topic.name(),
                partition.partition_index(),
                partition.error_code(),
                partition.is_success(),
            );
            if !partition.is_success() {
                return Err(Error::Broker {
                    code: partition.error_code(),
                    context: format!(
                        "alter replica log directory for {}-{}",
                        topic.name(),
                        partition.partition_index()
                    ),
                });
            }
        }
    }
    Ok(())
}

fn required_env(name: &str) -> kafrust::Result<String> {
    std::env::var(name)
        .map_err(|_| Error::Unsupported("required replica log directory variable is missing"))
}

fn required_env_i32(name: &str) -> kafrust::Result<i32> {
    required_env(name)?.parse().map_err(|_| {
        Error::Unsupported("replica log directory broker and partition variables must be integers")
    })
}
