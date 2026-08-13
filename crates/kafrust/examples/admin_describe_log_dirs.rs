mod common;

use kafrust::{AdminClient, ClientConfig, Error, LogDirTopic};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-describe-log-dirs-example"),
    )?;
    let admin = AdminClient::new(config);
    let broker_ids = parse_broker_ids()?;
    let topics = parse_topics()?;
    let results = admin
        .describe_log_dirs(broker_ids.as_deref(), topics.as_deref())
        .await?;

    for broker in &results {
        println!(
            "broker={} error_code={} total_bytes={} usable_bytes={} cordoned={}",
            broker.broker_id(),
            broker.error_code(),
            broker.total_bytes(),
            broker.usable_bytes(),
            broker.is_cordoned(),
        );
        if !broker.is_success() {
            return Err(Error::Broker {
                code: broker.error_code(),
                context: format!("describe log dirs on broker {}", broker.broker_id()),
            });
        }
        for log_dir in broker.log_dirs() {
            if !log_dir.is_success() {
                return Err(Error::Broker {
                    code: log_dir.error_code(),
                    context: format!("describe log directory {}", log_dir.path()),
                });
            }
            for topic in log_dir.topics() {
                for partition in topic.partitions() {
                    println!(
                        "  {}-{} size={} offset_lag={} future={} path={}",
                        topic.name(),
                        partition.partition_index(),
                        partition.partition_size(),
                        partition.offset_lag(),
                        partition.is_future(),
                        log_dir.path(),
                    );
                }
            }
        }
    }
    Ok(())
}

fn parse_broker_ids() -> kafrust::Result<Option<Vec<i32>>> {
    let Some(value) = std::env::var_os("KAFRUST_LOG_DIR_BROKERS") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .ok_or(Error::Unsupported("KAFRUST_LOG_DIR_BROKERS must be UTF-8"))?;
    let ids = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::Unsupported("KAFRUST_LOG_DIR_BROKERS must be CSV integers"))
        })
        .collect::<kafrust::Result<Vec<i32>>>()?;
    Ok(Some(ids))
}

fn parse_topics() -> kafrust::Result<Option<Vec<LogDirTopic>>> {
    let Some(topic) = std::env::var_os("KAFRUST_LOG_DIR_TOPIC") else {
        return Ok(None);
    };
    let topic = topic
        .into_string()
        .map_err(|_| Error::Unsupported("KAFRUST_LOG_DIR_TOPIC must be UTF-8"))?;
    let topic = LogDirTopic::new(topic);
    let topic = match std::env::var("KAFRUST_LOG_DIR_PARTITION") {
        Ok(value) => topic.partition(
            value
                .parse()
                .map_err(|_| Error::Unsupported("KAFRUST_LOG_DIR_PARTITION must be an integer"))?,
        ),
        Err(_) => topic,
    };
    Ok(Some(vec![topic]))
}
