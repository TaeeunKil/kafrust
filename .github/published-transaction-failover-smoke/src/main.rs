use std::env;
use std::io::{self, Write};
use std::time::Duration;

use kafrust::{
    ClientConfig, ConsumerConfig, Error, IsolationLevel, ProducerConfig, ProducerRecord,
};

fn required(name: &str) -> Result<String, Error> {
    env::var(name).map_err(|_| Error::Unsupported("published transaction smoke variable missing"))
}

fn max_retries() -> Result<u32, Error> {
    env::var("KAFRUST_MAX_RETRIES")
        .unwrap_or_else(|_| "300".to_owned())
        .parse()
        .map_err(|_| Error::Unsupported("KAFRUST_MAX_RETRIES must be an integer"))
}

fn pause() -> Result<Duration, Error> {
    env::var("KAFRUST_FAILOVER_PAUSE_MS")
        .unwrap_or_else(|_| "0".to_owned())
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| Error::Unsupported("KAFRUST_FAILOVER_PAUSE_MS must be milliseconds"))
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let transactional_id = required("KAFRUST_TRANSACTIONAL_ID")?;
    let max_retries = max_retries()?;

    let mut producer = ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-transaction-failover-producer")
        .transactional_id(transactional_id.clone())
        .max_retries(max_retries)
        .build()
        .await?;
    let mut coordinator_client = ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-transaction-failover-coordinator")
        .connect()
        .await?;
    let coordinator = coordinator_client
        .find_transaction_coordinator(transactional_id)
        .await?;
    if coordinator.error_code != 0 {
        return Err(Error::Broker {
            code: coordinator.error_code,
            context: "find published transaction coordinator".to_owned(),
        });
    }

    producer.begin_transaction()?;
    let produced = producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value("published transaction coordinator failover"),
        )
        .await?;
    println!("transaction coordinator node {}", coordinator.node_id);
    io::stdout().flush().map_err(Error::Io)?;

    tokio::time::sleep(pause()?).await;
    producer.commit_transaction().await?;

    let mut consumer = ConsumerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-transaction-failover-consumer")
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await?;
    let records = consumer
        .fetch(&topic, produced.partition(), produced.offset())
        .await?;
    if !records.iter().any(|record| {
        record.topic() == topic
            && record.partition() == produced.partition()
            && record.offset() == produced.offset()
            && record.value() == Some(b"published transaction coordinator failover")
    }) {
        return Err(Error::Unsupported(
            "published read_committed did not return the failover transaction",
        ));
    }

    println!(
        "published transaction failover committed and read_committed verified at {}-{}@{}",
        produced.topic(),
        produced.partition(),
        produced.offset()
    );
    Ok(())
}
