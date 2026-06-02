use kafrust::ConsumerGroupConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut group = ConsumerGroupConfig::new([bootstrap], group_id)
        .client_id("kafrust-consumer-group")
        .subscribe(topic)
        .join()
        .await?;

    println!(
        "joined group {} as member {} generation {} with {} assignments",
        group.group_id(),
        group.member_id(),
        group.generation_id(),
        group.assignments().len()
    );

    let records = group.poll().await?;
    for record in &records {
        println!(
            "{}-{}@{}",
            record.topic(),
            record.partition(),
            record.offset()
        );
    }
    group.commit_offsets().await?;
    println!("committed offsets for {} records", records.len());

    Ok(())
}
