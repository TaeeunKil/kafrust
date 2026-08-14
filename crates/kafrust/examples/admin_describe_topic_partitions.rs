mod common;

use kafrust::{AdminClient, ClientConfig, DescribeTopicPartitionsOptions, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let topic = std::env::var("KAFRUST_TOPIC")
        .map_err(|_| Error::Unsupported("KAFRUST_TOPIC must be set"))?;
    let expect_unsupported = std::env::var("KAFRUST_EXPECT_UNSUPPORTED")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false);
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-describe-topic-partitions-example"),
    )?;
    let admin = AdminClient::new(config);

    let result = admin
        .describe_topic_partitions(
            std::slice::from_ref(&topic),
            DescribeTopicPartitionsOptions::new().with_response_partition_limit(2000),
        )
        .await;
    match result {
        Err(Error::Unsupported(message)) if expect_unsupported => {
            println!("DescribeTopicPartitions is unsupported as expected: {message}");
            Ok(())
        }
        Err(error) => Err(error),
        Ok(_result) if expect_unsupported => Err(Error::Unsupported(
            "broker advertised DescribeTopicPartitions unexpectedly",
        )),
        Ok(result) => {
            let topic_result = result
                .topics()
                .iter()
                .find(|listed| listed.name() == Some(topic.as_str()))
                .ok_or(Error::Unsupported(
                    "DescribeTopicPartitions response did not include the requested topic",
                ))?;
            if !topic_result.is_success() || topic_result.partitions().is_empty() {
                return Err(Error::Broker {
                    code: topic_result.error_code(),
                    context: format!("describe topic partitions for {topic}"),
                });
            }
            println!(
                "topic={} id={:02x?} partitions={} next_cursor={}",
                topic,
                topic_result.topic_id(),
                topic_result.partitions().len(),
                result.next_cursor().is_some(),
            );
            Ok(())
        }
    }
}
