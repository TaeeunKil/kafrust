mod common;

use kafrust::{AdminClient, ClientConfig, DescribeQuorumTopic, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-describe-quorum-example"),
    )?;
    let admin = AdminClient::new(config);
    let result = admin
        .describe_quorum(&[DescribeQuorumTopic::new("__cluster_metadata").partition(0)])
        .await?;
    if !result.is_success() {
        return Err(Error::Broker {
            code: result.error_code(),
            context: "describe metadata quorum".to_owned(),
        });
    }
    let partition = result
        .topics()
        .iter()
        .flat_map(|topic| topic.partitions())
        .next()
        .ok_or(Error::Unsupported(
            "DescribeQuorum returned no metadata partition",
        ))?;
    println!(
        "api_version={} leader={} epoch={} high_watermark={} voters={} observers={} nodes={}",
        result.api_version(),
        partition.leader_id(),
        partition.leader_epoch(),
        partition.high_watermark(),
        partition.current_voters().len(),
        partition.observers().len(),
        result.nodes().len(),
    );
    Ok(())
}
