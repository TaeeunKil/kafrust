use kafrust::{
    AdminClient, ClientConfig, DescribeClusterEndpointType, DescribeClusterOptions,
};

fn parse_bootstrap_servers(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap = std::env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| kafrust::Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS is required"))?;
    let admin = AdminClient::new(
        ClientConfig::new(parse_bootstrap_servers(&bootstrap))
            .client_id("kafrust-published-describe-cluster"),
    );

    let metadata = admin.describe_cluster().await?;
    if metadata.brokers().is_empty() {
        return Err(kafrust::Error::Unsupported(
            "Metadata cluster description returned no brokers",
        ));
    }
    if metadata.cluster_id().is_some() || metadata.endpoint_type().is_some() {
        return Err(kafrust::Error::Unsupported(
            "Metadata cluster description unexpectedly contains DescribeCluster fields",
        ));
    }

    let dedicated = admin
        .describe_cluster_with_options(
            DescribeClusterOptions::new()
                .include_cluster_authorized_operations(true)
                .endpoint_type(DescribeClusterEndpointType::Controllers),
        )
        .await?;
    if dedicated.cluster_id().is_none()
        || dedicated.endpoint_type() != Some(DescribeClusterEndpointType::Controllers)
        || dedicated.cluster_authorized_operations().is_none()
        || dedicated.brokers().is_empty()
    {
        return Err(kafrust::Error::Unsupported(
            "DescribeCluster v1 returned incomplete cluster metadata",
        ));
    }

    println!(
        "api60 cluster_id_present=true endpoint_type={:?} authorized_ops_present=true brokers={}",
        dedicated.endpoint_type(),
        dedicated.brokers().len()
    );
    println!(
        "metadata controller_id={} brokers={}",
        metadata.controller_id(),
        metadata.brokers().len()
    );
    Ok(())
}
