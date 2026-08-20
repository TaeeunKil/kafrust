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
    let controller_bootstrap = std::env::var("KAFRUST_CONTROLLER_BOOTSTRAP_SERVERS").map_err(
        |_| kafrust::Error::Unsupported("KAFRUST_CONTROLLER_BOOTSTRAP_SERVERS is required"),
    )?;
    let admin = AdminClient::new(
        ClientConfig::new(parse_bootstrap_servers(&bootstrap))
            .controller_bootstrap_servers(parse_bootstrap_servers(&controller_bootstrap))
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
                .endpoint_type(DescribeClusterEndpointType::Brokers),
        )
        .await?;
    if dedicated.cluster_id().is_none()
        || dedicated.endpoint_type() != Some(DescribeClusterEndpointType::Brokers)
        || dedicated.cluster_authorized_operations().is_none()
        || dedicated.brokers().is_empty()
    {
        return Err(kafrust::Error::Unsupported(
            "DescribeCluster v1 returned incomplete cluster metadata",
        ));
    }

    let controllers = admin
        .describe_cluster_with_options(
            DescribeClusterOptions::new()
                .include_cluster_authorized_operations(true)
                .endpoint_type(DescribeClusterEndpointType::Controllers),
        )
        .await?;
    if controllers.cluster_id().is_none()
        || controllers.endpoint_type() != Some(DescribeClusterEndpointType::Controllers)
        || controllers.cluster_authorized_operations().is_none()
        || controllers.brokers().is_empty()
    {
        return Err(kafrust::Error::Unsupported(
            "DescribeCluster v1 returned incomplete controller metadata",
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
    println!(
        "api60_controller cluster_id_present=true endpoint_type={:?} authorized_ops_present=true brokers={}",
        controllers.endpoint_type(),
        controllers.brokers().len()
    );
    Ok(())
}
