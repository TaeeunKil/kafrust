mod common;

use std::io::{self, Write};

use kafrust::{AdminClient, ClientConfig, ClientMetrics, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(65)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let transactional_id = std::env::var("KAFRUST_TRANSACTIONAL_ID")
        .unwrap_or_else(|_| "kafrust-transaction-failover".to_owned());
    let discovery_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-admin-describe-transactions-failover-discovery"),
    )?;
    let mut bootstrap = discovery_config.clone().connect().await?;
    let coordinator = bootstrap
        .find_transaction_coordinator(transactional_id.clone())
        .await?;
    if coordinator.error_code != 0 {
        return Err(Error::Broker {
            code: coordinator.error_code,
            context: format!("find transaction coordinator for {transactional_id}"),
        });
    }
    println!("transaction coordinator node {}", coordinator.node_id);
    io::stdout().flush().map_err(Error::Io)?;

    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-describe-transactions-failover")
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);
    let result = admin
        .describe_transactions(std::slice::from_ref(&transactional_id))
        .await?;
    let transaction = result
        .transactions()
        .iter()
        .find(|candidate| candidate.transactional_id() == transactional_id)
        .ok_or_else(|| Error::Broker {
            code: -1,
            context: format!("DescribeTransactions omitted {transactional_id}"),
        })?;
    if !transaction.is_success() {
        return Err(Error::Broker {
            code: transaction.error_code(),
            context: format!("DescribeTransactions for {transactional_id}"),
        });
    }

    println!(
        "admin describe transactions failover completed {} state={} retries={}",
        transaction.transactional_id(),
        transaction.state(),
        metrics.snapshot().retries,
    );
    Ok(())
}
