mod common;

use kafrust::{AdminClient, ClientConfig, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let transactional_id = std::env::var("KAFRUST_TRANSACTIONAL_ID")
        .unwrap_or_else(|_| "kafrust-transactional-smoke".to_owned());
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-describe-transactions"),
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
        "described transaction {} state={} producer={} epoch={} timeout={:?} topics={} throttle={:?}",
        transaction.transactional_id(),
        transaction.state(),
        transaction.producer_id(),
        transaction.producer_epoch(),
        transaction.transaction_timeout(),
        transaction.topics().len(),
        result.throttle_time(),
    );
    Ok(())
}
