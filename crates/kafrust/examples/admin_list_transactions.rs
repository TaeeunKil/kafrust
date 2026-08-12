mod common;

use kafrust::{AdminClient, ClientConfig, Error, ListTransactionsOptions};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-list-transactions"),
    )?;
    let admin = AdminClient::new(config);
    let result = admin
        .list_transactions(ListTransactionsOptions::new())
        .await?;

    if !result.is_success() {
        return Err(Error::Broker {
            code: result.error_code(),
            context: "ListTransactions returned a top-level broker error".to_owned(),
        });
    }

    println!(
        "listed transactions={} unknown_states={} throttle={:?}",
        result.transactions().len(),
        result.unknown_state_filters().len(),
        result.throttle_time(),
    );
    Ok(())
}
