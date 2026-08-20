mod common;

use kafrust::{AdminClient, ClientConfig, Error};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let phase = std::env::var("KAFRUST_UNREGISTER_PHASE").unwrap_or_else(|_| "unregister".into());
    let broker_id = parse_i32_env("KAFRUST_UNREGISTER_BROKER_ID", 1)?;
    let mut config = ClientConfig::new(common::bootstrap_servers_from_env())
        .client_id("kafrust-admin-unregister-broker-rejoin");
    if let Some(servers) = common::controller_bootstrap_servers_from_env() {
        config = config.controller_bootstrap_servers(servers);
    }
    let config = common::apply_security(config)?;
    let admin = AdminClient::new(config);

    match phase.as_str() {
        "unregister" => unregister_broker(&admin, broker_id).await?,
        "rejoin" => wait_for_broker(&admin, broker_id, true, Duration::from_secs(60)).await?,
        _ => {
            return Err(Error::InvalidConfiguration {
                field: "KAFRUST_UNREGISTER_PHASE",
                reason: "value must be unregister or rejoin",
            })
        }
    }
    Ok(())
}

async fn unregister_broker(admin: &AdminClient, broker_id: i32) -> kafrust::Result<()> {
    wait_for_broker(admin, broker_id, true, Duration::from_secs(30)).await?;
    let result = admin.unregister_broker(broker_id).await?;
    if !result.is_success() {
        return Err(Error::Broker {
            code: result.error_code(),
            context: format!("unregister broker {broker_id}"),
        });
    }
    println!("UnregisterBroker accepted for broker {broker_id}");
    wait_for_broker(admin, broker_id, false, Duration::from_secs(30)).await
}

async fn wait_for_broker(
    admin: &AdminClient,
    broker_id: i32,
    expected_present: bool,
    timeout: Duration,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let cluster = admin.describe_cluster().await?;
        let present = cluster
            .brokers()
            .iter()
            .any(|broker| broker.id() == broker_id);
        if present == expected_present {
            println!(
                "broker {broker_id} registration state is present={present}; cluster brokers={}",
                cluster.brokers().len()
            );
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "broker registration state did not converge before the deadline",
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn parse_i32_env(name: &'static str, default: i32) -> kafrust::Result<i32> {
    let value = std::env::var(name).unwrap_or_else(|_| default.to_string());
    value.parse().map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "value must be a signed 32-bit integer",
    })
}
