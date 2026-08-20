mod common;

use kafrust::{AdminClient, ClientConfig, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let phase =
        std::env::var("KAFRUST_UNREGISTER_AUTH_PHASE").unwrap_or_else(|_| "denied".to_owned());
    let broker_id = std::env::var("KAFRUST_UNREGISTER_BROKER_ID")
        .unwrap_or_else(|_| "1".to_owned())
        .parse::<i32>()
        .map_err(|_| Error::InvalidConfiguration {
            field: "KAFRUST_UNREGISTER_BROKER_ID",
            reason: "value must be a signed 32-bit integer",
        })?;
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-unregister-broker-authorization"),
    )?;
    let admin = AdminClient::new(config);

    let cluster = admin.describe_cluster().await?;
    if !cluster
        .brokers()
        .iter()
        .any(|broker| broker.id() == broker_id)
    {
        return Err(Error::Unsupported(
            "the authorization gate could not observe the target broker",
        ));
    }

    let result = admin.unregister_broker(broker_id).await?;
    match phase.as_str() {
        "denied" => {
            if result.is_success() {
                return Err(Error::Unsupported(
                    "an unauthorized principal was allowed to unregister the broker",
                ));
            }
            if result.error_code() != 31 {
                return Err(Error::Broker {
                    code: result.error_code(),
                    context: "unexpected UnregisterBroker authorization error".to_owned(),
                });
            }
            println!(
                "restricted principal was denied UnregisterBroker with error code {}",
                result.error_code()
            );
        }
        "allowed" => {
            if !result.is_success() {
                return Err(Error::Broker {
                    code: result.error_code(),
                    context: "authorized UnregisterBroker unexpectedly failed".to_owned(),
                });
            }
            println!("authorized principal successfully sent UnregisterBroker");
        }
        _ => {
            return Err(Error::InvalidConfiguration {
                field: "KAFRUST_UNREGISTER_AUTH_PHASE",
                reason: "value must be denied or allowed",
            })
        }
    }

    Ok(())
}
