mod common;

use kafrust::{AdminClient, ClientConfig, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-group-example"),
    )?;
    let admin = AdminClient::new(config);
    let groups = admin.list_groups().await?;
    let listing = groups
        .iter()
        .find(|listing| listing.group_id() == group_id)
        .ok_or_else(|| Error::MissingGroupDescription {
            group_id: group_id.clone(),
        })?;
    println!(
        "listed group {} protocol={} coordinator={} throttle={:?}",
        listing.group_id(),
        listing.protocol_type(),
        listing.coordinator_id(),
        listing.throttle_time()
    );

    let descriptions = admin
        .describe_consumer_groups(std::slice::from_ref(&group_id))
        .await?;
    let description = descriptions
        .iter()
        .find(|description| description.group_id() == group_id)
        .ok_or_else(|| Error::MissingGroupDescription {
            group_id: group_id.clone(),
        })?;
    if !description.is_success() {
        return Err(Error::Broker {
            code: description.error_code(),
            context: format!("describe consumer group {group_id}"),
        });
    }

    println!(
        "group {} state={} protocol={}/{} members={} throttle={:?}",
        description.group_id(),
        description.state(),
        description.protocol_type(),
        description.protocol_name(),
        description.members().len(),
        description.throttle_time()
    );
    Ok(())
}
