mod common;

use kafrust::{
    AdminClient, ClientConfig, ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter,
    ClientQuotaFilterComponent, ClientQuotaMatchType, Error,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let user = std::env::var("KAFRUST_QUOTA_USER").unwrap_or_else(|_| "ANONYMOUS".to_owned());
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-client-quota-example"),
    )?;
    let admin = AdminClient::new(config);
    let entity = ClientQuotaEntity::user(&user);
    let quota_key = "producer_byte_rate";
    // Kafka validates byte-rate quotas as whole bytes per second.
    let quota_value = 1024.0;

    let altered = admin
        .alter_client_quotas(
            &[ClientQuotaAlteration::new(entity.clone()).set(quota_key, quota_value)],
            false,
        )
        .await?;
    if !altered.is_success() {
        let entry = altered.entries().first();
        return Err(Error::Broker {
            code: entry.map(|entry| entry.error_code()).unwrap_or(-1),
            context: format!(
                "set {quota_key} for user {user}: {}",
                entry
                    .and_then(|entry| entry.error_message())
                    .unwrap_or("broker returned no error message")
            ),
        });
    }

    let filter = ClientQuotaFilter::any().component(ClientQuotaFilterComponent::new(
        "user",
        ClientQuotaMatchType::Exact,
        Some(user.clone()),
    ));
    let mut described = admin.describe_client_quotas(&filter).await?;
    let mut value = quota_value_from_result(&described, quota_key);
    // KRaft applies controller metadata asynchronously to the broker serving describe.
    for attempt in 0..50 {
        if !described.is_success() || value == Some(quota_value) || attempt == 49 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        described = admin.describe_client_quotas(&filter).await?;
        value = quota_value_from_result(&described, quota_key);
    }
    if !described.is_success() || value != Some(quota_value) {
        let all_quotas = admin
            .describe_client_quotas(&ClientQuotaFilter::any())
            .await?;
        return Err(Error::Broker {
            code: described.error_code(),
            context: format!(
                "verify {quota_key} for user {user}, got {value:?}; exact={described:?}; all={all_quotas:?}"
            ),
        });
    }
    println!("set and described {quota_key}={quota_value} for user {user}");

    let removed = admin
        .alter_client_quotas(
            &[ClientQuotaAlteration::new(entity).remove(quota_key)],
            false,
        )
        .await?;
    if !removed.is_success() {
        let entry = removed.entries().first();
        return Err(Error::Broker {
            code: entry.map(|entry| entry.error_code()).unwrap_or(-1),
            context: format!(
                "remove {quota_key} for user {user}: {}",
                entry
                    .and_then(|entry| entry.error_message())
                    .unwrap_or("broker returned no error message")
            ),
        });
    }
    println!("removed {quota_key} for user {user}");

    Ok(())
}

fn quota_value_from_result(
    result: &kafrust::DescribeClientQuotasResult,
    quota_key: &str,
) -> Option<f64> {
    result
        .entries()
        .iter()
        .flat_map(|entry| entry.values())
        .find(|value| value.key() == quota_key)
        .map(|value| value.value())
}
