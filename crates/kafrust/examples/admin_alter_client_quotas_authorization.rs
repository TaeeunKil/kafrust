mod common;

use kafrust::{
    AdminClient, ClientConfig, ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter,
    ClientQuotaFilterComponent, ClientQuotaMatchType, Error,
};
use std::time::Duration;

const QUOTA_KEY: &str = "producer_byte_rate";
const QUOTA_VALUE: f64 = 1024.0;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let quota_user =
        std::env::var("KAFRUST_QUOTA_USER").map_err(|_| Error::InvalidConfiguration {
            field: "KAFRUST_QUOTA_USER",
            reason: "the authorization example requires a unique quota user",
        })?;
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-alter-client-quotas-authorization-example"),
    )?;
    let admin = AdminClient::new(config);

    if std::env::var_os("KAFRUST_VERIFY_QUOTA_ABSENT").is_some() {
        if let Some(value) = describe_quota(&admin, &quota_user).await? {
            return Err(Error::Broker {
                code: 0,
                context: format!("quota was unexpectedly present with value {value}"),
            });
        }
        println!("AlterClientQuotas denial left user {quota_user} without {QUOTA_KEY}");
        return Ok(());
    }

    let expected_error = parse_i16_env("KAFRUST_EXPECT_ALTER_CLIENT_QUOTAS_ERROR")?;
    let alteration = ClientQuotaAlteration::new(ClientQuotaEntity::user(&quota_user))
        .set(QUOTA_KEY, QUOTA_VALUE);

    let result = admin.alter_client_quotas(&[alteration], false).await?;
    let outcome = result.entries().first().ok_or(Error::Unsupported(
        "AlterClientQuotas returned no entity outcome",
    ))?;
    if outcome.error_code() != expected_error {
        return Err(Error::Broker {
            code: outcome.error_code(),
            context: format!(
                "AlterClientQuotas returned {}, expected {}",
                outcome.error_code(),
                expected_error
            ),
        });
    }

    if expected_error != 0 {
        if outcome.is_success() {
            return Err(Error::Broker {
                code: expected_error,
                context: "AlterClientQuotas succeeded despite the expected authorization error"
                    .to_owned(),
            });
        }
        println!(
            "AlterClientQuotas denied with expected error {expected_error}; broker response preserved"
        );
        return Ok(());
    }

    if !outcome.is_success() {
        return Err(Error::Broker {
            code: outcome.error_code(),
            context: "AlterClientQuotas success expectation was not successful".to_owned(),
        });
    }
    wait_for_quota_value(&admin, &quota_user).await?;
    let cleanup =
        ClientQuotaAlteration::new(ClientQuotaEntity::user(&quota_user)).remove(QUOTA_KEY);
    let cleanup_result = admin.alter_client_quotas(&[cleanup], false).await?;
    if !cleanup_result.is_success() {
        return Err(Error::Broker {
            code: cleanup_result
                .entries()
                .first()
                .map_or(-1, |entry| entry.error_code()),
            context: "AlterClientQuotas cleanup failed".to_owned(),
        });
    }
    println!("AlterClientQuotas allowed for user {quota_user}");
    Ok(())
}

async fn describe_quota(admin: &AdminClient, quota_user: &str) -> kafrust::Result<Option<f64>> {
    let filter = ClientQuotaFilter::any().component(ClientQuotaFilterComponent::new(
        "user",
        ClientQuotaMatchType::Exact,
        Some(quota_user),
    ));
    let result = admin.describe_client_quotas(&filter).await?;
    if !result.is_success() {
        return Err(Error::Broker {
            code: result.error_code(),
            context: "DescribeClientQuotas failed while checking authorization".to_owned(),
        });
    }
    Ok(result
        .entries()
        .iter()
        .flat_map(|entry| entry.values())
        .find(|value| value.key() == QUOTA_KEY)
        .map(|value| value.value()))
}

async fn wait_for_quota_value(admin: &AdminClient, quota_user: &str) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if describe_quota(admin, quota_user).await? == Some(QUOTA_VALUE) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "AlterClientQuotas readback did not observe the applied value",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn parse_i16_env(name: &'static str) -> kafrust::Result<i16> {
    let value = std::env::var(name).map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "the expected broker error code is required",
    })?;
    value.parse().map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "value must be a signed 16-bit integer",
    })
}
