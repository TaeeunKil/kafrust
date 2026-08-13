mod common;

use kafrust::{
    AdminClient, ClientConfig, CreateDelegationTokenOptions, DelegationTokenPrincipal, Error,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let renewer = std::env::var("KAFRUST_DELEGATION_TOKEN_RENEWER")
        .unwrap_or_else(|_| "User:kafrust".to_owned());
    let renewer = parse_principal(&renewer)?;
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-delegation-token-example"),
    )?;
    let admin = AdminClient::new(config);

    let created = admin
        .create_delegation_token(CreateDelegationTokenOptions::new().renewer(renewer))
        .await?;
    ensure_success(
        created.is_success(),
        created.error_code(),
        "create delegation token",
    )?;
    if created.hmac().is_empty() {
        return Err(Error::Unsupported(
            "CreateDelegationToken returned an empty HMAC",
        ));
    }
    println!(
        "created delegation token {} owner={} expiry={} hmac_len={}",
        created.token_id(),
        format_principal(created.owner()),
        created.expiry_timestamp_ms(),
        created.hmac().len(),
    );

    let mut described = admin.describe_delegation_tokens(None).await?;
    for _ in 0..20 {
        if described
            .tokens()
            .iter()
            .any(|token| token.token_id() == created.token_id())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        described = admin.describe_delegation_tokens(None).await?;
    }
    ensure_success(
        described.is_success(),
        described.error_code(),
        "describe delegation tokens",
    )?;
    let token = described
        .tokens()
        .iter()
        .find(|token| token.token_id() == created.token_id())
        .ok_or_else(|| Error::Broker {
            code: -1,
            context: format!(
                "DescribeDelegationToken did not return token {}",
                created.token_id()
            ),
        })?;
    println!(
        "described delegation token {} owner={} hmac_len={}",
        token.token_id(),
        format_principal(token.owner()),
        token.hmac().len(),
    );

    let renewed = admin
        .renew_delegation_token(created.hmac(), Duration::from_secs(60))
        .await?;
    ensure_success(
        renewed.is_success(),
        renewed.error_code(),
        "renew delegation token",
    )?;
    println!(
        "renewed delegation token {} expiry={}",
        created.token_id(),
        renewed.expiry_timestamp_ms()
    );

    let expired = admin
        .expire_delegation_token(created.hmac(), Duration::ZERO)
        .await?;
    ensure_success(
        expired.is_success(),
        expired.error_code(),
        "expire delegation token",
    )?;
    println!("expired delegation token {}", created.token_id());
    Ok(())
}

fn parse_principal(value: &str) -> kafrust::Result<DelegationTokenPrincipal> {
    let (principal_type, principal_name) = value.split_once(':').ok_or(Error::Unsupported(
        "KAFRUST_DELEGATION_TOKEN_RENEWER must be TYPE:NAME",
    ))?;
    if principal_type.is_empty() || principal_name.is_empty() {
        return Err(Error::Unsupported(
            "KAFRUST_DELEGATION_TOKEN_RENEWER must contain non-empty TYPE and NAME",
        ));
    }
    Ok(DelegationTokenPrincipal::new(
        principal_type,
        principal_name,
    ))
}

fn format_principal(principal: &DelegationTokenPrincipal) -> String {
    format!(
        "{}:{}",
        principal.principal_type(),
        principal.principal_name()
    )
}

fn ensure_success(success: bool, code: i16, operation: &str) -> kafrust::Result<()> {
    if success {
        return Ok(());
    }
    Err(Error::Broker {
        code,
        context: format!("{operation} failed"),
    })
}
