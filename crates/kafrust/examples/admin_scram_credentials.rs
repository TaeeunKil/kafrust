mod common;

use kafrust::{
    AdminClient, ClientConfig, Error, ScramCredentialDeletion, ScramCredentialMechanism,
    ScramCredentialUpsertion,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let username = std::env::var("KAFRUST_SCRAM_ADMIN_USER")
        .unwrap_or_else(|_| "kafrust-admin-api".to_owned());
    let password = std::env::var("KAFRUST_SCRAM_ADMIN_PASSWORD")
        .unwrap_or_else(|_| "kafrust-admin-api-secret".to_owned());
    let mechanism = ScramCredentialMechanism::Sha256;
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-scram-admin-example"),
    )?;
    let admin = AdminClient::new(config);

    let upsertion = ScramCredentialUpsertion::new(&username, mechanism, 4096, password.as_bytes())?;
    let altered = admin
        .alter_user_scram_credentials(&[], &[upsertion])
        .await?;
    ensure_success(
        altered.is_success(),
        altered.results().first().map(|result| result.error_code()),
        altered
            .results()
            .first()
            .and_then(|result| result.error_message()),
        "upsert SCRAM credential",
    )?;

    let users = [username.clone()];
    let mut described = admin.describe_user_scram_credentials(Some(&users)).await?;
    for attempt in 0..50 {
        let credential_visible = described.users().iter().any(|user| {
            user.username() == username
                && user.is_success()
                && user.credentials().iter().any(|credential| {
                    credential.mechanism() == mechanism && credential.iterations() == 4096
                })
        });
        if credential_visible || attempt == 49 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        described = admin.describe_user_scram_credentials(Some(&users)).await?;
    }
    let credential_visible = described.users().iter().any(|user| {
        user.username() == username
            && user.is_success()
            && user.credentials().iter().any(|credential| {
                credential.mechanism() == mechanism && credential.iterations() == 4096
            })
    });
    if !credential_visible {
        let user = described
            .users()
            .iter()
            .find(|user| user.username() == username);
        return Err(Error::Broker {
            code: user
                .map(|user| user.error_code())
                .unwrap_or(described.error_code()),
            context: format!(
                "verify SCRAM credential for user {username}: {}",
                user.and_then(|user| user.error_message())
                    .or_else(|| described.error_message())
                    .unwrap_or("credential was not visible after bounded polling")
            ),
        });
    }
    println!("upserted and described {mechanism:?} credential for user {username}");

    let deletion = ScramCredentialDeletion::new(&username, mechanism)?;
    let removed = admin.alter_user_scram_credentials(&[deletion], &[]).await?;
    ensure_success(
        removed.is_success(),
        removed.results().first().map(|result| result.error_code()),
        removed
            .results()
            .first()
            .and_then(|result| result.error_message()),
        "delete SCRAM credential",
    )?;
    println!("deleted {mechanism:?} credential for user {username}");

    Ok(())
}

fn ensure_success(
    success: bool,
    code: Option<i16>,
    message: Option<&str>,
    operation: &str,
) -> kafrust::Result<()> {
    if success {
        return Ok(());
    }
    Err(Error::Broker {
        code: code.unwrap_or(-1),
        context: format!(
            "{operation}: {}",
            message.unwrap_or("broker returned no error message")
        ),
    })
}
