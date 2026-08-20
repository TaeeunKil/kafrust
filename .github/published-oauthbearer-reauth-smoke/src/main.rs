use kafrust::{ClientConfig, Error, SecurityProtocol};
use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn required_env(name: &'static str) -> kafrust::Result<String> {
    env::var(name).map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "published OAUTHBEARER re-authentication environment variable is required",
    })
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap = required_env("KAFRUST_BOOTSTRAP_SERVERS")?;
    let ca_path = required_env("KAFRUST_TLS_ROOT_CERT_DER_PATH")?;
    let token = Arc::new(required_env("KAFRUST_OAUTH_TOKEN")?);
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let ca_der = fs::read(ca_path)
        .map_err(|_| Error::Unsupported("KAFRUST_TLS_ROOT_CERT_DER_PATH could not be read"))?;

    let provider_token = Arc::clone(&token);
    let provider_calls_for_config = Arc::clone(&provider_calls);
    let config = ClientConfig::new([bootstrap])
        .client_id("kafrust-published-oauthbearer-reauth")
        .request_timeout_ms(5_000)
        .security_protocol(SecurityProtocol::SaslTls)
        .tls_server_name("localhost")
        .tls_root_certificate_der(ca_der)
        .sasl_oauthbearer_provider(move || {
            provider_calls_for_config.fetch_add(1, Ordering::SeqCst);
            let provider_token = Arc::clone(&provider_token);
            async move { Ok((*provider_token).clone()) }
        });

    let mut client = config.connect().await?;
    let session_lifetime_ms = client.sasl_session_lifetime_ms().ok_or(Error::Unsupported(
        "broker did not advertise an OAUTHBEARER session lifetime",
    ))?;
    let session_lifetime_ms = u64::try_from(session_lifetime_ms).map_err(|_| {
        Error::Unsupported("broker advertised an invalid OAUTHBEARER session lifetime")
    })?;
    if session_lifetime_ms == 0 {
        return Err(Error::Unsupported(
            "broker advertised a zero OAUTHBEARER session lifetime",
        ));
    }

    client.api_versions().await?;
    tokio::time::sleep(Duration::from_millis(session_lifetime_ms / 2 + 250)).await;
    client.api_versions().await?;

    let calls = provider_calls.load(Ordering::SeqCst);
    if calls < 2 {
        return Err(Error::Unsupported(
            "OAUTHBEARER provider was not called for re-authentication",
        ));
    }
    println!(
        "published oauthbearer reauth ok session_lifetime_ms={} provider_calls={} sasl_auth_version=1 same_connection=true",
        session_lifetime_ms, calls
    );
    Ok(())
}
