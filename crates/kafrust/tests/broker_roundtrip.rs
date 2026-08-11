#![allow(clippy::expect_used)]

use kafrust::protocol::api::find_coordinator::FindCoordinatorResponseV1;
use kafrust::{ClientConfig, SecurityProtocol};
use tokio::time::{sleep, Duration, Instant};

#[tokio::test]
async fn api_versions_and_metadata_roundtrip_when_broker_is_configured() {
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping broker roundtrip; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };

    let mut client =
        client_config_from_env(parse_bootstrap_servers(&bootstrap), "kafrust-integration")
            .expect("valid broker test configuration")
            .connect()
            .await
            .expect("connect to Kafka broker");

    let api_versions = client
        .api_versions()
        .await
        .expect("ApiVersions roundtrip should succeed");
    assert!(!api_versions.api_keys.is_empty());

    let metadata = client
        .metadata(None)
        .await
        .expect("Metadata roundtrip should succeed");
    assert!(!metadata.brokers.is_empty());
    if let Some(expected_brokers) = expected_brokers_from_env() {
        assert!(
            metadata.brokers.len() >= expected_brokers,
            "expected at least {expected_brokers} brokers, got {}",
            metadata.brokers.len()
        );
    }
}

#[tokio::test]
async fn find_group_coordinator_roundtrip_when_broker_is_configured() {
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping group coordinator roundtrip; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut client =
        client_config_from_env(parse_bootstrap_servers(&bootstrap), "kafrust-integration")
            .expect("valid broker test configuration")
            .connect()
            .await
            .expect("connect to Kafka broker");

    let coordinator = wait_for_group_coordinator(&mut client, group_id)
        .await
        .expect("FindCoordinator should return a ready coordinator");

    assert!(coordinator.node_id >= 0);
    assert!(!coordinator.host.is_empty());
    assert!(coordinator.port > 0);
}

fn security_protocol_from_env() -> SecurityProtocol {
    let Ok(value) = std::env::var("KAFRUST_SECURITY_PROTOCOL") else {
        return SecurityProtocol::Plaintext;
    };

    parse_security_protocol(&value).expect("valid KAFRUST_SECURITY_PROTOCOL")
}

fn client_config_from_env(
    bootstrap_servers: Vec<String>,
    client_id: &str,
) -> kafrust::Result<ClientConfig> {
    let mut config = ClientConfig::new(bootstrap_servers)
        .client_id(client_id)
        .security_protocol(security_protocol_from_env());
    if let Some(credentials) = sasl_credentials_from_env()? {
        config = match credentials.mechanism {
            TestSaslMechanism::Plain => {
                config.sasl_plain(credentials.username, credentials.password)
            }
            TestSaslMechanism::ScramSha256 => {
                config.sasl_scram_sha_256(credentials.username, credentials.password)
            }
            TestSaslMechanism::ScramSha512 => {
                config.sasl_scram_sha_512(credentials.username, credentials.password)
            }
            TestSaslMechanism::OAuthBearer => {
                if credentials.username.is_empty() {
                    config.sasl_oauthbearer(credentials.token.unwrap_or_default())
                } else {
                    config.sasl_oauthbearer_with_username(
                        credentials.username,
                        credentials.token.unwrap_or_default(),
                    )
                }
            }
        };
    }
    if let Some(server_name) = tls_server_name_from_env() {
        config = config.tls_server_name(server_name);
    }
    if let Some(certificate) = tls_root_certificate_der_from_env()? {
        config = config.tls_root_certificate_der(certificate);
    }
    Ok(config)
}

struct TestSaslCredentials {
    mechanism: TestSaslMechanism,
    username: String,
    password: String,
    token: Option<String>,
}

enum TestSaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
}

fn sasl_credentials_from_env() -> kafrust::Result<Option<TestSaslCredentials>> {
    let mechanism = sasl_mechanism_from_env()?;
    if matches!(mechanism, TestSaslMechanism::OAuthBearer) {
        let token = std::env::var("KAFRUST_SASL_TOKEN").map_err(|_| {
            kafrust::Error::Unsupported("KAFRUST_SASL_TOKEN is required for SASL/OAUTHBEARER")
        })?;
        return Ok(Some(TestSaslCredentials {
            mechanism,
            username: std::env::var("KAFRUST_SASL_USERNAME").unwrap_or_default(),
            password: String::new(),
            token: Some(token),
        }));
    }

    let Some(username) = std::env::var("KAFRUST_SASL_USERNAME").ok() else {
        return Ok(None);
    };
    let password = std::env::var("KAFRUST_SASL_PASSWORD").map_err(|_| {
        kafrust::Error::Unsupported(
            "KAFRUST_SASL_PASSWORD is required when KAFRUST_SASL_USERNAME is set",
        )
    })?;
    Ok(Some(TestSaslCredentials {
        mechanism,
        username,
        password,
        token: None,
    }))
}

fn sasl_mechanism_from_env() -> kafrust::Result<TestSaslMechanism> {
    let Ok(value) = std::env::var("KAFRUST_SASL_MECHANISM") else {
        return Ok(TestSaslMechanism::Plain);
    };

    parse_sasl_mechanism(&value)
}

fn parse_sasl_mechanism(value: &str) -> kafrust::Result<TestSaslMechanism> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "" | "plain" => Ok(TestSaslMechanism::Plain),
        "scram-sha-256" => Ok(TestSaslMechanism::ScramSha256),
        "scram-sha-512" => Ok(TestSaslMechanism::ScramSha512),
        "oauthbearer" | "oauth-bearer" => Ok(TestSaslMechanism::OAuthBearer),
        _ => Err(kafrust::Error::Unsupported(
            "unsupported KAFRUST_SASL_MECHANISM; expected plain, scram-sha-256, scram-sha-512, or oauthbearer",
        )),
    }
}

fn tls_server_name_from_env() -> Option<String> {
    std::env::var("KAFRUST_TLS_SERVER_NAME").ok()
}

fn tls_root_certificate_der_from_env() -> kafrust::Result<Option<Vec<u8>>> {
    let Ok(path) = std::env::var("KAFRUST_TLS_ROOT_CERT_DER_PATH") else {
        return Ok(None);
    };

    Ok(Some(std::fs::read(path)?))
}

fn parse_security_protocol(value: &str) -> kafrust::Result<SecurityProtocol> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "" | "plaintext" => Ok(SecurityProtocol::Plaintext),
        "ssl" | "tls" => Ok(SecurityProtocol::Tls),
        "sasl_plaintext" => Ok(SecurityProtocol::SaslPlaintext),
        "sasl_ssl" | "sasl_tls" => Ok(SecurityProtocol::SaslTls),
        _ => Err(kafrust::Error::Unsupported(
            "unsupported KAFRUST_SECURITY_PROTOCOL; expected plaintext, tls, ssl, sasl_plaintext, sasl_ssl, or sasl_tls",
        )),
    }
}

fn parse_bootstrap_servers(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn expected_brokers_from_env() -> Option<usize> {
    std::env::var("KAFRUST_EXPECTED_BROKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

#[test]
fn parses_security_protocol_from_environment_value() {
    assert_eq!(
        parse_security_protocol("plaintext").expect("plaintext should parse"),
        SecurityProtocol::Plaintext
    );
    assert_eq!(
        parse_security_protocol("SSL").expect("SSL should parse"),
        SecurityProtocol::Tls
    );
    assert_eq!(
        parse_security_protocol("sasl-ssl").expect("sasl-ssl should parse"),
        SecurityProtocol::SaslTls
    );
}

#[test]
fn parses_sasl_mechanism_from_environment_value() {
    assert!(matches!(
        parse_sasl_mechanism("scram_sha_512").expect("SCRAM mechanism should parse"),
        TestSaslMechanism::ScramSha512
    ));
    assert!(matches!(
        parse_sasl_mechanism("oauthbearer").expect("OAuth mechanism should parse"),
        TestSaslMechanism::OAuthBearer
    ));
}

#[test]
fn parses_bootstrap_server_list_from_environment_value() {
    assert_eq!(
        parse_bootstrap_servers(" localhost:19092,localhost:19093,,localhost:19094 "),
        vec![
            "localhost:19092".to_owned(),
            "localhost:19093".to_owned(),
            "localhost:19094".to_owned(),
        ]
    );
}

async fn wait_for_group_coordinator(
    client: &mut kafrust::Client,
    group_id: String,
) -> kafrust::Result<FindCoordinatorResponseV1> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let coordinator = client.find_group_coordinator(group_id.clone()).await?;
        if coordinator.node_id >= 0 {
            return Ok(coordinator);
        }
        if Instant::now() >= deadline {
            return Ok(coordinator);
        }
        sleep(Duration::from_millis(500)).await;
    }
}
