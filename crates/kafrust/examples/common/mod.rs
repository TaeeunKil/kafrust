use std::{
    fmt,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use kafrust::{
    Acks, ClientConfig, Compression, ConsumerConfig, ConsumerGroupConfig, OAuthBearerTokenProvider,
    ProducerConfig, SecurityProtocol,
};
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{
    layer::SubscriberExt,
    layer::{Context, Layer},
    util::SubscriberInitExt,
    EnvFilter,
};

#[allow(dead_code)]
pub(crate) fn init_request_gate(api_key: i16) -> kafrust::Result<()> {
    let filter = EnvFilter::from_default_env();
    match (
        std::env::var_os("KAFRUST_REQUEST_WRITTEN_FILE"),
        std::env::var_os("KAFRUST_REQUEST_RELEASE_FILE"),
    ) {
        (Some(written_file), Some(release_file)) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .with(RequestGateLayer::new(
                api_key,
                written_file.into(),
                release_file.into(),
            ))
            .try_init()
            .map_err(|_| kafrust::Error::Unsupported("tracing subscriber was already initialized")),
        _ => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|_| kafrust::Error::Unsupported("tracing subscriber was already initialized")),
    }
}

#[allow(dead_code)]
struct RequestGateLayer {
    api_key: i16,
    written_file: PathBuf,
    release_file: PathBuf,
    entered: AtomicBool,
}

#[allow(dead_code)]
impl RequestGateLayer {
    fn new(api_key: i16, written_file: PathBuf, release_file: PathBuf) -> Self {
        Self {
            api_key,
            written_file,
            release_file,
            entered: AtomicBool::new(false),
        }
    }
}

#[allow(dead_code)]
impl<S> Layer<S> for RequestGateLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut visitor = RequestVisitor::default();
        event.record(&mut visitor);
        if visitor.api_key != Some(i64::from(self.api_key))
            || !visitor
                .message
                .as_deref()
                .is_some_and(|message| message.contains("kafka request written"))
            || self.entered.swap(true, Ordering::AcqRel)
        {
            return;
        }

        if let Err(error) = std::fs::write(&self.written_file, b"kafka-request-written\n") {
            eprintln!("failed to write request gate file: {error}");
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.release_file.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[allow(dead_code)]
#[derive(Default)]
struct RequestVisitor {
    api_key: Option<i64>,
    message: Option<String>,
}

#[allow(dead_code)]
impl Visit for RequestVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "api_key" {
            self.api_key = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "api_key" {
            self.api_key = format!("{value:?}").parse().ok();
        } else if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }
}

pub(crate) fn bootstrap_servers_from_env() -> Vec<String> {
    let value =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    parse_bootstrap_servers(&value)
}

fn parse_bootstrap_servers(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

pub(crate) fn apply_security<T>(config: T) -> kafrust::Result<T>
where
    T: ExampleSecurityConfig,
{
    let mut config = config.security_protocol(security_protocol_from_env()?);
    if let Some(credentials) = sasl_credentials_from_env()? {
        config = match credentials.mechanism {
            ExampleSaslMechanism::Plain => {
                config.sasl_plain(credentials.username, credentials.password)
            }
            ExampleSaslMechanism::ScramSha256 => {
                config.sasl_scram_sha_256(credentials.username, credentials.password)
            }
            ExampleSaslMechanism::ScramSha512 => {
                config.sasl_scram_sha_512(credentials.username, credentials.password)
            }
            ExampleSaslMechanism::OAuthBearer => {
                if let Ok(path) = std::env::var("KAFRUST_SASL_TOKEN_PATH") {
                    let provider = file_token_provider(PathBuf::from(path));
                    if credentials.username.is_empty() {
                        config.sasl_oauthbearer_provider(provider)
                    } else {
                        config.sasl_oauthbearer_with_username_and_provider(
                            credentials.username,
                            provider,
                        )
                    }
                } else if credentials.username.is_empty() {
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

#[allow(dead_code)]
pub(crate) fn compression_from_env() -> kafrust::Result<Compression> {
    let Ok(value) = std::env::var("KAFRUST_COMPRESSION") else {
        return Ok(Compression::None);
    };

    parse_compression(&value)
}

#[allow(dead_code)]
pub(crate) fn acks_from_env() -> kafrust::Result<Acks> {
    let Ok(value) = std::env::var("KAFRUST_ACKS") else {
        return Ok(Acks::Leader);
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "none" => Ok(Acks::None),
        "1" | "leader" => Ok(Acks::Leader),
        "-1" | "all" => Ok(Acks::All),
        _ => Err(kafrust::Error::Unsupported(
            "KAFRUST_ACKS must be none, leader, or all",
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn idempotence_from_env() -> kafrust::Result<bool> {
    let Ok(value) = std::env::var("KAFRUST_ENABLE_IDEMPOTENCE") else {
        return Ok(false);
    };
    parse_idempotence(&value)
}

fn parse_idempotence(value: &str) -> kafrust::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "no" => Ok(false),
        "1" | "true" | "yes" => Ok(true),
        _ => Err(kafrust::Error::Unsupported(
            "KAFRUST_ENABLE_IDEMPOTENCE must be true or false",
        )),
    }
}

pub(crate) trait ExampleSecurityConfig: Sized {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self;
    fn tls_server_name(self, server_name: String) -> Self;
    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self;
    fn sasl_plain(self, username: String, password: String) -> Self;
    fn sasl_scram_sha_256(self, username: String, password: String) -> Self;
    fn sasl_scram_sha_512(self, username: String, password: String) -> Self;
    fn sasl_oauthbearer(self, token: String) -> Self;
    fn sasl_oauthbearer_with_username(self, username: String, token: String) -> Self;
    fn sasl_oauthbearer_provider<P>(self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static;
    fn sasl_oauthbearer_with_username_and_provider<P>(self, username: String, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static;
}

impl ExampleSecurityConfig for ClientConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ClientConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ClientConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ClientConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ClientConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ClientConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ClientConfig::sasl_scram_sha_512(self, username, password)
    }

    fn sasl_oauthbearer(self, token: String) -> Self {
        ClientConfig::sasl_oauthbearer(self, token)
    }

    fn sasl_oauthbearer_with_username(self, username: String, token: String) -> Self {
        ClientConfig::sasl_oauthbearer_with_username(self, username, token)
    }

    fn sasl_oauthbearer_provider<P>(self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ClientConfig::sasl_oauthbearer_provider(self, provider)
    }

    fn sasl_oauthbearer_with_username_and_provider<P>(self, username: String, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ClientConfig::sasl_oauthbearer_with_username_and_provider(self, username, provider)
    }
}

impl ExampleSecurityConfig for ProducerConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ProducerConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ProducerConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ProducerConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ProducerConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ProducerConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ProducerConfig::sasl_scram_sha_512(self, username, password)
    }

    fn sasl_oauthbearer(self, token: String) -> Self {
        ProducerConfig::sasl_oauthbearer(self, token)
    }

    fn sasl_oauthbearer_with_username(self, username: String, token: String) -> Self {
        ProducerConfig::sasl_oauthbearer_with_username(self, username, token)
    }

    fn sasl_oauthbearer_provider<P>(self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ProducerConfig::sasl_oauthbearer_provider(self, provider)
    }

    fn sasl_oauthbearer_with_username_and_provider<P>(self, username: String, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ProducerConfig::sasl_oauthbearer_with_username_and_provider(self, username, provider)
    }
}

impl ExampleSecurityConfig for ConsumerConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ConsumerConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ConsumerConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ConsumerConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ConsumerConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ConsumerConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ConsumerConfig::sasl_scram_sha_512(self, username, password)
    }

    fn sasl_oauthbearer(self, token: String) -> Self {
        ConsumerConfig::sasl_oauthbearer(self, token)
    }

    fn sasl_oauthbearer_with_username(self, username: String, token: String) -> Self {
        ConsumerConfig::sasl_oauthbearer_with_username(self, username, token)
    }

    fn sasl_oauthbearer_provider<P>(self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ConsumerConfig::sasl_oauthbearer_provider(self, provider)
    }

    fn sasl_oauthbearer_with_username_and_provider<P>(self, username: String, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ConsumerConfig::sasl_oauthbearer_with_username_and_provider(self, username, provider)
    }
}

impl ExampleSecurityConfig for ConsumerGroupConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ConsumerGroupConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ConsumerGroupConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ConsumerGroupConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ConsumerGroupConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ConsumerGroupConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ConsumerGroupConfig::sasl_scram_sha_512(self, username, password)
    }

    fn sasl_oauthbearer(self, token: String) -> Self {
        ConsumerGroupConfig::sasl_oauthbearer(self, token)
    }

    fn sasl_oauthbearer_with_username(self, username: String, token: String) -> Self {
        ConsumerGroupConfig::sasl_oauthbearer_with_username(self, username, token)
    }

    fn sasl_oauthbearer_provider<P>(self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ConsumerGroupConfig::sasl_oauthbearer_provider(self, provider)
    }

    fn sasl_oauthbearer_with_username_and_provider<P>(self, username: String, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        ConsumerGroupConfig::sasl_oauthbearer_with_username_and_provider(self, username, provider)
    }
}

fn security_protocol_from_env() -> kafrust::Result<SecurityProtocol> {
    let Ok(value) = std::env::var("KAFRUST_SECURITY_PROTOCOL") else {
        return Ok(SecurityProtocol::Plaintext);
    };

    parse_security_protocol(&value)
}

struct ExampleSaslCredentials {
    mechanism: ExampleSaslMechanism,
    username: String,
    password: String,
    token: Option<String>,
}

enum ExampleSaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
    OAuthBearer,
}

fn sasl_credentials_from_env() -> kafrust::Result<Option<ExampleSaslCredentials>> {
    let mechanism = sasl_mechanism_from_env()?;
    if matches!(mechanism, ExampleSaslMechanism::OAuthBearer) {
        let token = std::env::var("KAFRUST_SASL_TOKEN").ok().or_else(|| {
            std::env::var("KAFRUST_SASL_TOKEN_PATH")
                .ok()
                .map(|_| String::new())
        });
        if token.is_none() {
            return Err(kafrust::Error::Unsupported(
                "KAFRUST_SASL_TOKEN or KAFRUST_SASL_TOKEN_PATH is required for SASL/OAUTHBEARER",
            ));
        }
        return Ok(Some(ExampleSaslCredentials {
            mechanism,
            username: std::env::var("KAFRUST_SASL_USERNAME").unwrap_or_default(),
            password: String::new(),
            token,
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
    Ok(Some(ExampleSaslCredentials {
        mechanism,
        username,
        password,
        token: None,
    }))
}

fn file_token_provider(path: PathBuf) -> impl OAuthBearerTokenProvider {
    move || {
        let path = path.clone();
        async move {
            let token = std::fs::read_to_string(path)?.trim().to_owned();
            if token.is_empty() {
                return Err(kafrust::Error::Unsupported(
                    "KAFRUST_SASL_TOKEN_PATH must contain a non-empty token",
                ));
            }
            Ok(token)
        }
    }
}

fn sasl_mechanism_from_env() -> kafrust::Result<ExampleSaslMechanism> {
    let Ok(value) = std::env::var("KAFRUST_SASL_MECHANISM") else {
        return Ok(ExampleSaslMechanism::Plain);
    };

    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "" | "plain" => Ok(ExampleSaslMechanism::Plain),
        "scram-sha-256" => Ok(ExampleSaslMechanism::ScramSha256),
        "scram-sha-512" => Ok(ExampleSaslMechanism::ScramSha512),
        "oauthbearer" | "oauth-bearer" => Ok(ExampleSaslMechanism::OAuthBearer),
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

#[allow(dead_code)]
fn parse_compression(value: &str) -> kafrust::Result<Compression> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "" | "none" => Ok(Compression::None),
        "gzip" => Ok(Compression::Gzip),
        "snappy" => Ok(Compression::Snappy),
        "lz4" => Ok(Compression::Lz4),
        "zstd" => Ok(Compression::Zstd),
        _ => Err(kafrust::Error::Unsupported(
            "unsupported KAFRUST_COMPRESSION; expected none, gzip, snappy, lz4, or zstd",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_idempotence;

    #[test]
    fn parses_idempotence_values() {
        assert!(parse_idempotence(" true ").expect("true should parse"));
        assert!(!parse_idempotence("no").expect("no should parse"));
        assert!(parse_idempotence("sometimes").is_err());
    }
}
