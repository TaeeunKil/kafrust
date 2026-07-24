use crate::client::{Client, DEFAULT_MAX_RESPONSE_BYTES};
use crate::error::{Error, Result};
use crate::metrics::ClientMetrics;
use crate::scram::{self, ScramHash};
use core::fmt;
use kafrust_protocol::codec::DecodeLimits;
use std::str;
#[cfg(feature = "tls")]
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kafka client security protocol.
///
/// `Plaintext` is the default transport. `Tls` is implemented when the
/// non-default `tls` crate feature is enabled. SASL/PLAIN authentication is
/// implemented for SASL security protocols when credentials are configured.
pub enum SecurityProtocol {
    /// Kafka `PLAINTEXT`: raw TCP without TLS or SASL authentication.
    Plaintext,
    /// Kafka `SSL`: TLS transport without SASL authentication.
    Tls,
    /// Kafka `SASL_PLAINTEXT`: SASL authentication over raw TCP.
    SaslPlaintext,
    /// Kafka `SASL_SSL`: SASL authentication over TLS.
    SaslTls,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kafka SASL mechanism selected for authentication.
pub enum SaslMechanism {
    /// Kafka `PLAIN` SASL mechanism.
    Plain,
    /// Kafka `SCRAM-SHA-256` SASL mechanism.
    ScramSha256,
    /// Kafka `SCRAM-SHA-512` SASL mechanism.
    ScramSha512,
}

impl SaslMechanism {
    /// Returns the Kafka protocol mechanism name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    fn scram_hash(self) -> Option<ScramHash> {
        match self {
            Self::Plain => None,
            Self::ScramSha256 => Some(ScramHash::Sha256),
            Self::ScramSha512 => Some(ScramHash::Sha512),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
/// SASL authentication material.
///
/// `Debug` output redacts the password so config diagnostics do not expose raw
/// credentials. The password accessor is still available because callers own
/// the configured secret and future authentication code needs the raw value.
pub struct SaslCredentials {
    mechanism: SaslMechanism,
    username: String,
    password: String,
}

impl SaslCredentials {
    /// Creates SASL/PLAIN credentials from a username and password.
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::Plain,
            username: username.into(),
            password: password.into(),
        }
    }

    /// Creates SASL/SCRAM-SHA-256 credentials from a username and password.
    pub fn scram_sha_256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha256,
            username: username.into(),
            password: password.into(),
        }
    }

    /// Creates SASL/SCRAM-SHA-512 credentials from a username and password.
    pub fn scram_sha_512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha512,
            username: username.into(),
            password: password.into(),
        }
    }

    /// Returns the configured SASL mechanism.
    pub fn mechanism(&self) -> SaslMechanism {
        self.mechanism
    }

    /// Returns the configured SASL username.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the configured SASL password.
    pub fn password(&self) -> &str {
        &self.password
    }
}

impl fmt::Debug for SaslCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslCredentials")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Connection settings shared by low-level clients, producers, and consumers.
pub struct ClientConfig {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    request_timeout: Duration,
    max_response_bytes: usize,
    decode_limits: DecodeLimits,
    security_protocol: SecurityProtocol,
    tls_server_name: Option<String>,
    tls_root_certificates_der: Vec<Vec<u8>>,
    sasl_credentials: Option<SaslCredentials>,
    metrics: ClientMetrics,
}

impl ClientConfig {
    /// Creates a client configuration from one or more Kafka bootstrap servers.
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into_iter().map(Into::into).collect(),
            client_id: None,
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            decode_limits: DecodeLimits::default(),
            security_protocol: SecurityProtocol::Plaintext,
            tls_server_name: None,
            tls_root_certificates_der: Vec::new(),
            sasl_credentials: None,
            metrics: ClientMetrics::new(),
        }
    }

    /// Sets the Kafka client ID sent in request headers.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets the request timeout applied after a broker connection is established.
    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.request_timeout = Duration::from_millis(request_timeout_ms);
        self
    }

    /// Sets the maximum broker response payload allocated for one request.
    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    /// Sets the maximum number of elements allocated for one Kafka array.
    pub fn max_decode_array_elements(mut self, max: usize) -> Self {
        self.decode_limits = self.decode_limits.with_max_array_elements(max);
        self
    }

    /// Sets the maximum uncompressed size of one fetched record batch.
    pub fn max_decompressed_record_bytes(mut self, max: usize) -> Self {
        self.decode_limits = self.decode_limits.with_max_decompressed_record_bytes(max);
        self
    }

    /// Sets the shared metrics handle used by every connection from this configuration.
    pub fn metrics(mut self, metrics: ClientMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Sets the Kafka security protocol used for broker connections.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.security_protocol = security_protocol;
        self
    }

    /// Sets the TLS server name used for certificate validation.
    ///
    /// By default kafrust derives the TLS server name from the bootstrap host.
    /// Use this when the bootstrap address differs from the broker certificate
    /// subject alternative name. The value is used by [`SecurityProtocol::Tls`]
    /// and [`SecurityProtocol::SaslTls`].
    pub fn tls_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.tls_server_name = Some(server_name.into());
        self
    }

    /// Adds a DER-encoded TLS root certificate for broker certificate validation.
    ///
    /// Extra roots augment the platform verifier when [`SecurityProtocol::Tls`]
    /// or [`SecurityProtocol::SaslTls`] is used. Platform roots are still used.
    pub fn tls_root_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.tls_root_certificates_der.push(certificate.into());
        self
    }

    /// Sets SASL/PLAIN credentials without changing the configured security protocol.
    ///
    /// Use [`SecurityProtocol::SaslPlaintext`] or [`SecurityProtocol::SaslTls`]
    /// to choose the transport. This separation mirrors Kafka's
    /// `security.protocol` and `sasl.mechanism` configuration model.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.sasl_credentials = Some(SaslCredentials::plain(username, password));
        self
    }

    /// Sets SASL/SCRAM-SHA-256 credentials without changing the configured security protocol.
    pub fn sasl_scram_sha_256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.sasl_credentials = Some(SaslCredentials::scram_sha_256(username, password));
        self
    }

    /// Sets SASL/SCRAM-SHA-512 credentials without changing the configured security protocol.
    pub fn sasl_scram_sha_512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.sasl_credentials = Some(SaslCredentials::scram_sha_512(username, password));
        self
    }

    /// Returns the configured bootstrap servers in connection order.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// Returns the configured Kafka client ID.
    pub fn client_id_ref(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Returns the configured request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the maximum broker response payload allocated for one request.
    pub fn max_response_bytes_ref(&self) -> usize {
        self.max_response_bytes
    }

    /// Returns the resource limits applied while decoding broker responses.
    pub fn decode_limits(&self) -> DecodeLimits {
        self.decode_limits
    }

    /// Returns the shared metrics handle used by connections from this configuration.
    pub fn metrics_ref(&self) -> ClientMetrics {
        self.metrics.clone()
    }

    pub(crate) fn record_retry(&self) {
        self.metrics.record_retry();
    }

    pub(crate) fn broker_error(&self, code: i16, context: String) -> Error {
        self.record_broker_error();
        Error::Broker { code, context }
    }

    pub(crate) fn record_broker_error(&self) {
        self.metrics.record_broker_error();
    }

    pub(crate) fn record_produce_batch(&self, records: usize) {
        self.metrics.record_produce_batch(records);
    }

    pub(crate) fn record_consumed(&self, records: usize) {
        self.metrics.record_consumed(records);
    }

    /// Returns the configured Kafka security protocol.
    pub fn security_protocol_ref(&self) -> SecurityProtocol {
        self.security_protocol
    }

    /// Returns the configured TLS server name override, when present.
    pub fn tls_server_name_ref(&self) -> Option<&str> {
        self.tls_server_name.as_deref()
    }

    /// Returns configured DER-encoded TLS root certificates.
    pub fn tls_root_certificates_der(&self) -> &[Vec<u8>] {
        &self.tls_root_certificates_der
    }

    /// Returns the configured SASL credentials, when present.
    pub fn sasl_credentials_ref(&self) -> Option<&SaslCredentials> {
        self.sasl_credentials.as_ref()
    }

    /// Connects to the first reachable bootstrap server.
    pub async fn connect(self) -> Result<Client> {
        if self.bootstrap_servers.is_empty() {
            return Err(Error::MissingBootstrapServer);
        }

        let mut last_error = None;
        for server in &self.bootstrap_servers {
            match self.connect_broker(server.clone()).await {
                Ok(client) => return Ok(client),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or(Error::MissingBootstrapServer))
    }

    pub(crate) async fn connect_broker(&self, server: String) -> Result<Client> {
        match self.security_protocol {
            SecurityProtocol::Plaintext => {
                Client::connect_with_request_timeout_and_metrics(
                    server,
                    self.client_id.clone(),
                    self.request_timeout,
                    self.max_response_bytes,
                    self.decode_limits,
                    self.metrics.clone(),
                )
                .await
            }
            SecurityProtocol::Tls => self.connect_tls_broker(server).await,
            SecurityProtocol::SaslPlaintext => self.connect_sasl_plaintext_broker(server).await,
            SecurityProtocol::SaslTls => self.connect_sasl_tls_broker(server).await,
        }
    }

    async fn connect_sasl_plaintext_broker(&self, server: String) -> Result<Client> {
        let credentials = self
            .sasl_credentials
            .as_ref()
            .ok_or(Error::MissingSaslCredentials)?;
        let mut client = Client::connect_with_request_timeout_and_metrics(
            server,
            self.client_id.clone(),
            self.request_timeout,
            self.max_response_bytes,
            self.decode_limits,
            self.metrics.clone(),
        )
        .await?;
        authenticate_sasl(&mut client, credentials).await?;
        Ok(client)
    }

    async fn connect_sasl_tls_broker(&self, server: String) -> Result<Client> {
        let credentials = self
            .sasl_credentials
            .as_ref()
            .ok_or(Error::MissingSaslCredentials)?;
        let mut client = self.connect_tls_broker(server).await?;
        authenticate_sasl(&mut client, credentials).await?;
        Ok(client)
    }

    #[cfg(feature = "tls")]
    async fn connect_tls_broker(&self, server: String) -> Result<Client> {
        use rustls::pki_types::{CertificateDer, ServerName};
        use rustls_platform_verifier::{BuilderVerifierExt, Verifier};
        use tokio_rustls::TlsConnector;

        let server_name_text = self.tls_server_name_for_connection(&server)?;
        let server_name = ServerName::try_from(server_name_text.clone()).map_err(|_| {
            Error::InvalidTlsServerName {
                server: server_name_text,
            }
        })?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls_builder = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|error| Error::TlsConfig {
                reason: format!("failed to configure TLS protocol versions: {error}"),
            })?;
        let tls_builder = if self.tls_root_certificates_der.is_empty() {
            tls_builder
                .with_platform_verifier()
                .map_err(|error| Error::TlsConfig {
                    reason: format!("failed to configure platform certificate verifier: {error}"),
                })?
        } else {
            let verifier = Verifier::new_with_extra_roots(
                self.tls_root_certificates_der
                    .iter()
                    .cloned()
                    .map(CertificateDer::from),
                provider,
            )
            .map_err(|error| Error::TlsConfig {
                reason: format!("failed to configure TLS certificate verifier: {error}"),
            })?;
            tls_builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
        };
        let tls_config = tls_builder.with_no_client_auth();

        let tcp_stream = tokio::net::TcpStream::connect(server.as_str()).await?;
        let tls_stream = TlsConnector::from(Arc::new(tls_config))
            .connect(server_name, tcp_stream)
            .await?;

        Ok(Client::from_stream_with_metrics(
            Box::new(tls_stream),
            self.client_id.clone(),
            Some(self.request_timeout),
            self.max_response_bytes,
            self.decode_limits,
            self.metrics.clone(),
        ))
    }

    #[cfg(not(feature = "tls"))]
    async fn connect_tls_broker(&self, _server: String) -> Result<Client> {
        Err(Error::Unsupported(
            "TLS connections require the `tls` feature",
        ))
    }
}

#[cfg(any(feature = "tls", test))]
fn tls_configured_server_name(server_name: &str) -> Result<String> {
    let trimmed = server_name.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidTlsServerName {
            server: server_name.to_owned(),
        });
    }

    Ok(trimmed.to_owned())
}

#[cfg(any(feature = "tls", test))]
impl ClientConfig {
    fn tls_server_name_for_connection(&self, server: &str) -> Result<String> {
        self.tls_server_name
            .as_deref()
            .map(tls_configured_server_name)
            .unwrap_or_else(|| tls_server_name_from_bootstrap_server(server))
    }
}

async fn authenticate_sasl(client: &mut Client, credentials: &SaslCredentials) -> Result<()> {
    let mechanism = credentials.mechanism().as_str();
    let handshake = client.sasl_handshake_v1(mechanism).await?;
    if handshake.error_code != 0 {
        return Err(
            client.broker_error(handshake.error_code, format!("sasl handshake {mechanism}"))
        );
    }

    if let Some(hash) = credentials.mechanism().scram_hash() {
        return authenticate_sasl_scram(client, credentials, mechanism, hash).await;
    }

    authenticate_sasl_plain(client, credentials, mechanism).await
}

async fn authenticate_sasl_plain(
    client: &mut Client,
    credentials: &SaslCredentials,
    mechanism: &'static str,
) -> Result<()> {
    let response = client
        .sasl_authenticate_v0(sasl_plain_auth_bytes(credentials))
        .await?;
    if response.error_code != 0 {
        return Err(client.broker_error(
            response.error_code,
            format!("sasl authenticate {mechanism}"),
        ));
    }

    Ok(())
}

async fn authenticate_sasl_scram(
    client: &mut Client,
    credentials: &SaslCredentials,
    mechanism: &'static str,
    hash: ScramHash,
) -> Result<()> {
    let nonce = scram::generate_nonce();
    let client_first = scram::client_first(credentials.username(), &nonce);

    let server_first = client
        .sasl_authenticate_v0(client_first.message.into_bytes())
        .await?;
    if server_first.error_code != 0 {
        return Err(client.broker_error(
            server_first.error_code,
            format!("sasl authenticate {mechanism} client-first"),
        ));
    }
    let server_first_text =
        str::from_utf8(&server_first.auth_bytes).map_err(|_| Error::InvalidSaslResponse {
            mechanism,
            reason: "server-first message was not UTF-8",
        })?;

    let client_final = scram::client_final(
        hash,
        credentials.password(),
        &client_first.bare,
        &nonce,
        server_first_text,
    )
    .map_err(|error| Error::InvalidSaslResponse {
        mechanism,
        reason: error.safe_reason(),
    })?;

    let server_final = client
        .sasl_authenticate_v0(client_final.message.into_bytes())
        .await?;
    if server_final.error_code != 0 {
        return Err(client.broker_error(
            server_final.error_code,
            format!("sasl authenticate {mechanism} client-final"),
        ));
    }
    let server_final_text =
        str::from_utf8(&server_final.auth_bytes).map_err(|_| Error::InvalidSaslResponse {
            mechanism,
            reason: "server-final message was not UTF-8",
        })?;
    scram::verify_server_final(&client_final.expected_server_signature, server_final_text)
        .map_err(|error| Error::InvalidSaslResponse {
            mechanism,
            reason: error.safe_reason(),
        })?;

    Ok(())
}

fn sasl_plain_auth_bytes(credentials: &SaslCredentials) -> Vec<u8> {
    let mut bytes =
        Vec::with_capacity(credentials.username().len() + credentials.password().len() + 2);
    bytes.push(0);
    bytes.extend_from_slice(credentials.username().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(credentials.password().as_bytes());
    bytes
}

#[cfg(any(feature = "tls", test))]
fn tls_server_name_from_bootstrap_server(server: &str) -> Result<String> {
    let trimmed = server.trim();
    let host = if let Some(without_opening_bracket) = trimmed.strip_prefix('[') {
        let (host, remainder) =
            without_opening_bracket
                .split_once(']')
                .ok_or_else(|| Error::InvalidTlsServerName {
                    server: server.to_owned(),
                })?;
        if !remainder.is_empty() && !remainder.starts_with(':') {
            return Err(Error::InvalidTlsServerName {
                server: server.to_owned(),
            });
        }
        host
    } else {
        trimmed
            .split_once(':')
            .map_or(trimmed, |(host, _port)| host)
    };

    if host.is_empty() {
        return Err(Error::InvalidTlsServerName {
            server: server.to_owned(),
        });
    }

    Ok(host.to_owned())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        tls_server_name_from_bootstrap_server, ClientConfig, SaslCredentials, SaslMechanism,
        SecurityProtocol,
    };
    use crate::scram::{self, ScramHash};
    use crate::{ClientMetrics, Error};
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use std::str;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn stores_bootstrap_servers_and_client_id() {
        let config = ClientConfig::new(["localhost:9092"])
            .client_id("kafrust-test")
            .request_timeout_ms(5_000)
            .max_response_bytes(8 * 1024 * 1024)
            .max_decode_array_elements(12_345)
            .max_decompressed_record_bytes(4 * 1024 * 1024);

        assert_eq!(config.bootstrap_servers(), &["localhost:9092".to_owned()]);
        assert_eq!(config.client_id_ref(), Some("kafrust-test"));
        assert_eq!(config.request_timeout(), Duration::from_millis(5_000));
        assert_eq!(config.max_response_bytes_ref(), 8 * 1024 * 1024);
        assert_eq!(config.decode_limits().max_array_elements(), 12_345);
        assert_eq!(
            config.decode_limits().max_decompressed_record_bytes(),
            4 * 1024 * 1024
        );
        assert_eq!(config.security_protocol_ref(), SecurityProtocol::Plaintext);
    }

    #[test]
    fn stores_security_protocol() {
        let config =
            ClientConfig::new(["localhost:9092"]).security_protocol(SecurityProtocol::SaslTls);

        assert_eq!(config.security_protocol_ref(), SecurityProtocol::SaslTls);
    }

    #[test]
    fn stores_tls_server_name_override() {
        let config = ClientConfig::new(["127.0.0.1:9093"])
            .security_protocol(SecurityProtocol::Tls)
            .tls_server_name("broker.example.com");

        assert_eq!(config.tls_server_name_ref(), Some("broker.example.com"));
        assert_eq!(
            config
                .tls_server_name_for_connection("127.0.0.1:9093")
                .unwrap(),
            "broker.example.com"
        );
    }

    #[test]
    fn stores_tls_root_certificate_der() {
        let config = ClientConfig::new(["localhost:9093"])
            .security_protocol(SecurityProtocol::Tls)
            .tls_root_certificate_der([1, 2, 3])
            .tls_root_certificate_der([4, 5, 6]);

        assert_eq!(
            config.tls_root_certificates_der(),
            &[vec![1, 2, 3], vec![4, 5, 6]]
        );
    }

    #[test]
    fn derives_tls_server_name_from_bootstrap_when_override_is_absent() {
        let config = ClientConfig::new(["broker.example.com:9093"]);

        assert_eq!(
            config
                .tls_server_name_for_connection("broker.example.com:9093")
                .unwrap(),
            "broker.example.com"
        );
    }

    #[test]
    fn rejects_empty_tls_server_name_override() {
        let config = ClientConfig::new(["localhost:9093"]).tls_server_name("   ");

        assert!(matches!(
            config.tls_server_name_for_connection("localhost:9093"),
            Err(Error::InvalidTlsServerName { .. })
        ));
    }

    #[test]
    fn stores_sasl_plain_credentials_without_changing_security_protocol() {
        let config = ClientConfig::new(["localhost:9092"]).sasl_plain("alice", "secret-password");
        let credentials = config.sasl_credentials_ref().unwrap();

        assert_eq!(config.security_protocol_ref(), SecurityProtocol::Plaintext);
        assert_eq!(credentials.mechanism(), SaslMechanism::Plain);
        assert_eq!(credentials.mechanism().as_str(), "PLAIN");
        assert_eq!(credentials.username(), "alice");
        assert_eq!(credentials.password(), "secret-password");
    }

    #[test]
    fn stores_sasl_scram_credentials_without_changing_security_protocol() {
        let config =
            ClientConfig::new(["localhost:9092"]).sasl_scram_sha_256("alice", "secret-password");
        let credentials = config.sasl_credentials_ref().unwrap();

        assert_eq!(config.security_protocol_ref(), SecurityProtocol::Plaintext);
        assert_eq!(credentials.mechanism(), SaslMechanism::ScramSha256);
        assert_eq!(credentials.mechanism().as_str(), "SCRAM-SHA-256");
        assert_eq!(credentials.username(), "alice");
        assert_eq!(credentials.password(), "secret-password");

        let config =
            ClientConfig::new(["localhost:9092"]).sasl_scram_sha_512("alice", "secret-password");
        let credentials = config.sasl_credentials_ref().unwrap();

        assert_eq!(config.security_protocol_ref(), SecurityProtocol::Plaintext);
        assert_eq!(credentials.mechanism(), SaslMechanism::ScramSha512);
        assert_eq!(credentials.mechanism().as_str(), "SCRAM-SHA-512");
    }

    #[test]
    fn redacts_sasl_password_in_debug_output() {
        let credentials = SaslCredentials::plain("alice", "secret-password");
        let config = ClientConfig::new(["localhost:9092"])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .sasl_plain("alice", "secret-password");

        assert!(!format!("{credentials:?}").contains("secret-password"));
        assert!(format!("{credentials:?}").contains("<redacted>"));
        assert!(!format!("{config:?}").contains("secret-password"));
        assert!(format!("{config:?}").contains("<redacted>"));
    }

    #[tokio::test]
    async fn connects_to_later_bootstrap_server_when_first_fails() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _accepted = listener.accept().await.unwrap();
        });

        let client = ClientConfig::new(["127.0.0.1:1".to_owned(), addr.to_string()])
            .request_timeout_ms(5_000)
            .connect()
            .await;

        assert!(client.is_ok());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn propagates_metrics_handle_to_connected_client() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _accepted = listener.accept().await.unwrap();
        });
        let metrics = ClientMetrics::new();

        let client = ClientConfig::new([addr.to_string()])
            .metrics(metrics.clone())
            .connect()
            .await
            .unwrap();

        assert_eq!(client.metrics(), metrics);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_sasl_security_protocol_without_credentials_before_connecting() {
        let error = ClientConfig::new(["localhost:9092"])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(error, Error::MissingSaslCredentials));
    }

    #[tokio::test]
    async fn sasl_plaintext_connect_sends_handshake_and_authenticate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            let handshake = read_frame(&mut socket).await;
            assert_eq!(
                handshake,
                [
                    0, 17, // api key
                    0, 1, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // null client id
                    0, 5, b'P', b'L', b'A', b'I', b'N', // mechanism
                ]
            );
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, // error code
                    0, 0, 0, 1, // mechanism count
                    0, 5, b'P', b'L', b'A', b'I', b'N', // mechanism
                ],
            )
            .await;

            let authenticate = read_frame(&mut socket).await;
            assert_eq!(
                authenticate,
                [
                    0, 36, // api key
                    0, 0, // api version
                    0, 0, 0, 2, // correlation id
                    0xff, 0xff, // null client id
                    0, 0, 0, 13, // auth bytes length
                    0, b'a', b'l', b'i', b'c', b'e', 0, b's', b'e', b'c', b'r', b'e', b't',
                ]
            );
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 2, // correlation id
                    0, 0, // error code
                    0xff, 0xff, // null error message
                    0, 0, 0, 0, // auth bytes
                ],
            )
            .await;
        });

        let client = ClientConfig::new([addr.to_string()])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .sasl_plain("alice", "secret")
            .request_timeout_ms(1_000)
            .connect()
            .await;

        assert!(client.is_ok());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sasl_scram_sha256_connect_sends_scram_exchange() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            let handshake = read_frame(&mut socket).await;
            assert_eq!(
                handshake,
                [
                    0, 17, // api key
                    0, 1, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // null client id
                    0, 13, b'S', b'C', b'R', b'A', b'M', b'-', b'S', b'H', b'A', b'-', b'2', b'5',
                    b'6', // mechanism
                ]
            );
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, // error code
                    0, 0, 0, 1, // mechanism count
                    0, 13, b'S', b'C', b'R', b'A', b'M', b'-', b'S', b'H', b'A', b'-', b'2', b'5',
                    b'6', // mechanism
                ],
            )
            .await;

            let client_first_frame = read_frame(&mut socket).await;
            let client_first =
                str::from_utf8(sasl_authenticate_auth_bytes(&client_first_frame, 2)).unwrap();
            assert!(client_first.starts_with("n,,n=alice,r="));
            let client_first_bare = client_first.strip_prefix("n,,").unwrap();
            let (_, client_nonce) = client_first_bare.split_once("r=").unwrap();
            let server_first = format!("r={client_nonce}servernonce,s=QSXCR+Q6sek8bf92,i=4096");
            write_sasl_authenticate_response(&mut socket, 2, server_first.as_bytes()).await;

            let client_final_frame = read_frame(&mut socket).await;
            let client_final =
                str::from_utf8(sasl_authenticate_auth_bytes(&client_final_frame, 3)).unwrap();
            let expected_final = scram::client_final(
                ScramHash::Sha256,
                "secret",
                client_first_bare,
                client_nonce,
                &server_first,
            )
            .unwrap();
            assert_eq!(client_final, expected_final.message);

            let server_signature = BASE64.encode(expected_final.expected_server_signature);
            let server_final = format!("v={server_signature}");
            write_sasl_authenticate_response(&mut socket, 3, server_final.as_bytes()).await;
        });

        let client = ClientConfig::new([addr.to_string()])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .sasl_scram_sha_256("alice", "secret")
            .request_timeout_ms(1_000)
            .connect()
            .await;

        assert!(client.is_ok());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sasl_authenticate_error_does_not_expose_secret() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metrics = ClientMetrics::new();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _handshake = read_frame(&mut socket).await;
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, // error code
                    0, 0, 0, 1, // mechanism count
                    0, 5, b'P', b'L', b'A', b'I', b'N', // mechanism
                ],
            )
            .await;

            let _authenticate = read_frame(&mut socket).await;
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 2, // correlation id
                    0, 58, // error code
                    0, 6, b's', b'e', b'c', b'r', b'e', b't', // error message
                    0, 0, 0, 0, // auth bytes
                ],
            )
            .await;
        });

        let error = ClientConfig::new([addr.to_string()])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .sasl_plain("alice", "secret")
            .request_timeout_ms(1_000)
            .metrics(metrics.clone())
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(
            &error,
            Error::Broker { code: 58, context } if context == "sasl authenticate PLAIN"
        ));
        assert!(!error.to_string().contains("secret"));
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sasl_scram_invalid_server_final_does_not_expose_secret() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _handshake = read_frame(&mut socket).await;
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, // error code
                    0, 0, 0, 1, // mechanism count
                    0, 13, b'S', b'C', b'R', b'A', b'M', b'-', b'S', b'H', b'A', b'-', b'2', b'5',
                    b'6', // mechanism
                ],
            )
            .await;

            let client_first_frame = read_frame(&mut socket).await;
            let client_first =
                str::from_utf8(sasl_authenticate_auth_bytes(&client_first_frame, 2)).unwrap();
            let client_first_bare = client_first.strip_prefix("n,,").unwrap();
            let (_, client_nonce) = client_first_bare.split_once("r=").unwrap();
            let server_first = format!("r={client_nonce}servernonce,s=QSXCR+Q6sek8bf92,i=4096");
            write_sasl_authenticate_response(&mut socket, 2, server_first.as_bytes()).await;

            let _client_final = read_frame(&mut socket).await;
            write_sasl_authenticate_response(&mut socket, 3, b"v=AAAA").await;
        });

        let error = ClientConfig::new([addr.to_string()])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .sasl_scram_sha_256("alice", "secret")
            .request_timeout_ms(1_000)
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(
            &error,
            Error::InvalidSaslResponse {
                mechanism: "SCRAM-SHA-256",
                reason: "server signature did not match"
            }
        ));
        assert!(!error.to_string().contains("secret"));
        server.await.unwrap();
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn rejects_tls_without_tls_feature() {
        let error = ClientConfig::new(["localhost:9092"])
            .security_protocol(SecurityProtocol::Tls)
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("TLS connections require the `tls` feature")
        ));
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn rejects_sasl_tls_without_tls_feature_after_credentials() {
        let error = ClientConfig::new(["localhost:9092"])
            .security_protocol(SecurityProtocol::SaslTls)
            .sasl_plain("alice", "secret")
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("TLS connections require the `tls` feature")
        ));
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn rejects_invalid_tls_server_name_before_connecting() {
        let error = ClientConfig::new([":9093"])
            .security_protocol(SecurityProtocol::Tls)
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(error, Error::InvalidTlsServerName { .. }));
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn rejects_invalid_tls_root_certificate_before_connecting() {
        let error = ClientConfig::new(["localhost:9093"])
            .security_protocol(SecurityProtocol::Tls)
            .tls_root_certificate_der([1, 2, 3])
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(error, Error::TlsConfig { .. }));
    }

    #[test]
    fn extracts_tls_server_name_from_bootstrap_server() {
        assert_eq!(
            tls_server_name_from_bootstrap_server("broker.example.com:9093").unwrap(),
            "broker.example.com"
        );
        assert_eq!(
            tls_server_name_from_bootstrap_server("localhost").unwrap(),
            "localhost"
        );
        assert_eq!(
            tls_server_name_from_bootstrap_server("[2001:db8::1]:9093").unwrap(),
            "2001:db8::1"
        );
    }

    #[test]
    fn rejects_empty_tls_server_name() {
        assert!(matches!(
            tls_server_name_from_bootstrap_server(":9093"),
            Err(Error::InvalidTlsServerName { .. })
        ));
        assert!(matches!(
            tls_server_name_from_bootstrap_server("[2001:db8::1"),
            Err(Error::InvalidTlsServerName { .. })
        ));
    }

    async fn read_frame(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut size = [0u8; 4];
        socket.read_exact(&mut size).await.unwrap();
        let size = usize::try_from(i32::from_be_bytes(size)).unwrap();
        let mut frame = vec![0u8; size];
        socket.read_exact(&mut frame).await.unwrap();
        frame
    }

    fn sasl_authenticate_auth_bytes(frame: &[u8], correlation_id: i32) -> &[u8] {
        assert_eq!(&frame[0..2], &[0, 36]);
        assert_eq!(&frame[2..4], &[0, 0]);
        assert_eq!(&frame[4..8], &correlation_id.to_be_bytes());
        assert_eq!(&frame[8..10], &[0xff, 0xff]);
        let auth_len = i32::from_be_bytes(frame[10..14].try_into().unwrap());
        let auth_len = usize::try_from(auth_len).unwrap();
        assert_eq!(frame.len(), 14 + auth_len);
        &frame[14..]
    }

    async fn write_sasl_authenticate_response(
        socket: &mut tokio::net::TcpStream,
        correlation_id: i32,
        auth_bytes: &[u8],
    ) {
        let mut frame = Vec::with_capacity(12 + auth_bytes.len());
        frame.extend_from_slice(&correlation_id.to_be_bytes());
        frame.extend_from_slice(&0i16.to_be_bytes());
        frame.extend_from_slice(&(-1i16).to_be_bytes());
        frame.extend_from_slice(&(auth_bytes.len() as i32).to_be_bytes());
        frame.extend_from_slice(auth_bytes);
        write_frame(socket, &frame).await;
    }

    async fn write_frame(socket: &mut tokio::net::TcpStream, frame: &[u8]) {
        socket
            .write_all(&(frame.len() as i32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(frame).await.unwrap();
        socket.flush().await.unwrap();
    }
}
