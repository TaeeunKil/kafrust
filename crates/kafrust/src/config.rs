use crate::broker_client_cache::{SharedBrokerClientCache, SharedBrokerClientCacheHandle};
use crate::client::{Client, DEFAULT_MAX_RESPONSE_BYTES};
use crate::error::{Error, Result};
use crate::metrics::ClientMetrics;
use crate::scram::{self, ScramHash};
use core::fmt;
use kafrust_protocol::codec::DecodeLimits;
use std::future::Future;
use std::pin::Pin;
use std::str;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_IDLE_BROKER_CONNECTIONS: usize = 64;

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
    /// Kafka `OAUTHBEARER` SASL mechanism.
    OAuthBearer,
}

impl SaslMechanism {
    /// Returns the Kafka protocol mechanism name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::OAuthBearer => "OAUTHBEARER",
        }
    }

    fn scram_hash(self) -> Option<ScramHash> {
        match self {
            Self::Plain => None,
            Self::ScramSha256 => Some(ScramHash::Sha256),
            Self::ScramSha512 => Some(ScramHash::Sha512),
            Self::OAuthBearer => None,
        }
    }
}

/// Future returned by an [`OAuthBearerTokenProvider`].
pub type OAuthBearerTokenFuture = Pin<Box<dyn Future<Output = Result<String>> + Send>>;

/// A bearer token together with the time at which the issuer says it expires.
///
/// The token is redacted from `Debug` output. Applications should derive the
/// expiration time from the issuer response rather than guessing it from a
/// local refresh interval.
#[derive(Clone)]
pub struct OAuthBearerToken {
    token: String,
    expires_at: std::time::SystemTime,
}

impl OAuthBearerToken {
    /// Creates a token value with its issuer-provided expiration time.
    pub fn new(token: impl Into<String>, expires_at: std::time::SystemTime) -> Self {
        Self {
            token: token.into(),
            expires_at,
        }
    }

    /// Returns the bearer token for SASL authentication.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Returns the issuer-provided expiration time.
    pub fn expires_at(&self) -> std::time::SystemTime {
        self.expires_at
    }

    fn is_fresh(&self, now: std::time::SystemTime, refresh_before: Duration) -> bool {
        self.expires_at
            .duration_since(now)
            .is_ok_and(|remaining| remaining > refresh_before)
    }

    fn is_valid(&self, now: std::time::SystemTime) -> bool {
        self.expires_at.duration_since(now).is_ok()
    }
}

impl fmt::Debug for OAuthBearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthBearerToken")
            .field("token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Future returned by an [`OAuthBearerTokenSource`].
pub type OAuthBearerTokenSourceFuture =
    Pin<Box<dyn Future<Output = Result<OAuthBearerToken>> + Send>>;

/// Supplies an issuer token and its expiration metadata to a cached provider.
pub trait OAuthBearerTokenSource: Send + Sync {
    /// Fetches a token without exposing it to tracing or debug output.
    fn fetch_token(&self) -> OAuthBearerTokenSourceFuture;
}

impl<F, Fut> OAuthBearerTokenSource for F
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<OAuthBearerToken>> + Send + 'static,
{
    fn fetch_token(&self) -> OAuthBearerTokenSourceFuture {
        Box::pin(self())
    }
}

/// OAUTHBEARER provider that caches a token until its refresh window.
///
/// A refresh failure falls back to the cached token while it is still valid.
/// Once that token has expired, the provider returns the refresh error so the
/// connection cannot authenticate with stale credentials. The cache is safe
/// to share across the broker connections created from one client config.
pub struct CachedOAuthBearerTokenProvider<S> {
    source: Arc<S>,
    refresh_before: Duration,
    cached: Arc<tokio::sync::Mutex<Option<OAuthBearerToken>>>,
}

impl<S> CachedOAuthBearerTokenProvider<S> {
    /// Creates a cached provider with a refresh window before token expiry.
    pub fn new(source: S, refresh_before: Duration) -> Self {
        Self {
            source: Arc::new(source),
            refresh_before,
            cached: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

impl<S> fmt::Debug for CachedOAuthBearerTokenProvider<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedOAuthBearerTokenProvider")
            .field("refresh_before", &self.refresh_before)
            .field("cached", &"<redacted>")
            .finish()
    }
}

impl<S> OAuthBearerTokenProvider for CachedOAuthBearerTokenProvider<S>
where
    S: OAuthBearerTokenSource + 'static,
{
    fn fetch_token(&self) -> OAuthBearerTokenFuture {
        let source = Arc::clone(&self.source);
        let cached = Arc::clone(&self.cached);
        let refresh_before = self.refresh_before;
        Box::pin(async move {
            let now = std::time::SystemTime::now();
            let cached_token = { cached.lock().await.clone() };
            if let Some(token) = cached_token
                .as_ref()
                .filter(|token| token.is_fresh(now, refresh_before))
            {
                return Ok(token.token.clone());
            }

            match source.fetch_token().await {
                Ok(token) => {
                    let value = token.token.clone();
                    *cached.lock().await = Some(token);
                    Ok(value)
                }
                Err(error) => {
                    if let Some(token) = cached_token
                        .as_ref()
                        .filter(|token| token.is_valid(std::time::SystemTime::now()))
                    {
                        Ok(token.token.clone())
                    } else {
                        Err(error)
                    }
                }
            }
        })
    }
}

/// Supplies a fresh SASL/OAUTHBEARER token for a broker connection.
///
/// The provider is called whenever kafrust authenticates a new broker
/// connection. Implementations can therefore refresh an expiring token before
/// a bootstrap, metadata, coordinator, or failover connection is used.
/// The call is bounded by [`ClientConfig::request_timeout_ms`]; a provider that
/// exceeds that limit returns [`Error::OAuthBearerTokenTimeout`].
pub trait OAuthBearerTokenProvider: Send + Sync {
    /// Fetches a token without exposing it to tracing or debug output.
    fn fetch_token(&self) -> OAuthBearerTokenFuture;
}

impl<F, Fut> OAuthBearerTokenProvider for F
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<String>> + Send + 'static,
{
    fn fetch_token(&self) -> OAuthBearerTokenFuture {
        Box::pin(self())
    }
}

#[derive(Clone)]
/// SASL authentication material.
///
/// `Debug` output redacts the password so config diagnostics do not expose raw
/// credentials. The password accessor is still available because callers own
/// the configured secret and future authentication code needs the raw value.
pub struct SaslCredentials {
    mechanism: SaslMechanism,
    username: String,
    password: String,
    oauthbearer_token: Option<String>,
    oauthbearer_token_provider: Option<Arc<dyn OAuthBearerTokenProvider>>,
}

impl SaslCredentials {
    /// Creates SASL/PLAIN credentials from a username and password.
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::Plain,
            username: username.into(),
            password: password.into(),
            oauthbearer_token: None,
            oauthbearer_token_provider: None,
        }
    }

    /// Creates SASL/SCRAM-SHA-256 credentials from a username and password.
    pub fn scram_sha_256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha256,
            username: username.into(),
            password: password.into(),
            oauthbearer_token: None,
            oauthbearer_token_provider: None,
        }
    }

    /// Creates SASL/SCRAM-SHA-512 credentials from a username and password.
    pub fn scram_sha_512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            mechanism: SaslMechanism::ScramSha512,
            username: username.into(),
            password: password.into(),
            oauthbearer_token: None,
            oauthbearer_token_provider: None,
        }
    }

    /// Creates SASL/OAUTHBEARER credentials from a bearer token.
    ///
    /// The token is sent only during the SASL exchange and is redacted from
    /// `Debug` output. Use [`Self::oauthbearer_with_username`] when the broker
    /// requires an authorization identity in the GS2 header.
    pub fn oauthbearer(token: impl Into<String>) -> Self {
        Self::oauthbearer_with_username("", token)
    }

    /// Creates SASL/OAUTHBEARER credentials with an optional authorization identity.
    pub fn oauthbearer_with_username(
        username: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            mechanism: SaslMechanism::OAuthBearer,
            username: username.into(),
            password: String::new(),
            oauthbearer_token: Some(token.into()),
            oauthbearer_token_provider: None,
        }
    }

    /// Creates SASL/OAUTHBEARER credentials from an async token provider.
    pub fn oauthbearer_with_provider<P>(provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        Self::oauthbearer_with_username_and_provider("", provider)
    }

    /// Creates SASL/OAUTHBEARER credentials with an authorization identity and
    /// an async token provider.
    pub fn oauthbearer_with_username_and_provider<P>(
        username: impl Into<String>,
        provider: P,
    ) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        Self {
            mechanism: SaslMechanism::OAuthBearer,
            username: username.into(),
            password: String::new(),
            oauthbearer_token: None,
            oauthbearer_token_provider: Some(Arc::new(provider)),
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

    /// Returns the configured OAUTHBEARER token, if this is an OAUTHBEARER credential.
    pub fn oauthbearer_token(&self) -> Option<&str> {
        self.oauthbearer_token.as_deref()
    }

    pub(crate) async fn oauthbearer_token_for_auth(
        &self,
        timeout: Option<Duration>,
    ) -> Result<String> {
        if let Some(token) = &self.oauthbearer_token {
            return Ok(token.clone());
        }
        let provider = self
            .oauthbearer_token_provider
            .as_ref()
            .ok_or(Error::Unsupported("SASL/OAUTHBEARER token is missing"))?;
        let future = provider.fetch_token();
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, future).await.map_err(|_| {
                Error::OAuthBearerTokenTimeout {
                    timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                }
            })?,
            None => future.await,
        }
    }

    pub(crate) fn supports_oauthbearer_reauthentication(&self) -> bool {
        self.mechanism == SaslMechanism::OAuthBearer && self.oauthbearer_token_provider.is_some()
    }
}

impl PartialEq for SaslCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.mechanism == other.mechanism
            && self.username == other.username
            && self.password == other.password
            && self.oauthbearer_token == other.oauthbearer_token
            && match (
                &self.oauthbearer_token_provider,
                &other.oauthbearer_token_provider,
            ) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
    }
}

impl Eq for SaslCredentials {}

impl fmt::Debug for SaslCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaslCredentials")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field(
                "oauthbearer_token",
                &self.oauthbearer_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "oauthbearer_token_provider",
                &self
                    .oauthbearer_token_provider
                    .as_ref()
                    .map(|_| "<configured>"),
            )
            .finish()
    }
}

#[derive(Clone)]
/// Connection settings shared by low-level clients, producers, and consumers.
pub struct ClientConfig {
    bootstrap_servers: Vec<String>,
    controller_bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    client_rack: Option<String>,
    request_timeout: Duration,
    max_response_bytes: usize,
    max_idle_broker_connections: usize,
    decode_limits: DecodeLimits,
    security_protocol: SecurityProtocol,
    tls_server_name: Option<String>,
    tls_root_certificates_der: Vec<Vec<u8>>,
    tls_client_certificates_der: Vec<Vec<u8>>,
    tls_client_private_key_der: Option<Vec<u8>>,
    sasl_credentials: Option<SaslCredentials>,
    metrics: ClientMetrics,
    shared_broker_clients: SharedBrokerClientCacheHandle,
}

impl PartialEq for ClientConfig {
    fn eq(&self, other: &Self) -> bool {
        self.bootstrap_servers == other.bootstrap_servers
            && self.controller_bootstrap_servers == other.controller_bootstrap_servers
            && self.client_id == other.client_id
            && self.client_rack == other.client_rack
            && self.request_timeout == other.request_timeout
            && self.max_response_bytes == other.max_response_bytes
            && self.max_idle_broker_connections == other.max_idle_broker_connections
            && self.decode_limits == other.decode_limits
            && self.security_protocol == other.security_protocol
            && self.tls_server_name == other.tls_server_name
            && self.tls_root_certificates_der == other.tls_root_certificates_der
            && self.tls_client_certificates_der == other.tls_client_certificates_der
            && self.tls_client_private_key_der == other.tls_client_private_key_der
            && self.sasl_credentials == other.sasl_credentials
            && self.metrics == other.metrics
    }
}

impl Eq for ClientConfig {}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field(
                "controller_bootstrap_servers",
                &self.controller_bootstrap_servers,
            )
            .field("client_id", &self.client_id)
            .field("client_rack", &self.client_rack)
            .field("request_timeout", &self.request_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field(
                "max_idle_broker_connections",
                &self.max_idle_broker_connections,
            )
            .field("decode_limits", &self.decode_limits)
            .field("security_protocol", &self.security_protocol)
            .field("tls_server_name", &self.tls_server_name)
            .field(
                "tls_root_certificate_count",
                &self.tls_root_certificates_der.len(),
            )
            .field(
                "tls_client_certificate_count",
                &self.tls_client_certificates_der.len(),
            )
            .field(
                "tls_client_private_key_der",
                &self
                    .tls_client_private_key_der
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("sasl_credentials", &self.sasl_credentials)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl ClientConfig {
    /// Creates a client configuration from one or more Kafka bootstrap servers.
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into_iter().map(Into::into).collect(),
            controller_bootstrap_servers: Vec::new(),
            client_id: None,
            client_rack: None,
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_idle_broker_connections: DEFAULT_MAX_IDLE_BROKER_CONNECTIONS,
            decode_limits: DecodeLimits::default(),
            security_protocol: SecurityProtocol::Plaintext,
            tls_server_name: None,
            tls_root_certificates_der: Vec::new(),
            tls_client_certificates_der: Vec::new(),
            tls_client_private_key_der: None,
            sasl_credentials: None,
            metrics: ClientMetrics::new(),
            shared_broker_clients: std::sync::Arc::new(SharedBrokerClientCache::default()),
        }
    }

    fn reset_shared_broker_clients(&mut self) {
        self.shared_broker_clients = std::sync::Arc::new(SharedBrokerClientCache::default());
    }

    /// Sets the Kafka client ID sent in request headers.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.reset_shared_broker_clients();
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets optional KRaft controller listener bootstrap servers.
    ///
    /// Most Kafka requests use [`Self::new`] bootstrap servers. Controller-only
    /// APIs such as `DescribeQuorum` may require a separately advertised
    /// controller listener, so AdminClient uses these addresses when they are
    /// configured. If omitted, controller-scoped operations retain their
    /// metadata-discovered broker routing behavior.
    pub fn controller_bootstrap_servers(
        mut self,
        servers: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.controller_bootstrap_servers = servers.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the rack ID advertised by rack-aware consumer Fetch requests.
    ///
    /// When configured, consumers negotiate Fetch v11 or newer-compatible
    /// broker support and include this value so Kafka can select a preferred
    /// replica in the same rack. Brokers that do not advertise Fetch v11 use
    /// the existing leader Fetch path instead.
    pub fn client_rack(mut self, client_rack: impl Into<String>) -> Self {
        self.client_rack = Some(client_rack.into());
        self
    }

    /// Sets the request timeout applied after a broker connection is established.
    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.reset_shared_broker_clients();
        self.request_timeout = Duration::from_millis(request_timeout_ms);
        self
    }

    /// Sets the maximum broker response payload allocated for one request.
    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.reset_shared_broker_clients();
        self.max_response_bytes = max_response_bytes;
        self
    }

    /// Sets the maximum number of idle broker connections retained by a
    /// producer or direct consumer built from this configuration.
    ///
    /// Connections are evicted in FIFO order after a successful request. A
    /// value of zero is rejected during [`Self::validate`].
    pub fn max_idle_broker_connections(mut self, max: usize) -> Self {
        self.reset_shared_broker_clients();
        self.max_idle_broker_connections = max;
        self
    }

    /// Sets the maximum number of elements allocated for one Kafka array.
    pub fn max_decode_array_elements(mut self, max: usize) -> Self {
        self.reset_shared_broker_clients();
        self.decode_limits = self.decode_limits.with_max_array_elements(max);
        self
    }

    /// Sets the maximum uncompressed size of one fetched record batch.
    pub fn max_decompressed_record_bytes(mut self, max: usize) -> Self {
        self.reset_shared_broker_clients();
        self.decode_limits = self.decode_limits.with_max_decompressed_record_bytes(max);
        self
    }

    /// Sets the shared metrics handle used by every connection from this configuration.
    pub fn metrics(mut self, metrics: ClientMetrics) -> Self {
        self.reset_shared_broker_clients();
        self.metrics = metrics;
        self
    }

    /// Sets the Kafka security protocol used for broker connections.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.reset_shared_broker_clients();
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
        self.reset_shared_broker_clients();
        self.tls_server_name = Some(server_name.into());
        self
    }

    /// Adds a DER-encoded TLS root certificate for broker certificate validation.
    ///
    /// Extra roots augment the platform verifier when [`SecurityProtocol::Tls`]
    /// or [`SecurityProtocol::SaslTls`] is used. Platform roots are still used.
    pub fn tls_root_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.reset_shared_broker_clients();
        self.tls_root_certificates_der.push(certificate.into());
        self
    }

    /// Adds a DER-encoded client certificate to the TLS certificate chain.
    ///
    /// The matching private key must be configured with
    /// [`Self::tls_client_private_key_der`]. The certificate chain is sent
    /// only for [`SecurityProtocol::Tls`] and [`SecurityProtocol::SaslTls`]
    /// connections. The bytes are retained as configuration material and are
    /// never included in `Debug` output.
    pub fn tls_client_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.reset_shared_broker_clients();
        self.tls_client_certificates_der.push(certificate.into());
        self
    }

    /// Sets the DER-encoded private key used for TLS client authentication.
    ///
    /// The key may use one of the formats accepted by
    /// `rustls-pki-types::PrivateKeyDer` (PKCS#1, PKCS#8, or SEC1). A client
    /// certificate must also be configured with
    /// [`Self::tls_client_certificate_der`].
    pub fn tls_client_private_key_der(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.reset_shared_broker_clients();
        self.tls_client_private_key_der = Some(key.into());
        self
    }

    /// Sets SASL/PLAIN credentials without changing the configured security protocol.
    ///
    /// Use [`SecurityProtocol::SaslPlaintext`] or [`SecurityProtocol::SaslTls`]
    /// to choose the transport. This separation mirrors Kafka's
    /// `security.protocol` and `sasl.mechanism` configuration model.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.reset_shared_broker_clients();
        self.sasl_credentials = Some(SaslCredentials::plain(username, password));
        self
    }

    /// Sets SASL/SCRAM-SHA-256 credentials without changing the configured security protocol.
    pub fn sasl_scram_sha_256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.reset_shared_broker_clients();
        self.sasl_credentials = Some(SaslCredentials::scram_sha_256(username, password));
        self
    }

    /// Sets SASL/SCRAM-SHA-512 credentials without changing the configured security protocol.
    pub fn sasl_scram_sha_512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.reset_shared_broker_clients();
        self.sasl_credentials = Some(SaslCredentials::scram_sha_512(username, password));
        self
    }

    /// Sets SASL/OAUTHBEARER credentials without changing the configured security protocol.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.reset_shared_broker_clients();
        self.sasl_credentials = Some(SaslCredentials::oauthbearer(token));
        self
    }

    /// Sets SASL/OAUTHBEARER credentials with an authorization identity.
    pub fn sasl_oauthbearer_with_username(
        mut self,
        username: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        self.reset_shared_broker_clients();
        self.sasl_credentials = Some(SaslCredentials::oauthbearer_with_username(username, token));
        self
    }

    /// Sets SASL/OAUTHBEARER credentials from an async token provider.
    pub fn sasl_oauthbearer_provider<P>(mut self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        self.reset_shared_broker_clients();
        self.sasl_credentials = Some(SaslCredentials::oauthbearer_with_provider(provider));
        self
    }

    /// Sets SASL/OAUTHBEARER credentials with an authorization identity and
    /// an async token provider.
    pub fn sasl_oauthbearer_with_username_and_provider<P>(
        mut self,
        username: impl Into<String>,
        provider: P,
    ) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        self.reset_shared_broker_clients();
        self.sasl_credentials = Some(SaslCredentials::oauthbearer_with_username_and_provider(
            username, provider,
        ));
        self
    }

    /// Returns the configured bootstrap servers in connection order.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// Returns explicitly configured KRaft controller listener bootstrap servers.
    pub fn controller_bootstrap_servers_ref(&self) -> &[String] {
        &self.controller_bootstrap_servers
    }

    /// Returns the configured Kafka client ID.
    pub fn client_id_ref(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Returns the configured rack ID, when rack-aware consumer fetching is enabled.
    pub fn client_rack_ref(&self) -> Option<&str> {
        self.client_rack.as_deref()
    }

    /// Returns the configured request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the maximum broker response payload allocated for one request.
    pub fn max_response_bytes_ref(&self) -> usize {
        self.max_response_bytes
    }

    /// Returns the maximum number of idle broker connections retained by a
    /// high-level client built from this configuration.
    pub fn max_idle_broker_connections_ref(&self) -> usize {
        self.max_idle_broker_connections
    }

    /// Returns the resource limits applied while decoding broker responses.
    pub fn decode_limits(&self) -> DecodeLimits {
        self.decode_limits
    }

    /// Returns the shared metrics handle used by connections from this configuration.
    pub fn metrics_ref(&self) -> ClientMetrics {
        self.metrics.clone()
    }

    pub(crate) fn shared_broker_clients(&self) -> SharedBrokerClientCacheHandle {
        std::sync::Arc::clone(&self.shared_broker_clients)
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

    /// Returns configured DER-encoded TLS client certificates.
    pub fn tls_client_certificates_der(&self) -> &[Vec<u8>] {
        &self.tls_client_certificates_der
    }

    /// Returns whether a TLS client private key is configured.
    ///
    /// The key bytes are intentionally not exposed through a public accessor.
    pub fn has_tls_client_private_key(&self) -> bool {
        self.tls_client_private_key_der.is_some()
    }

    /// Returns the configured SASL credentials, when present.
    pub fn sasl_credentials_ref(&self) -> Option<&SaslCredentials> {
        self.sasl_credentials.as_ref()
    }

    /// Validates this configuration without opening a broker connection.
    ///
    /// Use this during application startup when configuration errors should be
    /// reported before any network operation is attempted.
    pub fn validate(&self) -> Result<()> {
        if self.bootstrap_servers.is_empty() {
            return Err(Error::MissingBootstrapServer);
        }
        self.validate_values()
    }

    fn validate_values(&self) -> Result<()> {
        if self
            .bootstrap_servers
            .iter()
            .any(|server| server.trim().is_empty())
        {
            return Err(Error::InvalidConfiguration {
                field: "bootstrap_servers",
                reason: "entries must not be empty",
            });
        }
        if self
            .controller_bootstrap_servers
            .iter()
            .any(|server| server.trim().is_empty())
        {
            return Err(Error::InvalidConfiguration {
                field: "controller_bootstrap_servers",
                reason: "entries must not be empty",
            });
        }
        if self.request_timeout.is_zero() {
            return Err(Error::InvalidConfiguration {
                field: "request_timeout_ms",
                reason: "must be greater than zero",
            });
        }
        if self.max_response_bytes == 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_response_bytes",
                reason: "must be greater than zero",
            });
        }
        if self.max_idle_broker_connections == 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_idle_broker_connections",
                reason: "must be greater than zero",
            });
        }
        if self.decode_limits.max_array_elements() == 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_decode_array_elements",
                reason: "must be greater than zero",
            });
        }
        if self.decode_limits.max_decompressed_record_bytes() == 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_decompressed_record_bytes",
                reason: "must be greater than zero",
            });
        }
        if matches!(
            self.security_protocol,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslTls
        ) && self.sasl_credentials.is_none()
        {
            return Err(Error::MissingSaslCredentials);
        }
        if matches!(
            self.security_protocol,
            SecurityProtocol::Tls | SecurityProtocol::SaslTls
        ) && self
            .tls_server_name
            .as_deref()
            .is_some_and(|server_name| server_name.trim().is_empty())
        {
            return Err(Error::InvalidTlsServerName {
                server: self.tls_server_name.clone().unwrap_or_default(),
            });
        }
        let has_client_certificates = !self.tls_client_certificates_der.is_empty();
        let has_client_key = self.tls_client_private_key_der.is_some();
        if has_client_certificates != has_client_key {
            return Err(Error::InvalidConfiguration {
                field: if has_client_certificates {
                    "tls_client_private_key_der"
                } else {
                    "tls_client_certificates_der"
                },
                reason: "client certificate and private key must be configured together",
            });
        }
        if self.tls_client_certificates_der.iter().any(Vec::is_empty) {
            return Err(Error::InvalidConfiguration {
                field: "tls_client_certificates_der",
                reason: "entries must not be empty",
            });
        }
        if self
            .tls_client_private_key_der
            .as_deref()
            .is_some_and(<[u8]>::is_empty)
        {
            return Err(Error::InvalidConfiguration {
                field: "tls_client_private_key_der",
                reason: "must not be empty",
            });
        }
        if (has_client_certificates || has_client_key)
            && !matches!(
                self.security_protocol,
                SecurityProtocol::Tls | SecurityProtocol::SaslTls
            )
        {
            return Err(Error::InvalidConfiguration {
                field: "security_protocol",
                reason: "TLS client authentication requires TLS or SASL_SSL",
            });
        }
        Ok(())
    }

    /// Connects to the first reachable bootstrap server.
    pub async fn connect(self) -> Result<Client> {
        self.validate()?;

        self.connect_servers(&self.bootstrap_servers).await
    }

    pub(crate) async fn connect_controller(&self) -> Result<Client> {
        self.validate()?;
        let servers = if self.controller_bootstrap_servers.is_empty() {
            &self.bootstrap_servers
        } else {
            &self.controller_bootstrap_servers
        };
        self.connect_servers(servers).await
    }

    async fn connect_servers(&self, servers: &[String]) -> Result<Client> {
        self.connect_servers_rotating(servers, 0).await
    }

    async fn connect_servers_rotating(&self, servers: &[String], start: usize) -> Result<Client> {
        if servers.is_empty() {
            return Err(Error::MissingBootstrapServer);
        }
        let mut last_error = None;
        let start = start % servers.len();
        for offset in 0..servers.len() {
            let server = &servers[(start + offset) % servers.len()];
            match self.connect_broker(server.clone()).await {
                Ok(client) => return Ok(client),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or(Error::MissingBootstrapServer))
    }

    pub(crate) async fn connect_bootstrap_server_rotating(&self, start: usize) -> Result<Client> {
        self.connect_servers_rotating(&self.bootstrap_servers, start)
            .await
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
        authenticate_sasl(&mut client, credentials, Some(self.request_timeout)).await?;
        client.enable_sasl_reauthentication(credentials.clone());
        Ok(client)
    }

    async fn connect_sasl_tls_broker(&self, server: String) -> Result<Client> {
        let credentials = self
            .sasl_credentials
            .as_ref()
            .ok_or(Error::MissingSaslCredentials)?;
        let mut client = self.connect_tls_broker(server).await?;
        authenticate_sasl(&mut client, credentials, Some(self.request_timeout)).await?;
        client.enable_sasl_reauthentication(credentials.clone());
        Ok(client)
    }

    #[cfg(feature = "tls")]
    async fn connect_tls_broker(&self, server: String) -> Result<Client> {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
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
        let tls_config = match (
            self.tls_client_certificates_der.is_empty(),
            self.tls_client_private_key_der.as_ref(),
        ) {
            (true, None) => tls_builder.with_no_client_auth(),
            (false, Some(private_key)) => {
                let certificates = self
                    .tls_client_certificates_der
                    .iter()
                    .cloned()
                    .map(CertificateDer::from)
                    .collect::<Vec<_>>();
                let private_key =
                    PrivateKeyDer::try_from(private_key.clone()).map_err(|error| {
                        Error::TlsConfig {
                            reason: format!("failed to parse TLS client private key: {error}"),
                        }
                    })?;
                tls_builder
                    .with_client_auth_cert(certificates, private_key)
                    .map_err(|error| Error::TlsConfig {
                        reason: format!("failed to configure TLS client authentication: {error}"),
                    })?
            }
            _ => {
                return Err(Error::InvalidConfiguration {
                    field: "tls_client_certificates_der",
                    reason: "client certificate and private key must be configured together",
                });
            }
        };

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

async fn authenticate_sasl(
    client: &mut Client,
    credentials: &SaslCredentials,
    token_timeout: Option<Duration>,
) -> Result<()> {
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
    if credentials.mechanism() == SaslMechanism::OAuthBearer {
        return authenticate_sasl_oauthbearer(client, credentials, mechanism, token_timeout).await;
    }

    authenticate_sasl_plain(client, credentials, mechanism).await
}

async fn authenticate_sasl_oauthbearer(
    client: &mut Client,
    credentials: &SaslCredentials,
    mechanism: &'static str,
    token_timeout: Option<Duration>,
) -> Result<()> {
    let token = credentials
        .oauthbearer_token_for_auth(token_timeout)
        .await?;
    let response = client
        .sasl_authenticate_v2(sasl_oauthbearer_auth_bytes_with_token(credentials, &token)?)
        .await?;
    if response.error_code != 0 {
        client.acknowledge_oauthbearer_error().await;
        return Err(client.broker_error(
            response.error_code,
            format!("sasl authenticate {mechanism}"),
        ));
    }
    if !response.auth_bytes.is_empty() {
        client.acknowledge_oauthbearer_error().await;
        return Err(Error::InvalidSaslResponse {
            mechanism,
            reason: "broker rejected the OAUTHBEARER token",
        });
    }

    Ok(())
}

async fn authenticate_sasl_plain(
    client: &mut Client,
    credentials: &SaslCredentials,
    mechanism: &'static str,
) -> Result<()> {
    let response = client
        .sasl_authenticate_v1(sasl_plain_auth_bytes(credentials))
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
        .sasl_authenticate_v1(client_first.message.into_bytes())
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
        .sasl_authenticate_v1(client_final.message.into_bytes())
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

#[cfg(test)]
fn sasl_oauthbearer_auth_bytes(credentials: &SaslCredentials) -> Result<Vec<u8>> {
    let token = credentials
        .oauthbearer_token()
        .ok_or(Error::Unsupported("SASL/OAUTHBEARER token is missing"))?;
    sasl_oauthbearer_auth_bytes_with_token(credentials, token)
}

pub(crate) fn sasl_oauthbearer_auth_bytes_with_token(
    credentials: &SaslCredentials,
    token: &str,
) -> Result<Vec<u8>> {
    if token.is_empty() {
        return Err(Error::Unsupported(
            "SASL/OAUTHBEARER token must not be empty",
        ));
    }
    if credentials.username().contains('\u{1}') || token.contains('\u{1}') {
        return Err(Error::Unsupported(
            "SASL/OAUTHBEARER credentials contain a forbidden control character",
        ));
    }

    let mut bytes = Vec::with_capacity(
        4 + credentials.username().len() + token.len() + "auth=Bearer ".len() + 2,
    );
    if credentials.username().is_empty() {
        bytes.extend_from_slice(b"n,,");
    } else {
        bytes.extend_from_slice(b"n,a=");
        bytes.extend_from_slice(credentials.username().as_bytes());
        bytes.push(b',');
    }
    bytes.extend_from_slice(b"\x01auth=Bearer ");
    bytes.extend_from_slice(token.as_bytes());
    bytes.extend_from_slice(b"\x01\x01");
    Ok(bytes)
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
        sasl_oauthbearer_auth_bytes, tls_server_name_from_bootstrap_server,
        CachedOAuthBearerTokenProvider, ClientConfig, OAuthBearerToken, OAuthBearerTokenProvider,
        SaslCredentials, SaslMechanism, SecurityProtocol,
    };
    use crate::scram::{self, ScramHash};
    use crate::{ClientMetrics, Error};
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use std::str;
    use std::time::{Duration, SystemTime};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn stores_bootstrap_servers_and_client_id() {
        let config = ClientConfig::new(["localhost:9092"])
            .controller_bootstrap_servers(["controller:9093"])
            .client_id("kafrust-test")
            .client_rack("rack-a")
            .request_timeout_ms(5_000)
            .max_response_bytes(8 * 1024 * 1024)
            .max_idle_broker_connections(3)
            .max_decode_array_elements(12_345)
            .max_decompressed_record_bytes(4 * 1024 * 1024);

        assert_eq!(config.bootstrap_servers(), &["localhost:9092".to_owned()]);
        assert_eq!(
            config.controller_bootstrap_servers_ref(),
            &["controller:9093".to_owned()]
        );
        assert_eq!(config.client_id_ref(), Some("kafrust-test"));
        assert_eq!(config.client_rack_ref(), Some("rack-a"));
        assert_eq!(config.request_timeout(), Duration::from_millis(5_000));
        assert_eq!(config.max_response_bytes_ref(), 8 * 1024 * 1024);
        assert_eq!(config.max_idle_broker_connections_ref(), 3);
        assert_eq!(config.decode_limits().max_array_elements(), 12_345);
        assert_eq!(
            config.decode_limits().max_decompressed_record_bytes(),
            4 * 1024 * 1024
        );
        assert_eq!(config.security_protocol_ref(), SecurityProtocol::Plaintext);
    }

    #[test]
    fn stores_tls_client_authentication_material_without_debugging_secrets() {
        let config = ClientConfig::new(["broker.example.com:9093"])
            .security_protocol(SecurityProtocol::Tls)
            .tls_client_certificate_der([1, 2, 3])
            .tls_client_certificate_der([4, 5])
            .tls_client_private_key_der([9, 8, 7]);

        assert!(config.validate().is_ok());
        assert_eq!(
            config.tls_client_certificates_der(),
            &[vec![1, 2, 3], vec![4, 5]]
        );
        assert!(config.has_tls_client_private_key());

        let debug = format!("{config:?}");
        assert!(!debug.contains("[1, 2, 3]"));
        assert!(!debug.contains("[9, 8, 7]"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn rejects_incomplete_or_inapplicable_tls_client_authentication() {
        let cases = [
            (
                ClientConfig::new(["broker:9092"])
                    .tls_client_certificate_der([1])
                    .validate(),
                "tls_client_private_key_der",
            ),
            (
                ClientConfig::new(["broker:9092"])
                    .tls_client_private_key_der([1])
                    .validate(),
                "tls_client_certificates_der",
            ),
            (
                ClientConfig::new(["broker:9092"])
                    .tls_client_certificate_der([1])
                    .tls_client_private_key_der([2])
                    .validate(),
                "security_protocol",
            ),
            (
                ClientConfig::new(["broker:9092"])
                    .security_protocol(SecurityProtocol::Tls)
                    .tls_client_certificate_der([])
                    .tls_client_private_key_der([2])
                    .validate(),
                "tls_client_certificates_der",
            ),
            (
                ClientConfig::new(["broker:9092"])
                    .security_protocol(SecurityProtocol::Tls)
                    .tls_client_certificate_der([1])
                    .tls_client_private_key_der([])
                    .validate(),
                "tls_client_private_key_der",
            ),
        ];

        for (result, field) in cases {
            assert!(matches!(
                result,
                Err(Error::InvalidConfiguration {
                    field: actual,
                    ..
                }) if actual == field
            ));
        }
    }

    #[test]
    fn rejects_invalid_connection_limits_before_network_access() {
        let cases = [
            (ClientConfig::new([""]).validate(), "bootstrap_servers"),
            (
                ClientConfig::new(["localhost:9092"])
                    .controller_bootstrap_servers([""])
                    .validate(),
                "controller_bootstrap_servers",
            ),
            (
                ClientConfig::new(["localhost:9092"])
                    .request_timeout_ms(0)
                    .validate(),
                "request_timeout_ms",
            ),
            (
                ClientConfig::new(["localhost:9092"])
                    .max_response_bytes(0)
                    .validate(),
                "max_response_bytes",
            ),
            (
                ClientConfig::new(["localhost:9092"])
                    .max_idle_broker_connections(0)
                    .validate(),
                "max_idle_broker_connections",
            ),
            (
                ClientConfig::new(["localhost:9092"])
                    .max_decode_array_elements(0)
                    .validate(),
                "max_decode_array_elements",
            ),
            (
                ClientConfig::new(["localhost:9092"])
                    .max_decompressed_record_bytes(0)
                    .validate(),
                "max_decompressed_record_bytes",
            ),
        ];

        for (result, field) in cases {
            assert!(matches!(
                result,
                Err(Error::InvalidConfiguration {
                    field: actual,
                    ..
                }) if actual == field
            ));
        }

        assert!(matches!(
            ClientConfig::new(std::iter::empty::<String>()).validate(),
            Err(Error::MissingBootstrapServer)
        ));
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
    fn stores_sasl_oauthbearer_credentials_without_changing_security_protocol() {
        let config = ClientConfig::new(["localhost:9092"])
            .sasl_oauthbearer_with_username("alice", "jwt-token");
        let credentials = config.sasl_credentials_ref().unwrap();

        assert_eq!(config.security_protocol_ref(), SecurityProtocol::Plaintext);
        assert_eq!(credentials.mechanism(), SaslMechanism::OAuthBearer);
        assert_eq!(credentials.mechanism().as_str(), "OAUTHBEARER");
        assert_eq!(credentials.username(), "alice");
        assert_eq!(credentials.password(), "");
        assert_eq!(credentials.oauthbearer_token(), Some("jwt-token"));
    }

    #[tokio::test]
    async fn fetches_sasl_oauthbearer_token_from_provider() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let provider_calls = calls.clone();
        let credentials =
            SaslCredentials::oauthbearer_with_username_and_provider("alice", move || {
                let provider_calls = provider_calls.clone();
                async move {
                    provider_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok("fresh-jwt-token".to_owned())
                }
            });

        assert_eq!(credentials.oauthbearer_token(), None);
        assert_eq!(
            credentials
                .oauthbearer_token_for_auth(Some(Duration::from_secs(1)))
                .await
                .unwrap(),
            "fresh-jwt-token"
        );
        assert_eq!(
            credentials
                .oauthbearer_token_for_auth(Some(Duration::from_secs(1)))
                .await
                .unwrap(),
            "fresh-jwt-token"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(format!("{credentials:?}").contains("<configured>"));
        assert!(!format!("{credentials:?}").contains("fresh-jwt-token"));
    }

    #[tokio::test]
    async fn cached_oauth_provider_reuses_unexpired_tokens() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source_calls = calls.clone();
        let provider = CachedOAuthBearerTokenProvider::new(
            move || {
                let source_calls = source_calls.clone();
                async move {
                    let call = source_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(OAuthBearerToken::new(
                        format!("cached-token-{call}"),
                        SystemTime::now() + Duration::from_secs(3_600),
                    ))
                }
            },
            Duration::from_secs(60),
        );

        assert_eq!(provider.fetch_token().await.unwrap(), "cached-token-0");
        assert_eq!(provider.fetch_token().await.unwrap(), "cached-token-0");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!format!("{provider:?}").contains("cached-token-0"));
    }

    #[tokio::test]
    async fn cached_oauth_provider_rotates_inside_refresh_window() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source_calls = calls.clone();
        let provider = CachedOAuthBearerTokenProvider::new(
            move || {
                let source_calls = source_calls.clone();
                async move {
                    let call = source_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let lifetime = if call == 0 {
                        Duration::from_millis(10)
                    } else {
                        Duration::from_secs(3_600)
                    };
                    Ok(OAuthBearerToken::new(
                        format!("rotated-token-{call}"),
                        SystemTime::now() + lifetime,
                    ))
                }
            },
            Duration::from_secs(1),
        );

        assert_eq!(provider.fetch_token().await.unwrap(), "rotated-token-0");
        assert_eq!(provider.fetch_token().await.unwrap(), "rotated-token-1");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cached_oauth_provider_uses_valid_token_during_source_outage() {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source_calls = calls.clone();
        let provider = CachedOAuthBearerTokenProvider::new(
            move || {
                let source_calls = source_calls.clone();
                async move {
                    if source_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        Ok(OAuthBearerToken::new(
                            "outage-fallback-token",
                            SystemTime::now() + Duration::from_secs(3_600),
                        ))
                    } else {
                        Err(Error::Unsupported("oauth issuer unavailable"))
                    }
                }
            },
            Duration::from_secs(7_200),
        );

        assert_eq!(
            provider.fetch_token().await.unwrap(),
            "outage-fallback-token"
        );
        assert_eq!(
            provider.fetch_token().await.unwrap(),
            "outage-fallback-token"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn propagates_oauthbearer_provider_error_without_logging_token() {
        let credentials = SaslCredentials::oauthbearer_with_provider(|| async {
            Err(Error::Unsupported("oauth token provider failed"))
        });

        let error = credentials
            .oauthbearer_token_for_auth(Some(Duration::from_secs(1)))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("oauth token provider failed")
        ));
        assert!(!format!("{error}").contains("Bearer"));
    }

    #[tokio::test]
    async fn times_out_oauthbearer_provider_without_exposing_token_material() {
        let credentials = SaslCredentials::oauthbearer_with_provider(|| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok("late-token".to_owned())
        });

        let error = credentials
            .oauthbearer_token_for_auth(Some(Duration::from_millis(1)))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::OAuthBearerTokenTimeout { timeout_ms: 1 }
        ));
        assert!(!format!("{error}").contains("late-token"));
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

        let oauth = SaslCredentials::oauthbearer("jwt-token");
        assert!(!format!("{oauth:?}").contains("jwt-token"));
        assert!(format!("{oauth:?}").contains("<redacted>"));
    }

    #[test]
    fn encodes_sasl_oauthbearer_initial_response() {
        let credentials = SaslCredentials::oauthbearer_with_username("alice", "jwt-token");

        assert_eq!(
            sasl_oauthbearer_auth_bytes(&credentials).unwrap(),
            b"n,a=alice,\x01auth=Bearer jwt-token\x01\x01"
        );

        let credentials = SaslCredentials::oauthbearer("jwt-token");
        assert_eq!(
            sasl_oauthbearer_auth_bytes(&credentials).unwrap(),
            b"n,,\x01auth=Bearer jwt-token\x01\x01"
        );

        assert!(matches!(
            sasl_oauthbearer_auth_bytes(&SaslCredentials::oauthbearer("")),
            Err(Error::Unsupported(
                "SASL/OAUTHBEARER token must not be empty"
            ))
        ));
        assert!(matches!(
            sasl_oauthbearer_auth_bytes(&SaslCredentials::oauthbearer("bad\u{1}token")),
            Err(Error::Unsupported(
                "SASL/OAUTHBEARER credentials contain a forbidden control character"
            ))
        ));
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
                    0, 1, // api version
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
                    0, 0, 0, 0, 0, 0, 0, 0, // session lifetime ms
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
    async fn sasl_oauthbearer_connect_sends_handshake_and_authenticate() {
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
                    0, 11, b'O', b'A', b'U', b'T', b'H', b'B', b'E', b'A', b'R', b'E',
                    b'R', // mechanism
                ]
            );
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, // error code
                    0, 0, 0, 1, // mechanism count
                    0, 11, b'O', b'A', b'U', b'T', b'H', b'B', b'E', b'A', b'R', b'E',
                    b'R', // mechanism
                ],
            )
            .await;

            let authenticate = read_frame(&mut socket).await;
            assert_eq!(
                sasl_authenticate_v2_auth_bytes(&authenticate, 2),
                b"n,a=alice,\x01auth=Bearer jwt-token\x01\x01"
            );
            write_sasl_authenticate_v2_response(&mut socket, 2, &[]).await;
        });

        let client = ClientConfig::new([addr.to_string()])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .sasl_oauthbearer_with_username_and_provider("alice", || async {
                Ok("jwt-token".to_owned())
            })
            .request_timeout_ms(1_000)
            .connect()
            .await;

        assert!(client.is_ok());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sasl_oauthbearer_rejects_broker_error_challenge() {
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
                    0, 11, b'O', b'A', b'U', b'T', b'H', b'B', b'E', b'A', b'R', b'E',
                    b'R', // mechanism
                ],
            )
            .await;

            let _authenticate = read_frame(&mut socket).await;
            write_sasl_authenticate_v2_response(&mut socket, 2, br#"{"status":"invalid_token"}"#)
                .await;
            let acknowledgement = read_frame(&mut socket).await;
            assert_eq!(sasl_authenticate_v2_auth_bytes(&acknowledgement, 3), &[1]);
        });

        let error = ClientConfig::new([addr.to_string()])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .sasl_oauthbearer("jwt-token")
            .request_timeout_ms(1_000)
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidSaslResponse {
                mechanism: "OAUTHBEARER",
                reason: "broker rejected the OAUTHBEARER token"
            }
        ));
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
                    0, 0, 0, 0, 0, 0, 0, 0, // session lifetime ms
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
        assert_eq!(&frame[2..4], &[0, 1]);
        assert_eq!(&frame[4..8], &correlation_id.to_be_bytes());
        assert_eq!(&frame[8..10], &[0xff, 0xff]);
        let auth_len = i32::from_be_bytes(frame[10..14].try_into().unwrap());
        let auth_len = usize::try_from(auth_len).unwrap();
        assert_eq!(frame.len(), 14 + auth_len);
        &frame[14..]
    }

    fn sasl_authenticate_v2_auth_bytes(frame: &[u8], correlation_id: i32) -> &[u8] {
        assert_eq!(&frame[0..2], &[0, 36]);
        assert_eq!(&frame[2..4], &[0, 2]);
        assert_eq!(&frame[4..8], &correlation_id.to_be_bytes());
        assert_eq!(&frame[8..10], &[0xff, 0xff]);
        assert_eq!(frame[10], 0); // request header tagged fields
        let auth_len = usize::from(frame[11]) - 1;
        assert_eq!(frame.len(), 12 + auth_len + 1);
        assert_eq!(frame[12 + auth_len], 0); // request tagged fields
        &frame[12..12 + auth_len]
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
        frame.extend_from_slice(&0i64.to_be_bytes());
        write_frame(socket, &frame).await;
    }

    async fn write_sasl_authenticate_v2_response(
        socket: &mut tokio::net::TcpStream,
        correlation_id: i32,
        auth_bytes: &[u8],
    ) {
        let mut frame = Vec::with_capacity(14 + auth_bytes.len());
        frame.extend_from_slice(&correlation_id.to_be_bytes());
        frame.push(0); // response header tagged fields
        frame.extend_from_slice(&0i16.to_be_bytes());
        frame.push(0); // null compact error message
        frame.push((auth_bytes.len() + 1) as u8);
        frame.extend_from_slice(auth_bytes);
        frame.extend_from_slice(&0i64.to_be_bytes());
        frame.push(0); // response tagged fields
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
