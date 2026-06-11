use crate::client::Client;
use crate::error::{Error, Result};
#[cfg(feature = "tls")]
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kafka client security protocol.
///
/// `Plaintext` is the default transport. `Tls` is implemented when the
/// non-default `tls` crate feature is enabled. SASL variants are explicit
/// configuration targets for M11 and currently return [`Error::Unsupported`]
/// instead of silently falling back to plaintext.
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

#[derive(Debug, Clone, PartialEq, Eq)]
/// Connection settings shared by low-level clients, producers, and consumers.
pub struct ClientConfig {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    request_timeout: Duration,
    security_protocol: SecurityProtocol,
}

impl ClientConfig {
    /// Creates a client configuration from one or more Kafka bootstrap servers.
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into_iter().map(Into::into).collect(),
            client_id: None,
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            security_protocol: SecurityProtocol::Plaintext,
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

    /// Sets the Kafka security protocol used for broker connections.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.security_protocol = security_protocol;
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

    /// Returns the configured Kafka security protocol.
    pub fn security_protocol_ref(&self) -> SecurityProtocol {
        self.security_protocol
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
                Client::connect_with_request_timeout(
                    server,
                    self.client_id.clone(),
                    self.request_timeout,
                )
                .await
            }
            SecurityProtocol::Tls => self.connect_tls_broker(server).await,
            SecurityProtocol::SaslPlaintext => Err(Error::Unsupported(
                "SASL authentication is not implemented yet",
            )),
            SecurityProtocol::SaslTls => {
                Err(Error::Unsupported("SASL over TLS is not implemented yet"))
            }
        }
    }

    #[cfg(feature = "tls")]
    async fn connect_tls_broker(&self, server: String) -> Result<Client> {
        use rustls::pki_types::ServerName;
        use rustls_platform_verifier::BuilderVerifierExt;
        use tokio_rustls::TlsConnector;

        let server_name_text = tls_server_name_from_bootstrap_server(&server)?;
        let server_name =
            ServerName::try_from(server_name_text).map_err(|_| Error::InvalidTlsServerName {
                server: server.clone(),
            })?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| Error::TlsConfig {
                reason: format!("failed to configure TLS protocol versions: {error}"),
            })?
            .with_platform_verifier()
            .map_err(|error| Error::TlsConfig {
                reason: format!("failed to configure platform certificate verifier: {error}"),
            })?
            .with_no_client_auth();

        let tcp_stream = tokio::net::TcpStream::connect(server.as_str()).await?;
        let tls_stream = TlsConnector::from(Arc::new(tls_config))
            .connect(server_name, tcp_stream)
            .await?;

        Ok(Client::from_stream(
            Box::new(tls_stream),
            self.client_id.clone(),
            Some(self.request_timeout),
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
    use super::{tls_server_name_from_bootstrap_server, ClientConfig, SecurityProtocol};
    use crate::Error;
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[test]
    fn stores_bootstrap_servers_and_client_id() {
        let config = ClientConfig::new(["localhost:9092"])
            .client_id("kafrust-test")
            .request_timeout_ms(5_000);

        assert_eq!(config.bootstrap_servers(), &["localhost:9092".to_owned()]);
        assert_eq!(config.client_id_ref(), Some("kafrust-test"));
        assert_eq!(config.request_timeout(), Duration::from_millis(5_000));
        assert_eq!(config.security_protocol_ref(), SecurityProtocol::Plaintext);
    }

    #[test]
    fn stores_security_protocol() {
        let config =
            ClientConfig::new(["localhost:9092"]).security_protocol(SecurityProtocol::SaslTls);

        assert_eq!(config.security_protocol_ref(), SecurityProtocol::SaslTls);
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
    async fn rejects_sasl_security_protocol_before_connecting() {
        let error = ClientConfig::new(["localhost:9092"])
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("SASL authentication is not implemented yet")
        ));
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
}
