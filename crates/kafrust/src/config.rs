use crate::client::Client;
use crate::error::{Error, Result};
use std::time::Duration;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kafka client security protocol.
///
/// `Plaintext` is the only implemented transport in the current alpha. TLS and
/// SASL variants are explicit configuration targets for M11 and currently
/// return [`Error::Unsupported`] instead of silently falling back to plaintext.
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

impl SecurityProtocol {
    fn unsupported_feature(self) -> Option<&'static str> {
        match self {
            Self::Plaintext => None,
            Self::Tls => Some("TLS connections are not implemented yet"),
            Self::SaslPlaintext => Some("SASL authentication is not implemented yet"),
            Self::SaslTls => Some("SASL over TLS is not implemented yet"),
        }
    }
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

    pub(crate) async fn connect_broker(
        &self,
        server: impl tokio::net::ToSocketAddrs,
    ) -> Result<Client> {
        if let Some(feature) = self.security_protocol.unsupported_feature() {
            return Err(Error::Unsupported(feature));
        }

        Client::connect_with_request_timeout(server, self.client_id.clone(), self.request_timeout)
            .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{ClientConfig, SecurityProtocol};
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
    async fn rejects_unsupported_security_protocol_before_connecting() {
        let error = ClientConfig::new(["localhost:9092"])
            .security_protocol(SecurityProtocol::Tls)
            .connect()
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("TLS connections are not implemented yet")
        ));
    }
}
