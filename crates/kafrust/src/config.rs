use crate::client::Client;
use crate::error::{Error, Result};
use std::time::Duration;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
    request_timeout: Duration,
}

impl ClientConfig {
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into_iter().map(Into::into).collect(),
            client_id: None,
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
        }
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.request_timeout = Duration::from_millis(request_timeout_ms);
        self
    }

    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    pub fn client_id_ref(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub async fn connect(self) -> Result<Client> {
        let server = self
            .bootstrap_servers
            .first()
            .ok_or(Error::MissingBootstrapServer)?
            .clone();
        Client::connect_with_request_timeout(server, self.client_id, self.request_timeout).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::ClientConfig;
    use std::time::Duration;

    #[test]
    fn stores_bootstrap_servers_and_client_id() {
        let config = ClientConfig::new(["localhost:9092"])
            .client_id("kafrust-test")
            .request_timeout_ms(5_000);

        assert_eq!(config.bootstrap_servers(), &["localhost:9092".to_owned()]);
        assert_eq!(config.client_id_ref(), Some("kafrust-test"));
        assert_eq!(config.request_timeout(), Duration::from_millis(5_000));
    }
}
