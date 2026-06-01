use crate::client::Client;
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    bootstrap_servers: Vec<String>,
    client_id: Option<String>,
}

impl ClientConfig {
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into_iter().map(Into::into).collect(),
            client_id: None,
        }
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    pub fn client_id_ref(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub async fn connect(self) -> Result<Client> {
        let server = self
            .bootstrap_servers
            .first()
            .ok_or(Error::MissingBootstrapServer)?
            .clone();
        Client::connect(server, self.client_id).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::ClientConfig;

    #[test]
    fn stores_bootstrap_servers_and_client_id() {
        let config = ClientConfig::new(["localhost:9092"]).client_id("kafrust-test");

        assert_eq!(config.bootstrap_servers(), &["localhost:9092".to_owned()]);
        assert_eq!(config.client_id_ref(), Some("kafrust-test"));
    }
}
