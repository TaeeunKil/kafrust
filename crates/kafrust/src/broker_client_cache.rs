use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::client::Client;

/// Bounded cache of idle broker connections owned by one high-level client.
///
/// Connections are removed while a request is in flight and returned only
/// after a successful request. The queue therefore tracks idle connections;
/// eviction is deterministic FIFO rather than closing a connection that may
/// still be in use.
#[derive(Debug, Default)]
pub(crate) struct BrokerClientCache {
    clients: BTreeMap<String, Client>,
    idle_order: VecDeque<String>,
}

impl BrokerClientCache {
    pub(crate) fn take(&mut self, broker_addr: &str) -> Option<Client> {
        let client = self.clients.remove(broker_addr);
        if client.is_some() {
            self.remove_from_idle_order(broker_addr);
        }
        client
    }

    pub(crate) fn insert(&mut self, broker_addr: String, client: Client, max_connections: usize) {
        let already_cached = self.clients.contains_key(&broker_addr);
        if already_cached {
            self.remove_from_idle_order(&broker_addr);
        }

        let max_connections = max_connections.max(1);
        if !already_cached {
            while self.clients.len() >= max_connections {
                if !self.evict_oldest() {
                    break;
                }
            }
        }

        self.clients.insert(broker_addr.clone(), client);
        self.idle_order.push_back(broker_addr);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.clients.len()
    }

    fn remove_from_idle_order(&mut self, broker_addr: &str) {
        self.idle_order.retain(|address| address != broker_addr);
    }

    fn evict_oldest(&mut self) -> bool {
        if let Some(address) = self.idle_order.pop_front() {
            self.clients.remove(&address);
            return true;
        }

        let Some(address) = self.clients.keys().next().cloned() else {
            return false;
        };
        self.clients.remove(&address);
        true
    }
}

/// Shared idle broker cache carried by cloned [`ClientConfig`](crate::config::ClientConfig)
/// values.
///
/// The mutex protects only cache bookkeeping. Callers take ownership of a
/// connection before awaiting broker I/O and return it after a successful
/// request, so a poisoned connection is never reinserted and the cache lock
/// is not held across network work.
#[derive(Debug, Default)]
pub(crate) struct SharedBrokerClientCache {
    cache: tokio::sync::Mutex<BrokerClientCache>,
}

impl SharedBrokerClientCache {
    pub(crate) async fn take(&self, broker_addr: &str) -> Option<Client> {
        self.cache.lock().await.take(broker_addr)
    }

    pub(crate) async fn insert(&self, broker_addr: String, client: Client, max_connections: usize) {
        self.cache
            .lock()
            .await
            .insert(broker_addr, client, max_connections);
    }

    #[cfg(test)]
    pub(crate) async fn len(&self) -> usize {
        self.cache.lock().await.len()
    }
}

pub(crate) type SharedBrokerClientCacheHandle = Arc<SharedBrokerClientCache>;

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::BrokerClientCache;
    use crate::client::Client;
    use crate::config::{ClientConfig, SecurityProtocol};
    use std::sync::Arc;
    use tokio::io::duplex;

    fn client() -> Client {
        let (stream, _peer) = duplex(16);
        Client::from_stream(Box::new(stream), None, None)
    }

    #[test]
    fn evicts_idle_connections_in_fifo_order() {
        let mut cache = BrokerClientCache::default();
        cache.insert("broker-a".to_owned(), client(), 2);
        cache.insert("broker-b".to_owned(), client(), 2);

        let broker_a = cache.take("broker-a").expect("broker-a is cached");
        cache.insert("broker-a".to_owned(), broker_a, 2);
        cache.insert("broker-c".to_owned(), client(), 2);

        assert!(cache.take("broker-b").is_none());
        assert!(cache.take("broker-a").is_some());
        assert!(cache.take("broker-c").is_some());
    }

    #[test]
    fn zero_limit_keeps_one_connection_instead_of_growing_unbounded() {
        let mut cache = BrokerClientCache::default();
        cache.insert("broker-a".to_owned(), client(), 0);
        cache.insert("broker-b".to_owned(), client(), 0);

        assert_eq!(cache.len(), 1);
        assert!(cache.take("broker-a").is_none());
        assert!(cache.take("broker-b").is_some());
    }

    #[tokio::test]
    async fn cloned_client_configs_share_idle_connections() {
        let config = ClientConfig::new(["broker-a"]);
        let cloned = config.clone();
        let first = config.shared_broker_clients();
        let second = cloned.shared_broker_clients();
        let (stream, _peer) = duplex(16);

        first
            .insert(
                "broker-a".to_owned(),
                Client::from_stream(Box::new(stream), None, None),
                2,
            )
            .await;

        assert!(second.take("broker-a").await.is_some());
        assert_eq!(first.len().await, 0);
    }

    #[test]
    fn connection_identity_changes_reset_the_shared_cache() {
        let config = ClientConfig::new(["broker-a"]);
        let changed = config
            .clone()
            .security_protocol(SecurityProtocol::SaslPlaintext);

        assert!(!Arc::ptr_eq(
            &config.shared_broker_clients(),
            &changed.shared_broker_clients()
        ));
    }
}
