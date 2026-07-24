use std::collections::BTreeMap;
use std::time::Duration;

use kafrust_protocol::api::create_topics::{
    CreateTopicsAssignmentV2, CreateTopicsConfigV2, CreateTopicsTopicResultV2, CreateTopicsTopicV2,
};

use crate::config::ClientConfig;
use crate::error::{BrokerErrorKind, Error, Result};
use crate::metrics::ClientMetrics;

/// Kafka administration client.
///
/// Each controller-scoped operation discovers the active controller through
/// cluster metadata before opening the controller connection.
#[derive(Debug, Clone)]
pub struct AdminClient {
    config: ClientConfig,
}

impl AdminClient {
    /// Creates an admin client from shared Kafka connection configuration.
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    /// Returns the shared metrics handle used by admin broker connections.
    pub fn metrics(&self) -> ClientMetrics {
        self.config.metrics_ref()
    }

    /// Creates Kafka topics on the active controller using CreateTopics v2.
    ///
    /// Kafka can accept some topics and reject others in the same request.
    /// Therefore broker-level topic failures are returned in
    /// [`CreateTopicsResult`] rather than collapsing the response into one
    /// [`Error`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.create_topics",
        skip_all,
        fields(topic_count = topics.len(), validate_only = options.validate_only),
        err
    )]
    pub async fn create_topics(
        &self,
        topics: &[NewTopic],
        options: CreateTopicsOptions,
    ) -> Result<CreateTopicsResult> {
        let mut bootstrap = self.config.clone().connect().await?;
        let metadata = bootstrap.metadata(None).await?;
        let controller = metadata
            .brokers
            .iter()
            .find(|broker| broker.node_id == metadata.controller_id)
            .ok_or(Error::MissingBroker {
                node_id: metadata.controller_id,
            })?;
        let mut controller_client = self
            .config
            .connect_broker(format!("{}:{}", controller.host, controller.port))
            .await?;
        let response = controller_client
            .create_topics_v2(
                topics.iter().map(NewTopic::as_protocol).collect(),
                duration_millis_i32(options.timeout),
                options.validate_only,
            )
            .await?;

        for topic in &response.topics {
            if topic.error_code != 0 {
                self.config.record_broker_error();
            }
        }

        Ok(CreateTopicsResult {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .topics
                .into_iter()
                .map(CreateTopicResult::from_protocol)
                .collect(),
        })
    }
}

/// Definition of one Kafka topic to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    name: String,
    num_partitions: i32,
    replication_factor: i16,
    assignments: BTreeMap<i32, Vec<i32>>,
    configs: BTreeMap<String, Option<String>>,
}

impl NewTopic {
    /// Creates a topic using automatic replica assignment.
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            assignments: BTreeMap::new(),
            configs: BTreeMap::new(),
        }
    }

    /// Creates a topic using explicit partition-to-broker assignments.
    ///
    /// Kafka requires partition count and replication factor to be `-1` when
    /// manual assignments are supplied.
    pub fn with_assignments(
        name: impl Into<String>,
        assignments: impl IntoIterator<Item = (i32, Vec<i32>)>,
    ) -> Self {
        Self {
            name: name.into(),
            num_partitions: -1,
            replication_factor: -1,
            assignments: assignments.into_iter().collect(),
            configs: BTreeMap::new(),
        }
    }

    /// Adds or replaces a topic configuration value.
    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.insert(name.into(), Some(value.into()));
        self
    }

    /// Adds or replaces a nullable topic configuration value.
    pub fn nullable_config(mut self, name: impl Into<String>, value: Option<String>) -> Self {
        self.configs.insert(name.into(), value);
        self
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the requested partition count, or `-1` for manual assignment.
    pub fn num_partitions(&self) -> i32 {
        self.num_partitions
    }

    /// Returns the requested replication factor, or `-1` for manual assignment.
    pub fn replication_factor(&self) -> i16 {
        self.replication_factor
    }

    /// Returns explicit partition assignments in partition order.
    pub fn assignments(&self) -> &BTreeMap<i32, Vec<i32>> {
        &self.assignments
    }

    /// Returns topic configuration values in configuration-name order.
    pub fn configs(&self) -> &BTreeMap<String, Option<String>> {
        &self.configs
    }

    fn as_protocol(&self) -> CreateTopicsTopicV2 {
        CreateTopicsTopicV2 {
            name: self.name.clone(),
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
            assignments: self
                .assignments
                .iter()
                .map(|(partition_index, broker_ids)| CreateTopicsAssignmentV2 {
                    partition_index: *partition_index,
                    broker_ids: broker_ids.clone(),
                })
                .collect(),
            configs: self
                .configs
                .iter()
                .map(|(name, value)| CreateTopicsConfigV2 {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }
}

/// Options for one CreateTopics operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateTopicsOptions {
    timeout: Duration,
    validate_only: bool,
}

impl CreateTopicsOptions {
    /// Creates options with a 30-second broker timeout and topic creation enabled.
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            validate_only: false,
        }
    }

    /// Sets how long the controller may wait for topic creation.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets whether Kafka should validate without creating topics.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Returns the configured broker-side timeout.
    pub fn timeout_ref(&self) -> Duration {
        self.timeout
    }

    /// Returns whether this operation only validates topic definitions.
    pub fn is_validate_only(&self) -> bool {
        self.validate_only
    }
}

impl Default for CreateTopicsOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete response from one CreateTopics operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsResult {
    throttle_time: Duration,
    topics: Vec<CreateTopicResult>,
}

impl CreateTopicsResult {
    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-topic outcomes in broker response order.
    pub fn topics(&self) -> &[CreateTopicResult] {
        &self.topics
    }

    /// Consumes this response and returns per-topic outcomes.
    pub fn into_topics(self) -> Vec<CreateTopicResult> {
        self.topics
    }

    /// Returns whether at least one topic was rejected.
    pub fn has_errors(&self) -> bool {
        self.topics.iter().any(|topic| !topic.is_success())
    }
}

/// Outcome for one topic in a CreateTopics response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicResult {
    name: String,
    error_code: i16,
    error_message: Option<String>,
}

impl CreateTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether Kafka created or successfully validated the topic.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw error code, or zero for success.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's optional topic error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(result: CreateTopicsTopicResultV2) -> Self {
        Self {
            name: result.name,
            error_code: result.error_code,
            error_message: result.error_message,
        }
    }
}

fn duration_millis_i32(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

fn nonnegative_i32_to_u64(value: i32) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{AdminClient, CreateTopicsOptions, NewTopic};
    use crate::{BrokerErrorKind, ClientConfig, ClientMetrics};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn builds_automatic_and_manual_topic_definitions() {
        let automatic = NewTopic::new("orders", 6, 3)
            .config("cleanup.policy", "compact")
            .nullable_config("retention.ms", None);
        assert_eq!(automatic.name(), "orders");
        assert_eq!(automatic.num_partitions(), 6);
        assert_eq!(automatic.replication_factor(), 3);
        assert!(automatic.assignments().is_empty());
        assert_eq!(
            automatic
                .configs()
                .get("cleanup.policy")
                .and_then(|value| value.as_deref()),
            Some("compact")
        );
        assert_eq!(automatic.configs().get("retention.ms"), Some(&None));

        let manual = NewTopic::with_assignments("payments", [(0, vec![1, 2]), (1, vec![2, 1])]);
        assert_eq!(manual.num_partitions(), -1);
        assert_eq!(manual.replication_factor(), -1);
        assert_eq!(manual.assignments().get(&1), Some(&vec![2, 1]));
    }

    #[test]
    fn builds_create_topics_options() {
        let options = CreateTopicsOptions::new()
            .timeout(Duration::from_secs(5))
            .validate_only(true);

        assert_eq!(options.timeout_ref(), Duration::from_secs(5));
        assert!(options.is_validate_only());
    }

    #[tokio::test]
    async fn routes_create_topics_to_controller_and_preserves_partial_result() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut controller, _) = listener.accept().await.unwrap();
            let create_request = read_frame(&mut controller).await;
            assert_eq!(&create_request[0..4], &[0, 19, 0, 2]);
            write_frame(&mut controller, &create_topics_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .create_topics(&[NewTopic::new("orders", 3, 1)], CreateTopicsOptions::new())
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(7));
        assert!(result.has_errors());
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(result.topics()[0].error_code(), 36);
        assert_eq!(result.topics()[0].error_message(), Some("exists"));
        assert_eq!(
            result.topics()[0].broker_error_kind(),
            Some(BrokerErrorKind::TopicAlreadyExists)
        );
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    async fn read_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let length = stream.read_i32().await.unwrap();
        let mut frame = vec![0; usize::try_from(length).unwrap()];
        stream.read_exact(&mut frame).await.unwrap();
        frame
    }

    async fn write_frame(stream: &mut tokio::net::TcpStream, payload: &[u8]) {
        stream
            .write_i32(i32::try_from(payload.len()).unwrap())
            .await
            .unwrap();
        stream.write_all(payload).await.unwrap();
    }

    fn metadata_response(port: u16) -> Vec<u8> {
        let mut response = vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 1, // broker count
            0, 0, 0, 1, // broker node ID
            0, 9, b'1', b'2', b'7', b'.', b'0', b'.', b'0', b'.', b'1', // host
        ];
        response.extend_from_slice(&i32::from(port).to_be_bytes());
        response.extend_from_slice(&[
            0xff, 0xff, // null rack
            0, 0, 0, 1, // controller ID
            0, 0, 0, 0, // topic count
        ]);
        response
    }

    fn create_topics_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 7, // throttle time
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 36, // topic already exists
            0, 6, b'e', b'x', b'i', b's', b't', b's', // error message
        ]
    }
}
