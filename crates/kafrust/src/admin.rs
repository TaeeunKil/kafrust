use std::collections::BTreeMap;
use std::time::Duration;

use kafrust_protocol::api::create_topics::{
    CreateTopicsAssignmentV2, CreateTopicsConfigV2, CreateTopicsTopicResultV2, CreateTopicsTopicV2,
};
use kafrust_protocol::api::delete_topics::DeleteTopicsTopicResultV3;
use kafrust_protocol::api::describe_configs::{
    DescribeConfigsEntryV1, DescribeConfigsResourceV1, DescribeConfigsResultV1,
    DescribeConfigsSynonymV1,
};
use kafrust_protocol::api::incremental_alter_configs::{
    IncrementalAlterConfigsEntryV0, IncrementalAlterConfigsResourceResponseV0,
    IncrementalAlterConfigsResourceV0,
};
use kafrust_protocol::api::metadata::{BrokerMetadata, TopicMetadata};

use crate::client::Client;
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

    /// Describes the Kafka cluster brokers and active controller.
    #[tracing::instrument(level = "debug", name = "kafka.admin.describe_cluster", skip_all, err)]
    pub async fn describe_cluster(&self) -> Result<ClusterDescription> {
        let mut client = self.config.clone().connect().await?;
        let metadata = client.metadata(Some(Vec::new())).await?;

        Ok(ClusterDescription {
            controller_id: metadata.controller_id,
            brokers: metadata
                .brokers
                .into_iter()
                .map(BrokerDescription::from_protocol)
                .collect(),
        })
    }

    /// Lists topics visible to the configured Kafka principal.
    ///
    /// Topic-level Kafka errors remain attached to their listings instead of
    /// failing the entire operation.
    #[tracing::instrument(level = "debug", name = "kafka.admin.list_topics", skip_all, err)]
    pub async fn list_topics(&self) -> Result<Vec<TopicListing>> {
        let mut client = self.config.clone().connect().await?;
        let metadata = client.metadata(None).await?;

        for topic in &metadata.topics {
            if topic.error_code != 0 {
                self.config.record_broker_error();
            }
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
        }

        Ok(metadata
            .topics
            .into_iter()
            .map(TopicListing::from_protocol)
            .collect())
    }

    /// Describes configurations for Kafka topics using DescribeConfigs v1.
    ///
    /// Resource-level Kafka failures remain in [`DescribeConfigsResult`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_topic_configs",
        skip_all,
        fields(resource_count = resources.len(), include_synonyms = options.include_synonyms),
        err
    )]
    pub async fn describe_topic_configs(
        &self,
        resources: &[TopicConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<DescribeConfigsResult> {
        let mut client = self.config.clone().connect().await?;
        let response = client
            .describe_configs_v1(
                resources
                    .iter()
                    .map(TopicConfigResource::as_protocol)
                    .collect(),
                options.include_synonyms,
            )
            .await?;

        for resource in &response.results {
            if resource.error_code != 0 {
                self.config.record_broker_error();
            }
        }

        Ok(DescribeConfigsResult {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            resources: response
                .results
                .into_iter()
                .map(ConfigResourceResult::from_protocol)
                .collect(),
        })
    }

    /// Incrementally alters Kafka topic configurations.
    ///
    /// Kafka applies operations atomically within one resource, while separate
    /// resources can succeed or fail independently.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.incremental_alter_topic_configs",
        skip_all,
        fields(resource_count = resources.len(), validate_only = options.validate_only),
        err
    )]
    pub async fn incremental_alter_topic_configs(
        &self,
        resources: &[TopicConfigAlteration],
        options: AlterConfigsOptions,
    ) -> Result<AlterConfigsResult> {
        let mut client = self.config.clone().connect().await?;
        let response = client
            .incremental_alter_configs_v0(
                resources
                    .iter()
                    .map(TopicConfigAlteration::as_protocol)
                    .collect(),
                options.validate_only,
            )
            .await?;

        for resource in &response.responses {
            if resource.error_code != 0 {
                self.config.record_broker_error();
            }
        }

        Ok(AlterConfigsResult {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            resources: response
                .responses
                .into_iter()
                .map(AlterConfigResourceResult::from_protocol)
                .collect(),
        })
    }

    async fn controller_client(&self) -> Result<Client> {
        let mut bootstrap = self.config.clone().connect().await?;
        let metadata = bootstrap.metadata(Some(Vec::new())).await?;
        let controller = metadata
            .brokers
            .iter()
            .find(|broker| broker.node_id == metadata.controller_id)
            .ok_or(Error::MissingBroker {
                node_id: metadata.controller_id,
            })?;
        self.config
            .connect_broker(format!("{}:{}", controller.host, controller.port))
            .await
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
        let mut controller_client = self.controller_client().await?;
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

    /// Deletes Kafka topics on the active controller using DeleteTopics v3.
    ///
    /// Kafka can accept some topic deletions and reject others in the same
    /// request. Per-topic broker failures are retained in [`DeleteTopicsResult`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_topics",
        skip_all,
        fields(topic_count = topic_names.len()),
        err
    )]
    pub async fn delete_topics(
        &self,
        topic_names: &[String],
        options: DeleteTopicsOptions,
    ) -> Result<DeleteTopicsResult> {
        let mut controller_client = self.controller_client().await?;
        let response = controller_client
            .delete_topics_v3(topic_names.to_vec(), duration_millis_i32(options.timeout))
            .await?;

        for topic in &response.topics {
            if topic.error_code != 0 {
                self.config.record_broker_error();
            }
        }

        Ok(DeleteTopicsResult {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .topics
                .into_iter()
                .map(DeleteTopicResult::from_protocol)
                .collect(),
        })
    }
}

/// Broker metadata returned by [`AdminClient::describe_cluster`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerDescription {
    id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
}

impl BrokerDescription {
    /// Returns the Kafka broker node ID.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Returns the broker's advertised host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the broker's advertised port.
    pub fn port(&self) -> i32 {
        self.port
    }

    /// Returns the broker rack identifier when Kafka advertised one.
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    fn from_protocol(broker: BrokerMetadata) -> Self {
        Self {
            id: broker.node_id,
            host: broker.host,
            port: broker.port,
            rack: broker.rack,
        }
    }
}

/// Kafka cluster metadata returned by [`AdminClient::describe_cluster`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    controller_id: i32,
    brokers: Vec<BrokerDescription>,
}

impl ClusterDescription {
    /// Returns the active controller node ID, or Kafka's negative sentinel.
    pub fn controller_id(&self) -> i32 {
        self.controller_id
    }

    /// Returns all brokers advertised by the cluster.
    pub fn brokers(&self) -> &[BrokerDescription] {
        &self.brokers
    }

    /// Returns the active controller broker when it is present in metadata.
    pub fn controller(&self) -> Option<&BrokerDescription> {
        self.brokers
            .iter()
            .find(|broker| broker.id == self.controller_id)
    }
}

/// One topic returned by [`AdminClient::list_topics`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicListing {
    name: String,
    is_internal: bool,
    partition_count: usize,
    error_code: i16,
}

impl TopicListing {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether Kafka marks this as an internal topic.
    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    /// Returns the number of partitions included in metadata.
    pub fn partition_count(&self) -> usize {
        self.partition_count
    }

    /// Returns Kafka's raw topic-level metadata error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns whether Kafka reported a successful topic metadata result.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(topic: TopicMetadata) -> Self {
        Self {
            name: topic.name,
            is_internal: topic.is_internal,
            partition_count: topic.partitions.len(),
            error_code: topic.error_code,
        }
    }
}

/// One Kafka topic whose configuration should be described.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigResource {
    name: String,
    configuration_keys: Option<Vec<String>>,
}

impl TopicConfigResource {
    /// Requests all configuration keys for a topic.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            configuration_keys: None,
        }
    }

    /// Requests selected configuration keys for a topic.
    pub fn with_keys(
        name: impl Into<String>,
        configuration_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            configuration_keys: Some(configuration_keys.into_iter().map(Into::into).collect()),
        }
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns selected keys, or `None` when all keys were requested.
    pub fn configuration_keys(&self) -> Option<&[String]> {
        self.configuration_keys.as_deref()
    }

    fn as_protocol(&self) -> DescribeConfigsResourceV1 {
        DescribeConfigsResourceV1 {
            resource_type: 2,
            resource_name: self.name.clone(),
            configuration_keys: self.configuration_keys.clone(),
        }
    }
}

/// Options for one DescribeConfigs operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DescribeConfigsOptions {
    include_synonyms: bool,
}

impl DescribeConfigsOptions {
    /// Creates options that omit configuration synonyms.
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects whether Kafka should return configuration synonyms.
    pub fn include_synonyms(mut self, include_synonyms: bool) -> Self {
        self.include_synonyms = include_synonyms;
        self
    }

    /// Returns whether configuration synonyms were requested.
    pub fn includes_synonyms(&self) -> bool {
        self.include_synonyms
    }
}

/// Complete response from one DescribeConfigs operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    throttle_time: Duration,
    resources: Vec<ConfigResourceResult>,
}

impl DescribeConfigsResult {
    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns resource results in broker response order.
    pub fn resources(&self) -> &[ConfigResourceResult] {
        &self.resources
    }

    /// Consumes this response and returns its resource results.
    pub fn into_resources(self) -> Vec<ConfigResourceResult> {
        self.resources
    }

    /// Returns whether at least one resource was rejected.
    pub fn has_errors(&self) -> bool {
        self.resources.iter().any(|resource| !resource.is_success())
    }
}

/// Configuration result for one Kafka resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigResourceResult {
    resource_type: i8,
    name: String,
    error_code: i16,
    error_message: Option<String>,
    entries: Vec<ConfigEntry>,
}

impl ConfigResourceResult {
    /// Returns Kafka's raw resource type value.
    pub fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether Kafka described the resource successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw resource-level error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's optional resource-level error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns configuration entries in broker response order.
    pub fn entries(&self) -> &[ConfigEntry] {
        &self.entries
    }

    fn from_protocol(result: DescribeConfigsResultV1) -> Self {
        Self {
            resource_type: result.resource_type,
            name: result.resource_name,
            error_code: result.error_code,
            error_message: result.error_message,
            entries: result
                .configs
                .into_iter()
                .map(ConfigEntry::from_protocol)
                .collect(),
        }
    }
}

/// One Kafka configuration entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    name: String,
    value: Option<String>,
    read_only: bool,
    source: ConfigSource,
    is_sensitive: bool,
    synonyms: Vec<ConfigSynonym>,
}

impl ConfigEntry {
    /// Returns the configuration key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the value, which is absent for sensitive or null settings.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns whether Kafka marks this entry read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Returns the source selected by Kafka for this value.
    pub fn source(&self) -> ConfigSource {
        self.source
    }

    /// Returns whether Kafka marks this entry sensitive.
    pub fn is_sensitive(&self) -> bool {
        self.is_sensitive
    }

    /// Returns configuration synonyms in broker response order.
    pub fn synonyms(&self) -> &[ConfigSynonym] {
        &self.synonyms
    }

    fn from_protocol(entry: DescribeConfigsEntryV1) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            read_only: entry.read_only,
            source: ConfigSource::from_code(entry.config_source),
            is_sensitive: entry.is_sensitive,
            synonyms: entry
                .synonyms
                .into_iter()
                .map(ConfigSynonym::from_protocol)
                .collect(),
        }
    }
}

/// One synonym contributing to a Kafka configuration value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigSynonym {
    name: String,
    value: Option<String>,
    source: ConfigSource,
}

impl ConfigSynonym {
    /// Returns the synonym configuration key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the synonym value.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Returns the synonym's configuration source.
    pub fn source(&self) -> ConfigSource {
        self.source
    }

    fn from_protocol(synonym: DescribeConfigsSynonymV1) -> Self {
        Self {
            name: synonym.name,
            value: synonym.value,
            source: ConfigSource::from_code(synonym.source),
        }
    }
}

/// Kafka configuration source classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// Kafka could not determine the configuration source.
    Unknown,
    /// A dynamic topic-level value.
    DynamicTopicConfig,
    /// A dynamic broker-level value.
    DynamicBrokerConfig,
    /// A dynamic default applied to all brokers.
    DynamicDefaultBrokerConfig,
    /// A static broker-level value.
    StaticBrokerConfig,
    /// Kafka's default value.
    DefaultConfig,
    /// A dynamic broker logger value.
    DynamicBrokerLoggerConfig,
    /// A dynamic client metrics value.
    DynamicClientMetricsConfig,
    /// A dynamic consumer-group value.
    DynamicGroupConfig,
    /// A source code not recognized by this kafrust version.
    Other(i8),
}

impl ConfigSource {
    /// Classifies Kafka's raw configuration source value.
    pub fn from_code(code: i8) -> Self {
        match code {
            0 => Self::Unknown,
            1 => Self::DynamicTopicConfig,
            2 => Self::DynamicBrokerConfig,
            3 => Self::DynamicDefaultBrokerConfig,
            4 => Self::StaticBrokerConfig,
            5 => Self::DefaultConfig,
            6 => Self::DynamicBrokerLoggerConfig,
            7 => Self::DynamicClientMetricsConfig,
            8 => Self::DynamicGroupConfig,
            other => Self::Other(other),
        }
    }

    /// Returns Kafka's raw configuration source value.
    pub fn code(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::DynamicTopicConfig => 1,
            Self::DynamicBrokerConfig => 2,
            Self::DynamicDefaultBrokerConfig => 3,
            Self::StaticBrokerConfig => 4,
            Self::DefaultConfig => 5,
            Self::DynamicBrokerLoggerConfig => 6,
            Self::DynamicClientMetricsConfig => 7,
            Self::DynamicGroupConfig => 8,
            Self::Other(code) => code,
        }
    }
}

/// Incremental configuration changes for one Kafka topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigAlteration {
    name: String,
    operations: Vec<ConfigAlterOperation>,
}

impl TopicConfigAlteration {
    /// Creates an empty set of operations for a topic.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            operations: Vec::new(),
        }
    }

    /// Sets a configuration value.
    pub fn set(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.operations.push(ConfigAlterOperation::set(name, value));
        self
    }

    /// Removes a dynamic configuration value.
    pub fn delete(mut self, name: impl Into<String>) -> Self {
        self.operations.push(ConfigAlterOperation::delete(name));
        self
    }

    /// Appends values to a list configuration.
    pub fn append(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.operations
            .push(ConfigAlterOperation::append(name, value));
        self
    }

    /// Subtracts values from a list configuration.
    pub fn subtract(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.operations
            .push(ConfigAlterOperation::subtract(name, value));
        self
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns operations in request order.
    pub fn operations(&self) -> &[ConfigAlterOperation] {
        &self.operations
    }

    fn as_protocol(&self) -> IncrementalAlterConfigsResourceV0 {
        IncrementalAlterConfigsResourceV0 {
            resource_type: 2,
            resource_name: self.name.clone(),
            configs: self
                .operations
                .iter()
                .map(ConfigAlterOperation::as_protocol)
                .collect(),
        }
    }
}

/// One operation in an incremental Kafka configuration update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigAlterOperation {
    name: String,
    kind: ConfigAlterOperationKind,
    value: Option<String>,
}

impl ConfigAlterOperation {
    /// Creates a SET operation.
    pub fn set(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::with_value(name, ConfigAlterOperationKind::Set, value)
    }

    /// Creates a DELETE operation with Kafka's required null value.
    pub fn delete(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: ConfigAlterOperationKind::Delete,
            value: None,
        }
    }

    /// Creates an APPEND operation.
    pub fn append(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::with_value(name, ConfigAlterOperationKind::Append, value)
    }

    /// Creates a SUBTRACT operation.
    pub fn subtract(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::with_value(name, ConfigAlterOperationKind::Subtract, value)
    }

    /// Returns the configuration key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Kafka operation kind.
    pub fn kind(&self) -> ConfigAlterOperationKind {
        self.kind
    }

    /// Returns the operation value, or `None` for DELETE.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    fn with_value(
        name: impl Into<String>,
        kind: ConfigAlterOperationKind,
        value: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            value: Some(value.into()),
        }
    }

    fn as_protocol(&self) -> IncrementalAlterConfigsEntryV0 {
        IncrementalAlterConfigsEntryV0 {
            name: self.name.clone(),
            operation: self.kind.code(),
            value: self.value.clone(),
        }
    }
}

/// Kafka IncrementalAlterConfigs operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigAlterOperationKind {
    /// Sets one configuration value.
    Set,
    /// Deletes one dynamic configuration value.
    Delete,
    /// Appends values to a list configuration.
    Append,
    /// Subtracts values from a list configuration.
    Subtract,
}

impl ConfigAlterOperationKind {
    /// Returns Kafka's raw operation value.
    pub fn code(self) -> i8 {
        match self {
            Self::Set => 0,
            Self::Delete => 1,
            Self::Append => 2,
            Self::Subtract => 3,
        }
    }
}

/// Options for one IncrementalAlterConfigs operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AlterConfigsOptions {
    validate_only: bool,
}

impl AlterConfigsOptions {
    /// Creates options that apply valid changes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects validation without applying changes.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Returns whether Kafka should only validate the changes.
    pub fn is_validate_only(&self) -> bool {
        self.validate_only
    }
}

/// Complete response from one IncrementalAlterConfigs operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResult {
    throttle_time: Duration,
    resources: Vec<AlterConfigResourceResult>,
}

impl AlterConfigsResult {
    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns resource outcomes in broker response order.
    pub fn resources(&self) -> &[AlterConfigResourceResult] {
        &self.resources
    }

    /// Consumes this response and returns resource outcomes.
    pub fn into_resources(self) -> Vec<AlterConfigResourceResult> {
        self.resources
    }

    /// Returns whether at least one resource update was rejected.
    pub fn has_errors(&self) -> bool {
        self.resources.iter().any(|resource| !resource.is_success())
    }
}

/// Outcome for one resource in IncrementalAlterConfigs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigResourceResult {
    resource_type: i8,
    name: String,
    error_code: i16,
    error_message: Option<String>,
}

impl AlterConfigResourceResult {
    /// Returns Kafka's raw resource type value.
    pub fn resource_type(&self) -> i8 {
        self.resource_type
    }

    /// Returns the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether Kafka applied or validated this resource.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw resource error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's optional resource error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(response: IncrementalAlterConfigsResourceResponseV0) -> Self {
        Self {
            resource_type: response.resource_type,
            name: response.resource_name,
            error_code: response.error_code,
            error_message: response.error_message,
        }
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

/// Options for one DeleteTopics operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteTopicsOptions {
    timeout: Duration,
}

impl DeleteTopicsOptions {
    /// Creates options with a 30-second broker timeout.
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }

    /// Sets how long the controller may wait for topic deletion.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the configured broker-side timeout.
    pub fn timeout_ref(&self) -> Duration {
        self.timeout
    }
}

impl Default for DeleteTopicsOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete response from one DeleteTopics operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsResult {
    throttle_time: Duration,
    topics: Vec<DeleteTopicResult>,
}

impl DeleteTopicsResult {
    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-topic outcomes in broker response order.
    pub fn topics(&self) -> &[DeleteTopicResult] {
        &self.topics
    }

    /// Consumes this response and returns per-topic outcomes.
    pub fn into_topics(self) -> Vec<DeleteTopicResult> {
        self.topics
    }

    /// Returns whether at least one topic deletion was rejected.
    pub fn has_errors(&self) -> bool {
        self.topics.iter().any(|topic| !topic.is_success())
    }
}

/// Outcome for one topic in a DeleteTopics response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicResult {
    name: String,
    error_code: i16,
}

impl DeleteTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether Kafka accepted the topic deletion.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw error code, or zero for success.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(result: DeleteTopicsTopicResultV3) -> Self {
        Self {
            name: result.name,
            error_code: result.error_code,
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
    use super::{
        AdminClient, AlterConfigsOptions, ConfigAlterOperationKind, ConfigSource,
        CreateTopicsOptions, DeleteTopicsOptions, DescribeConfigsOptions, NewTopic,
        TopicConfigAlteration, TopicConfigResource,
    };
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

    #[test]
    fn builds_delete_topics_options() {
        let options = DeleteTopicsOptions::new().timeout(Duration::from_secs(9));

        assert_eq!(options.timeout_ref(), Duration::from_secs(9));
    }

    #[test]
    fn builds_topic_config_queries_and_options() {
        let all = TopicConfigResource::new("orders");
        assert_eq!(all.name(), "orders");
        assert_eq!(all.configuration_keys(), None);

        let selected =
            TopicConfigResource::with_keys("payments", ["cleanup.policy", "retention.ms"]);
        assert_eq!(
            selected.configuration_keys(),
            Some(&["cleanup.policy".to_owned(), "retention.ms".to_owned()][..])
        );

        let options = DescribeConfigsOptions::new().include_synonyms(true);
        assert!(options.includes_synonyms());
        assert_eq!(ConfigSource::DynamicTopicConfig.code(), 1);
        assert_eq!(ConfigSource::DynamicDefaultBrokerConfig.code(), 3);
        assert_eq!(ConfigSource::DynamicGroupConfig.code(), 8);
        assert_eq!(ConfigSource::from_code(99), ConfigSource::Other(99));
    }

    #[test]
    fn builds_incremental_topic_config_alterations() {
        let alteration = TopicConfigAlteration::new("orders")
            .set("retention.ms", "60000")
            .delete("segment.ms")
            .append("cleanup.policy", "compact")
            .subtract("cleanup.policy", "delete");

        assert_eq!(alteration.name(), "orders");
        assert_eq!(alteration.operations().len(), 4);
        assert_eq!(
            alteration.operations()[0].kind(),
            ConfigAlterOperationKind::Set
        );
        assert_eq!(alteration.operations()[0].name(), "retention.ms");
        assert_eq!(alteration.operations()[0].value(), Some("60000"));
        assert_eq!(
            alteration.operations()[1].kind(),
            ConfigAlterOperationKind::Delete
        );
        assert_eq!(alteration.operations()[1].value(), None);
        assert_eq!(alteration.operations()[2].kind().code(), 2);
        assert_eq!(alteration.operations()[3].kind().code(), 3);

        let options = AlterConfigsOptions::new().validate_only(true);
        assert!(options.is_validate_only());
    }

    #[tokio::test]
    async fn describes_cluster_with_controller_broker() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            assert_eq!(&request[request.len() - 4..], &[0, 0, 0, 0]);
            write_frame(&mut connection, &metadata_response(addr.port())).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let cluster = admin.describe_cluster().await.unwrap();

        assert_eq!(cluster.controller_id(), 1);
        assert_eq!(cluster.brokers().len(), 1);
        assert_eq!(cluster.brokers()[0].id(), 1);
        assert_eq!(cluster.brokers()[0].host(), "127.0.0.1");
        assert_eq!(cluster.brokers()[0].port(), i32::from(addr.port()));
        assert_eq!(cluster.brokers()[0].rack(), None);
        assert_eq!(cluster.controller(), Some(&cluster.brokers()[0]));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lists_topics_and_preserves_metadata_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            assert_eq!(&request[request.len() - 4..], &[0xff, 0xff, 0xff, 0xff]);
            write_frame(&mut connection, &topic_metadata_response(addr.port())).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let topics = admin.list_topics().await.unwrap();

        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].name(), "orders");
        assert!(!topics[0].is_internal());
        assert_eq!(topics[0].partition_count(), 1);
        assert!(topics[0].is_success());
        assert_eq!(topics[0].broker_error_kind(), None);
        assert_eq!(topics[1].name(), "__consumer_offsets");
        assert!(topics[1].is_internal());
        assert_eq!(topics[1].partition_count(), 0);
        assert_eq!(topics[1].error_code(), 3);
        assert_eq!(
            topics[1].broker_error_kind(),
            Some(BrokerErrorKind::UnknownTopicOrPartition)
        );
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_topic_configs_and_preserves_resource_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 32, 0, 1]);
            assert_eq!(request.last(), Some(&1));
            write_frame(&mut connection, &describe_configs_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_topic_configs(
                &[
                    TopicConfigResource::with_keys("orders", ["cleanup.policy"]),
                    TopicConfigResource::new("missing"),
                ],
                DescribeConfigsOptions::new().include_synonyms(true),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(9));
        assert!(result.has_errors());
        assert_eq!(result.resources().len(), 2);
        let orders = &result.resources()[0];
        assert_eq!(orders.resource_type(), 2);
        assert_eq!(orders.name(), "orders");
        assert!(orders.is_success());
        assert_eq!(orders.error_message(), None);
        assert_eq!(orders.entries().len(), 1);
        assert_eq!(orders.entries()[0].name(), "cleanup.policy");
        assert_eq!(orders.entries()[0].value(), Some("compact"));
        assert!(!orders.entries()[0].is_read_only());
        assert!(!orders.entries()[0].is_sensitive());
        assert_eq!(
            orders.entries()[0].source(),
            ConfigSource::DynamicTopicConfig
        );
        assert_eq!(orders.entries()[0].synonyms().len(), 1);
        assert_eq!(orders.entries()[0].synonyms()[0].name(), "cleanup.policy");
        assert_eq!(orders.entries()[0].synonyms()[0].value(), Some("delete"));
        assert_eq!(
            orders.entries()[0].synonyms()[0].source(),
            ConfigSource::DefaultConfig
        );
        let missing = &result.resources()[1];
        assert_eq!(missing.name(), "missing");
        assert_eq!(missing.error_code(), 3);
        assert_eq!(missing.error_message(), Some("missing"));
        assert_eq!(
            missing.broker_error_kind(),
            Some(BrokerErrorKind::UnknownTopicOrPartition)
        );
        assert_eq!(metrics.snapshot().broker_errors, 1);
        assert_eq!(result.clone().into_resources().len(), 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn alters_topic_configs_and_preserves_resource_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 44, 0, 0]);
            assert_eq!(request.last(), Some(&1));
            write_frame(&mut connection, &incremental_alter_configs_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .incremental_alter_topic_configs(
                &[
                    TopicConfigAlteration::new("orders").set("retention.ms", "60000"),
                    TopicConfigAlteration::new("payments").delete("retention.ms"),
                ],
                AlterConfigsOptions::new().validate_only(true),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(6));
        assert!(result.has_errors());
        assert_eq!(result.resources().len(), 2);
        assert_eq!(result.resources()[0].resource_type(), 2);
        assert_eq!(result.resources()[0].name(), "orders");
        assert!(result.resources()[0].is_success());
        assert_eq!(result.resources()[0].error_message(), None);
        assert_eq!(result.resources()[1].name(), "payments");
        assert_eq!(result.resources()[1].error_code(), 40);
        assert_eq!(result.resources()[1].error_message(), Some("invalid"));
        assert_eq!(
            result.resources()[1].broker_error_kind(),
            Some(BrokerErrorKind::InvalidConfig)
        );
        assert_eq!(metrics.snapshot().broker_errors, 1);
        assert_eq!(result.clone().into_resources().len(), 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_create_topics_to_controller_and_preserves_partial_result() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            assert_eq!(
                &metadata_request[metadata_request.len() - 4..],
                &[0, 0, 0, 0]
            );
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

    #[tokio::test]
    async fn routes_delete_topics_to_controller_and_preserves_partial_result() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            assert_eq!(
                &metadata_request[metadata_request.len() - 4..],
                &[0, 0, 0, 0]
            );
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut controller, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut controller).await;
            assert_eq!(&delete_request[0..4], &[0, 20, 0, 3]);
            write_frame(&mut controller, &delete_topics_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .delete_topics(&["orders".to_owned()], DeleteTopicsOptions::new())
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(8));
        assert!(result.has_errors());
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(result.topics()[0].error_code(), 3);
        assert_eq!(
            result.topics()[0].broker_error_kind(),
            Some(BrokerErrorKind::UnknownTopicOrPartition)
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

    fn topic_metadata_response(port: u16) -> Vec<u8> {
        let mut response = metadata_response(port);
        response.truncate(response.len() - 4);
        response.extend_from_slice(&[
            0, 0, 0, 2, // topic count
            0, 0, // success
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0,    // not internal
            0, 0, 0, 1, // partition count
            0, 0, // success
            0, 0, 0, 0, // partition index
            0, 0, 0, 1, // leader
            0, 0, 0, 1, // replica count
            0, 0, 0, 1, // replica
            0, 0, 0, 1, // ISR count
            0, 0, 0, 1, // ISR
            0, 3, // unknown topic or partition
            0, 18, b'_', b'_', b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', b'_', b'o', b'f',
            b'f', b's', b'e', b't', b's', // topic name
            1,    // internal
            0, 0, 0, 0, // partition count
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

    fn delete_topics_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 8, // throttle time
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 3, // unknown topic or partition
        ]
    }

    fn describe_configs_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 9, // throttle time
            0, 0, 0, 2, // result count
            0, 0, // success
            0xff, 0xff, // null error message
            2,    // topic resource
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
            0, 0, 0, 1, // config count
            0, 14, b'c', b'l', b'e', b'a', b'n', b'u', b'p', b'.', b'p', b'o', b'l', b'i', b'c',
            b'y', // config name
            0, 7, b'c', b'o', b'm', b'p', b'a', b'c', b't', // value
            0,    // read only
            1,    // dynamic topic config
            0,    // not sensitive
            0, 0, 0, 1, // synonym count
            0, 14, b'c', b'l', b'e', b'a', b'n', b'u', b'p', b'.', b'p', b'o', b'l', b'i', b'c',
            b'y', // synonym name
            0, 6, b'd', b'e', b'l', b'e', b't', b'e', // value
            5,    // default config
            0, 3, // unknown topic or partition
            0, 7, b'm', b'i', b's', b's', b'i', b'n', b'g', // error message
            2,    // topic resource
            0, 7, b'm', b'i', b's', b's', b'i', b'n', b'g', // resource name
            0, 0, 0, 0, // config count
        ]
    }

    fn incremental_alter_configs_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 6, // throttle time
            0, 0, 0, 2, // response count
            0, 0, // success
            0xff, 0xff, // null error message
            2,    // topic resource
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
            0, 40, // invalid config
            0, 7, b'i', b'n', b'v', b'a', b'l', b'i', b'd', // error message
            2,    // topic resource
            0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // resource name
        ]
    }
}
