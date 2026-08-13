use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::time::Duration;

use kafrust_protocol::api::alter_client_quotas::{
    AlterClientQuotasEntityV0, AlterClientQuotasEntryV0, AlterClientQuotasOperationV0,
};
use kafrust_protocol::api::alter_configs::{
    AlterConfigsResourceResponseV1, AlterConfigsResourceV1, AlterableConfigV1,
};
use kafrust_protocol::api::alter_partition_reassignments::{
    AlterPartitionReassignmentsPartitionV0, AlterPartitionReassignmentsTopicV0,
};
use kafrust_protocol::api::alter_user_scram_credentials::{
    AlterUserScramCredentialsDeletionV0, AlterUserScramCredentialsUpsertionV0,
};
use kafrust_protocol::api::create_acls::CreateAclsCreationV1;
use kafrust_protocol::api::create_partitions::{
    CreatePartitionsAssignmentV0, CreatePartitionsTopicResultV0, CreatePartitionsTopicV0,
};
use kafrust_protocol::api::create_topics::{
    CreateTopicsAssignmentV2, CreateTopicsConfigV2, CreateTopicsTopicResultV2, CreateTopicsTopicV2,
};
use kafrust_protocol::api::delete_acls::{
    DeleteAclsFilterResultV1, DeleteAclsFilterV1, DeleteAclsMatchingAclV1,
};
use kafrust_protocol::api::delete_groups::DeleteGroupResultV1;
use kafrust_protocol::api::delete_records::{
    DeleteRecordsPartitionResponseV1, DeleteRecordsTopicResponseV1, DeleteRecordsTopicV1,
};
use kafrust_protocol::api::delete_topics::DeleteTopicsTopicResultV3;
use kafrust_protocol::api::describe_acls::{DescribeAclsEntryV1, DescribeAclsResponseV1};
use kafrust_protocol::api::describe_client_quotas::{
    DescribeClientQuotasComponentV0, DescribeClientQuotasEntityV0, DescribeClientQuotasEntryV0,
    DescribeClientQuotasResponseV0, DescribeClientQuotasValueV0,
};
use kafrust_protocol::api::describe_configs::{
    DescribeConfigsEntryV1, DescribeConfigsResourceV1, DescribeConfigsResultV1,
    DescribeConfigsSynonymV1,
};
use kafrust_protocol::api::describe_groups::{DescribeGroupsGroupV1, DescribeGroupsMemberV1};
use kafrust_protocol::api::describe_log_dirs::{DescribeLogDirsResponse, DescribeLogDirsTopic};
use kafrust_protocol::api::describe_producers::{
    DescribeProducersActiveProducerV0, DescribeProducersPartitionResponseV0,
    DescribeProducersTopicResponseV0,
};
use kafrust_protocol::api::describe_transactions::{
    DescribeTransactionsStateV0, DescribeTransactionsTopicV0,
};
use kafrust_protocol::api::describe_user_scram_credentials::{
    DescribeUserScramCredentialsResponseV0, ScramCredentialInfoV0,
};
use kafrust_protocol::api::elect_leaders::{
    ElectLeadersResponseV0, ElectLeadersResponseV1, ElectLeadersResponseV2,
    ElectLeadersTopicResultV0, ElectLeadersTopicV0,
};
use kafrust_protocol::api::incremental_alter_configs::{
    IncrementalAlterConfigsEntryV0, IncrementalAlterConfigsResourceResponseV0,
    IncrementalAlterConfigsResourceV0,
};
use kafrust_protocol::api::list_groups::ListedGroupV1;
use kafrust_protocol::api::list_partition_reassignments::ListPartitionReassignmentsTopicV0;
use kafrust_protocol::api::list_transactions::ListedTransactionV0;
use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1, TopicMetadata};
use kafrust_protocol::api::offset_commit::{
    OffsetCommitPartition, OffsetCommitPartitionV9, OffsetCommitTopic, OffsetCommitTopicResponse,
    OffsetCommitTopicV9,
};
use kafrust_protocol::api::offset_delete::{
    OffsetDeleteRequestPartitionV0, OffsetDeleteRequestTopicV0, OffsetDeleteResponsePartitionV0,
    OffsetDeleteResponseTopicV0,
};
use kafrust_protocol::api::offset_fetch::{
    OffsetFetchGroupResponse, OffsetFetchTopic, OffsetFetchTopicResponse, OffsetFetchTopicV9,
};

use crate::client::Client;
use crate::config::ClientConfig;
use crate::error::{BrokerErrorKind, Error, Result};
use crate::metrics::ClientMetrics;
use crate::scram::{derive_salted_password, ScramHash};
use rand::RngCore;

const ADMIN_COORDINATOR_MAX_RETRIES: u32 = 5;
const ADMIN_COORDINATOR_RETRY_BACKOFF_BASE: Duration = Duration::from_millis(50);
const ADMIN_COORDINATOR_MAX_RETRY_BACKOFF: Duration = Duration::from_millis(800);

/// Kafka administration client.
///
/// Each controller-scoped operation discovers the active controller through
/// cluster metadata before opening the controller connection.
#[derive(Debug, Clone)]
pub struct AdminClient {
    config: ClientConfig,
    max_retries: u32,
}

impl AdminClient {
    /// Creates an admin client from shared Kafka connection configuration.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            max_retries: ADMIN_COORDINATOR_MAX_RETRIES,
        }
    }

    /// Validates the shared connection configuration without opening a broker
    /// connection.
    pub fn validate(&self) -> Result<()> {
        self.config.validate()
    }

    /// Sets the maximum retry attempts for transient coordinator discovery and
    /// coordinator-routed request failures.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Returns the configured maximum retry count.
    pub fn max_retries_ref(&self) -> u32 {
        self.max_retries
    }

    /// Returns the shared metrics handle used by admin broker connections.
    pub fn metrics(&self) -> ClientMetrics {
        self.config.metrics_ref()
    }

    async fn metadata_with_admin_retries(
        &self,
        topics: Option<Vec<String>>,
    ) -> Result<MetadataResponseV1> {
        let mut retry = 0;
        self.metadata_with_admin_retries_from(topics, &mut retry)
            .await
    }

    async fn metadata_with_admin_retries_from(
        &self,
        topics: Option<Vec<String>>,
        retry: &mut u32,
    ) -> Result<MetadataResponseV1> {
        loop {
            let mut client = match self.config.clone().connect().await {
                Ok(client) => client,
                Err(error)
                    if *retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                {
                    *retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(*retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match client.metadata(topics.clone()).await {
                Ok(metadata)
                    if *retry < self.max_retries && is_retryable_metadata_response(&metadata) =>
                {
                    self.config.record_broker_error();
                    *retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(*retry)).await;
                }
                Ok(metadata) => return Ok(metadata),
                Err(error)
                    if *retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                {
                    *retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(*retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Describes the Kafka cluster brokers and active controller.
    #[tracing::instrument(level = "debug", name = "kafka.admin.describe_cluster", skip_all, err)]
    pub async fn describe_cluster(&self) -> Result<ClusterDescription> {
        let metadata = self.metadata_with_admin_retries(Some(Vec::new())).await?;

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
        let metadata = self.metadata_with_admin_retries(None).await?;

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

    /// Lists ACL bindings matching a Kafka ACL filter using DescribeAcls v1.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_acls",
        skip_all,
        fields(resource_type = ?filter.resource_type, pattern_type = ?filter.pattern_type),
        err
    )]
    pub async fn describe_acls(&self, filter: &AclFilter) -> Result<DescribeAclsResult> {
        let mut retry = 0;
        let response = loop {
            let mut client = match self.config.clone().connect().await {
                Ok(client) => client,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match client
                .describe_acls_v1(
                    filter.resource_type.code(),
                    filter.resource_name.clone(),
                    filter.pattern_type.code(),
                    filter.principal.clone(),
                    filter.host.clone(),
                    filter.operation.code(),
                    filter.permission_type.code(),
                )
                .await
            {
                Ok(response)
                    if retry < self.max_retries
                        && is_retryable_admin_read_code(response.error_code) =>
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) => break response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };
        if response.error_code != 0 {
            self.config.record_broker_error();
        }

        Ok(DescribeAclsResult::from_protocol(response))
    }

    /// Creates ACL bindings using CreateAcls v1.
    ///
    /// Kafka applies each creation independently. The returned results retain
    /// the input binding alongside its per-entry broker outcome.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.create_acls",
        skip_all,
        fields(acl_count = bindings.len()),
        err
    )]
    pub async fn create_acls(&self, bindings: &[AclBinding]) -> Result<CreateAclsResult> {
        let mut client = self.bootstrap_client_with_retries().await?;
        let response = client
            .create_acls_v1(bindings.iter().map(AclBinding::as_protocol).collect())
            .await?;
        for result in &response.results {
            if result.error_code != 0 {
                self.config.record_broker_error();
            }
        }

        if response.results.len() != bindings.len() {
            return Err(Error::ResponseCountMismatch {
                operation: "CreateAcls",
                expected: bindings.len(),
                actual: response.results.len(),
            });
        }

        Ok(CreateAclsResult::from_protocol(response, bindings))
    }

    /// Deletes ACL bindings matching each filter using DeleteAcls v1.
    ///
    /// Each filter has an independent result and includes the bindings Kafka
    /// matched and removed. A filter is not transactional with other filters.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_acls",
        skip_all,
        fields(filter_count = filters.len()),
        err
    )]
    pub async fn delete_acls(&self, filters: &[AclFilter]) -> Result<DeleteAclsResult> {
        let mut client = self.bootstrap_client_with_retries().await?;
        let response = client
            .delete_acls_v1(filters.iter().map(AclFilter::as_protocol).collect())
            .await?;
        for result in &response.filter_results {
            if result.error_code != 0 || result.matching_acls.iter().any(|acl| acl.error_code != 0)
            {
                self.config.record_broker_error();
            }
        }

        if response.filter_results.len() != filters.len() {
            return Err(Error::ResponseCountMismatch {
                operation: "DeleteAcls",
                expected: filters.len(),
                actual: response.filter_results.len(),
            });
        }

        Ok(DeleteAclsResult::from_protocol(response, filters))
    }

    /// Describes client quotas matching a typed Kafka quota filter.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_client_quotas",
        skip_all,
        fields(strict = filter.strict, component_count = filter.components.len()),
        err
    )]
    pub async fn describe_client_quotas(
        &self,
        filter: &ClientQuotaFilter,
    ) -> Result<DescribeClientQuotasResult> {
        let components = filter
            .components
            .iter()
            .map(ClientQuotaFilterComponent::as_protocol)
            .collect::<Vec<_>>();
        let mut retry = 0;
        let response = loop {
            let mut client = match self.config.clone().connect().await {
                Ok(client) => client,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match client
                .describe_client_quotas_v0(components.clone(), filter.strict)
                .await
            {
                Ok(response)
                    if retry < self.max_retries
                        && is_retryable_admin_read_code(response.error_code) =>
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) => break response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        Ok(DescribeClientQuotasResult::from_protocol(response))
    }

    /// Alters client quotas and preserves each entity-level broker outcome.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_client_quotas",
        skip_all,
        fields(alteration_count = alterations.len(), validate_only),
        err
    )]
    pub async fn alter_client_quotas(
        &self,
        alterations: &[ClientQuotaAlteration],
        validate_only: bool,
    ) -> Result<AlterClientQuotasResult> {
        let mut client = self.bootstrap_client_with_retries().await?;
        let response = client
            .alter_client_quotas_v0(
                alterations
                    .iter()
                    .map(ClientQuotaAlteration::as_protocol)
                    .collect(),
                validate_only,
            )
            .await?;
        for result in &response.entries {
            if result.error_code != 0 {
                self.config.record_broker_error();
            }
        }
        if response.entries.len() != alterations.len() {
            return Err(Error::ResponseCountMismatch {
                operation: "AlterClientQuotas",
                expected: alterations.len(),
                actual: response.entries.len(),
            });
        }
        Ok(AlterClientQuotasResult::from_protocol(
            response,
            alterations,
        ))
    }

    /// Describes SCRAM credentials through DescribeUserScramCredentials v0.
    ///
    /// `None` asks Kafka to return every user. A user name filter is sent as
    /// an explicit nullable array, matching Kafka's distinction between all
    /// users and a selected user list.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_user_scram_credentials",
        skip_all,
        fields(user_count = users.map_or(0, <[String]>::len)),
        err
    )]
    pub async fn describe_user_scram_credentials(
        &self,
        users: Option<&[String]>,
    ) -> Result<DescribeUserScramCredentialsResult> {
        let users = users.map(ToOwned::to_owned);
        let mut retry = 0;
        let response = loop {
            let mut client = match self.config.clone().connect().await {
                Ok(client) => client,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match client
                .describe_user_scram_credentials_v0(users.clone())
                .await
            {
                Ok(response)
                    if retry < self.max_retries
                        && is_retryable_admin_read_code(response.error_code) =>
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) => break response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        for result in &response.results {
            if result.error_code != 0 {
                self.config.record_broker_error();
            }
        }
        Ok(DescribeUserScramCredentialsResult::from_protocol(response))
    }

    /// Creates, replaces, or deletes SCRAM credentials through AlterUserScramCredentials v0.
    ///
    /// Kafka applies all changes for one user as a unit and returns one result
    /// per affected user. The request is sent to the active controller.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_user_scram_credentials",
        skip_all,
        fields(deletion_count = deletions.len(), upsertion_count = upsertions.len()),
        err
    )]
    pub async fn alter_user_scram_credentials(
        &self,
        deletions: &[ScramCredentialDeletion],
        upsertions: &[ScramCredentialUpsertion],
    ) -> Result<AlterUserScramCredentialsResult> {
        let mut client = self.controller_client_with_retries().await?;
        let response = client
            .alter_user_scram_credentials_v0(
                deletions
                    .iter()
                    .map(ScramCredentialDeletion::as_protocol)
                    .collect(),
                upsertions
                    .iter()
                    .map(ScramCredentialUpsertion::as_protocol)
                    .collect(),
            )
            .await?;
        for result in &response.results {
            if result.error_code != 0 {
                self.config.record_broker_error();
            }
        }
        Ok(AlterUserScramCredentialsResult::from_protocol(response))
    }

    /// Triggers a preferred or unclean leader election through the active
    /// controller.
    ///
    /// Pass `None` for `elections` to ask Kafka to consider every eligible
    /// partition. A non-empty slice targets the explicitly listed partitions.
    /// Kafka returns independent topic and partition outcomes; the method does
    /// not retry after the request is transmitted because an ambiguous retry
    /// could duplicate a controller-side election request.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.elect_leaders",
        skip_all,
        fields(
            election_count = elections.map_or(0, <[LeaderElection]>::len),
            election_type = ?election_type
        ),
        err
    )]
    pub async fn elect_leaders(
        &self,
        elections: Option<&[LeaderElection]>,
        election_type: ElectionType,
        options: ElectLeadersOptions,
    ) -> Result<ElectLeadersResult> {
        if elections.is_some_and(|items| items.iter().any(|item| item.partitions.is_empty())) {
            return Err(Error::Unsupported(
                "ElectLeaders topic filters must include at least one partition",
            ));
        }

        let mut controller_client = self.controller_client_with_retries().await?;
        let api_versions = controller_client
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        let Some(version) = api_versions.highest_supported_version(43, 2) else {
            return Err(Error::Unsupported(
                "broker does not advertise a supported ElectLeaders API version",
            ));
        };
        let topics = elections.map(|items| {
            items
                .iter()
                .map(LeaderElection::as_protocol)
                .collect::<Vec<ElectLeadersTopicV0>>()
        });
        let timeout_ms = duration_millis_i32(options.timeout);

        let result = match version {
            0 => {
                if election_type != ElectionType::Preferred {
                    return Err(Error::Unsupported(
                        "unclean leader election requires ElectLeaders v1 or newer",
                    ));
                }
                ElectLeadersResult::from_protocol_v0(
                    controller_client
                        .elect_leaders_v0(topics, timeout_ms)
                        .await?,
                )
            }
            1 => ElectLeadersResult::from_protocol_v1(
                controller_client
                    .elect_leaders_v1(election_type.as_i8(), topics, timeout_ms)
                    .await?,
            ),
            _ => ElectLeadersResult::from_protocol_v2(
                controller_client
                    .elect_leaders_v2(election_type.as_i8(), topics, timeout_ms)
                    .await?,
            ),
        };

        if result.error_code != 0 {
            self.config.record_broker_error();
        }
        for topic in &result.topics {
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
        }
        Ok(result)
    }

    /// Starts or cancels partition reassignments on the active controller.
    ///
    /// A partition with `Some` replicas is reassigned to that broker order. A
    /// partition with `None` replicas cancels its pending reassignment. Kafka
    /// returns independent outcomes for each submitted partition.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_partition_reassignments",
        skip_all,
        fields(topic_count = reassignments.len()),
        err
    )]
    pub async fn alter_partition_reassignments(
        &self,
        reassignments: &[PartitionReassignment],
        options: PartitionReassignmentOptions,
    ) -> Result<AlterPartitionReassignmentsResult> {
        let mut controller_client = self.controller_client_with_retries().await?;
        let response = controller_client
            .alter_partition_reassignments_v0(
                duration_millis_i32(options.timeout),
                reassignments
                    .iter()
                    .map(PartitionReassignment::as_protocol)
                    .collect(),
            )
            .await?;

        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        for topic in &response.responses {
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
        }

        Ok(AlterPartitionReassignmentsResult::from_protocol(response))
    }

    /// Lists partition reassignments still in progress on the active controller.
    ///
    /// Pass `None` to ask Kafka for every ongoing reassignment, or pass topic
    /// and partition filters to limit the response.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.list_partition_reassignments",
        skip_all,
        fields(topic_filter_count = topics.map_or(0, <[PartitionReassignmentQuery]>::len)),
        err
    )]
    pub async fn list_partition_reassignments(
        &self,
        topics: Option<&[PartitionReassignmentQuery]>,
        options: PartitionReassignmentOptions,
    ) -> Result<ListPartitionReassignmentsResult> {
        let request_topics = topics.map(|topics| {
            topics
                .iter()
                .map(PartitionReassignmentQuery::as_protocol)
                .collect::<Vec<ListPartitionReassignmentsTopicV0>>()
        });
        let mut retry = 0;
        let response = loop {
            let mut controller_client = match self.controller_client().await {
                Ok(client) => client,
                Err(error)
                    if retry < self.max_retries
                        && is_retryable_admin_controller_read_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match controller_client
                .list_partition_reassignments_v0(
                    duration_millis_i32(options.timeout),
                    request_topics.clone(),
                )
                .await
            {
                Ok(response)
                    if retry < self.max_retries
                        && is_retryable_admin_read_code(response.error_code) =>
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) => break response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };

        if response.error_code != 0 {
            self.config.record_broker_error();
        }

        Ok(ListPartitionReassignmentsResult::from_protocol(response))
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
        let request_resources = resources
            .iter()
            .map(TopicConfigResource::as_protocol)
            .collect::<Vec<_>>();
        let mut retry = 0;
        let response = loop {
            let mut client = match self.config.clone().connect().await {
                Ok(client) => client,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match client
                .describe_configs_v1(request_resources.clone(), options.include_synonyms)
                .await
            {
                Ok(response)
                    if retry < self.max_retries
                        && response
                            .results
                            .iter()
                            .any(|resource| is_retryable_admin_read_code(resource.error_code)) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) => break response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };

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
        let mut client = self.bootstrap_client_with_retries().await?;
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

    /// Replaces dynamic Kafka topic configuration values using AlterConfigs v1.
    ///
    /// Unlike [`Self::incremental_alter_topic_configs`], this classic API
    /// replaces the complete dynamic configuration map represented by each
    /// resource. A null value removes a dynamic key. Resource-level Kafka
    /// failures remain in [`AlterConfigsResult`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_topic_configs",
        skip_all,
        fields(resource_count = resources.len(), validate_only = options.validate_only),
        err
    )]
    pub async fn alter_topic_configs(
        &self,
        resources: &[TopicConfigUpdate],
        options: AlterConfigsOptions,
    ) -> Result<AlterConfigsResult> {
        let mut client = self.bootstrap_client_with_retries().await?;
        let response = client
            .alter_configs_v1(
                resources
                    .iter()
                    .map(TopicConfigUpdate::as_protocol)
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
                .map(AlterConfigResourceResult::from_classic_protocol)
                .collect(),
        })
    }

    /// Describes consumer groups through their active coordinators.
    ///
    /// Each group is routed independently because group IDs can hash to
    /// different coordinators.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_consumer_groups",
        skip_all,
        fields(group_count = group_ids.len()),
        err
    )]
    pub async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
    ) -> Result<Vec<ConsumerGroupDescription>> {
        let mut descriptions = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let mut retry = 0;
            let response = loop {
                let mut coordinator = self.group_coordinator_client(group_id).await?;
                match coordinator.describe_groups_v1(vec![group_id.clone()]).await {
                    Ok(response) => {
                        let retryable = response.groups.iter().any(|group| {
                            group.group_id == *group_id
                                && is_retryable_admin_coordinator_code(group.error_code)
                        });
                        if retry < self.max_retries && retryable {
                            self.config.record_broker_error();
                            retry += 1;
                            self.config.record_retry();
                            tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        } else {
                            break response;
                        }
                    }
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    }
                    Err(error) => return Err(error),
                }
            };
            let throttle_time =
                Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms));
            let group = response
                .groups
                .into_iter()
                .find(|group| group.group_id == *group_id)
                .ok_or_else(|| Error::MissingGroupDescription {
                    group_id: group_id.clone(),
                })?;
            if group.error_code != 0 {
                self.config.record_broker_error();
            }
            descriptions.push(ConsumerGroupDescription::from_protocol(
                group,
                throttle_time,
            ));
        }
        Ok(descriptions)
    }

    /// Lists Kafka groups by querying every broker in the cluster.
    ///
    /// ListGroups is broker-scoped because each group coordinator only reports
    /// the groups it owns. Results are sorted by group ID and deduplicated.
    #[tracing::instrument(level = "debug", name = "kafka.admin.list_groups", skip_all, err)]
    pub async fn list_groups(&self) -> Result<Vec<GroupListing>> {
        let metadata = self.metadata_with_admin_retries(Some(Vec::new())).await?;
        let mut groups = BTreeMap::new();

        for broker in metadata.brokers {
            let endpoint = format!("{}:{}", broker.host, broker.port);
            let mut retry = 0;
            let response = loop {
                let mut client = match self.config.connect_broker(endpoint.clone()).await {
                    Ok(client) => client,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                match client.list_groups_v1().await {
                    Ok(response)
                        if retry < self.max_retries
                            && is_retryable_admin_read_code(response.error_code) =>
                    {
                        self.config.record_broker_error();
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    }
                    Ok(response) => break response,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    }
                    Err(error) => return Err(error),
                }
            };
            if response.error_code != 0 {
                return Err(self.config.broker_error(
                    response.error_code,
                    format!("list groups on broker {}", broker.node_id),
                ));
            }
            let throttle_time =
                Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms));
            for group in response.groups {
                groups.insert(
                    group.group_id.clone(),
                    GroupListing::from_protocol(group, broker.node_id, throttle_time),
                );
            }
        }

        Ok(groups.into_values().collect())
    }

    /// Describes broker-local log directories and replica storage state.
    ///
    /// Pass `None` for `broker_ids` or `topics` to query every advertised
    /// broker or every topic, respectively. A selected topic with an empty
    /// partition list asks Kafka for all of that topic's partitions. Results
    /// remain grouped by broker because log-directory paths and capacity are
    /// local to each broker.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_log_dirs",
        skip_all,
        fields(
            broker_count = broker_ids.map_or(0, <[i32]>::len),
            topic_count = topics.map_or(0, <[LogDirTopic]>::len)
        ),
        err
    )]
    pub async fn describe_log_dirs(
        &self,
        broker_ids: Option<&[i32]>,
        topics: Option<&[LogDirTopic]>,
    ) -> Result<Vec<DescribeLogDirsBrokerResult>> {
        let metadata = self.metadata_with_admin_retries(Some(Vec::new())).await?;
        if let Some(broker_ids) = broker_ids {
            for broker_id in broker_ids {
                if !metadata
                    .brokers
                    .iter()
                    .any(|broker| broker.node_id == *broker_id)
                {
                    return Err(Error::MissingBroker {
                        node_id: *broker_id,
                    });
                }
            }
        }

        let selected_brokers = metadata
            .brokers
            .iter()
            .filter(|broker| broker_ids.map_or(true, |ids| ids.contains(&broker.node_id)))
            .cloned()
            .collect::<Vec<_>>();
        let request_topics = topics.map(|topics| {
            topics
                .iter()
                .map(LogDirTopic::as_protocol)
                .collect::<Vec<DescribeLogDirsTopic>>()
        });
        let mut results = Vec::with_capacity(selected_brokers.len());

        for broker in selected_brokers {
            let endpoint = format!("{}:{}", broker.host, broker.port);
            let mut retry = 0;
            let response = loop {
                let mut client = match self.config.connect_broker(endpoint.clone()).await {
                    Ok(client) => client,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let api_versions = match client
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let Some(version) = api_versions.highest_supported_version(35, 5) else {
                    return Err(Error::Unsupported(
                        "broker does not advertise DescribeLogDirs v1 or newer",
                    ));
                };
                let response = match version {
                    1 => client.describe_log_dirs_v1(request_topics.clone()).await,
                    2 => client.describe_log_dirs_v2(request_topics.clone()).await,
                    3 => client.describe_log_dirs_v3(request_topics.clone()).await,
                    4 => client.describe_log_dirs_v4(request_topics.clone()).await,
                    _ => client.describe_log_dirs_v5(request_topics.clone()).await,
                };
                match response {
                    Ok(response)
                        if retry < self.max_retries
                            && is_retryable_admin_read_code(response.error_code) =>
                    {
                        self.config.record_broker_error();
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    }
                    Ok(response) => break response,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    }
                    Err(error) => return Err(error),
                }
            };

            if response.error_code != 0 {
                self.config.record_broker_error();
            }
            for log_dir in &response.results {
                if log_dir.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
            results.push(DescribeLogDirsBrokerResult::from_protocol(
                broker.node_id,
                response,
            ));
        }

        Ok(results)
    }

    /// Deletes consumer groups through their active coordinators.
    ///
    /// Kafka only deletes groups without active members. Per-group broker
    /// errors remain attached to the returned results.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_consumer_groups",
        skip_all,
        fields(group_count = group_ids.len()),
        err
    )]
    pub async fn delete_consumer_groups(
        &self,
        group_ids: &[String],
    ) -> Result<Vec<DeleteConsumerGroupResult>> {
        let mut results = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let mut retry = 0;
            let response = loop {
                let mut coordinator = self.group_coordinator_client(group_id).await?;
                match coordinator.delete_groups_v1(vec![group_id.clone()]).await {
                    Ok(response) => {
                        let retryable = response.results.iter().any(|result| {
                            result.group_id == *group_id
                                && is_retryable_admin_coordinator_code(result.error_code)
                        });
                        if retry < self.max_retries && retryable {
                            self.config.record_broker_error();
                            retry += 1;
                            self.config.record_retry();
                            tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        } else {
                            break response;
                        }
                    }
                    Err(error) => return Err(error),
                }
            };
            let throttle_time =
                Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms));
            let result = response
                .results
                .into_iter()
                .find(|result| result.group_id == *group_id)
                .ok_or_else(|| Error::MissingDeleteGroupResult {
                    group_id: group_id.clone(),
                })?;
            if result.error_code != 0 {
                self.config.record_broker_error();
            }
            results.push(DeleteConsumerGroupResult::from_protocol(
                result,
                throttle_time,
            ));
        }
        Ok(results)
    }

    /// Deletes committed offsets for selected consumer-group partitions.
    ///
    /// The request is routed to the group's active coordinator. Kafka can
    /// reject the whole request or individual partitions, so both levels are
    /// preserved in [`DeleteConsumerGroupOffsetsResult`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_consumer_group_offsets",
        skip_all,
        fields(group_id, topic_count = topics.len()),
        err
    )]
    pub async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        topics: &[ConsumerGroupOffsetDelete],
    ) -> Result<DeleteConsumerGroupOffsetsResult> {
        let request_topics = topics
            .iter()
            .map(ConsumerGroupOffsetDelete::as_protocol)
            .collect::<Vec<_>>();
        let mut retry = 0;
        let response = loop {
            let mut coordinator = self.group_coordinator_client(group_id).await?;
            match coordinator
                .offset_delete_v0(group_id, request_topics.clone())
                .await
            {
                Ok(response) => {
                    let retryable = is_retryable_admin_coordinator_code(response.error_code)
                        || response.topics.iter().any(|topic| {
                            topic.partitions.iter().any(|partition| {
                                is_retryable_admin_coordinator_code(partition.error_code)
                            })
                        });
                    if retry < self.max_retries && retryable {
                        self.config.record_broker_error();
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    } else {
                        break response;
                    }
                }
                Err(error) => return Err(error),
            }
        };

        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        for topic in &response.topics {
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
        }

        Ok(DeleteConsumerGroupOffsetsResult {
            group_id: group_id.to_owned(),
            error_code: response.error_code,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .topics
                .into_iter()
                .map(DeleteConsumerGroupOffsetsTopicResult::from_protocol)
                .collect(),
        })
    }

    /// Lists committed offsets for a consumer group through OffsetFetch v2.
    ///
    /// Pass `None` to request every topic known to the group, or pass topic
    /// and partition filters to limit the response. This admin form targets
    /// classic group offset semantics; KIP-848 member-aware offset fetch is
    /// exposed through the joined [`ConsumerGroup`](crate::group::ConsumerGroup)
    /// path and remains a separate qualification surface.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.list_consumer_group_offsets",
        skip_all,
        fields(group_id, has_topic_filter = topics.is_some()),
        err
    )]
    pub async fn list_consumer_group_offsets(
        &self,
        group_id: &str,
        topics: Option<&[ConsumerGroupOffsetQuery]>,
    ) -> Result<ListConsumerGroupOffsetsResult> {
        let mut retry = 0;
        let response = loop {
            let mut coordinator = self.group_coordinator_client(group_id).await?;
            match coordinator
                .offset_fetch_v2(
                    group_id,
                    topics.map(|topics| {
                        topics
                            .iter()
                            .map(ConsumerGroupOffsetQuery::as_protocol)
                            .collect()
                    }),
                )
                .await
            {
                Ok(response)
                    if retry < self.max_retries
                        && is_retryable_admin_coordinator_code(response.error_code) =>
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) => break response,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };

        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        for topic in &response.topics {
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
        }

        Ok(ListConsumerGroupOffsetsResult::from_protocol(
            group_id,
            response.error_code,
            response.topics,
        ))
    }

    /// Lists committed offsets through the KIP-848 member-aware OffsetFetch v9 API.
    ///
    /// The member ID may be omitted for an unjoined consumer-protocol request;
    /// joined members should pass the current ID and member epoch from
    /// [`ConsumerGroup::metadata`](crate::ConsumerGroup::metadata). A fresh
    /// metadata snapshot is required after every rejoin. `require_stable`
    /// requests that Kafka wait for unstable transactional offsets.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.list_consumer_group_offsets_with_member",
        skip_all,
        fields(group_id, member_epoch, has_topic_filter = topics.is_some(), require_stable),
        err
    )]
    pub async fn list_consumer_group_offsets_with_member(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        member_epoch: i32,
        topics: Option<&[ConsumerGroupOffsetQuery]>,
        require_stable: bool,
    ) -> Result<ListConsumerGroupOffsetsResult> {
        let request_topics = topics.map(|topics| {
            topics
                .iter()
                .map(ConsumerGroupOffsetQuery::as_protocol_v9)
                .collect::<Vec<_>>()
        });
        let member_id = member_id.map(str::to_owned);
        let mut retry = 0;
        let response = loop {
            let mut coordinator = self.group_coordinator_client(group_id).await?;
            match coordinator
                .offset_fetch_v9_with_require_stable(
                    group_id,
                    member_id.clone(),
                    member_epoch,
                    request_topics.clone(),
                    require_stable,
                )
                .await
            {
                Ok(response)
                    if retry < self.max_retries && is_retryable_offset_fetch_v9(&response) =>
                {
                    record_offset_fetch_v9_errors(&self.config, &response);
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) => break response,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };

        record_offset_fetch_v9_errors(&self.config, &response);
        let throttle_time_ms = response.throttle_time_ms;
        let group = response
            .groups
            .into_iter()
            .find(|group| group.group_id == group_id)
            .ok_or_else(|| Error::MissingGroupDescription {
                group_id: group_id.to_owned(),
            })?;
        Ok(ListConsumerGroupOffsetsResult::from_protocol_v9(
            group_id,
            throttle_time_ms,
            group,
        ))
    }

    /// Alters committed offsets for a consumer group through OffsetCommit v2.
    ///
    /// This is an administrative commit with generation `-1`, an empty member
    /// ID, and no retention override. Partition-level Kafka errors remain in
    /// the typed result instead of being collapsed into one boolean.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_consumer_group_offsets",
        skip_all,
        fields(group_id, offset_count = offsets.len()),
        err
    )]
    pub async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &[ConsumerGroupOffset],
    ) -> Result<AlterConsumerGroupOffsetsResult> {
        let mut topics = BTreeMap::<String, Vec<OffsetCommitPartition>>::new();
        for offset in offsets {
            topics
                .entry(offset.topic.clone())
                .or_default()
                .push(OffsetCommitPartition {
                    partition_index: offset.partition,
                    committed_offset: offset.offset,
                    committed_metadata: offset.metadata.clone(),
                });
        }
        let topics = topics
            .into_iter()
            .map(|(name, partitions)| OffsetCommitTopic { name, partitions })
            .collect::<Vec<_>>();
        let mut retry = 0;
        let response = loop {
            let mut coordinator = self.group_coordinator_client(group_id).await?;
            match coordinator
                .offset_commit_v2(group_id, -1, "", -1, topics.clone())
                .await
            {
                Ok(response) => {
                    let retryable = response.topics.iter().any(|topic| {
                        topic.partitions.iter().any(|partition| {
                            is_retryable_admin_coordinator_code(partition.error_code)
                        })
                    });
                    if retry < self.max_retries && retryable {
                        for topic in &response.topics {
                            for partition in &topic.partitions {
                                if partition.error_code != 0 {
                                    self.config.record_broker_error();
                                }
                            }
                        }
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    } else {
                        break response;
                    }
                }
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };

        for topic in &response.topics {
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
        }

        Ok(AlterConsumerGroupOffsetsResult::from_protocol(
            group_id,
            response.topics,
        ))
    }

    /// Alters committed offsets through the KIP-848 member-aware
    /// OffsetCommit v9 API.
    ///
    /// `member_id`, `member_epoch`, and `group_instance_id` must describe the
    /// current joined member when the broker enforces consumer-protocol
    /// membership. The exact offset and metadata values are repeated on a
    /// transient coordinator failure; a stale member epoch is returned rather
    /// than retried with an identity that may no longer be valid.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_consumer_group_offsets_with_member",
        skip_all,
        fields(group_id, member_epoch, offset_count = offsets.len()),
        err
    )]
    pub async fn alter_consumer_group_offsets_with_member(
        &self,
        group_id: &str,
        member_id: &str,
        member_epoch: i32,
        group_instance_id: Option<&str>,
        offsets: &[ConsumerGroupOffset],
    ) -> Result<AlterConsumerGroupOffsetsResult> {
        let mut topics = BTreeMap::<String, Vec<OffsetCommitPartitionV9>>::new();
        for offset in offsets {
            topics
                .entry(offset.topic.clone())
                .or_default()
                .push(OffsetCommitPartitionV9 {
                    partition_index: offset.partition,
                    committed_offset: offset.offset,
                    committed_leader_epoch: offset.leader_epoch,
                    committed_metadata: offset.metadata.clone(),
                });
        }
        let topics = topics
            .into_iter()
            .map(|(name, partitions)| OffsetCommitTopicV9 { name, partitions })
            .collect::<Vec<_>>();
        let group_instance_id = group_instance_id.map(str::to_owned);
        let mut retry = 0;
        let response = loop {
            let mut coordinator = self.group_coordinator_client(group_id).await?;
            match coordinator
                .offset_commit_v9(
                    group_id,
                    member_epoch,
                    member_id,
                    group_instance_id.clone(),
                    topics.clone(),
                )
                .await
            {
                Ok(response) => {
                    let retryable = response.topics.iter().any(|topic| {
                        topic.partitions.iter().any(|partition| {
                            is_retryable_admin_coordinator_code(partition.error_code)
                        })
                    });
                    if retry < self.max_retries && retryable {
                        record_offset_commit_v9_errors(&self.config, &response);
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    } else {
                        break response;
                    }
                }
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        };

        record_offset_commit_v9_errors(&self.config, &response);
        Ok(AlterConsumerGroupOffsetsResult::from_protocol_v9(
            group_id,
            response.throttle_time_ms,
            response.topics,
        ))
    }

    async fn group_coordinator_client(&self, group_id: &str) -> Result<Client> {
        let mut retry = 0;
        loop {
            match self.group_coordinator_client_once(group_id).await {
                Ok(client) => return Ok(client),
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn group_coordinator_client_once(&self, group_id: &str) -> Result<Client> {
        let mut bootstrap = self.config.clone().connect().await?;
        let coordinator = bootstrap.find_group_coordinator(group_id).await?;
        if coordinator.error_code != 0 {
            self.config.record_broker_error();
            return Err(Error::Broker {
                code: coordinator.error_code,
                context: format!("find coordinator for consumer group {group_id}"),
            });
        }
        self.config
            .connect_broker(format!("{}:{}", coordinator.host, coordinator.port))
            .await
    }

    async fn controller_client(&self) -> Result<Client> {
        let metadata = self.metadata_with_admin_retries(Some(Vec::new())).await?;
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

    // Discovery is safe to retry because no controller-scoped request has been
    // transmitted yet. The write itself remains single-attempt to avoid
    // duplicating an ambiguous broker-side mutation.
    async fn controller_client_with_retries(&self) -> Result<Client> {
        let mut retry = 0;
        loop {
            match self.controller_client().await {
                Ok(client) => return Ok(client),
                Err(error)
                    if retry < self.max_retries
                        && is_retryable_admin_controller_read_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    // Bootstrap connection retries are safe because no admin request has been
    // transmitted yet. Mutation requests remain single-attempt below so an
    // ambiguous transport failure cannot duplicate a broker-side change.
    async fn bootstrap_client_with_retries(&self) -> Result<Client> {
        retry_admin_connection(
            self.max_retries,
            || self.config.clone().connect(),
            || self.config.record_retry(),
        )
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
        let mut controller_client = self.controller_client_with_retries().await?;
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

    /// Increases partition counts on the active controller using CreatePartitions v0.
    ///
    /// The requested count is the new total partition count, not the number of
    /// partitions to add. Per-topic broker failures remain in
    /// [`CreatePartitionsResult`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.create_partitions",
        skip_all,
        fields(topic_count = topics.len(), validate_only = options.validate_only),
        err
    )]
    pub async fn create_partitions(
        &self,
        topics: &[NewPartitions],
        options: CreatePartitionsOptions,
    ) -> Result<CreatePartitionsResult> {
        let mut controller_client = self.controller_client_with_retries().await?;
        let response = controller_client
            .create_partitions_v0(
                topics.iter().map(NewPartitions::as_protocol).collect(),
                duration_millis_i32(options.timeout),
                options.validate_only,
            )
            .await?;

        for topic in &response.results {
            if topic.error_code != 0 {
                self.config.record_broker_error();
            }
        }

        Ok(CreatePartitionsResult {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .results
                .into_iter()
                .map(CreatePartitionsTopicResult::from_protocol)
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
        let mut controller_client = self.controller_client_with_retries().await?;
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

    /// Deletes records before the requested offsets using DeleteRecords v1.
    ///
    /// Kafka routes the request to the partition leaders. The broker response
    /// retains each partition's resulting low watermark and independent error
    /// code, so a partial deletion is observable without flattening it into a
    /// single operation error.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_records",
        skip_all,
        fields(topic_count = topics.len()),
        err
    )]
    pub async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
        options: DeleteRecordsOptions,
    ) -> Result<DeleteRecordsResult> {
        // DeleteRecords is idempotent for a fixed topic/partition/offset, so a
        // dropped request or a moved leader can safely restart the full route.
        let mut retry = 0;
        let responses = 'attempt: loop {
            let metadata = self
                .metadata_with_admin_retries_from(
                    Some(topics.iter().map(|topic| topic.name.clone()).collect()),
                    &mut retry,
                )
                .await?;
            let requests = match delete_records_requests(&metadata, topics) {
                Ok(requests) => requests,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_leader_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue 'attempt;
                }
                Err(error) => return Err(error),
            };
            let mut responses = Vec::with_capacity(requests.len());
            for (broker_addr, request_topics) in requests {
                let mut client = match self.config.connect_broker(broker_addr).await {
                    Ok(client) => client,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_leader_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                match client
                    .delete_records_v1(request_topics, duration_millis_i32(options.timeout))
                    .await
                {
                    Ok(response) => responses.push(response),
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_leader_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                }
            }

            let retryable = responses.iter().any(|response| {
                response.topics.iter().any(|topic| {
                    topic
                        .partitions
                        .iter()
                        .any(|partition| is_retryable_admin_leader_code(partition.error_code))
                })
            });
            if retry < self.max_retries && retryable {
                for response in &responses {
                    for topic in &response.topics {
                        for partition in &topic.partitions {
                            if partition.error_code != 0 {
                                self.config.record_broker_error();
                            }
                        }
                    }
                }
                retry += 1;
                self.config.record_retry();
                tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                continue 'attempt;
            }
            break responses;
        };

        let mut returned_partitions = 0usize;
        for response in &responses {
            for topic in &response.topics {
                for partition in &topic.partitions {
                    returned_partitions += 1;
                    if partition.error_code != 0 {
                        self.config.record_broker_error();
                    }
                }
            }
        }
        let requested_partitions = topics.iter().map(|topic| topic.partitions.len()).sum();
        if returned_partitions != requested_partitions {
            return Err(Error::ResponseCountMismatch {
                operation: "DeleteRecords",
                expected: requested_partitions,
                actual: returned_partitions,
            });
        }

        Ok(DeleteRecordsResult::from_protocol_responses(responses))
    }

    /// Describes active producers for selected topic partitions.
    ///
    /// Metadata is resolved first and requests are grouped by current
    /// partition leader. Transient leader movement, broker transport, and
    /// request-timeout failures are retried through fresh metadata while
    /// per-partition broker errors and producer sequence state remain
    /// available in the typed result.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_producers",
        skip_all,
        fields(topic_count = topics.len()),
        err
    )]
    pub async fn describe_producers(
        &self,
        topics: &[DescribeProducersTopic],
    ) -> Result<DescribeProducersResult> {
        let mut retry = 0;
        let responses = 'attempt: loop {
            let metadata = self
                .metadata_with_admin_retries_from(
                    Some(topics.iter().map(|topic| topic.name.clone()).collect()),
                    &mut retry,
                )
                .await?;
            let requests = match describe_producers_requests(&metadata, topics) {
                Ok(requests) => requests,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_leader_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue 'attempt;
                }
                Err(error) => return Err(error),
            };
            let mut responses = Vec::with_capacity(requests.len());
            for (broker_addr, request_topics) in requests {
                let mut client = match self.config.connect_broker(broker_addr).await {
                    Ok(client) => client,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_leader_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                match client.describe_producers_v0(request_topics).await {
                    Ok(response) => responses.push(response),
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_leader_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                }
            }

            let retryable = responses.iter().any(|response| {
                response.topics.iter().any(|topic| {
                    topic
                        .partitions
                        .iter()
                        .any(|partition| is_retryable_admin_leader_code(partition.error_code))
                })
            });
            if retry < self.max_retries && retryable {
                for response in &responses {
                    for topic in &response.topics {
                        for partition in &topic.partitions {
                            if partition.error_code != 0 {
                                self.config.record_broker_error();
                            }
                        }
                    }
                }
                retry += 1;
                self.config.record_retry();
                tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                continue 'attempt;
            }
            break responses;
        };

        let requested_partitions = topics.iter().map(|topic| topic.partitions.len()).sum();
        let returned_partitions = responses
            .iter()
            .flat_map(|response| &response.topics)
            .map(|topic| topic.partitions.len())
            .sum();
        if returned_partitions != requested_partitions {
            return Err(Error::ResponseCountMismatch {
                operation: "DescribeProducers",
                expected: requested_partitions,
                actual: returned_partitions,
            });
        }

        for response in &responses {
            for topic in &response.topics {
                for partition in &topic.partitions {
                    if partition.error_code != 0 {
                        self.config.record_broker_error();
                    }
                }
            }
        }
        Ok(DescribeProducersResult::from_protocol_responses(responses))
    }

    /// Describes transactional IDs through their active transaction
    /// coordinators. IDs are grouped by coordinator so a request never goes
    /// to a broker that does not own the transaction state. Coordinator
    /// movement, transport failures, and transient coordinator responses are
    /// retried through fresh discovery.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_transactions",
        skip_all,
        fields(transactional_id_count = transactional_ids.len()),
        err
    )]
    pub async fn describe_transactions(
        &self,
        transactional_ids: &[String],
    ) -> Result<DescribeTransactionsResult> {
        let mut retry = 0;
        let responses = 'attempt: loop {
            let mut coordinator_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
            let mut bootstrap = match self.config.clone().connect().await {
                Ok(client) => client,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue 'attempt;
                }
                Err(error) => return Err(error),
            };
            for transactional_id in transactional_ids {
                let coordinator = match bootstrap
                    .find_transaction_coordinator(transactional_id)
                    .await
                {
                    Ok(coordinator) => coordinator,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                if coordinator.error_code != 0 {
                    self.config.record_broker_error();
                    if retry < self.max_retries
                        && is_retryable_admin_coordinator_code(coordinator.error_code)
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    return Err(Error::Broker {
                        code: coordinator.error_code,
                        context: format!(
                            "find coordinator for transactional ID {transactional_id}"
                        ),
                    });
                }
                coordinator_ids
                    .entry(format!("{}:{}", coordinator.host, coordinator.port))
                    .or_default()
                    .push(transactional_id.clone());
            }

            let mut responses = Vec::with_capacity(coordinator_ids.len());
            for (broker_addr, request_ids) in coordinator_ids {
                let mut client = match self.config.connect_broker(broker_addr).await {
                    Ok(client) => client,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                match client.describe_transactions_v0(request_ids).await {
                    Ok(response) => responses.push(response),
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                }
            }

            let retryable = responses.iter().any(|response| {
                response
                    .transaction_states
                    .iter()
                    .any(|state| is_retryable_admin_coordinator_code(state.error_code))
            });
            if retry < self.max_retries && retryable {
                for response in &responses {
                    for state in &response.transaction_states {
                        if state.error_code != 0 {
                            self.config.record_broker_error();
                        }
                    }
                }
                retry += 1;
                self.config.record_retry();
                tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                continue 'attempt;
            }
            break responses;
        };

        for response in &responses {
            for state in &response.transaction_states {
                if state.error_code != 0 {
                    self.config.record_broker_error();
                }
            }
        }
        Ok(DescribeTransactionsResult::from_protocol_responses(
            responses,
        ))
    }

    /// Lists active transactions across all transaction-coordinator shards.
    ///
    /// Kafka stores transaction state in partitions spread across the cluster,
    /// so this operation queries every broker returned by metadata and
    /// aggregates the broker-local results. The duration filter uses
    /// ListTransactions v1 when the broker advertises it; brokers limited to
    /// v0 still support state and producer-ID filters.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.list_transactions",
        skip_all,
        fields(
            state_filter_count = options.state_filters.len(),
            producer_id_filter_count = options.producer_id_filters.len(),
            has_duration_filter = options.duration_filter.is_some()
        ),
        err
    )]
    pub async fn list_transactions(
        &self,
        options: ListTransactionsOptions,
    ) -> Result<ListTransactionsResult> {
        let mut retry = 0;
        let responses = 'attempt: loop {
            let metadata = self.metadata_with_admin_retries(None).await?;
            let mut responses = Vec::with_capacity(metadata.brokers.len());
            for broker in metadata.brokers {
                let broker_addr = format!("{}:{}", broker.host, broker.port);
                let mut client = match self.config.connect_broker(broker_addr).await {
                    Ok(client) => client,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                let api_versions = match client
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(response) => response,
                    Err(error)
                        if retry < self.max_retries && is_retryable_admin_read_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                let Some(version) = api_versions.highest_supported_version(66, 1) else {
                    return Err(Error::Unsupported(
                        "ListTransactions API is not advertised by broker",
                    ));
                };
                let response = match options.duration_filter {
                    Some(duration) if version >= 1 => {
                        client
                            .list_transactions_v1(
                                options.state_filters.clone(),
                                options.producer_id_filters.clone(),
                                duration_millis_i64(duration),
                            )
                            .await
                    }
                    Some(_) => {
                        return Err(Error::Unsupported(
                            "ListTransactions duration filters require API v1",
                        ));
                    }
                    None if version >= 1 => {
                        client
                            .list_transactions_v1(
                                options.state_filters.clone(),
                                options.producer_id_filters.clone(),
                                -1,
                            )
                            .await
                    }
                    None => {
                        client
                            .list_transactions_v0(
                                options.state_filters.clone(),
                                options.producer_id_filters.clone(),
                            )
                            .await
                    }
                };
                let response = match response {
                    Ok(response) => response,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                if retry < self.max_retries
                    && is_retryable_list_transactions_code(response.error_code)
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue 'attempt;
                }
                responses.push(response);
            }
            break responses;
        };

        for response in &responses {
            if response.error_code != 0 {
                self.config.record_broker_error();
            }
        }
        Ok(ListTransactionsResult::from_protocol_responses(responses))
    }
}

/// Kafka ACL resource type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclResourceType {
    /// Unknown resource type.
    Unknown,
    /// Match any resource type in a filter.
    Any,
    /// Topic resource.
    Topic,
    /// Consumer group resource.
    Group,
    /// Cluster resource.
    Cluster,
    /// Transactional ID resource.
    TransactionalId,
    /// Delegation token resource.
    DelegationToken,
    /// User resource.
    User,
    /// Resource type code introduced by a newer broker.
    Other(i8),
}

impl AclResourceType {
    fn code(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::Topic => 2,
            Self::Group => 3,
            Self::Cluster => 4,
            Self::TransactionalId => 5,
            Self::DelegationToken => 6,
            Self::User => 7,
            Self::Other(code) => code,
        }
    }

    fn from_code(code: i8) -> Self {
        match code {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::Topic,
            3 => Self::Group,
            4 => Self::Cluster,
            5 => Self::TransactionalId,
            6 => Self::DelegationToken,
            7 => Self::User,
            code => Self::Other(code),
        }
    }
}

/// Kafka ACL resource pattern type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPatternType {
    /// Unknown pattern type.
    Unknown,
    /// Match any pattern type in a filter.
    Any,
    /// Match literal resource names.
    Literal,
    /// Match resource-name prefixes.
    Prefixed,
    /// Match literal or prefixed patterns in a filter.
    Match,
    /// Pattern type code introduced by a newer broker.
    Other(i8),
}

impl AclPatternType {
    fn code(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::Match => 2,
            Self::Literal => 3,
            Self::Prefixed => 4,
            Self::Other(code) => code,
        }
    }

    fn from_code(code: i8) -> Self {
        match code {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::Match,
            3 => Self::Literal,
            4 => Self::Prefixed,
            code => Self::Other(code),
        }
    }
}

/// Kafka ACL operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclOperation {
    /// Unknown operation.
    Unknown,
    /// Match any operation in a filter.
    Any,
    /// All operations.
    All,
    /// Read operation.
    Read,
    /// Write operation.
    Write,
    /// Create operation.
    Create,
    /// Delete operation.
    Delete,
    /// Alter operation.
    Alter,
    /// Describe operation.
    Describe,
    /// Cluster action operation.
    ClusterAction,
    /// Describe-configs operation.
    DescribeConfigs,
    /// Alter-configs operation.
    AlterConfigs,
    /// Idempotent-write operation.
    IdempotentWrite,
    /// Operation code introduced by a newer broker.
    Other(i8),
}

impl AclOperation {
    fn code(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::All => 2,
            Self::Read => 3,
            Self::Write => 4,
            Self::Create => 5,
            Self::Delete => 6,
            Self::Alter => 7,
            Self::Describe => 8,
            Self::ClusterAction => 9,
            Self::DescribeConfigs => 10,
            Self::AlterConfigs => 11,
            Self::IdempotentWrite => 12,
            Self::Other(code) => code,
        }
    }

    fn from_code(code: i8) -> Self {
        match code {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::All,
            3 => Self::Read,
            4 => Self::Write,
            5 => Self::Create,
            6 => Self::Delete,
            7 => Self::Alter,
            8 => Self::Describe,
            9 => Self::ClusterAction,
            10 => Self::DescribeConfigs,
            11 => Self::AlterConfigs,
            12 => Self::IdempotentWrite,
            code => Self::Other(code),
        }
    }
}

/// Kafka ACL permission type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPermissionType {
    /// Unknown permission type.
    Unknown,
    /// Match any permission type in a filter.
    Any,
    /// Deny the operation.
    Deny,
    /// Allow the operation.
    Allow,
    /// Permission code introduced by a newer broker.
    Other(i8),
}

impl AclPermissionType {
    fn code(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Any => 1,
            Self::Deny => 2,
            Self::Allow => 3,
            Self::Other(code) => code,
        }
    }

    fn from_code(code: i8) -> Self {
        match code {
            0 => Self::Unknown,
            1 => Self::Any,
            2 => Self::Deny,
            3 => Self::Allow,
            code => Self::Other(code),
        }
    }
}

/// One concrete Kafka ACL binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBinding {
    resource_type: AclResourceType,
    resource_name: String,
    pattern_type: AclPatternType,
    principal: String,
    host: String,
    operation: AclOperation,
    permission_type: AclPermissionType,
}

impl AclBinding {
    /// Creates an ACL binding for one Kafka resource and principal.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        resource_type: AclResourceType,
        resource_name: impl Into<String>,
        pattern_type: AclPatternType,
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type,
            principal: principal.into(),
            host: host.into(),
            operation,
            permission_type,
        }
    }

    /// Returns the ACL resource type.
    pub fn resource_type(&self) -> AclResourceType {
        self.resource_type
    }

    /// Returns the resource name.
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Returns the resource pattern type.
    pub fn pattern_type(&self) -> AclPatternType {
        self.pattern_type
    }

    /// Returns the principal expression.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Returns the host expression.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the allowed or denied operation.
    pub fn operation(&self) -> AclOperation {
        self.operation
    }

    /// Returns whether this binding allows or denies the operation.
    pub fn permission_type(&self) -> AclPermissionType {
        self.permission_type
    }

    fn as_protocol(&self) -> CreateAclsCreationV1 {
        CreateAclsCreationV1 {
            resource_type: self.resource_type.code(),
            resource_name: self.resource_name.clone(),
            resource_pattern_type: self.pattern_type.code(),
            principal: self.principal.clone(),
            host: self.host.clone(),
            operation: self.operation.code(),
            permission_type: self.permission_type.code(),
        }
    }

    fn from_protocol(
        resource_type: i8,
        resource_name: String,
        pattern_type: i8,
        acl: DescribeAclsEntryV1,
    ) -> Self {
        Self::new(
            AclResourceType::from_code(resource_type),
            resource_name,
            AclPatternType::from_code(pattern_type),
            acl.principal,
            acl.host,
            AclOperation::from_code(acl.operation),
            AclPermissionType::from_code(acl.permission_type),
        )
    }
}

/// Filter used by Kafka ACL describe and delete operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclFilter {
    resource_type: AclResourceType,
    resource_name: Option<String>,
    pattern_type: AclPatternType,
    principal: Option<String>,
    host: Option<String>,
    operation: AclOperation,
    permission_type: AclPermissionType,
}

impl Default for AclFilter {
    fn default() -> Self {
        Self {
            resource_type: AclResourceType::Any,
            resource_name: None,
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }
}

impl AclFilter {
    /// Creates a filter that matches every ACL.
    pub fn any() -> Self {
        Self::default()
    }

    /// Sets the resource type to match.
    pub fn resource_type(mut self, resource_type: AclResourceType) -> Self {
        self.resource_type = resource_type;
        self
    }

    /// Sets an optional exact resource name to match.
    pub fn resource_name(mut self, resource_name: impl Into<String>) -> Self {
        self.resource_name = Some(resource_name.into());
        self
    }

    /// Sets the resource pattern type to match.
    pub fn pattern_type(mut self, pattern_type: AclPatternType) -> Self {
        self.pattern_type = pattern_type;
        self
    }

    /// Sets an optional principal to match.
    pub fn principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Sets an optional host to match.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Sets the operation to match.
    pub fn operation(mut self, operation: AclOperation) -> Self {
        self.operation = operation;
        self
    }

    /// Sets the permission type to match.
    pub fn permission_type(mut self, permission_type: AclPermissionType) -> Self {
        self.permission_type = permission_type;
        self
    }

    fn as_protocol(&self) -> DeleteAclsFilterV1 {
        DeleteAclsFilterV1 {
            resource_type_filter: self.resource_type.code(),
            resource_name_filter: self.resource_name.clone(),
            pattern_type_filter: self.pattern_type.code(),
            principal_filter: self.principal.clone(),
            host_filter: self.host.clone(),
            operation: self.operation.code(),
            permission_type: self.permission_type.code(),
        }
    }
}

/// Result returned by [`AdminClient::describe_acls`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeAclsResult {
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
    bindings: Vec<AclBinding>,
}

impl DescribeAclsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the top-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the top-level Kafka error message, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the broker error classification when the request failed.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns whether the broker accepted the request.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns the ACL bindings returned by Kafka.
    pub fn bindings(&self) -> &[AclBinding] {
        &self.bindings
    }
}

impl DescribeAclsResult {
    fn from_protocol(response: DescribeAclsResponseV1) -> Self {
        let bindings = response
            .resources
            .into_iter()
            .flat_map(|resource| {
                resource.acls.into_iter().map(move |acl| {
                    AclBinding::from_protocol(
                        resource.resource_type,
                        resource.resource_name.clone(),
                        resource.pattern_type,
                        acl,
                    )
                })
            })
            .collect();
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
            bindings,
        }
    }
}

/// Per-binding result returned by [`AdminClient::create_acls`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsEntryResult {
    binding: AclBinding,
    error_code: i16,
    error_message: Option<String>,
}

impl CreateAclsEntryResult {
    /// Returns the ACL binding submitted for this result.
    pub fn binding(&self) -> &AclBinding {
        &self.binding
    }

    /// Returns the Kafka result error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the Kafka result error message, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka created this ACL successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns the broker error classification when this creation failed.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }
}

/// Result returned by [`AdminClient::create_acls`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAclsResult {
    throttle_time: Duration,
    results: Vec<CreateAclsEntryResult>,
}

impl CreateAclsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the per-binding creation results.
    pub fn results(&self) -> &[CreateAclsEntryResult] {
        &self.results
    }

    /// Returns whether every ACL creation succeeded.
    pub fn is_success(&self) -> bool {
        self.results.iter().all(CreateAclsEntryResult::is_success)
    }

    /// Returns whether any ACL creation failed.
    pub fn has_errors(&self) -> bool {
        !self.is_success()
    }
}

impl CreateAclsResult {
    fn from_protocol(
        response: kafrust_protocol::api::create_acls::CreateAclsResponseV1,
        bindings: &[AclBinding],
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            results: response
                .results
                .into_iter()
                .zip(bindings.iter().cloned())
                .map(|(result, binding)| CreateAclsEntryResult {
                    binding,
                    error_code: result.error_code,
                    error_message: result.error_message,
                })
                .collect(),
        }
    }
}

/// Per-filter result returned by [`AdminClient::delete_acls`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsFilterResult {
    filter: AclFilter,
    error_code: i16,
    error_message: Option<String>,
    matching_acls: Vec<DeletedAclResult>,
}

impl DeleteAclsFilterResult {
    /// Returns the filter submitted for this result.
    pub fn filter(&self) -> &AclFilter {
        &self.filter
    }

    /// Returns the Kafka filter error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the Kafka filter error message, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the ACLs matched by this filter.
    pub fn matching_acls(&self) -> &[DeletedAclResult] {
        &self.matching_acls
    }

    /// Returns whether the filter or one of its matching ACL deletions failed.
    pub fn has_errors(&self) -> bool {
        self.error_code != 0 || self.matching_acls.iter().any(|acl| !acl.is_success())
    }
}

/// Per-ACL deletion result nested under a delete filter result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedAclResult {
    binding: AclBinding,
    error_code: i16,
    error_message: Option<String>,
}

impl DeletedAclResult {
    /// Returns the ACL Kafka matched.
    pub fn binding(&self) -> &AclBinding {
        &self.binding
    }

    /// Returns the Kafka deletion error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the Kafka deletion error message, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka deleted this ACL successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }
}

/// Result returned by [`AdminClient::delete_acls`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAclsResult {
    throttle_time: Duration,
    filter_results: Vec<DeleteAclsFilterResult>,
}

impl DeleteAclsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the per-filter deletion results.
    pub fn filter_results(&self) -> &[DeleteAclsFilterResult] {
        &self.filter_results
    }

    /// Returns whether every filter and matching ACL deletion succeeded.
    pub fn is_success(&self) -> bool {
        self.filter_results
            .iter()
            .all(|result| !result.has_errors())
    }

    /// Returns whether any filter or matching ACL deletion failed.
    pub fn has_errors(&self) -> bool {
        !self.is_success()
    }
}

impl DeleteAclsResult {
    fn from_protocol(
        response: kafrust_protocol::api::delete_acls::DeleteAclsResponseV1,
        filters: &[AclFilter],
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            filter_results: response
                .filter_results
                .into_iter()
                .zip(filters.iter().cloned())
                .map(|(result, filter)| DeleteAclsFilterResult::from_protocol(result, filter))
                .collect(),
        }
    }
}

impl DeleteAclsFilterResult {
    fn from_protocol(result: DeleteAclsFilterResultV1, filter: AclFilter) -> Self {
        Self {
            filter,
            error_code: result.error_code,
            error_message: result.error_message,
            matching_acls: result
                .matching_acls
                .into_iter()
                .map(|acl| {
                    let DeleteAclsMatchingAclV1 {
                        error_code,
                        error_message,
                        resource_type,
                        resource_name,
                        pattern_type,
                        principal,
                        host,
                        operation,
                        permission_type,
                    } = acl;
                    DeletedAclResult {
                        binding: AclBinding::new(
                            AclResourceType::from_code(resource_type),
                            resource_name,
                            AclPatternType::from_code(pattern_type),
                            principal,
                            host,
                            AclOperation::from_code(operation),
                            AclPermissionType::from_code(permission_type),
                        ),
                        error_code,
                        error_message,
                    }
                })
                .collect(),
        }
    }
}

/// How a client quota filter matches an entity name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientQuotaMatchType {
    /// Match one exact entity name.
    Exact,
    /// Match the broker's default entity.
    Default,
    /// Match any explicitly named entity.
    Any,
    /// Preserve a future Kafka match type.
    Other(i8),
}

impl ClientQuotaMatchType {
    fn code(self) -> i8 {
        match self {
            Self::Exact => 0,
            Self::Default => 1,
            Self::Any => 2,
            Self::Other(code) => code,
        }
    }
}

/// One typed entity component in a client quota entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntityComponent {
    entity_type: String,
    entity_name: Option<String>,
}

impl ClientQuotaEntityComponent {
    /// Creates an entity component, using `None` for Kafka's default entity.
    pub fn new(entity_type: impl Into<String>, entity_name: Option<impl Into<String>>) -> Self {
        Self {
            entity_type: entity_type.into(),
            entity_name: entity_name.map(Into::into),
        }
    }

    /// Returns the Kafka entity type, such as `user` or `client-id`.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the optional entity name.
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }
}

/// A compound Kafka client quota entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaEntity {
    components: Vec<ClientQuotaEntityComponent>,
}

impl ClientQuotaEntity {
    /// Creates a quota entity from its components.
    pub fn new(components: impl IntoIterator<Item = ClientQuotaEntityComponent>) -> Self {
        Self {
            components: components.into_iter().collect(),
        }
    }

    /// Creates a quota entity for one Kafka user.
    pub fn user(name: impl Into<String>) -> Self {
        Self::new([ClientQuotaEntityComponent::new("user", Some(name))])
    }

    /// Creates a quota entity for one Kafka client ID.
    pub fn client_id(name: impl Into<String>) -> Self {
        Self::new([ClientQuotaEntityComponent::new("client-id", Some(name))])
    }

    /// Returns the entity components.
    pub fn components(&self) -> &[ClientQuotaEntityComponent] {
        &self.components
    }

    fn as_protocol(&self) -> Vec<AlterClientQuotasEntityV0> {
        self.components
            .iter()
            .map(|component| AlterClientQuotasEntityV0 {
                entity_type: component.entity_type.clone(),
                entity_name: component.entity_name.clone(),
            })
            .collect()
    }

    fn from_protocol(components: Vec<DescribeClientQuotasEntityV0>) -> Self {
        Self::new(components.into_iter().map(|component| {
            ClientQuotaEntityComponent::new(component.entity_type, component.entity_name)
        }))
    }

    fn from_alter_protocol(components: Vec<AlterClientQuotasEntityV0>) -> Self {
        Self::new(components.into_iter().map(|component| {
            ClientQuotaEntityComponent::new(component.entity_type, component.entity_name)
        }))
    }
}

/// One component in a client quota describe filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaFilterComponent {
    entity_type: String,
    match_type: ClientQuotaMatchType,
    match_value: Option<String>,
}

impl ClientQuotaFilterComponent {
    /// Creates a quota filter component.
    pub fn new(
        entity_type: impl Into<String>,
        match_type: ClientQuotaMatchType,
        match_value: Option<impl Into<String>>,
    ) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_type,
            match_value: match_value.map(Into::into),
        }
    }

    /// Returns the Kafka entity type.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the matching mode.
    pub fn match_type(&self) -> ClientQuotaMatchType {
        self.match_type
    }

    /// Returns the optional exact or named match value.
    pub fn match_value(&self) -> Option<&str> {
        self.match_value.as_deref()
    }

    fn as_protocol(&self) -> DescribeClientQuotasComponentV0 {
        DescribeClientQuotasComponentV0 {
            entity_type: self.entity_type.clone(),
            match_type: self.match_type.code(),
            match_value: self.match_value.clone(),
        }
    }
}

/// Filter used by `AdminClient::describe_client_quotas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuotaFilter {
    components: Vec<ClientQuotaFilterComponent>,
    strict: bool,
}

impl ClientQuotaFilter {
    /// Creates an empty, non-strict filter that describes all quota entities.
    pub fn any() -> Self {
        Self {
            components: Vec::new(),
            strict: false,
        }
    }

    /// Adds one entity component to the filter.
    pub fn component(mut self, component: ClientQuotaFilterComponent) -> Self {
        self.components.push(component);
        self
    }

    /// Sets whether entities with unspecified types are excluded.
    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Returns filter components.
    pub fn components(&self) -> &[ClientQuotaFilterComponent] {
        &self.components
    }

    /// Returns whether the filter is strict.
    pub fn is_strict(&self) -> bool {
        self.strict
    }
}

/// One client quota value alteration.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaOperation {
    key: String,
    value: f64,
    remove: bool,
}

impl ClientQuotaOperation {
    /// Creates a quota value operation.
    pub fn new(key: impl Into<String>, value: f64, remove: bool) -> Self {
        Self {
            key: key.into(),
            value,
            remove,
        }
    }

    /// Creates an operation that sets a quota value.
    pub fn set(key: impl Into<String>, value: f64) -> Self {
        Self::new(key, value, false)
    }

    /// Creates an operation that removes a quota value.
    pub fn remove(key: impl Into<String>) -> Self {
        Self::new(key, 0.0, true)
    }

    /// Returns the quota key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the value sent to Kafka.
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Returns whether this operation removes the value.
    pub fn is_remove(&self) -> bool {
        self.remove
    }

    fn as_protocol(&self) -> AlterClientQuotasOperationV0 {
        AlterClientQuotasOperationV0 {
            key: self.key.clone(),
            value: self.value,
            remove: self.remove,
        }
    }
}

/// One compound client quota alteration.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaAlteration {
    entity: ClientQuotaEntity,
    operations: Vec<ClientQuotaOperation>,
}

impl ClientQuotaAlteration {
    /// Creates an alteration for one quota entity.
    pub fn new(entity: ClientQuotaEntity) -> Self {
        Self {
            entity,
            operations: Vec::new(),
        }
    }

    /// Adds a set operation.
    pub fn set(mut self, key: impl Into<String>, value: f64) -> Self {
        self.operations.push(ClientQuotaOperation::set(key, value));
        self
    }

    /// Adds a remove operation.
    pub fn remove(mut self, key: impl Into<String>) -> Self {
        self.operations.push(ClientQuotaOperation::remove(key));
        self
    }

    /// Returns the target entity.
    pub fn entity(&self) -> &ClientQuotaEntity {
        &self.entity
    }

    /// Returns operations in request order.
    pub fn operations(&self) -> &[ClientQuotaOperation] {
        &self.operations
    }

    fn as_protocol(&self) -> AlterClientQuotasEntryV0 {
        AlterClientQuotasEntryV0 {
            entities: self.entity.as_protocol(),
            operations: self
                .operations
                .iter()
                .map(ClientQuotaOperation::as_protocol)
                .collect(),
        }
    }
}

/// One quota key/value returned by Kafka.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaValue {
    key: String,
    value: f64,
}

impl ClientQuotaValue {
    /// Returns the quota key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the quota value.
    pub fn value(&self) -> f64 {
        self.value
    }
}

/// One entity and its quota values.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQuotaEntry {
    entity: ClientQuotaEntity,
    values: Vec<ClientQuotaValue>,
}

impl ClientQuotaEntry {
    /// Returns the quota entity.
    pub fn entity(&self) -> &ClientQuotaEntity {
        &self.entity
    }

    /// Returns quota values.
    pub fn values(&self) -> &[ClientQuotaValue] {
        &self.values
    }
}

/// Result returned by [`AdminClient::describe_client_quotas`].
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeClientQuotasResult {
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
    entries: Vec<ClientQuotaEntry>,
}

impl DescribeClientQuotasResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the top-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the top-level Kafka error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the broker error classification, when the request failed.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns whether the top-level quota request succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns quota entries.
    pub fn entries(&self) -> &[ClientQuotaEntry] {
        &self.entries
    }

    fn from_protocol(response: DescribeClientQuotasResponseV0) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
            entries: response
                .entries
                .into_iter()
                .map(ClientQuotaEntry::from_protocol)
                .collect(),
        }
    }
}

impl ClientQuotaEntry {
    fn from_protocol(entry: DescribeClientQuotasEntryV0) -> Self {
        Self {
            entity: ClientQuotaEntity::from_protocol(entry.entities),
            values: entry
                .values
                .into_iter()
                .map(|value: DescribeClientQuotasValueV0| ClientQuotaValue {
                    key: value.key,
                    value: value.value,
                })
                .collect(),
        }
    }
}

/// Per-entity result returned by [`AdminClient::alter_client_quotas`].
#[derive(Debug, Clone, PartialEq)]
pub struct AlterClientQuotaEntryResult {
    alteration: ClientQuotaAlteration,
    error_code: i16,
    error_message: Option<String>,
    entity: ClientQuotaEntity,
}

impl AlterClientQuotaEntryResult {
    /// Returns the submitted alteration.
    pub fn alteration(&self) -> &ClientQuotaAlteration {
        &self.alteration
    }

    /// Returns the broker error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the entity echoed by Kafka.
    pub fn entity(&self) -> &ClientQuotaEntity {
        &self.entity
    }

    /// Returns whether this alteration succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }
}

/// Result returned by [`AdminClient::alter_client_quotas`].
#[derive(Debug, Clone, PartialEq)]
pub struct AlterClientQuotasResult {
    throttle_time: Duration,
    entries: Vec<AlterClientQuotaEntryResult>,
}

impl AlterClientQuotasResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-entity alteration outcomes.
    pub fn entries(&self) -> &[AlterClientQuotaEntryResult] {
        &self.entries
    }

    /// Returns whether every alteration succeeded.
    pub fn is_success(&self) -> bool {
        self.entries
            .iter()
            .all(AlterClientQuotaEntryResult::is_success)
    }

    /// Returns whether any alteration failed.
    pub fn has_errors(&self) -> bool {
        !self.is_success()
    }

    fn from_protocol(
        response: kafrust_protocol::api::alter_client_quotas::AlterClientQuotasResponseV0,
        alterations: &[ClientQuotaAlteration],
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            entries: response
                .entries
                .into_iter()
                .zip(alterations.iter().cloned())
                .map(|(result, alteration)| AlterClientQuotaEntryResult {
                    entity: ClientQuotaEntity::from_alter_protocol(result.entities),
                    error_code: result.error_code,
                    error_message: result.error_message,
                    alteration,
                })
                .collect(),
        }
    }
}

/// SCRAM mechanism identifiers accepted by Kafka's credential administration APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramCredentialMechanism {
    /// SCRAM with SHA-256 and a 32-byte salted password.
    Sha256,
    /// SCRAM with SHA-512 and a 64-byte salted password.
    Sha512,
    /// Preserve a mechanism code returned by a newer broker.
    Other(i8),
}

impl ScramCredentialMechanism {
    fn code(self) -> i8 {
        match self {
            Self::Sha256 => 1,
            Self::Sha512 => 2,
            Self::Other(code) => code,
        }
    }

    fn hash(self) -> Option<ScramHash> {
        match self {
            Self::Sha256 => Some(ScramHash::Sha256),
            Self::Sha512 => Some(ScramHash::Sha512),
            Self::Other(_) => None,
        }
    }

    /// Converts Kafka's mechanism code into a typed value.
    pub fn from_code(code: i8) -> Self {
        match code {
            1 => Self::Sha256,
            2 => Self::Sha512,
            code => Self::Other(code),
        }
    }

    /// Returns Kafka's wire-level mechanism code.
    pub fn code_value(self) -> i8 {
        self.code()
    }
}

/// A SCRAM credential deletion request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialDeletion {
    username: String,
    mechanism: ScramCredentialMechanism,
}

impl ScramCredentialDeletion {
    /// Creates a deletion for one user's SCRAM mechanism.
    pub fn new(username: impl Into<String>, mechanism: ScramCredentialMechanism) -> Result<Self> {
        let username = username.into();
        if username.is_empty() {
            return Err(Error::InvalidScramCredential {
                reason: "username must not be empty",
            });
        }
        Ok(Self {
            username,
            mechanism,
        })
    }

    /// Returns the affected Kafka user.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the affected SCRAM mechanism.
    pub fn mechanism(&self) -> ScramCredentialMechanism {
        self.mechanism
    }

    fn as_protocol(&self) -> AlterUserScramCredentialsDeletionV0 {
        AlterUserScramCredentialsDeletionV0 {
            name: self.username.clone(),
            mechanism: self.mechanism.code(),
        }
    }
}

/// A SCRAM credential upsertion request.
///
/// The caller supplies a password only while constructing this value. The
/// value retains the generated salt and PBKDF2 salted password required by
/// Kafka, never the original password.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredentialUpsertion {
    username: String,
    mechanism: ScramCredentialMechanism,
    iterations: u32,
    salt: Vec<u8>,
    salted_password: Vec<u8>,
}

impl fmt::Debug for ScramCredentialUpsertion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramCredentialUpsertion")
            .field("username", &self.username)
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .field("salt_len", &self.salt.len())
            .field("salted_password_len", &self.salted_password.len())
            .finish()
    }
}

impl ScramCredentialUpsertion {
    /// Creates a credential using a cryptographically random 32-byte salt.
    pub fn new(
        username: impl Into<String>,
        mechanism: ScramCredentialMechanism,
        iterations: u32,
        password: impl AsRef<[u8]>,
    ) -> Result<Self> {
        let mut salt = vec![0; 32];
        rand::thread_rng().fill_bytes(&mut salt);
        Self::with_salt(username, mechanism, iterations, password, salt)
    }

    /// Creates a credential with a caller-provided salt for deterministic tests or migration.
    pub fn with_salt(
        username: impl Into<String>,
        mechanism: ScramCredentialMechanism,
        iterations: u32,
        password: impl AsRef<[u8]>,
        salt: impl Into<Vec<u8>>,
    ) -> Result<Self> {
        let username = username.into();
        if username.is_empty() {
            return Err(Error::InvalidScramCredential {
                reason: "username must not be empty",
            });
        }
        if iterations == 0 {
            return Err(Error::InvalidScramCredential {
                reason: "iteration count must be greater than zero",
            });
        }
        let iterations_i32 =
            i32::try_from(iterations).map_err(|_| Error::InvalidScramCredential {
                reason: "iteration count exceeds Kafka's signed 32-bit field",
            })?;
        let salt = salt.into();
        if salt.is_empty() {
            return Err(Error::InvalidScramCredential {
                reason: "salt must not be empty",
            });
        }
        let hash = mechanism.hash().ok_or(Error::InvalidScramCredential {
            reason: "only SCRAM-SHA-256 and SCRAM-SHA-512 can be derived locally",
        })?;
        let salted_password = derive_salted_password(hash, password.as_ref(), &salt, iterations);
        debug_assert!(iterations_i32 > 0);
        Ok(Self {
            username,
            mechanism,
            iterations,
            salt,
            salted_password,
        })
    }

    /// Returns the affected Kafka user.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the SCRAM mechanism.
    pub fn mechanism(&self) -> ScramCredentialMechanism {
        self.mechanism
    }

    /// Returns the PBKDF2 iteration count sent to Kafka.
    pub fn iterations(&self) -> u32 {
        self.iterations
    }

    fn as_protocol(&self) -> AlterUserScramCredentialsUpsertionV0 {
        AlterUserScramCredentialsUpsertionV0 {
            name: self.username.clone(),
            mechanism: self.mechanism.code(),
            iterations: self.iterations as i32,
            salt: self.salt.clone(),
            salted_password: self.salted_password.clone(),
        }
    }
}

/// One SCRAM mechanism and iteration count returned for a Kafka user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialInfo {
    mechanism: ScramCredentialMechanism,
    iterations: i32,
}

impl ScramCredentialInfo {
    /// Returns the mechanism stored by Kafka.
    pub fn mechanism(&self) -> ScramCredentialMechanism {
        self.mechanism
    }

    /// Returns Kafka's reported PBKDF2 iteration count.
    pub fn iterations(&self) -> i32 {
        self.iterations
    }
}

/// Credentials and per-user outcome returned by DescribeUserScramCredentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramUserCredentials {
    username: String,
    error_code: i16,
    error_message: Option<String>,
    credentials: Vec<ScramCredentialInfo>,
}

impl ScramUserCredentials {
    /// Returns the Kafka user name.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the per-user Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the per-user Kafka error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the typed credentials reported for this user.
    pub fn credentials(&self) -> &[ScramCredentialInfo] {
        &self.credentials
    }

    /// Returns whether Kafka accepted this user's describe result.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns the classified per-user broker error, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }
}

/// Result returned by [`AdminClient::describe_user_scram_credentials`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserScramCredentialsResult {
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
    users: Vec<ScramUserCredentials>,
}

impl DescribeUserScramCredentialsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the top-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the top-level Kafka error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the top-level broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns whether the top-level request succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns user-level credential descriptions.
    pub fn users(&self) -> &[ScramUserCredentials] {
        &self.users
    }

    /// Returns whether the request or any user-level result failed.
    pub fn has_errors(&self) -> bool {
        !self.is_success() || self.users.iter().any(|user| !user.is_success())
    }

    fn from_protocol(response: DescribeUserScramCredentialsResponseV0) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
            users: response
                .results
                .into_iter()
                .map(|result| ScramUserCredentials {
                    username: result.user,
                    error_code: result.error_code,
                    error_message: result.error_message,
                    credentials: result
                        .credential_infos
                        .into_iter()
                        .map(|info: ScramCredentialInfoV0| ScramCredentialInfo {
                            mechanism: ScramCredentialMechanism::from_code(info.mechanism),
                            iterations: info.iterations,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// Per-user outcome returned by AlterUserScramCredentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterScramCredentialResult {
    username: String,
    error_code: i16,
    error_message: Option<String>,
}

impl AlterScramCredentialResult {
    /// Returns the affected Kafka user.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the broker error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka accepted this user's changes.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns the classified broker error, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }
}

/// Result returned by [`AdminClient::alter_user_scram_credentials`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserScramCredentialsResult {
    throttle_time: Duration,
    results: Vec<AlterScramCredentialResult>,
}

impl AlterUserScramCredentialsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns one result per affected Kafka user.
    pub fn results(&self) -> &[AlterScramCredentialResult] {
        &self.results
    }

    /// Returns whether every user-level change succeeded.
    pub fn is_success(&self) -> bool {
        self.results
            .iter()
            .all(AlterScramCredentialResult::is_success)
    }

    /// Returns whether any user-level change failed.
    pub fn has_errors(&self) -> bool {
        !self.is_success()
    }

    fn from_protocol(
        response: kafrust_protocol::api::alter_user_scram_credentials::
            AlterUserScramCredentialsResponseV0,
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            results: response
                .results
                .into_iter()
                .map(|result| AlterScramCredentialResult {
                    username: result.user,
                    error_code: result.error_code,
                    error_message: result.error_message,
                })
                .collect(),
        }
    }
}

/// Selects the kind of leader election requested from Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionType {
    /// Move leadership to the preferred replica when possible.
    Preferred,
    /// Elect the first live replica even when it is not in the ISR.
    Unclean,
}

impl ElectionType {
    fn as_i8(self) -> i8 {
        match self {
            Self::Preferred => 0,
            Self::Unclean => 1,
        }
    }
}

/// A topic and explicit partition set submitted to ElectLeaders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderElection {
    topic: String,
    partitions: Vec<i32>,
}

impl LeaderElection {
    /// Creates an empty topic filter. Add at least one partition with
    /// [`Self::partition`] before submitting it.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partitions: Vec::new(),
        }
    }

    /// Adds a partition to this topic filter.
    pub fn partition(mut self, partition_index: i32) -> Self {
        self.partitions.push(partition_index);
        self
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the explicit partition indexes in request order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    fn as_protocol(&self) -> ElectLeadersTopicV0 {
        ElectLeadersTopicV0 {
            name: self.topic.clone(),
            partitions: self.partitions.clone(),
        }
    }
}

/// Options shared by leader election requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectLeadersOptions {
    timeout: Duration,
}

impl ElectLeadersOptions {
    /// Creates options with a 30-second broker request timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the broker-side election timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the configured broker request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.timeout
    }
}

impl Default for ElectLeadersOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }
}

/// Per-partition outcome returned by ElectLeaders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersPartitionResult {
    partition_index: i32,
    error_code: i16,
    error_message: Option<String>,
}

impl ElectLeadersPartitionResult {
    /// Returns the affected partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns Kafka's raw error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's error message, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka accepted this partition election.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }
}

/// Per-topic outcomes returned by ElectLeaders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersTopicResult {
    name: String,
    partitions: Vec<ElectLeadersPartitionResult>,
}

impl ElectLeadersTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns partition outcomes in broker response order.
    pub fn partitions(&self) -> &[ElectLeadersPartitionResult] {
        &self.partitions
    }

    /// Returns whether every returned partition succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(ElectLeadersPartitionResult::is_success)
    }
}

/// Result returned by [`AdminClient::elect_leaders`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectLeadersResult {
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
    topics: Vec<ElectLeadersTopicResult>,
}

impl ElectLeadersResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns Kafka's top-level error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns a top-level error message, when the protocol provides one.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's top-level broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns topic-level partition outcomes.
    pub fn topics(&self) -> &[ElectLeadersTopicResult] {
        &self.topics
    }

    /// Returns whether the request and all returned partition requests succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0 && self.topics.iter().all(ElectLeadersTopicResult::is_success)
    }

    /// Returns whether any top-level or partition-level error was reported.
    pub fn has_errors(&self) -> bool {
        !self.is_success()
    }

    fn from_protocol_v0(response: ElectLeadersResponseV0) -> Self {
        Self::from_protocol_parts(response.throttle_time_ms, 0, None, response.results)
    }

    fn from_protocol_v1(response: ElectLeadersResponseV1) -> Self {
        Self::from_protocol_parts(
            response.throttle_time_ms,
            response.error_code,
            None,
            response.results,
        )
    }

    fn from_protocol_v2(response: ElectLeadersResponseV2) -> Self {
        Self::from_protocol_parts(
            response.throttle_time_ms,
            response.error_code,
            None,
            response.results,
        )
    }

    fn from_protocol_parts(
        throttle_time_ms: i32,
        error_code: i16,
        error_message: Option<String>,
        results: Vec<ElectLeadersTopicResultV0>,
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(throttle_time_ms)),
            error_code,
            error_message,
            topics: results
                .into_iter()
                .map(|topic| ElectLeadersTopicResult {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition| ElectLeadersPartitionResult {
                            partition_index: partition.partition_index,
                            error_code: partition.error_code,
                            error_message: partition.error_message,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// A topic and optional partition filter submitted to DescribeLogDirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirTopic {
    name: String,
    partition_indexes: Vec<i32>,
}

impl LogDirTopic {
    /// Creates a topic filter. An empty partition list means all partitions.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partition_indexes: Vec::new(),
        }
    }

    /// Adds a partition to this topic filter.
    pub fn partition(mut self, partition_index: i32) -> Self {
        self.partition_indexes.push(partition_index);
        self
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the selected partitions. Empty means all partitions.
    pub fn partition_indexes(&self) -> &[i32] {
        &self.partition_indexes
    }

    fn as_protocol(&self) -> DescribeLogDirsTopic {
        DescribeLogDirsTopic {
            name: self.name.clone(),
            partition_indexes: self.partition_indexes.clone(),
        }
    }
}

/// One broker's DescribeLogDirs response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeLogDirsBrokerResult {
    broker_id: i32,
    throttle_time: Duration,
    error_code: i16,
    log_dirs: Vec<LogDirectoryResult>,
    total_bytes: i64,
    usable_bytes: i64,
    is_cordoned: bool,
}

impl DescribeLogDirsBrokerResult {
    /// Returns the broker node ID.
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns Kafka's top-level error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's top-level broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns one result for each broker log directory.
    pub fn log_dirs(&self) -> &[LogDirectoryResult] {
        &self.log_dirs
    }

    /// Returns total bytes on the broker's log volume, or `-1` when the
    /// negotiated broker version does not expose volume capacity.
    pub fn total_bytes(&self) -> i64 {
        self.total_bytes
    }

    /// Returns usable bytes on the broker's log volume, or `-1` when
    /// unavailable.
    pub fn usable_bytes(&self) -> i64 {
        self.usable_bytes
    }

    /// Returns whether the broker has cordoned the log volume.
    pub fn is_cordoned(&self) -> bool {
        self.is_cordoned
    }

    /// Returns whether the broker and every returned log directory succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0 && self.log_dirs.iter().all(LogDirectoryResult::is_success)
    }

    fn from_protocol(broker_id: i32, response: DescribeLogDirsResponse) -> Self {
        let total_bytes = response.total_bytes;
        let usable_bytes = response.usable_bytes;
        let is_cordoned = response.is_cordoned;
        Self {
            broker_id,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            log_dirs: response
                .results
                .into_iter()
                .map(LogDirectoryResult::from_protocol)
                .collect(),
            total_bytes,
            usable_bytes,
            is_cordoned,
        }
    }
}

/// One broker log directory returned by DescribeLogDirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirectoryResult {
    error_code: i16,
    path: String,
    topics: Vec<LogDirectoryTopicResult>,
}

impl LogDirectoryResult {
    /// Returns the log-directory error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the absolute log-directory path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns topic storage results in broker response order.
    pub fn topics(&self) -> &[LogDirectoryTopicResult] {
        &self.topics
    }

    /// Returns whether Kafka accepted this log-directory query.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's log-directory error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(
        result: kafrust_protocol::api::describe_log_dirs::DescribeLogDirsResult,
    ) -> Self {
        Self {
            error_code: result.error_code,
            path: result.log_dir,
            topics: result
                .topics
                .into_iter()
                .map(LogDirectoryTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// Topic storage results returned for one broker log directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirectoryTopicResult {
    name: String,
    partitions: Vec<LogDirectoryPartitionResult>,
}

impl LogDirectoryTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns partition storage results.
    pub fn partitions(&self) -> &[LogDirectoryPartitionResult] {
        &self.partitions
    }

    fn from_protocol(
        result: kafrust_protocol::api::describe_log_dirs::DescribeLogDirsTopicResult,
    ) -> Self {
        Self {
            name: result.name,
            partitions: result
                .partitions
                .into_iter()
                .map(LogDirectoryPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// Partition storage state returned by DescribeLogDirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirectoryPartitionResult {
    partition_index: i32,
    partition_size: i64,
    offset_lag: i64,
    is_future: bool,
}

impl LogDirectoryPartitionResult {
    /// Returns the Kafka partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the partition's log size in bytes.
    pub fn partition_size(&self) -> i64 {
        self.partition_size
    }

    /// Returns the log end offset lag relative to the high watermark/current
    /// replica log end offset.
    pub fn offset_lag(&self) -> i64 {
        self.offset_lag
    }

    /// Returns whether this is a future log created by replica-directory
    /// movement.
    pub fn is_future(&self) -> bool {
        self.is_future
    }

    fn from_protocol(
        result: kafrust_protocol::api::describe_log_dirs::DescribeLogDirsPartitionResult,
    ) -> Self {
        Self {
            partition_index: result.partition_index,
            partition_size: result.partition_size,
            offset_lag: result.offset_lag,
            is_future: result.is_future,
        }
    }
}

/// A topic and partition set submitted to AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    topic: String,
    partitions: Vec<PartitionReassignmentPartition>,
}

impl PartitionReassignment {
    /// Creates an empty reassignment request for one topic.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partitions: Vec::new(),
        }
    }

    /// Adds a partition target. Replica order is the preferred replica order.
    pub fn partition(
        mut self,
        partition_index: i32,
        replicas: impl IntoIterator<Item = i32>,
    ) -> Self {
        self.partitions.push(PartitionReassignmentPartition {
            partition_index,
            replicas: Some(replicas.into_iter().collect()),
        });
        self
    }

    /// Adds a partition cancellation request.
    pub fn cancel(mut self, partition_index: i32) -> Self {
        self.partitions.push(PartitionReassignmentPartition {
            partition_index,
            replicas: None,
        });
        self
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns partition targets in request order.
    pub fn partitions(&self) -> &[PartitionReassignmentPartition] {
        &self.partitions
    }

    fn as_protocol(&self) -> AlterPartitionReassignmentsTopicV0 {
        AlterPartitionReassignmentsTopicV0 {
            name: self.topic.clone(),
            partitions: self
                .partitions
                .iter()
                .map(PartitionReassignmentPartition::as_protocol)
                .collect(),
        }
    }
}

/// One partition target in a [`PartitionReassignment`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentPartition {
    partition_index: i32,
    replicas: Option<Vec<i32>>,
}

impl PartitionReassignmentPartition {
    /// Returns the Kafka partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the target replica order, or `None` for cancellation.
    pub fn replicas(&self) -> Option<&[i32]> {
        self.replicas.as_deref()
    }

    fn as_protocol(&self) -> AlterPartitionReassignmentsPartitionV0 {
        AlterPartitionReassignmentsPartitionV0 {
            partition_index: self.partition_index,
            replicas: self.replicas.clone(),
        }
    }
}

/// Options shared by partition reassignment start and status requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionReassignmentOptions {
    timeout: Duration,
}

impl PartitionReassignmentOptions {
    /// Creates options with a 30-second broker request timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the broker-side reassignment request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the configured broker request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.timeout
    }
}

impl Default for PartitionReassignmentOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }
}

/// A topic and optional partition filter for ListPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignmentQuery {
    topic: String,
    partition_indexes: Vec<i32>,
}

impl PartitionReassignmentQuery {
    /// Creates a query for one topic. An empty partition list asks for all
    /// ongoing partitions of that topic.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition_indexes: Vec::new(),
        }
    }

    /// Adds a partition index to this query.
    pub fn partition(mut self, partition_index: i32) -> Self {
        self.partition_indexes.push(partition_index);
        self
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the selected partition indexes. Empty means all partitions.
    pub fn partition_indexes(&self) -> &[i32] {
        &self.partition_indexes
    }

    fn as_protocol(&self) -> ListPartitionReassignmentsTopicV0 {
        ListPartitionReassignmentsTopicV0 {
            name: self.topic.clone(),
            partition_indexes: self.partition_indexes.clone(),
        }
    }
}

/// Per-partition outcome returned by AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentResult {
    partition_index: i32,
    error_code: i16,
    error_message: Option<String>,
}

impl AlterPartitionReassignmentResult {
    /// Returns the affected partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns Kafka's raw error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's error message, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka accepted this partition request.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }
}

/// Per-topic outcomes returned by AlterPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentTopicResult {
    name: String,
    partitions: Vec<AlterPartitionReassignmentResult>,
}

impl AlterPartitionReassignmentTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns partition outcomes in broker response order.
    pub fn partitions(&self) -> &[AlterPartitionReassignmentResult] {
        &self.partitions
    }

    /// Returns whether every returned partition succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(AlterPartitionReassignmentResult::is_success)
    }
}

/// Result returned by [`AdminClient::alter_partition_reassignments`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsResult {
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
    topics: Vec<AlterPartitionReassignmentTopicResult>,
}

impl AlterPartitionReassignmentsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns Kafka's top-level error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's top-level error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's top-level broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns topic-level partition outcomes.
    pub fn topics(&self) -> &[AlterPartitionReassignmentTopicResult] {
        &self.topics
    }

    /// Returns whether the request and all returned partition requests succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
            && self
                .topics
                .iter()
                .all(AlterPartitionReassignmentTopicResult::is_success)
    }

    /// Returns whether any top-level or partition-level error was reported.
    pub fn has_errors(&self) -> bool {
        !self.is_success()
    }

    fn from_protocol(
        response: kafrust_protocol::api::alter_partition_reassignments::
            AlterPartitionReassignmentsResponseV0,
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
            topics: response
                .responses
                .into_iter()
                .map(|topic| AlterPartitionReassignmentTopicResult {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition| AlterPartitionReassignmentResult {
                            partition_index: partition.partition_index,
                            error_code: partition.error_code,
                            error_message: partition.error_message,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// An ongoing reassignment returned by ListPartitionReassignments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingPartitionReassignment {
    partition_index: i32,
    replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    removing_replicas: Vec<i32>,
}

impl OngoingPartitionReassignment {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the full target replica set.
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// Returns replicas still being added.
    pub fn adding_replicas(&self) -> &[i32] {
        &self.adding_replicas
    }

    /// Returns replicas still being removed.
    pub fn removing_replicas(&self) -> &[i32] {
        &self.removing_replicas
    }
}

/// Ongoing reassignments for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingPartitionReassignmentTopic {
    name: String,
    partitions: Vec<OngoingPartitionReassignment>,
}

impl OngoingPartitionReassignmentTopic {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns ongoing partition details.
    pub fn partitions(&self) -> &[OngoingPartitionReassignment] {
        &self.partitions
    }
}

/// Result returned by [`AdminClient::list_partition_reassignments`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPartitionReassignmentsResult {
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
    topics: Vec<OngoingPartitionReassignmentTopic>,
}

impl ListPartitionReassignmentsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns Kafka's top-level error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's top-level error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's top-level broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns topics with ongoing reassignments.
    pub fn topics(&self) -> &[OngoingPartitionReassignmentTopic] {
        &self.topics
    }

    /// Returns whether Kafka accepted the status request.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(
        response: kafrust_protocol::api::list_partition_reassignments::
            ListPartitionReassignmentsResponseV0,
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
            topics: response
                .topics
                .into_iter()
                .map(|topic| OngoingPartitionReassignmentTopic {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition| OngoingPartitionReassignment {
                            partition_index: partition.partition_index,
                            replicas: partition.replicas,
                            adding_replicas: partition.adding_replicas,
                            removing_replicas: partition.removing_replicas,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// One Kafka group returned by [`AdminClient::list_groups`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListing {
    group_id: String,
    protocol_type: String,
    coordinator_id: i32,
    throttle_time: Duration,
}

impl GroupListing {
    /// Returns the Kafka group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the group protocol type, such as `consumer` or `connect`.
    pub fn protocol_type(&self) -> &str {
        &self.protocol_type
    }

    /// Returns the broker ID that coordinates this group.
    pub fn coordinator_id(&self) -> i32 {
        self.coordinator_id
    }

    /// Returns the coordinator's throttle time for the ListGroups request.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    fn from_protocol(group: ListedGroupV1, coordinator_id: i32, throttle_time: Duration) -> Self {
        Self {
            group_id: group.group_id,
            protocol_type: group.protocol_type,
            coordinator_id,
            throttle_time,
        }
    }
}

/// Outcome for one group in [`AdminClient::delete_consumer_groups`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupResult {
    group_id: String,
    error_code: i16,
    throttle_time: Duration,
}

impl DeleteConsumerGroupResult {
    /// Returns the Kafka group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns whether Kafka deleted this group.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw group error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns the coordinator's throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    fn from_protocol(result: DeleteGroupResultV1, throttle_time: Duration) -> Self {
        Self {
            group_id: result.group_id,
            error_code: result.error_code,
            throttle_time,
        }
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

/// Complete dynamic topic configuration values for one topic in classic
/// AlterConfigs.
///
/// Each builder call adds one key. Supplying `None` through [`Self::delete`]
/// removes the dynamic value and lets Kafka resolve the configuration from its
/// lower-precedence source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigUpdate {
    topic: String,
    configs: Vec<TopicConfigUpdateEntry>,
}

impl TopicConfigUpdate {
    /// Creates an update for one topic.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            configs: Vec::new(),
        }
    }

    /// Adds a dynamic topic configuration value.
    pub fn set(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push(TopicConfigUpdateEntry {
            name: name.into(),
            value: Some(value.into()),
        });
        self
    }

    /// Removes a dynamic topic configuration value.
    pub fn delete(mut self, name: impl Into<String>) -> Self {
        self.configs.push(TopicConfigUpdateEntry {
            name: name.into(),
            value: None,
        });
        self
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns configuration entries in request order.
    pub fn configs(&self) -> &[TopicConfigUpdateEntry] {
        &self.configs
    }

    fn as_protocol(&self) -> AlterConfigsResourceV1 {
        AlterConfigsResourceV1 {
            resource_type: 2,
            resource_name: self.topic.clone(),
            configs: self
                .configs
                .iter()
                .map(|config| AlterableConfigV1 {
                    name: config.name.clone(),
                    value: config.value.clone(),
                })
                .collect(),
        }
    }
}

/// One configuration entry in a classic [`TopicConfigUpdate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigUpdateEntry {
    name: String,
    value: Option<String>,
}

impl TopicConfigUpdateEntry {
    /// Returns the configuration key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the replacement value, or `None` when the key is deleted.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
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

/// Options shared by classic and incremental AlterConfigs operations.
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

/// Complete response from one classic or incremental AlterConfigs operation.
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

/// Outcome for one resource in a classic or incremental AlterConfigs operation.
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

    fn from_classic_protocol(response: AlterConfigsResourceResponseV1) -> Self {
        Self {
            resource_type: response.resource_type,
            name: response.resource_name,
            error_code: response.error_code,
            error_message: response.error_message,
        }
    }
}

/// Description of one Kafka consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDescription {
    group_id: String,
    state: String,
    protocol_type: String,
    protocol_name: String,
    members: Vec<ConsumerGroupMember>,
    error_code: i16,
    throttle_time: Duration,
}

impl ConsumerGroupDescription {
    /// Returns the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns Kafka's current group state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns the group protocol type, such as `consumer`.
    pub fn protocol_type(&self) -> &str {
        &self.protocol_type
    }

    /// Returns the selected group protocol name, such as `range`.
    pub fn protocol_name(&self) -> &str {
        &self.protocol_name
    }

    /// Returns current members in broker response order.
    pub fn members(&self) -> &[ConsumerGroupMember] {
        &self.members
    }

    /// Returns whether Kafka described this group successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw group error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns the coordinator's throttle time for this request.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    fn from_protocol(group: DescribeGroupsGroupV1, throttle_time: Duration) -> Self {
        Self {
            group_id: group.group_id,
            state: group.state,
            protocol_type: group.protocol_type,
            protocol_name: group.protocol_data,
            members: group
                .members
                .into_iter()
                .map(ConsumerGroupMember::from_protocol)
                .collect(),
            error_code: group.error_code,
            throttle_time,
        }
    }
}

/// One member in a Kafka consumer group description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMember {
    member_id: String,
    client_id: String,
    client_host: String,
    member_metadata: Vec<u8>,
    member_assignment: Vec<u8>,
}

impl ConsumerGroupMember {
    /// Returns the member ID assigned by Kafka.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the member's client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the member's client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns raw group-protocol member metadata.
    pub fn member_metadata(&self) -> &[u8] {
        &self.member_metadata
    }

    /// Returns raw group-protocol member assignment.
    pub fn member_assignment(&self) -> &[u8] {
        &self.member_assignment
    }

    fn from_protocol(member: DescribeGroupsMemberV1) -> Self {
        Self {
            member_id: member.member_id,
            client_id: member.client_id,
            client_host: member.client_host,
            member_metadata: member.member_metadata,
            member_assignment: member.member_assignment,
        }
    }
}

fn is_retryable_admin_coordinator_error(error: &Error) -> bool {
    match error {
        Error::Broker { code, .. } => is_retryable_admin_coordinator_code(*code),
        Error::Io(_) | Error::RequestTimedOut { .. } => true,
        _ => false,
    }
}

async fn retry_admin_connection<T, Connect, ConnectFuture, OnRetry>(
    max_retries: u32,
    mut connect: Connect,
    mut on_retry: OnRetry,
) -> Result<T>
where
    Connect: FnMut() -> ConnectFuture,
    ConnectFuture: Future<Output = Result<T>>,
    OnRetry: FnMut(),
{
    let mut retry = 0;
    loop {
        match connect().await {
            Ok(value) => return Ok(value),
            Err(error) if retry < max_retries && is_retryable_admin_connection_error(&error) => {
                retry += 1;
                on_retry();
                tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_retryable_admin_connection_error(error: &Error) -> bool {
    matches!(error, Error::Io(_) | Error::RequestTimedOut { .. })
}

fn is_retryable_admin_read_error(error: &Error) -> bool {
    match error {
        Error::Broker { code, .. } => is_retryable_admin_read_code(*code),
        Error::Io(_) | Error::RequestTimedOut { .. } => true,
        _ => false,
    }
}

fn is_retryable_admin_controller_read_error(error: &Error) -> bool {
    is_retryable_admin_read_error(error) || matches!(error, Error::MissingBroker { .. })
}

fn is_retryable_metadata_response(metadata: &MetadataResponseV1) -> bool {
    metadata.topics.iter().any(|topic| {
        is_retryable_admin_read_code(topic.error_code)
            || topic
                .partitions
                .iter()
                .any(|partition| is_retryable_admin_read_code(partition.error_code))
    })
}

fn is_retryable_admin_read_code(code: i16) -> bool {
    matches!(
        BrokerErrorKind::from_code(code),
        BrokerErrorKind::LeaderNotAvailable
            | BrokerErrorKind::NotLeaderOrFollower
            | BrokerErrorKind::RequestTimedOut
            | BrokerErrorKind::ReplicaNotAvailable
            | BrokerErrorKind::NotController
    )
}

fn is_retryable_admin_coordinator_code(code: i16) -> bool {
    matches!(
        BrokerErrorKind::from_code(code),
        BrokerErrorKind::CoordinatorLoadInProgress
            | BrokerErrorKind::CoordinatorNotAvailable
            | BrokerErrorKind::NotCoordinator
    )
}

fn is_retryable_list_transactions_code(code: i16) -> bool {
    is_retryable_admin_coordinator_code(code) || is_retryable_admin_read_code(code)
}

fn is_retryable_offset_fetch_v9(
    response: &kafrust_protocol::api::offset_fetch::OffsetFetchResponseV9,
) -> bool {
    response.groups.iter().any(|group| {
        is_retryable_admin_coordinator_code(group.error_code)
            || group.topics.iter().any(|topic| {
                topic
                    .partitions
                    .iter()
                    .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
            })
    })
}

fn record_offset_fetch_v9_errors(
    config: &ClientConfig,
    response: &kafrust_protocol::api::offset_fetch::OffsetFetchResponseV9,
) {
    for group in &response.groups {
        if group.error_code != 0 {
            config.record_broker_error();
        }
        for topic in &group.topics {
            for partition in &topic.partitions {
                if partition.error_code != 0 {
                    config.record_broker_error();
                }
            }
        }
    }
}

fn record_offset_commit_v9_errors(
    config: &ClientConfig,
    response: &kafrust_protocol::api::offset_commit::OffsetCommitResponseV9,
) {
    for topic in &response.topics {
        for partition in &topic.partitions {
            if partition.error_code != 0 {
                config.record_broker_error();
            }
        }
    }
}

fn is_retryable_admin_leader_error(error: &Error) -> bool {
    match error {
        Error::Broker { code, .. } => is_retryable_admin_leader_code(*code),
        Error::Io(_)
        | Error::RequestTimedOut { .. }
        | Error::MissingLeader { .. }
        | Error::MissingBroker { .. } => true,
        _ => false,
    }
}

fn is_retryable_admin_leader_code(code: i16) -> bool {
    matches!(code, 5..=9)
}

fn admin_coordinator_retry_backoff(retry_attempt: u32) -> Duration {
    let exponent = retry_attempt.saturating_sub(1).min(4);
    let multiplier = 1u64 << exponent;
    let milliseconds =
        (ADMIN_COORDINATOR_RETRY_BACKOFF_BASE.as_millis() as u64).saturating_mul(multiplier);
    Duration::from_millis(milliseconds).min(ADMIN_COORDINATOR_MAX_RETRY_BACKOFF)
}

/// A topic and partition filter for consumer-group offset inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetQuery {
    topic: String,
    partitions: Vec<i32>,
}

impl ConsumerGroupOffsetQuery {
    /// Creates an offset query for one topic.
    pub fn new(topic: impl Into<String>, partitions: impl IntoIterator<Item = i32>) -> Self {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns partition indexes in request order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    fn as_protocol(&self) -> OffsetFetchTopic {
        OffsetFetchTopic {
            name: self.topic.clone(),
            partition_indexes: self.partitions.clone(),
        }
    }

    fn as_protocol_v9(&self) -> OffsetFetchTopicV9 {
        OffsetFetchTopicV9 {
            name: self.topic.clone(),
            partition_indexes: self.partitions.clone(),
        }
    }
}

/// One committed consumer-group offset to set administratively.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffset {
    topic: String,
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    metadata: Option<String>,
}

impl ConsumerGroupOffset {
    /// Creates an administrative offset update.
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            leader_epoch: -1,
            metadata: None,
        }
    }

    /// Sets the broker leader epoch associated with this committed offset.
    ///
    /// The default `-1` sentinel preserves Kafka's standard behavior when the
    /// caller does not have a leader epoch. KIP-848 OffsetCommit v9 carries
    /// this field; classic OffsetCommit v2 ignores it.
    pub fn leader_epoch(mut self, leader_epoch: i32) -> Self {
        self.leader_epoch = leader_epoch;
        self
    }

    /// Sets optional group-offset metadata.
    pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
        self.metadata = Some(metadata.into());
        self
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the committed offset to set.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the leader epoch to send with OffsetCommit v9.
    pub fn leader_epoch_ref(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns optional group-offset metadata.
    pub fn metadata_ref(&self) -> Option<&str> {
        self.metadata.as_deref()
    }
}

/// Complete response from one OffsetFetch v2 consumer-group offset query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsResult {
    group_id: String,
    error_code: i16,
    throttle_time: Duration,
    topics: Vec<ConsumerGroupOffsetTopicResult>,
}

impl ListConsumerGroupOffsetsResult {
    /// Returns the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns whether Kafka accepted the group query and every partition.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
            && self
                .topics
                .iter()
                .all(ConsumerGroupOffsetTopicResult::is_success)
    }

    /// Returns the top-level Kafka group error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker throttle time for this offset query.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns kafrust's classification for a top-level group error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns topic outcomes in broker response order.
    pub fn topics(&self) -> &[ConsumerGroupOffsetTopicResult] {
        &self.topics
    }

    fn from_protocol(
        group_id: &str,
        error_code: i16,
        topics: Vec<OffsetFetchTopicResponse>,
    ) -> Self {
        Self {
            group_id: group_id.to_owned(),
            error_code,
            throttle_time: Duration::ZERO,
            topics: topics
                .into_iter()
                .map(ConsumerGroupOffsetTopicResult::from_fetch_protocol)
                .collect(),
        }
    }

    fn from_protocol_v9(
        group_id: &str,
        throttle_time_ms: i32,
        group: OffsetFetchGroupResponse,
    ) -> Self {
        Self {
            group_id: group_id.to_owned(),
            error_code: group.error_code,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(throttle_time_ms)),
            topics: group
                .topics
                .into_iter()
                .map(ConsumerGroupOffsetTopicResult::from_fetch_protocol)
                .collect(),
        }
    }
}

/// OffsetFetch results for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetTopicResult {
    topic: String,
    partitions: Vec<ConsumerGroupOffsetPartitionResult>,
}

impl ConsumerGroupOffsetTopicResult {
    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns whether every partition query succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(ConsumerGroupOffsetPartitionResult::is_success)
    }

    /// Returns partition outcomes in broker response order.
    pub fn partitions(&self) -> &[ConsumerGroupOffsetPartitionResult] {
        &self.partitions
    }

    fn from_fetch_protocol(topic: OffsetFetchTopicResponse) -> Self {
        Self {
            topic: topic.name,
            partitions: topic
                .partitions
                .into_iter()
                .map(ConsumerGroupOffsetPartitionResult::from_fetch_protocol)
                .collect(),
        }
    }
}

/// One OffsetFetch partition outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetPartitionResult {
    partition_index: i32,
    committed_offset: i64,
    metadata: Option<String>,
    error_code: i16,
}

impl ConsumerGroupOffsetPartitionResult {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the committed offset, or Kafka's sentinel when absent.
    pub fn committed_offset(&self) -> i64 {
        self.committed_offset
    }

    /// Returns optional group-offset metadata.
    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    /// Returns whether Kafka returned this partition without an error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw partition error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a partition error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_fetch_protocol(
        partition: kafrust_protocol::api::offset_fetch::OffsetFetchPartitionResponse,
    ) -> Self {
        Self {
            partition_index: partition.partition_index,
            committed_offset: partition.committed_offset,
            metadata: partition.metadata,
            error_code: partition.error_code,
        }
    }
}

/// Complete response from one administrative OffsetCommit v2 operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetsResult {
    group_id: String,
    throttle_time: Duration,
    topics: Vec<AlterConsumerGroupOffsetsTopicResult>,
}

impl AlterConsumerGroupOffsetsResult {
    /// Returns the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the broker throttle time for this offset commit.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns whether every requested partition commit succeeded.
    pub fn is_success(&self) -> bool {
        self.topics
            .iter()
            .all(AlterConsumerGroupOffsetsTopicResult::is_success)
    }

    /// Returns topic outcomes in broker response order.
    pub fn topics(&self) -> &[AlterConsumerGroupOffsetsTopicResult] {
        &self.topics
    }

    fn from_protocol(group_id: &str, topics: Vec<OffsetCommitTopicResponse>) -> Self {
        Self {
            group_id: group_id.to_owned(),
            throttle_time: Duration::ZERO,
            topics: topics
                .into_iter()
                .map(AlterConsumerGroupOffsetsTopicResult::from_protocol)
                .collect(),
        }
    }

    fn from_protocol_v9(
        group_id: &str,
        throttle_time_ms: i32,
        topics: Vec<OffsetCommitTopicResponse>,
    ) -> Self {
        Self {
            group_id: group_id.to_owned(),
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(throttle_time_ms)),
            topics: topics
                .into_iter()
                .map(AlterConsumerGroupOffsetsTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// OffsetCommit results for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetsTopicResult {
    topic: String,
    partitions: Vec<AlterConsumerGroupOffsetsPartitionResult>,
}

impl AlterConsumerGroupOffsetsTopicResult {
    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns whether every partition commit succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(AlterConsumerGroupOffsetsPartitionResult::is_success)
    }

    /// Returns partition outcomes in broker response order.
    pub fn partitions(&self) -> &[AlterConsumerGroupOffsetsPartitionResult] {
        &self.partitions
    }

    fn from_protocol(topic: OffsetCommitTopicResponse) -> Self {
        Self {
            topic: topic.name,
            partitions: topic
                .partitions
                .into_iter()
                .map(AlterConsumerGroupOffsetsPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// One OffsetCommit partition outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlterConsumerGroupOffsetsPartitionResult {
    partition_index: i32,
    error_code: i16,
}

impl AlterConsumerGroupOffsetsPartitionResult {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns whether Kafka accepted this partition commit.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw partition error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a partition error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(
        partition: kafrust_protocol::api::offset_commit::OffsetCommitPartitionResponse,
    ) -> Self {
        Self {
            partition_index: partition.partition_index,
            error_code: partition.error_code,
        }
    }
}

/// Topic partitions whose committed offsets should be deleted for a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupOffsetDelete {
    topic: String,
    partitions: Vec<i32>,
}

impl ConsumerGroupOffsetDelete {
    /// Creates an offset-deletion request for one topic.
    pub fn new(topic: impl Into<String>, partitions: impl IntoIterator<Item = i32>) -> Self {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns partition indexes in request order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    fn as_protocol(&self) -> OffsetDeleteRequestTopicV0 {
        OffsetDeleteRequestTopicV0 {
            name: self.topic.clone(),
            partitions: self
                .partitions
                .iter()
                .map(|partition_index| OffsetDeleteRequestPartitionV0 {
                    partition_index: *partition_index,
                })
                .collect(),
        }
    }
}

/// Complete response from one OffsetDelete operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsResult {
    group_id: String,
    error_code: i16,
    throttle_time: Duration,
    topics: Vec<DeleteConsumerGroupOffsetsTopicResult>,
}

impl DeleteConsumerGroupOffsetsResult {
    /// Returns the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns whether Kafka accepted the request and every partition.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
            && self
                .topics
                .iter()
                .all(DeleteConsumerGroupOffsetsTopicResult::is_success)
    }

    /// Returns Kafka's top-level group error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a top-level group error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns the coordinator's throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns topic outcomes in broker response order.
    pub fn topics(&self) -> &[DeleteConsumerGroupOffsetsTopicResult] {
        &self.topics
    }

    /// Consumes the response and returns topic outcomes.
    pub fn into_topics(self) -> Vec<DeleteConsumerGroupOffsetsTopicResult> {
        self.topics
    }
}

/// Offset-deletion outcomes for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsTopicResult {
    topic: String,
    partitions: Vec<DeleteConsumerGroupOffsetsPartitionResult>,
}

impl DeleteConsumerGroupOffsetsTopicResult {
    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns whether every partition offset was deleted.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(DeleteConsumerGroupOffsetsPartitionResult::is_success)
    }

    /// Returns partition outcomes in broker response order.
    pub fn partitions(&self) -> &[DeleteConsumerGroupOffsetsPartitionResult] {
        &self.partitions
    }

    fn from_protocol(topic: OffsetDeleteResponseTopicV0) -> Self {
        Self {
            topic: topic.name,
            partitions: topic
                .partitions
                .into_iter()
                .map(DeleteConsumerGroupOffsetsPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// Offset-deletion outcome for one topic partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteConsumerGroupOffsetsPartitionResult {
    partition_index: i32,
    error_code: i16,
}

impl DeleteConsumerGroupOffsetsPartitionResult {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns whether Kafka deleted this partition's committed offset.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw partition error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a partition error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(partition: OffsetDeleteResponsePartitionV0) -> Self {
        Self {
            partition_index: partition.partition_index,
            error_code: partition.error_code,
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

/// New total partition count for an existing Kafka topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartitions {
    name: String,
    count: i32,
    assignments: Option<Vec<Vec<i32>>>,
}

impl NewPartitions {
    /// Uses Kafka's automatic replica assignment for newly added partitions.
    pub fn new(name: impl Into<String>, count: i32) -> Self {
        Self {
            name: name.into(),
            count,
            assignments: None,
        }
    }

    /// Uses explicit broker assignments for each newly added partition.
    ///
    /// Assignment order corresponds to ascending new partition indexes.
    pub fn with_assignments(
        name: impl Into<String>,
        count: i32,
        assignments: impl IntoIterator<Item = Vec<i32>>,
    ) -> Self {
        Self {
            name: name.into(),
            count,
            assignments: Some(assignments.into_iter().collect()),
        }
    }

    /// Returns the existing topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the requested new total partition count.
    pub fn count(&self) -> i32 {
        self.count
    }

    /// Returns explicit assignments, or `None` for automatic assignment.
    pub fn assignments(&self) -> Option<&[Vec<i32>]> {
        self.assignments.as_deref()
    }

    fn as_protocol(&self) -> CreatePartitionsTopicV0 {
        CreatePartitionsTopicV0 {
            name: self.name.clone(),
            count: self.count,
            assignments: self.assignments.as_ref().map(|assignments| {
                assignments
                    .iter()
                    .map(|broker_ids| CreatePartitionsAssignmentV0 {
                        broker_ids: broker_ids.clone(),
                    })
                    .collect()
            }),
        }
    }
}

/// Options for one CreatePartitions operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatePartitionsOptions {
    timeout: Duration,
    validate_only: bool,
}

impl CreatePartitionsOptions {
    /// Creates options with a 30-second broker timeout and mutation enabled.
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            validate_only: false,
        }
    }

    /// Sets how long the controller may wait for partition creation.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets whether Kafka should validate without adding partitions.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Returns the configured broker-side timeout.
    pub fn timeout_ref(&self) -> Duration {
        self.timeout
    }

    /// Returns whether this operation only validates the request.
    pub fn is_validate_only(&self) -> bool {
        self.validate_only
    }
}

impl Default for CreatePartitionsOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete response from one CreatePartitions operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsResult {
    throttle_time: Duration,
    topics: Vec<CreatePartitionsTopicResult>,
}

impl CreatePartitionsResult {
    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-topic outcomes in broker response order.
    pub fn topics(&self) -> &[CreatePartitionsTopicResult] {
        &self.topics
    }

    /// Consumes this response and returns per-topic outcomes.
    pub fn into_topics(self) -> Vec<CreatePartitionsTopicResult> {
        self.topics
    }

    /// Returns whether at least one topic was rejected.
    pub fn has_errors(&self) -> bool {
        self.topics.iter().any(|topic| !topic.is_success())
    }
}

/// Outcome for one topic in a CreatePartitions response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopicResult {
    name: String,
    error_code: i16,
    error_message: Option<String>,
}

impl CreatePartitionsTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether Kafka added or successfully validated the partitions.
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

    fn from_protocol(result: CreatePartitionsTopicResultV0) -> Self {
        Self {
            name: result.name,
            error_code: result.error_code,
            error_message: result.error_message,
        }
    }
}

/// Topic and partition targets for one DescribeProducers operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersTopic {
    name: String,
    partitions: Vec<i32>,
}

impl DescribeProducersTopic {
    /// Creates an empty topic target.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partitions: Vec::new(),
        }
    }

    /// Adds a partition to the producer-state query.
    pub fn partition(mut self, partition_index: i32) -> Self {
        self.partitions.push(partition_index);
        self
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns requested partition indexes.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// Complete response from one DescribeProducers operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersResult {
    throttle_time: Duration,
    topics: Vec<DescribeProducersTopicResult>,
}

impl DescribeProducersResult {
    /// Returns the maximum broker throttle time observed across leader requests.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-topic producer-state results.
    pub fn topics(&self) -> &[DescribeProducersTopicResult] {
        &self.topics
    }

    /// Consumes this response and returns per-topic results.
    pub fn into_topics(self) -> Vec<DescribeProducersTopicResult> {
        self.topics
    }

    /// Returns whether at least one partition query failed.
    pub fn has_errors(&self) -> bool {
        self.topics
            .iter()
            .any(DescribeProducersTopicResult::has_errors)
    }

    fn from_protocol_responses(
        responses: Vec<kafrust_protocol::api::describe_producers::DescribeProducersResponseV0>,
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(
                responses
                    .iter()
                    .map(|response| nonnegative_i32_to_u64(response.throttle_time_ms))
                    .max()
                    .unwrap_or(0),
            ),
            topics: responses
                .into_iter()
                .flat_map(|response| response.topics)
                .map(DescribeProducersTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// Outcome for one topic in a DescribeProducers response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersTopicResult {
    name: String,
    partitions: Vec<DescribeProducersPartitionResult>,
}

impl DescribeProducersTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns per-partition producer-state results.
    pub fn partitions(&self) -> &[DescribeProducersPartitionResult] {
        &self.partitions
    }

    /// Returns whether at least one partition query failed.
    pub fn has_errors(&self) -> bool {
        self.partitions
            .iter()
            .any(|partition| !partition.is_success())
    }

    fn from_protocol(result: DescribeProducersTopicResponseV0) -> Self {
        Self {
            name: result.name,
            partitions: result
                .partitions
                .into_iter()
                .map(DescribeProducersPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// Producer state for one partition in a DescribeProducers response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeProducersPartitionResult {
    partition_index: i32,
    error_code: i16,
    error_message: Option<String>,
    active_producers: Vec<DescribeProducersActiveProducer>,
}

impl DescribeProducersPartitionResult {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns whether Kafka returned producer state successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw partition error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's optional partition error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns active producer state entries.
    pub fn active_producers(&self) -> &[DescribeProducersActiveProducer] {
        &self.active_producers
    }

    fn from_protocol(result: DescribeProducersPartitionResponseV0) -> Self {
        Self {
            partition_index: result.partition_index,
            error_code: result.error_code,
            error_message: result.error_message,
            active_producers: result
                .active_producers
                .into_iter()
                .map(DescribeProducersActiveProducer::from_protocol)
                .collect(),
        }
    }
}

/// Active producer sequence state returned by Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribeProducersActiveProducer {
    producer_id: i64,
    producer_epoch: i32,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_txn_start_offset: i64,
}

impl DescribeProducersActiveProducer {
    /// Returns the producer ID.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the producer epoch.
    pub fn producer_epoch(&self) -> i32 {
        self.producer_epoch
    }

    /// Returns the last accepted sequence.
    pub fn last_sequence(&self) -> i32 {
        self.last_sequence
    }

    /// Returns the broker timestamp of the last append.
    pub fn last_timestamp(&self) -> i64 {
        self.last_timestamp
    }

    /// Returns the transaction coordinator epoch.
    pub fn coordinator_epoch(&self) -> i32 {
        self.coordinator_epoch
    }

    /// Returns the current transaction start offset, or Kafka's sentinel.
    pub fn current_txn_start_offset(&self) -> i64 {
        self.current_txn_start_offset
    }

    fn from_protocol(result: DescribeProducersActiveProducerV0) -> Self {
        Self {
            producer_id: result.producer_id,
            producer_epoch: result.producer_epoch,
            last_sequence: result.last_sequence,
            last_timestamp: result.last_timestamp,
            coordinator_epoch: result.coordinator_epoch,
            current_txn_start_offset: result.current_txn_start_offset,
        }
    }
}

/// Complete response from one DescribeTransactions operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTransactionsResult {
    throttle_time: Duration,
    transactions: Vec<TransactionDescription>,
}

impl DescribeTransactionsResult {
    /// Returns the maximum broker throttle time observed across coordinators.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns transaction descriptions in broker response order.
    pub fn transactions(&self) -> &[TransactionDescription] {
        &self.transactions
    }

    /// Consumes this response and returns transaction descriptions.
    pub fn into_transactions(self) -> Vec<TransactionDescription> {
        self.transactions
    }

    /// Returns whether at least one transactional ID was rejected.
    pub fn has_errors(&self) -> bool {
        self.transactions
            .iter()
            .any(|transaction| !transaction.is_success())
    }

    fn from_protocol_responses(
        responses: Vec<
            kafrust_protocol::api::describe_transactions::DescribeTransactionsResponseV0,
        >,
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(
                responses
                    .iter()
                    .map(|response| nonnegative_i32_to_u64(response.throttle_time_ms))
                    .max()
                    .unwrap_or(0),
            ),
            transactions: responses
                .into_iter()
                .flat_map(|response| response.transaction_states)
                .map(TransactionDescription::from_protocol)
                .collect(),
        }
    }
}

/// Filters for [`AdminClient::list_transactions`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListTransactionsOptions {
    state_filters: Vec<String>,
    producer_id_filters: Vec<i64>,
    duration_filter: Option<Duration>,
}

impl ListTransactionsOptions {
    /// Creates an unfiltered transaction listing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a Kafka transaction-state filter such as `Ongoing` or `PrepareCommit`.
    pub fn state(mut self, state: impl Into<String>) -> Self {
        self.state_filters.push(state.into());
        self
    }

    /// Adds a producer ID filter.
    pub fn producer_id(mut self, producer_id: i64) -> Self {
        self.producer_id_filters.push(producer_id);
        self
    }

    /// Filters transactions running longer than the given duration.
    ///
    /// This requires ListTransactions v1. The call returns a typed
    /// [`Error::Unsupported`] if any target broker only advertises v0.
    pub fn duration_filter(mut self, duration: Duration) -> Self {
        self.duration_filter = Some(duration);
        self
    }

    /// Returns the configured transaction-state filters.
    pub fn state_filters(&self) -> &[String] {
        &self.state_filters
    }

    /// Returns the configured producer ID filters.
    pub fn producer_id_filters(&self) -> &[i64] {
        &self.producer_id_filters
    }

    /// Returns the configured duration filter.
    pub fn duration_filter_ref(&self) -> Option<Duration> {
        self.duration_filter
    }
}

/// Complete response aggregated from all ListTransactions broker shards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTransactionsResult {
    throttle_time: Duration,
    error_code: i16,
    unknown_state_filters: Vec<String>,
    transactions: Vec<ListedTransaction>,
}

impl ListTransactionsResult {
    /// Returns the maximum broker throttle time observed across shards.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the first non-zero top-level Kafka error code, or zero.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns whether all broker shards completed without a top-level error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's classification for the top-level broker error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns state filters not recognized by at least one broker.
    pub fn unknown_state_filters(&self) -> &[String] {
        &self.unknown_state_filters
    }

    /// Returns active transactions aggregated across broker shards.
    pub fn transactions(&self) -> &[ListedTransaction] {
        &self.transactions
    }

    /// Consumes this response and returns the aggregated transactions.
    pub fn into_transactions(self) -> Vec<ListedTransaction> {
        self.transactions
    }

    fn from_protocol_responses(
        responses: Vec<kafrust_protocol::api::list_transactions::ListTransactionsResponseV0>,
    ) -> Self {
        let error_code = responses
            .iter()
            .map(|response| response.error_code)
            .find(|code| *code != 0)
            .unwrap_or(0);
        Self {
            throttle_time: Duration::from_millis(
                responses
                    .iter()
                    .map(|response| nonnegative_i32_to_u64(response.throttle_time_ms))
                    .max()
                    .unwrap_or(0),
            ),
            error_code,
            unknown_state_filters: responses
                .iter()
                .flat_map(|response| response.unknown_state_filters.iter().cloned())
                .collect(),
            transactions: responses
                .into_iter()
                .flat_map(|response| response.transaction_states)
                .map(ListedTransaction::from_protocol)
                .collect(),
        }
    }
}

/// One active transaction returned by ListTransactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedTransaction {
    transactional_id: String,
    producer_id: i64,
    transaction_state: String,
}

impl ListedTransaction {
    /// Returns the transactional ID.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns the producer ID.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns Kafka's transaction state string.
    pub fn state(&self) -> &str {
        &self.transaction_state
    }

    fn from_protocol(result: ListedTransactionV0) -> Self {
        Self {
            transactional_id: result.transactional_id,
            producer_id: result.producer_id,
            transaction_state: result.transaction_state,
        }
    }
}

/// State of one transactional ID returned by Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionDescription {
    error_code: i16,
    transactional_id: String,
    transaction_state: String,
    transaction_timeout: Duration,
    transaction_start_time_ms: i64,
    producer_id: i64,
    producer_epoch: i16,
    topics: Vec<TransactionDescriptionTopic>,
}

impl TransactionDescription {
    /// Returns whether Kafka described this transactional ID successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw transaction error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the transactional ID.
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns Kafka's transaction state string.
    pub fn state(&self) -> &str {
        &self.transaction_state
    }

    /// Returns the configured transaction timeout.
    pub fn transaction_timeout(&self) -> Duration {
        self.transaction_timeout
    }

    /// Returns the transaction start timestamp in milliseconds, or Kafka's sentinel.
    pub fn transaction_start_time_ms(&self) -> i64 {
        self.transaction_start_time_ms
    }

    /// Returns the producer ID.
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Returns the producer epoch.
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    /// Returns topic partitions currently associated with the transaction.
    pub fn topics(&self) -> &[TransactionDescriptionTopic] {
        &self.topics
    }

    /// Returns kafrust's classification for a non-zero Kafka error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(result: DescribeTransactionsStateV0) -> Self {
        Self {
            error_code: result.error_code,
            transactional_id: result.transactional_id,
            transaction_state: result.transaction_state,
            transaction_timeout: Duration::from_millis(nonnegative_i32_to_u64(
                result.transaction_timeout_ms,
            )),
            transaction_start_time_ms: result.transaction_start_time_ms,
            producer_id: result.producer_id,
            producer_epoch: result.producer_epoch,
            topics: result
                .topics
                .into_iter()
                .map(TransactionDescriptionTopic::from_protocol)
                .collect(),
        }
    }
}

/// Topic partitions currently associated with one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionDescriptionTopic {
    topic: String,
    partitions: Vec<i32>,
}

impl TransactionDescriptionTopic {
    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition indexes.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    fn from_protocol(result: DescribeTransactionsTopicV0) -> Self {
        Self {
            topic: result.topic,
            partitions: result.partitions,
        }
    }
}

/// Partition offset target for one DeleteRecords operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRecordsPartition {
    partition_index: i32,
    offset: i64,
}

impl DeleteRecordsPartition {
    /// Creates a deletion target for a partition.
    pub fn new(partition_index: i32, offset: i64) -> Self {
        Self {
            partition_index,
            offset,
        }
    }

    /// Returns the target partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the offset before which Kafka should delete records.
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

/// Topic and partition targets for one DeleteRecords operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopic {
    name: String,
    partitions: Vec<DeleteRecordsPartition>,
}

impl DeleteRecordsTopic {
    /// Creates an empty topic target.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partitions: Vec::new(),
        }
    }

    /// Adds a partition offset target to this topic.
    pub fn partition(mut self, partition_index: i32, offset: i64) -> Self {
        self.partitions
            .push(DeleteRecordsPartition::new(partition_index, offset));
        self
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the requested partition deletion targets.
    pub fn partitions(&self) -> &[DeleteRecordsPartition] {
        &self.partitions
    }
}

/// Options for one DeleteRecords operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRecordsOptions {
    timeout: Duration,
}

impl DeleteRecordsOptions {
    /// Creates options with a 30-second broker timeout.
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(30),
        }
    }

    /// Sets how long the broker may wait for record deletion.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the configured broker-side timeout.
    pub fn timeout_ref(&self) -> Duration {
        self.timeout
    }
}

impl Default for DeleteRecordsOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete response from one DeleteRecords operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsResult {
    throttle_time: Duration,
    topics: Vec<DeleteRecordsTopicResult>,
}

impl DeleteRecordsResult {
    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-topic outcomes in broker response order.
    pub fn topics(&self) -> &[DeleteRecordsTopicResult] {
        &self.topics
    }

    /// Consumes this response and returns per-topic outcomes.
    pub fn into_topics(self) -> Vec<DeleteRecordsTopicResult> {
        self.topics
    }

    /// Returns whether at least one partition deletion was rejected.
    pub fn has_errors(&self) -> bool {
        self.topics.iter().any(DeleteRecordsTopicResult::has_errors)
    }

    fn from_protocol_responses(
        responses: Vec<kafrust_protocol::api::delete_records::DeleteRecordsResponseV1>,
    ) -> Self {
        Self {
            throttle_time: Duration::from_millis(
                responses
                    .iter()
                    .map(|response| nonnegative_i32_to_u64(response.throttle_time_ms))
                    .max()
                    .unwrap_or(0),
            ),
            topics: responses
                .into_iter()
                .flat_map(|response| response.topics)
                .map(DeleteRecordsTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// Outcome for one topic in a DeleteRecords response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRecordsTopicResult {
    name: String,
    partitions: Vec<DeleteRecordsPartitionResult>,
}

impl DeleteRecordsTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns per-partition deletion outcomes.
    pub fn partitions(&self) -> &[DeleteRecordsPartitionResult] {
        &self.partitions
    }

    /// Returns whether at least one partition deletion was rejected.
    pub fn has_errors(&self) -> bool {
        self.partitions
            .iter()
            .any(|partition| !partition.is_success())
    }

    fn from_protocol(result: DeleteRecordsTopicResponseV1) -> Self {
        Self {
            name: result.name,
            partitions: result
                .partitions
                .into_iter()
                .map(DeleteRecordsPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// Outcome for one partition in a DeleteRecords response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRecordsPartitionResult {
    partition_index: i32,
    low_watermark: i64,
    error_code: i16,
}

impl DeleteRecordsPartitionResult {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the low watermark after deletion.
    pub fn low_watermark(&self) -> i64 {
        self.low_watermark
    }

    /// Returns whether Kafka accepted this partition deletion.
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

    fn from_protocol(result: DeleteRecordsPartitionResponseV1) -> Self {
        Self {
            partition_index: result.partition_index,
            low_watermark: result.low_watermark,
            error_code: result.error_code,
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

fn delete_records_requests(
    metadata: &MetadataResponseV1,
    topics: &[DeleteRecordsTopic],
) -> Result<BTreeMap<String, Vec<DeleteRecordsTopicV1>>> {
    let mut requests: BTreeMap<String, BTreeMap<String, Vec<_>>> = BTreeMap::new();

    for topic in topics {
        for partition in &topic.partitions {
            let topic_metadata = metadata
                .topics
                .iter()
                .find(|candidate| candidate.name == topic.name)
                .ok_or_else(|| Error::UnknownTopicOrPartition {
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                })?;
            if topic_metadata.error_code != 0 {
                return Err(Error::Broker {
                    code: topic_metadata.error_code,
                    context: format!("metadata for topic {}", topic.name),
                });
            }

            let partition_metadata = topic_metadata
                .partitions
                .iter()
                .find(|candidate| candidate.partition_index == partition.partition_index)
                .ok_or_else(|| Error::UnknownTopicOrPartition {
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                })?;
            if partition_metadata.error_code != 0 {
                return Err(Error::Broker {
                    code: partition_metadata.error_code,
                    context: format!("metadata for {}-{}", topic.name, partition.partition_index),
                });
            }
            let leader_id = partition_metadata.leader_id;
            if leader_id < 0 {
                return Err(Error::MissingLeader {
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                });
            }
            let broker = metadata
                .brokers
                .iter()
                .find(|broker| broker.node_id == leader_id)
                .ok_or(Error::MissingBroker { node_id: leader_id })?;
            let broker_address = format!("{}:{}", broker.host, broker.port);
            requests
                .entry(broker_address)
                .or_default()
                .entry(topic.name.clone())
                .or_default()
                .push(
                    kafrust_protocol::api::delete_records::DeleteRecordsPartitionV1 {
                        partition_index: partition.partition_index,
                        offset: partition.offset,
                    },
                );
        }
    }

    Ok(requests
        .into_iter()
        .map(|(broker_address, topics)| {
            (
                broker_address,
                topics
                    .into_iter()
                    .map(|(name, partitions)| DeleteRecordsTopicV1 { name, partitions })
                    .collect(),
            )
        })
        .collect())
}

fn describe_producers_requests(
    metadata: &MetadataResponseV1,
    topics: &[DescribeProducersTopic],
) -> Result<
    BTreeMap<String, Vec<kafrust_protocol::api::describe_producers::DescribeProducersTopicV0>>,
> {
    let mut requests: BTreeMap<String, BTreeMap<String, Vec<i32>>> = BTreeMap::new();

    for topic in topics {
        for &partition_index in &topic.partitions {
            let topic_metadata = metadata
                .topics
                .iter()
                .find(|candidate| candidate.name == topic.name)
                .ok_or_else(|| Error::UnknownTopicOrPartition {
                    topic: topic.name.clone(),
                    partition: partition_index,
                })?;
            if topic_metadata.error_code != 0 {
                return Err(Error::Broker {
                    code: topic_metadata.error_code,
                    context: format!("metadata for topic {}", topic.name),
                });
            }

            let partition_metadata = topic_metadata
                .partitions
                .iter()
                .find(|candidate| candidate.partition_index == partition_index)
                .ok_or_else(|| Error::UnknownTopicOrPartition {
                    topic: topic.name.clone(),
                    partition: partition_index,
                })?;
            if partition_metadata.error_code != 0 {
                return Err(Error::Broker {
                    code: partition_metadata.error_code,
                    context: format!("metadata for {}-{}", topic.name, partition_index),
                });
            }
            let leader_id = partition_metadata.leader_id;
            if leader_id < 0 {
                return Err(Error::MissingLeader {
                    topic: topic.name.clone(),
                    partition: partition_index,
                });
            }
            let broker = metadata
                .brokers
                .iter()
                .find(|broker| broker.node_id == leader_id)
                .ok_or(Error::MissingBroker { node_id: leader_id })?;
            let broker_address = format!("{}:{}", broker.host, broker.port);
            requests
                .entry(broker_address)
                .or_default()
                .entry(topic.name.clone())
                .or_default()
                .push(partition_index);
        }
    }

    Ok(requests
        .into_iter()
        .map(|(broker_address, topics)| {
            (
                broker_address,
                topics
                    .into_iter()
                    .map(|(name, partition_indexes)| {
                        kafrust_protocol::api::describe_producers::DescribeProducersTopicV0 {
                            name,
                            partition_indexes,
                        }
                    })
                    .collect(),
            )
        })
        .collect())
}

fn duration_millis_i32(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn nonnegative_i32_to_u64(value: i32) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AclFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType, AdminClient,
        AlterConfigsOptions, ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter,
        ClientQuotaFilterComponent, ClientQuotaMatchType, ConfigAlterOperationKind, ConfigSource,
        ConsumerGroupOffset, ConsumerGroupOffsetDelete, ConsumerGroupOffsetQuery,
        CreatePartitionsOptions, CreateTopicsOptions, DeleteRecordsOptions, DeleteRecordsTopic,
        DeleteTopicsOptions, DescribeConfigsOptions, DescribeProducersTopic, ElectLeadersOptions,
        ElectionType, LeaderElection, ListTransactionsOptions, LogDirTopic, NewPartitions,
        NewTopic, PartitionReassignment, PartitionReassignmentOptions, PartitionReassignmentQuery,
        ScramCredentialDeletion, ScramCredentialMechanism, ScramCredentialUpsertion,
        TopicConfigAlteration, TopicConfigResource, TopicConfigUpdate,
    };
    use crate::{BrokerErrorKind, ClientConfig, ClientMetrics, Error};
    use kafrust_protocol::codec::Encoder;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn admin_coordinator_retry_backoff_is_bounded_and_exponential() {
        assert_eq!(
            super::admin_coordinator_retry_backoff(1),
            Duration::from_millis(50)
        );
        assert_eq!(
            super::admin_coordinator_retry_backoff(2),
            Duration::from_millis(100)
        );
        assert_eq!(
            super::admin_coordinator_retry_backoff(5),
            Duration::from_millis(800)
        );
        assert_eq!(
            super::admin_coordinator_retry_backoff(u32::MAX),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn configures_admin_coordinator_retry_budget() {
        let admin = AdminClient::new(ClientConfig::new(["127.0.0.1:9092"])).max_retries(0);
        assert_eq!(admin.max_retries_ref(), 0);

        let admin = AdminClient::new(ClientConfig::new(["127.0.0.1:9092"])).max_retries(9);
        assert_eq!(admin.max_retries_ref(), 9);
    }

    #[test]
    fn validates_admin_connection_configuration_without_network_access() {
        assert!(matches!(
            AdminClient::new(ClientConfig::new(std::iter::empty::<String>())).validate(),
            Err(Error::MissingBootstrapServer)
        ));
        assert!(matches!(
            AdminClient::new(
                ClientConfig::new(["127.0.0.1:9092"])
                    .security_protocol(crate::SecurityProtocol::SaslPlaintext)
            )
            .validate(),
            Err(Error::MissingSaslCredentials)
        ));
        assert!(matches!(
            AdminClient::new(
                ClientConfig::new(["127.0.0.1:9092"])
                    .security_protocol(crate::SecurityProtocol::Tls)
                    .tls_server_name("  ")
            )
            .validate(),
            Err(Error::InvalidTlsServerName { .. })
        ));
    }

    #[tokio::test]
    async fn retries_retryable_admin_bootstrap_connection_before_request() {
        let mut attempts = 0;
        let mut retries = 0;
        let result = super::retry_admin_connection(
            1,
            || {
                attempts += 1;
                let result = if attempts == 1 {
                    Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "bootstrap unavailable",
                    )))
                } else {
                    Ok(42)
                };
                std::future::ready(result)
            },
            || retries += 1,
        )
        .await
        .unwrap();

        assert_eq!(result, 42);
        assert_eq!(attempts, 2);
        assert_eq!(retries, 1);
    }

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
    fn builds_partition_expansion_definitions_and_options() {
        let automatic = NewPartitions::new("orders", 6);
        assert_eq!(automatic.name(), "orders");
        assert_eq!(automatic.count(), 6);
        assert_eq!(automatic.assignments(), None);

        let manual = NewPartitions::with_assignments("payments", 4, [vec![1, 2], vec![2, 1]]);
        assert_eq!(manual.count(), 4);
        assert_eq!(manual.assignments(), Some(&[vec![1, 2], vec![2, 1]][..]));

        let options = CreatePartitionsOptions::new()
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
    fn builds_delete_records_targets_and_options() {
        let topic = DeleteRecordsTopic::new("orders")
            .partition(0, 100)
            .partition(1, -1);
        assert_eq!(topic.name(), "orders");
        assert_eq!(topic.partitions().len(), 2);
        assert_eq!(topic.partitions()[0].partition_index(), 0);
        assert_eq!(topic.partitions()[0].offset(), 100);
        assert_eq!(
            DeleteRecordsOptions::new()
                .timeout(Duration::from_secs(5))
                .timeout_ref(),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn builds_describe_producer_targets() {
        let topic = DescribeProducersTopic::new("orders")
            .partition(0)
            .partition(2);

        assert_eq!(topic.name(), "orders");
        assert_eq!(topic.partitions(), &[0, 2]);
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

    #[test]
    fn builds_classic_topic_config_updates_without_exposing_protocol_types() {
        let update = TopicConfigUpdate::new("orders")
            .set("retention.ms", "60000")
            .delete("segment.ms");

        assert_eq!(update.topic(), "orders");
        assert_eq!(update.configs().len(), 2);
        assert_eq!(update.configs()[0].name(), "retention.ms");
        assert_eq!(update.configs()[0].value(), Some("60000"));
        assert_eq!(update.configs()[1].name(), "segment.ms");
        assert_eq!(update.configs()[1].value(), None);
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
    async fn retries_describe_cluster_after_metadata_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut second, &metadata_response(addr.port())).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin.describe_cluster().await.unwrap();

        assert_eq!(result.controller_id(), 1);
        assert_eq!(result.brokers().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
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
    async fn retries_list_topics_after_metadata_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            assert_eq!(&request[request.len() - 4..], &[0xff, 0xff, 0xff, 0xff]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            assert_eq!(&request[request.len() - 4..], &[0xff, 0xff, 0xff, 0xff]);
            write_frame(&mut second, &topic_metadata_response(addr.port())).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin.list_topics().await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name(), "orders");
        assert_eq!(result[1].error_code(), 3);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_list_topics_after_retryable_metadata_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            assert_eq!(&request[request.len() - 4..], &[0xff, 0xff, 0xff, 0xff]);
            write_frame(&mut first, &topic_metadata_retryable_response(addr.port())).await;

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 3, 0, 1]);
            assert_eq!(&request[request.len() - 4..], &[0xff, 0xff, 0xff, 0xff]);
            write_frame(&mut second, &topic_metadata_response(addr.port())).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin.list_topics().await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name(), "orders");
        assert!(result[0].is_success());
        assert_eq!(result[1].error_code(), 3);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_acls_and_maps_typed_bindings() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 29, 0, 1]);
            write_frame(&mut connection, &describe_acls_response()).await;
        });
        let admin = AdminClient::new(ClientConfig::new([addr.to_string()]));

        let result = admin
            .describe_acls(
                &AclFilter::any()
                    .resource_type(AclResourceType::Topic)
                    .resource_name("orders")
                    .pattern_type(AclPatternType::Literal)
                    .principal("User:alice")
                    .host("*")
                    .operation(AclOperation::Read)
                    .permission_type(AclPermissionType::Allow),
            )
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.bindings().len(), 1);
        let binding = &result.bindings()[0];
        assert_eq!(binding.resource_type(), AclResourceType::Topic);
        assert_eq!(binding.resource_name(), "orders");
        assert_eq!(binding.pattern_type(), AclPatternType::Literal);
        assert_eq!(binding.principal(), "User:alice");
        assert_eq!(binding.operation(), AclOperation::Read);
        assert_eq!(binding.permission_type(), AclPermissionType::Allow);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_acls_after_connection_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 29, 0, 1]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 29, 0, 1]);
            write_frame(&mut second, &describe_acls_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin.describe_acls(&AclFilter::any()).await.unwrap();

        assert!(result.is_success());
        assert_eq!(result.bindings().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn creates_and_deletes_acls_with_partial_results() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut create_connection, _) = listener.accept().await.unwrap();
            let create_request = read_frame(&mut create_connection).await;
            assert_eq!(&create_request[0..4], &[0, 30, 0, 1]);
            write_frame(&mut create_connection, &create_acls_response()).await;

            let (mut delete_connection, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut delete_connection).await;
            assert_eq!(&delete_request[0..4], &[0, 31, 0, 1]);
            write_frame(&mut delete_connection, &delete_acls_response()).await;
        });
        let admin = AdminClient::new(ClientConfig::new([addr.to_string()]));
        let bindings = vec![
            super::AclBinding::new(
                AclResourceType::Topic,
                "orders",
                AclPatternType::Literal,
                "User:alice",
                "*",
                AclOperation::Read,
                AclPermissionType::Allow,
            ),
            super::AclBinding::new(
                AclResourceType::Topic,
                "payments",
                AclPatternType::Literal,
                "User:bob",
                "10.0.0.1",
                AclOperation::Write,
                AclPermissionType::Deny,
            ),
        ];

        let created = admin.create_acls(&bindings).await.unwrap();
        assert!(created.has_errors());
        assert_eq!(created.results().len(), 2);
        assert!(created.results()[0].is_success());
        assert_eq!(created.results()[1].error_code(), 29);
        assert_eq!(created.results()[1].binding().resource_name(), "payments");

        let deleted = admin
            .delete_acls(&[AclFilter::any().resource_type(AclResourceType::Topic)])
            .await
            .unwrap();
        assert!(deleted.is_success());
        assert_eq!(deleted.filter_results().len(), 1);
        assert_eq!(deleted.filter_results()[0].matching_acls().len(), 1);
        assert_eq!(
            deleted.filter_results()[0].matching_acls()[0]
                .binding()
                .principal(),
            "User:alice"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_and_alters_client_quotas_with_typed_results() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut describe_connection, _) = listener.accept().await.unwrap();
            let describe_request = read_frame(&mut describe_connection).await;
            assert_eq!(&describe_request[0..4], &[0, 48, 0, 0]);
            write_frame(&mut describe_connection, &describe_client_quotas_response()).await;

            let (mut alter_connection, _) = listener.accept().await.unwrap();
            let alter_request = read_frame(&mut alter_connection).await;
            assert_eq!(&alter_request[0..4], &[0, 49, 0, 0]);
            write_frame(&mut alter_connection, &alter_client_quotas_response()).await;
        });
        let admin = AdminClient::new(ClientConfig::new([addr.to_string()]));
        let filter = ClientQuotaFilter::any().component(ClientQuotaFilterComponent::new(
            "user",
            ClientQuotaMatchType::Exact,
            Some("alice"),
        ));

        let described = admin.describe_client_quotas(&filter).await.unwrap();
        assert!(described.is_success());
        assert_eq!(described.entries().len(), 1);
        assert_eq!(
            described.entries()[0].entity().components()[0].entity_type(),
            "user"
        );
        assert_eq!(
            described.entries()[0].values()[0].key(),
            "producer_byte_rate"
        );
        assert_eq!(described.entries()[0].values()[0].value(), 1024.5);

        let altered = admin
            .alter_client_quotas(
                &[ClientQuotaAlteration::new(ClientQuotaEntity::user("alice"))
                    .set("producer_byte_rate", 1024.5)],
                false,
            )
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(altered.entries().len(), 1);
        assert_eq!(
            altered.entries()[0].entity().components()[0].entity_name(),
            Some("alice")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_and_alters_scram_credentials_with_controller_routing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut describe_connection, _) = listener.accept().await.unwrap();
            let describe_request = read_frame(&mut describe_connection).await;
            assert_eq!(&describe_request[0..4], &[0, 50, 0, 0]);
            write_frame(
                &mut describe_connection,
                &describe_user_scram_credentials_response(),
            )
            .await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut controller, _) = listener.accept().await.unwrap();
            let alter_request = read_frame(&mut controller).await;
            assert_eq!(&alter_request[0..4], &[0, 51, 0, 0]);
            write_frame(&mut controller, &alter_user_scram_credentials_response()).await;
        });
        let admin = AdminClient::new(ClientConfig::new([addr.to_string()]));
        let users = ["alice".to_owned()];

        let described = admin
            .describe_user_scram_credentials(Some(&users))
            .await
            .unwrap();
        assert!(described.is_success());
        assert_eq!(described.users().len(), 1);
        assert_eq!(described.users()[0].username(), "alice");
        assert_eq!(described.users()[0].credentials().len(), 2);
        assert_eq!(
            described.users()[0].credentials()[0].mechanism(),
            ScramCredentialMechanism::Sha256
        );
        assert_eq!(described.users()[0].credentials()[1].iterations(), 8192);

        let upsertion = ScramCredentialUpsertion::with_salt(
            "alice",
            ScramCredentialMechanism::Sha256,
            4096,
            b"secret",
            [1, 2, 3],
        )
        .unwrap();
        let deletion =
            ScramCredentialDeletion::new("alice", ScramCredentialMechanism::Sha512).unwrap();
        let debug = format!("{upsertion:?}");
        assert!(!debug.contains("secret"));
        let altered = admin
            .alter_user_scram_credentials(&[deletion], &[upsertion])
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(altered.results().len(), 1);
        assert_eq!(altered.results()[0].username(), "alice");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_user_scram_credentials_after_connection_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 50, 0, 0]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 50, 0, 0]);
            write_frame(&mut second, &describe_user_scram_credentials_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );
        let users = ["alice".to_owned()];

        let result = admin
            .describe_user_scram_credentials(Some(&users))
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.users().len(), 1);
        assert_eq!(result.users()[0].username(), "alice");
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_user_scram_credentials_after_retryable_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 50, 0, 0]);
            write_frame(
                &mut first,
                &describe_user_scram_credentials_retryable_response(),
            )
            .await;

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 50, 0, 0]);
            write_frame(&mut second, &describe_user_scram_credentials_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin.describe_user_scram_credentials(None).await.unwrap();

        assert!(result.is_success());
        assert_eq!(result.users().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_client_quotas_after_connection_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 48, 0, 0]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 48, 0, 0]);
            write_frame(&mut second, &describe_client_quotas_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_client_quotas(&ClientQuotaFilter::any())
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.entries().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_list_partition_reassignments_after_controller_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut first_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut first_bootstrap, &metadata_response(addr.port())).await;

            let (mut first_controller, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first_controller).await;
            assert_eq!(&request[0..4], &[0, 46, 0, 0]);
            drop(first_controller);

            let (mut second_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut second_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut second_bootstrap, &metadata_response(addr.port())).await;

            let (mut second_controller, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second_controller).await;
            assert_eq!(&request[0..4], &[0, 46, 0, 0]);
            write_frame(
                &mut second_controller,
                &list_partition_reassignments_response(),
            )
            .await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );
        let query = [PartitionReassignmentQuery::new("orders").partition(0)];

        let result = admin
            .list_partition_reassignments(
                Some(&query),
                PartitionReassignmentOptions::new().timeout(Duration::from_secs(5)),
            )
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.topics().len(), 1);
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_list_partition_reassignments_after_retryable_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut first_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut first_bootstrap, &metadata_response(addr.port())).await;

            let (mut first_controller, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first_controller).await;
            assert_eq!(&request[0..4], &[0, 46, 0, 0]);
            write_frame(
                &mut first_controller,
                &list_partition_reassignments_retryable_response(),
            )
            .await;

            let (mut second_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut second_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut second_bootstrap, &metadata_response(addr.port())).await;

            let (mut second_controller, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second_controller).await;
            assert_eq!(&request[0..4], &[0, 46, 0, 0]);
            write_frame(
                &mut second_controller,
                &list_partition_reassignments_response(),
            )
            .await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .list_partition_reassignments(None, PartitionReassignmentOptions::new())
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.topics().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn alters_and_lists_partition_reassignments_with_controller_routing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut alter_bootstrap, _) = listener.accept().await.unwrap();
            let alter_metadata_request = read_frame(&mut alter_bootstrap).await;
            assert_eq!(&alter_metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut alter_bootstrap, &metadata_response(addr.port())).await;

            let (mut alter_controller, _) = listener.accept().await.unwrap();
            let alter_request = read_frame(&mut alter_controller).await;
            assert_eq!(&alter_request[0..4], &[0, 45, 0, 0]);
            write_frame(
                &mut alter_controller,
                &alter_partition_reassignments_response(),
            )
            .await;

            let (mut list_bootstrap, _) = listener.accept().await.unwrap();
            let list_metadata_request = read_frame(&mut list_bootstrap).await;
            assert_eq!(&list_metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut list_bootstrap, &metadata_response(addr.port())).await;

            let (mut list_controller, _) = listener.accept().await.unwrap();
            let list_request = read_frame(&mut list_controller).await;
            assert_eq!(&list_request[0..4], &[0, 46, 0, 0]);
            write_frame(
                &mut list_controller,
                &list_partition_reassignments_response(),
            )
            .await;
        });
        let admin = AdminClient::new(ClientConfig::new([addr.to_string()]));

        let request = [PartitionReassignment::new("orders").partition(0, [3, 1, 2])];
        let altered = admin
            .alter_partition_reassignments(&request, PartitionReassignmentOptions::new())
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(altered.topics()[0].name(), "orders");
        assert_eq!(altered.topics()[0].partitions()[0].partition_index(), 0);

        let query = [PartitionReassignmentQuery::new("orders").partition(0)];
        let ongoing = admin
            .list_partition_reassignments(
                Some(&query),
                PartitionReassignmentOptions::new().timeout(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        assert!(ongoing.is_success());
        assert_eq!(ongoing.topics()[0].name(), "orders");
        assert_eq!(ongoing.topics()[0].partitions()[0].replicas(), [1, 2, 3]);
        assert_eq!(ongoing.topics()[0].partitions()[0].adding_replicas(), [3]);
        assert_eq!(ongoing.topics()[0].partitions()[0].removing_replicas(), [1]);
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
    async fn retries_topic_config_describe_after_connection_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut first).await;
            assert_eq!(&request[0..4], &[0, 32, 0, 1]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut second).await;
            assert_eq!(&request[0..4], &[0, 32, 0, 1]);
            write_frame(&mut second, &describe_configs_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_topic_configs(
                &[TopicConfigResource::with_keys("orders", ["cleanup.policy"])],
                DescribeConfigsOptions::new(),
            )
            .await
            .unwrap();

        assert!(result.resources()[0].is_success());
        assert_eq!(metrics.snapshot().retries, 1);
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
    async fn alters_classic_topic_configs_and_preserves_resource_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 33, 0, 1]);
            assert_eq!(request.last(), Some(&1));
            write_frame(&mut connection, &classic_alter_configs_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .alter_topic_configs(
                &[
                    TopicConfigUpdate::new("orders").set("retention.ms", "60000"),
                    TopicConfigUpdate::new("payments").delete("retention.ms"),
                ],
                AlterConfigsOptions::new().validate_only(true),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(7));
        assert!(result.has_errors());
        assert_eq!(result.resources().len(), 2);
        assert_eq!(result.resources()[0].resource_type(), 2);
        assert_eq!(result.resources()[0].name(), "orders");
        assert!(result.resources()[0].is_success());
        assert_eq!(result.resources()[1].name(), "payments");
        assert_eq!(result.resources()[1].error_code(), 40);
        assert_eq!(result.resources()[1].error_message(), Some("invalid"));
        assert_eq!(
            result.resources()[1].broker_error_kind(),
            Some(BrokerErrorKind::InvalidConfig)
        );
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_describe_group_to_coordinator_and_preserves_member_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            assert_eq!(coordinator_request.last(), Some(&0));
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 15, 0, 1]);
            write_frame(&mut coordinator, &describe_groups_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let descriptions = admin
            .describe_consumer_groups(&["orders-group".to_owned()])
            .await
            .unwrap();

        assert_eq!(descriptions.len(), 1);
        let description = &descriptions[0];
        assert_eq!(description.group_id(), "orders-group");
        assert_eq!(description.state(), "Stable");
        assert_eq!(description.protocol_type(), "consumer");
        assert_eq!(description.protocol_name(), "range");
        assert!(description.is_success());
        assert_eq!(description.error_code(), 0);
        assert_eq!(description.broker_error_kind(), None);
        assert_eq!(description.throttle_time(), Duration::from_millis(4));
        assert_eq!(description.members().len(), 1);
        let member = &description.members()[0];
        assert_eq!(member.member_id(), "member-1");
        assert_eq!(member.client_id(), "client-1");
        assert_eq!(member.client_host(), "/127.0.0.1");
        assert_eq!(member.member_metadata(), [1, 2]);
        assert_eq!(member.member_assignment(), [3, 4, 5]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_group_after_coordinator_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 15, 0, 1]);
            drop(coordinator);

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 15, 0, 1]);
            write_frame(&mut coordinator, &describe_groups_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let descriptions = admin
            .describe_consumer_groups(&["orders-group".to_owned()])
            .await
            .unwrap();
        assert_eq!(descriptions.len(), 1);
        assert!(descriptions[0].is_success());
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lists_groups_from_cluster_brokers_in_group_id_order() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut broker, _) = listener.accept().await.unwrap();
            let list_request = read_frame(&mut broker).await;
            assert_eq!(&list_request[0..4], &[0, 16, 0, 1]);
            write_frame(&mut broker, &list_groups_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let groups = admin.list_groups().await.unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].group_id(), "connect-cluster");
        assert_eq!(groups[0].protocol_type(), "connect");
        assert_eq!(groups[1].group_id(), "orders-group");
        assert_eq!(groups[1].protocol_type(), "consumer");
        assert_eq!(groups[1].coordinator_id(), 1);
        assert_eq!(groups[1].throttle_time(), Duration::from_millis(7));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_list_groups_after_broker_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut first, _) = listener.accept().await.unwrap();
            let list_request = read_frame(&mut first).await;
            assert_eq!(&list_request[0..4], &[0, 16, 0, 1]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let list_request = read_frame(&mut second).await;
            assert_eq!(&list_request[0..4], &[0, 16, 0, 1]);
            write_frame(&mut second, &list_groups_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let groups = admin.list_groups().await.unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].group_id(), "connect-cluster");
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_list_groups_after_metadata_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut first_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            drop(first_bootstrap);

            let (mut second_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut second_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut second_bootstrap, &metadata_response(addr.port())).await;

            let (mut broker, _) = listener.accept().await.unwrap();
            let list_request = read_frame(&mut broker).await;
            assert_eq!(&list_request[0..4], &[0, 16, 0, 1]);
            write_frame(&mut broker, &list_groups_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let groups = admin.list_groups().await.unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].group_id(), "connect-cluster");
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_delete_group_to_coordinator_and_preserves_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut coordinator).await;
            assert_eq!(&delete_request[0..4], &[0, 42, 0, 1]);
            write_frame(&mut coordinator, &delete_groups_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let results = admin
            .delete_consumer_groups(&["orders-group".to_owned()])
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].group_id(), "orders-group");
        assert!(!results[0].is_success());
        assert_eq!(results[0].error_code(), 68);
        assert_eq!(
            results[0].broker_error_kind(),
            Some(BrokerErrorKind::NonEmptyGroup)
        );
        assert_eq!(results[0].throttle_time(), Duration::from_millis(5));
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_delete_group_after_transient_broker_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut coordinator).await;
            assert_eq!(&delete_request[0..4], &[0, 42, 0, 1]);
            write_frame(&mut coordinator, &delete_groups_response_with_error(16)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut coordinator).await;
            assert_eq!(&delete_request[0..4], &[0, 42, 0, 1]);
            write_frame(&mut coordinator, &delete_groups_response_with_error(0)).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let results = admin
            .delete_consumer_groups(&["orders-group".to_owned()])
            .await
            .unwrap();

        assert!(results[0].is_success());
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_offset_delete_to_coordinator_and_preserves_partition_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut coordinator).await;
            assert_eq!(&delete_request[0..4], &[0, 47, 0, 0]);
            assert_eq!(
                &delete_request[delete_request.len() - 8..],
                &[0, 0, 0, 0, 0, 0, 0, 2]
            );
            write_frame(&mut coordinator, &offset_delete_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .delete_consumer_group_offsets(
                "orders-group",
                &[ConsumerGroupOffsetDelete::new("orders", [0, 2])],
            )
            .await
            .unwrap();

        assert_eq!(result.group_id(), "orders-group");
        assert_eq!(result.error_code(), 0);
        assert_eq!(result.broker_error_kind(), None);
        assert_eq!(result.throttle_time(), Duration::from_millis(5));
        assert!(!result.is_success());
        assert_eq!(result.topics().len(), 1);
        assert_eq!(result.topics()[0].topic(), "orders");
        assert!(!result.topics()[0].is_success());
        assert_eq!(result.topics()[0].partitions().len(), 2);
        assert!(result.topics()[0].partitions()[0].is_success());
        let rejected = result.topics()[0].partitions()[1];
        assert_eq!(rejected.partition_index(), 2);
        assert_eq!(rejected.error_code(), 86);
        assert_eq!(
            rejected.broker_error_kind(),
            Some(BrokerErrorKind::GroupSubscribedToTopic)
        );
        assert_eq!(result.clone().into_topics().len(), 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_offset_delete_after_transient_broker_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut coordinator).await;
            assert_eq!(&delete_request[0..4], &[0, 47, 0, 0]);
            write_frame(&mut coordinator, &offset_delete_retryable_response(16)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let delete_request = read_frame(&mut coordinator).await;
            assert_eq!(&delete_request[0..4], &[0, 47, 0, 0]);
            write_frame(&mut coordinator, &offset_delete_success_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .delete_consumer_group_offsets(
                "orders-group",
                &[ConsumerGroupOffsetDelete::new("orders", [0])],
            )
            .await
            .unwrap();

        assert_eq!(result.error_code(), 0);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lists_and_alters_consumer_group_offsets_through_coordinator() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let fetch_request = read_frame(&mut coordinator).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 2]);
            write_frame(&mut coordinator, &offset_fetch_response()).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let commit_request = read_frame(&mut coordinator).await;
            assert_eq!(&commit_request[0..4], &[0, 8, 0, 2]);
            assert!(commit_request
                .windows(4)
                .any(|bytes| bytes == [0xff, 0xff, 0xff, 0xff]));
            write_frame(&mut coordinator, &offset_commit_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let listed = admin
            .list_consumer_group_offsets(
                "orders-group",
                Some(&[ConsumerGroupOffsetQuery::new("orders", [0, 2])]),
            )
            .await
            .unwrap();
        assert_eq!(listed.group_id(), "orders-group");
        assert_eq!(listed.error_code(), 0);
        assert!(!listed.is_success());
        assert_eq!(listed.topics().len(), 1);
        assert_eq!(listed.topics()[0].topic(), "orders");
        assert_eq!(listed.topics()[0].partitions()[0].committed_offset(), 42);
        assert_eq!(listed.topics()[0].partitions()[0].metadata(), None);
        assert_eq!(listed.topics()[0].partitions()[1].error_code(), 3);
        assert_eq!(
            listed.topics()[0].partitions()[1].broker_error_kind(),
            Some(BrokerErrorKind::UnknownTopicOrPartition)
        );

        let altered = admin
            .alter_consumer_group_offsets(
                "orders-group",
                &[
                    ConsumerGroupOffset::new("orders", 0, 42),
                    ConsumerGroupOffset::new("orders", 2, 99).metadata("admin-reset"),
                ],
            )
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(altered.group_id(), "orders-group");
        assert_eq!(altered.topics().len(), 1);
        assert_eq!(altered.topics()[0].partitions().len(), 2);
        assert_eq!(altered.topics()[0].partitions()[1].partition_index(), 2);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lists_and_alters_member_aware_consumer_group_offsets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let fetch_request = read_frame(&mut coordinator).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 9]);
            assert!(fetch_request
                .windows(9)
                .any(|bytes| bytes[0] == 9 && &bytes[1..] == b"member-a"));
            assert!(fetch_request.windows(4).any(|bytes| bytes == [0, 0, 0, 7]));
            assert_eq!(fetch_request[fetch_request.len() - 2], 1);
            write_frame(&mut coordinator, &offset_fetch_v9_response(0)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let commit_request = read_frame(&mut coordinator).await;
            assert_eq!(&commit_request[0..4], &[0, 8, 0, 9]);
            assert!(commit_request
                .windows(9)
                .any(|bytes| bytes[0] == 9 && &bytes[1..] == b"member-a"));
            assert!(commit_request.windows(4).any(|bytes| bytes == [0, 0, 0, 7]));
            assert!(commit_request.windows(4).any(|bytes| bytes == [0, 0, 0, 5]));
            write_frame(&mut coordinator, &offset_commit_v9_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );
        let query = [ConsumerGroupOffsetQuery::new("orders", [0])];

        let listed = admin
            .list_consumer_group_offsets_with_member(
                "orders-group",
                Some("member-a"),
                7,
                Some(&query),
                true,
            )
            .await
            .unwrap();
        assert_eq!(listed.group_id(), "orders-group");
        assert!(listed.is_success());
        assert_eq!(listed.throttle_time(), Duration::from_millis(12));
        assert_eq!(listed.topics()[0].partitions()[0].committed_offset(), 42);
        assert_eq!(
            listed.topics()[0].partitions()[0].metadata(),
            Some("processed")
        );

        let altered = admin
            .alter_consumer_group_offsets_with_member(
                "orders-group",
                "member-a",
                7,
                None,
                &[ConsumerGroupOffset::new("orders", 0, 43)
                    .leader_epoch(5)
                    .metadata("member-aware")],
            )
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(altered.throttle_time(), Duration::from_millis(12));
        assert_eq!(altered.topics()[0].partitions()[0].partition_index(), 0);
        assert_eq!(metrics.snapshot().broker_errors, 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_member_aware_offset_fetch_after_transient_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            read_frame(&mut coordinator).await;
            write_frame(&mut coordinator, &offset_fetch_v9_response(14)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            read_frame(&mut coordinator).await;
            write_frame(&mut coordinator, &offset_fetch_v9_response(0)).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );
        let listed = admin
            .list_consumer_group_offsets_with_member(
                "orders-group",
                Some("member-a"),
                7,
                None,
                false,
            )
            .await
            .unwrap();

        assert!(listed.is_success());
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_group_coordinator_discovery_after_transient_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(&mut bootstrap, &find_group_coordinator_error_response(14)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let fetch_request = read_frame(&mut coordinator).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 2]);
            write_frame(&mut coordinator, &offset_fetch_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let listed = admin
            .list_consumer_group_offsets("orders-group", None)
            .await
            .unwrap();
        assert_eq!(listed.error_code(), 0);
        assert_eq!(listed.topics().len(), 1);
        assert_eq!(metrics.snapshot().broker_errors, 2);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_consumer_group_offset_fetch_after_coordinator_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let fetch_request = read_frame(&mut coordinator).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 2]);
            drop(coordinator);

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let fetch_request = read_frame(&mut coordinator).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 2]);
            write_frame(&mut coordinator, &offset_fetch_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let listed = admin
            .list_consumer_group_offsets("orders-group", None)
            .await
            .unwrap();
        assert_eq!(listed.error_code(), 0);
        assert_eq!(listed.topics().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_consumer_group_offset_fetch_after_transient_broker_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let fetch_request = read_frame(&mut coordinator).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 2]);
            write_frame(&mut coordinator, &offset_fetch_error_response(14)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let fetch_request = read_frame(&mut coordinator).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 2]);
            write_frame(&mut coordinator, &offset_fetch_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let listed = admin
            .list_consumer_group_offsets("orders-group", None)
            .await
            .unwrap();
        assert_eq!(listed.error_code(), 0);
        assert_eq!(listed.topics().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_consumer_group_offset_commit_after_coordinator_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let commit_request = read_frame(&mut coordinator).await;
            assert_eq!(&commit_request[0..4], &[0, 8, 0, 2]);
            assert!(commit_request
                .windows(4)
                .any(|bytes| bytes == [0xff, 0xff, 0xff, 0xff]));
            drop(coordinator);

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let commit_request = read_frame(&mut coordinator).await;
            assert_eq!(&commit_request[0..4], &[0, 8, 0, 2]);
            assert!(commit_request
                .windows(4)
                .any(|bytes| bytes == [0xff, 0xff, 0xff, 0xff]));
            write_frame(&mut coordinator, &offset_commit_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let altered = admin
            .alter_consumer_group_offsets(
                "orders-group",
                &[ConsumerGroupOffset::new("orders", 0, 42)],
            )
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_consumer_group_offset_commit_after_transient_broker_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let commit_request = read_frame(&mut coordinator).await;
            assert_eq!(&commit_request[0..4], &[0, 8, 0, 2]);
            write_frame(&mut coordinator, &offset_commit_error_response(14)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let commit_request = read_frame(&mut coordinator).await;
            assert_eq!(&commit_request[0..4], &[0, 8, 0, 2]);
            write_frame(&mut coordinator, &offset_commit_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let altered = admin
            .alter_consumer_group_offsets(
                "orders-group",
                &[ConsumerGroupOffset::new("orders", 0, 42)],
            )
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(metrics.snapshot().retries, 1);
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
    async fn retries_create_topics_after_controller_discovery_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut first_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            drop(first_bootstrap);

            let (mut second_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut second_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut second_bootstrap, &metadata_response(addr.port())).await;

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

        assert!(result.has_errors());
        assert_eq!(result.topics()[0].error_code(), 36);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_create_topics_after_retryable_controller_metadata_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut first_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(
                &mut first_bootstrap,
                &topic_metadata_retryable_response(addr.port()),
            )
            .await;

            let (mut second_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut second_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut second_bootstrap, &metadata_response(addr.port())).await;

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

        assert!(result.has_errors());
        assert_eq!(result.topics()[0].error_code(), 36);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_create_partitions_to_controller_and_preserves_partial_result() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut controller, _) = listener.accept().await.unwrap();
            let create_request = read_frame(&mut controller).await;
            assert_eq!(&create_request[0..4], &[0, 37, 0, 0]);
            write_frame(&mut controller, &create_partitions_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .create_partitions(
                &[NewPartitions::new("orders", 3)],
                CreatePartitionsOptions::new(),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(6));
        assert!(result.has_errors());
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(result.topics()[0].error_code(), 37);
        assert_eq!(result.topics()[0].error_message(), Some("invalid"));
        assert_eq!(
            result.topics()[0].broker_error_kind(),
            Some(BrokerErrorKind::InvalidPartitions)
        );
        assert_eq!(result.clone().into_topics().len(), 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_elect_leaders_to_controller_with_negotiated_flexible_version() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut controller, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut controller).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut controller, &api_versions_with_elect_leaders(2)).await;

            let elect_request = read_frame(&mut controller).await;
            assert_eq!(&elect_request[0..4], &[0, 43, 0, 2]);
            assert!(elect_request
                .windows(7)
                .any(|bytes| { bytes == [7, b'o', b'r', b'd', b'e', b'r', b's'] }));
            write_frame(&mut controller, &elect_leaders_v2_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );
        let elections = [LeaderElection::new("orders").partition(0)];

        let result = admin
            .elect_leaders(
                Some(&elections),
                ElectionType::Preferred,
                ElectLeadersOptions::new().timeout(Duration::from_secs(5)),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(9));
        assert!(result.is_success());
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(result.topics()[0].partitions()[0].partition_index(), 0);
        assert_eq!(metrics.snapshot().broker_errors, 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn falls_back_to_preferred_elect_leaders_v0() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut controller, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut controller).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut controller, &api_versions_with_elect_leaders(0)).await;

            let elect_request = read_frame(&mut controller).await;
            assert_eq!(&elect_request[0..4], &[0, 43, 0, 0]);
            write_frame(&mut controller, &elect_leaders_v0_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));
        let elections = [LeaderElection::new("orders").partition(0)];

        let result = admin
            .elect_leaders(
                Some(&elections),
                ElectionType::Preferred,
                ElectLeadersOptions::new(),
            )
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.topics()[0].partitions()[0].error_code(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_log_dirs_on_selected_broker_with_negotiated_v5() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;

            let (mut broker, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut broker).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut broker, &api_versions_with_describe_log_dirs(5)).await;

            let request = read_frame(&mut broker).await;
            assert_eq!(&request[0..4], &[0, 35, 0, 5]);
            assert!(request
                .windows(7)
                .any(|bytes| { bytes == [7, b'o', b'r', b'd', b'e', b'r', b's'] }));
            write_frame(&mut broker, &describe_log_dirs_v5_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));
        let topics = [LogDirTopic::new("orders").partition(0)];

        let results = admin
            .describe_log_dirs(Some(&[1]), Some(&topics))
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert!(results[0].is_success());
        assert_eq!(results[0].broker_id(), 1);
        assert_eq!(results[0].total_bytes(), 100_000);
        assert_eq!(results[0].usable_bytes(), 90_000);
        assert!(!results[0].is_cordoned());
        assert_eq!(results[0].log_dirs()[0].path(), "/var/lib/kafka");
        assert_eq!(results[0].log_dirs()[0].topics()[0].name(), "orders");
        assert_eq!(
            results[0].log_dirs()[0].topics()[0].partitions()[0].partition_size(),
            4096
        );
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

    #[tokio::test]
    async fn routes_delete_records_and_preserves_partition_results() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(
                &mut bootstrap,
                &delete_records_metadata_response(addr.port()),
            )
            .await;

            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 21, 0, 1]);
            assert_eq!(&request[request.len() - 4..], &[0, 0, 117, 48]);
            write_frame(&mut connection, &delete_records_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .delete_records(
                &[
                    DeleteRecordsTopic::new("orders")
                        .partition(0, 100)
                        .partition(1, -1),
                    DeleteRecordsTopic::new("payments").partition(2, 40),
                ],
                DeleteRecordsOptions::new(),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(8));
        assert!(result.has_errors());
        assert_eq!(result.topics().len(), 2);
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(result.topics()[0].partitions()[0].low_watermark(), 100);
        assert!(!result.topics()[0].partitions()[1].is_success());
        assert_eq!(result.topics()[0].partitions()[1].error_code(), 3);
        assert_eq!(
            result.topics()[0].partitions()[1].broker_error_kind(),
            Some(BrokerErrorKind::UnknownTopicOrPartition)
        );
        assert_eq!(result.topics()[1].partitions()[0].partition_index(), 2);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        assert_eq!(result.clone().into_topics().len(), 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_delete_records_after_dropped_leader_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(
                &mut bootstrap,
                &delete_records_metadata_response(addr.port()),
            )
            .await;

            let (mut dropped_leader, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut dropped_leader).await;
            assert_eq!(&request[0..4], &[0, 21, 0, 1]);
            drop(dropped_leader);

            let (mut retry_bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut retry_bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(
                &mut retry_bootstrap,
                &delete_records_metadata_response(addr.port()),
            )
            .await;

            let (mut retry_leader, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut retry_leader).await;
            assert_eq!(&request[0..4], &[0, 21, 0, 1]);
            write_frame(&mut retry_leader, &delete_records_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        )
        .max_retries(1);

        let result = admin
            .delete_records(
                &[
                    DeleteRecordsTopic::new("orders")
                        .partition(0, 100)
                        .partition(1, -1),
                    DeleteRecordsTopic::new("payments").partition(2, 40),
                ],
                DeleteRecordsOptions::new(),
            )
            .await
            .unwrap();

        assert!(result.has_errors());
        assert_eq!(result.topics()[0].partitions()[0].low_watermark(), 100);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_delete_records_after_retryable_metadata_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_frame(&mut first).await;
            write_frame(
                &mut first,
                &delete_records_retryable_metadata_response(addr.port()),
            )
            .await;

            let (mut second, _) = listener.accept().await.unwrap();
            read_frame(&mut second).await;
            write_frame(&mut second, &delete_records_metadata_response(addr.port())).await;

            let (mut leader, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut leader).await;
            assert_eq!(&request[0..4], &[0, 21, 0, 1]);
            write_frame(&mut leader, &delete_records_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .delete_records(
                &[
                    DeleteRecordsTopic::new("orders")
                        .partition(0, 100)
                        .partition(1, -1),
                    DeleteRecordsTopic::new("payments").partition(2, 40),
                ],
                DeleteRecordsOptions::new(),
            )
            .await
            .unwrap();

        assert!(result.has_errors());
        assert_eq!(result.topics()[0].partitions()[0].low_watermark(), 100);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_describe_producers_to_partition_leader() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let metadata_request = read_frame(&mut bootstrap).await;
            assert_eq!(&metadata_request[0..4], &[0, 3, 0, 1]);
            write_frame(
                &mut bootstrap,
                &describe_producers_metadata_response(addr.port()),
            )
            .await;

            let (mut leader, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut leader).await;
            assert_eq!(&request[0..4], &[0, 61, 0, 0]);
            assert_eq!(request.last(), Some(&0));
            write_frame(&mut leader, &describe_producers_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_producers(&[DescribeProducersTopic::new("orders")
                .partition(0)
                .partition(1)])
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(6));
        assert!(result.has_errors());
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(
            result.topics()[0].partitions()[0].active_producers()[0].producer_id(),
            42
        );
        assert_eq!(
            result.topics()[0].partitions()[0].active_producers()[0].last_sequence(),
            17
        );
        assert_eq!(result.topics()[0].partitions()[1].error_code(), 29);
        assert_eq!(
            result.topics()[0].partitions()[1].error_message(),
            Some("denied")
        );
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_producers_after_leader_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &describe_producers_metadata_response(addr.port()),
            )
            .await;

            let (mut leader, _) = listener.accept().await.unwrap();
            read_frame(&mut leader).await;
            drop(leader);

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &describe_producers_metadata_response(addr.port()),
            )
            .await;

            let (mut leader, _) = listener.accept().await.unwrap();
            read_frame(&mut leader).await;
            write_frame(&mut leader, &describe_producers_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_producers(&[DescribeProducersTopic::new("orders")
                .partition(0)
                .partition(1)])
            .await
            .unwrap();

        assert_eq!(result.topics().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_producers_after_transient_leader_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &describe_producers_metadata_response(addr.port()),
            )
            .await;

            let (mut leader, _) = listener.accept().await.unwrap();
            read_frame(&mut leader).await;
            write_frame(&mut leader, &describe_producers_error_response(6)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &describe_producers_metadata_response(addr.port()),
            )
            .await;

            let (mut leader, _) = listener.accept().await.unwrap();
            read_frame(&mut leader).await;
            write_frame(&mut leader, &describe_producers_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_producers(&[DescribeProducersTopic::new("orders")
                .partition(0)
                .partition(1)])
            .await
            .unwrap();

        assert_eq!(result.topics().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 3);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_producers_after_retryable_metadata_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_frame(&mut first).await;
            write_frame(
                &mut first,
                &describe_producers_retryable_metadata_response(addr.port()),
            )
            .await;

            let (mut second, _) = listener.accept().await.unwrap();
            read_frame(&mut second).await;
            write_frame(
                &mut second,
                &describe_producers_metadata_response(addr.port()),
            )
            .await;

            let (mut leader, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut leader).await;
            assert_eq!(&request[0..4], &[0, 61, 0, 0]);
            write_frame(&mut leader, &describe_producers_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_producers(&[DescribeProducersTopic::new("orders")
                .partition(0)
                .partition(1)])
            .await
            .unwrap();

        assert_eq!(result.topics().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        assert_eq!(metrics.snapshot().broker_errors, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_describe_transactions_to_transaction_coordinator() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            assert_eq!(coordinator_request.last(), Some(&1));
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut coordinator).await;
            assert_eq!(&request[0..4], &[0, 65, 0, 0]);
            assert_eq!(request.last(), Some(&0));
            write_frame(&mut coordinator, &describe_transactions_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let result = admin
            .describe_transactions(&["payments-tx".to_owned()])
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(7));
        assert_eq!(result.transactions().len(), 1);
        let transaction = &result.transactions()[0];
        assert_eq!(transaction.transactional_id(), "payments-tx");
        assert_eq!(transaction.state(), "Ongoing");
        assert_eq!(transaction.producer_id(), 99);
        assert_eq!(transaction.producer_epoch(), 4);
        assert_eq!(transaction.topics()[0].topic(), "orders");
        assert_eq!(transaction.topics()[0].partitions(), &[0, 2]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lists_transactions_from_all_broker_shards_with_v1() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(&mut bootstrap, &metadata_response(addr.port())).await;
            drop(bootstrap);

            let (mut broker, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut broker).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut broker, &api_versions_with_list_transactions()).await;
            let list_request = read_frame(&mut broker).await;
            assert_eq!(&list_request[0..4], &[0, 66, 0, 1]);
            write_frame(&mut broker, &list_transactions_response()).await;
        });
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .client_id("kafrust-admin-test")
                .request_timeout_ms(1_000),
        );

        let result = admin
            .list_transactions(
                ListTransactionsOptions::new()
                    .state("Ongoing")
                    .producer_id(99),
            )
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.transactions().len(), 1);
        assert_eq!(result.transactions()[0].transactional_id(), "payments-tx");
        assert_eq!(result.transactions()[0].state(), "Ongoing");
        assert_eq!(result.transactions()[0].producer_id(), 99);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_transactions_after_coordinator_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            read_frame(&mut coordinator).await;
            drop(coordinator);

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            read_frame(&mut coordinator).await;
            write_frame(&mut coordinator, &describe_transactions_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_transactions(&["payments-tx".to_owned()])
            .await
            .unwrap();

        assert_eq!(result.transactions().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_describe_transactions_after_transient_coordinator_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            read_frame(&mut coordinator).await;
            write_frame(&mut coordinator, &describe_transactions_error_response(14)).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            read_frame(&mut bootstrap).await;
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            read_frame(&mut coordinator).await;
            write_frame(&mut coordinator, &describe_transactions_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let result = admin
            .describe_transactions(&["payments-tx".to_owned()])
            .await
            .unwrap();

        assert_eq!(result.transactions().len(), 1);
        assert_eq!(metrics.snapshot().retries, 1);
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

    fn topic_metadata_retryable_response(port: u16) -> Vec<u8> {
        let mut response = topic_metadata_response(port);
        response[37..39].copy_from_slice(&5i16.to_be_bytes());
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

    fn create_partitions_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 6, // throttle time
            0, 0, 0, 1, // result count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 37, // invalid partitions
            0, 7, b'i', b'n', b'v', b'a', b'l', b'i', b'd', // error message
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

    fn delete_records_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 8, // throttle time
            0, 0, 0, 2, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic
            0, 0, 0, 2, // partition count
            0, 0, 0, 0, // partition 0
            0, 0, 0, 0, 0, 0, 0, 100, // low watermark
            0, 0, // success
            0, 0, 0, 1, // partition 1
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // low watermark
            0, 3, // unknown topic or partition
            0, 8, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', // topic
            0, 0, 0, 1, // partition count
            0, 0, 0, 2, // partition 2
            0, 0, 0, 0, 0, 0, 0, 40, // low watermark
            0, 0, // success
        ]
    }

    fn delete_records_metadata_response(port: u16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i32(1); // broker count
        encoder.write_i32(1); // broker node ID
        encoder.write_string("127.0.0.1").unwrap();
        encoder.write_i32(i32::from(port));
        encoder.write_nullable_string(None).unwrap();
        encoder.write_i32(1); // controller ID
        encoder.write_i32(2); // topic count
        write_topic_metadata(&mut encoder, "orders", &[0, 1]);
        write_topic_metadata(&mut encoder, "payments", &[2]);
        encoder.into_bytes()
    }

    fn describe_producers_metadata_response(port: u16) -> Vec<u8> {
        describe_producers_metadata_response_with_error(port, 0)
    }

    fn describe_producers_retryable_metadata_response(port: u16) -> Vec<u8> {
        describe_producers_metadata_response_with_error(port, 5)
    }

    fn describe_producers_metadata_response_with_error(port: u16, topic_error: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i32(1); // broker count
        encoder.write_i32(1); // broker node ID
        encoder.write_string("127.0.0.1").unwrap();
        encoder.write_i32(i32::from(port));
        encoder.write_nullable_string(None).unwrap();
        encoder.write_i32(1); // controller ID
        encoder.write_i32(1); // topic count
        write_topic_metadata_with_error(&mut encoder, "orders", &[0, 1], topic_error);
        encoder.into_bytes()
    }

    fn delete_records_retryable_metadata_response(port: u16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i32(1); // broker count
        encoder.write_i32(1); // broker node ID
        encoder.write_string("127.0.0.1").unwrap();
        encoder.write_i32(i32::from(port));
        encoder.write_nullable_string(None).unwrap();
        encoder.write_i32(1); // controller ID
        encoder.write_i32(2); // topic count
        write_topic_metadata_with_error(&mut encoder, "orders", &[0, 1], 5);
        write_topic_metadata(&mut encoder, "payments", &[2]);
        encoder.into_bytes()
    }

    fn describe_producers_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(6); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_compact_array(Some(&[0_i32, 1_i32]), |encoder, partition| {
                    encoder.write_i32(*partition);
                    if *partition == 0 {
                        encoder.write_i16(0);
                        encoder.write_compact_nullable_string(None)?;
                        encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                            encoder.write_i64(42);
                            encoder.write_i32(3);
                            encoder.write_i32(17);
                            encoder.write_i64(1_700_000_000_000);
                            encoder.write_i32(9);
                            encoder.write_i64(-1);
                            encoder.write_empty_tagged_fields();
                            Ok(())
                        })?;
                    } else {
                        encoder.write_i16(29);
                        encoder.write_compact_nullable_string(Some("denied"))?;
                        encoder.write_compact_array(Some(&[]), |_, ()| Ok(()))?;
                    }
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_producers_error_response(error_code: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(0); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_compact_array(Some(&[0_i32, 1_i32]), |encoder, partition| {
                    encoder.write_i32(*partition);
                    encoder.write_i16(error_code);
                    encoder.write_compact_nullable_string(Some("retry"))?;
                    encoder.write_compact_array(Some(&[]), |_, ()| Ok(()))?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_transactions_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(7); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i16(0);
                encoder.write_compact_string("payments-tx")?;
                encoder.write_compact_string("Ongoing")?;
                encoder.write_i32(60_000);
                encoder.write_i64(1_700_000_000_000);
                encoder.write_i64(99);
                encoder.write_i16(4);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("orders")?;
                    encoder.write_array(Some(&[0_i32, 2_i32]), |encoder, partition| {
                        encoder.write_i32(*partition);
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_list_transactions() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(66);
        encoder.write_i16(0);
        encoder.write_i16(1);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_elect_leaders(max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(43);
        encoder.write_i16(0);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_describe_log_dirs(max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(35);
        encoder.write_i16(1);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn elect_leaders_v2_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(9); // throttle time
        encoder.write_i16(0); // top-level success
        encoder.write_unsigned_varint(2); // one topic result
        encoder.write_compact_string("orders").unwrap();
        encoder.write_unsigned_varint(2); // one partition result
        encoder.write_i32(0); // partition index
        encoder.write_i16(0); // success
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_empty_tagged_fields(); // partition tags
        encoder.write_empty_tagged_fields(); // topic tags
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn describe_log_dirs_v5_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder.write_i16(0); // top-level success
        encoder.write_unsigned_varint(2); // one log directory
        encoder.write_i16(0); // log-directory success
        encoder.write_compact_string("/var/lib/kafka").unwrap();
        encoder.write_unsigned_varint(2); // one topic
        encoder.write_compact_string("orders").unwrap();
        encoder.write_unsigned_varint(2); // one partition
        encoder.write_i32(0);
        encoder.write_i64(4096);
        encoder.write_i64(0);
        encoder.write_bool(false);
        encoder.write_empty_tagged_fields(); // partition tags
        encoder.write_empty_tagged_fields(); // topic tags
        encoder.write_empty_tagged_fields(); // result tags
        encoder.write_i64(100_000);
        encoder.write_i64(90_000);
        encoder.write_bool(false);
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn elect_leaders_v0_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i32(0); // throttle time
        encoder.write_i32(1); // legacy array count: one topic
        encoder.write_string("orders").unwrap();
        encoder.write_i32(1); // legacy array count: one partition
        encoder.write_i32(0);
        encoder.write_i16(0);
        encoder.write_nullable_string(None).unwrap();
        encoder.into_bytes()
    }

    fn list_transactions_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(5); // throttle time
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(1); // no unknown state filters
        encoder.write_unsigned_varint(2); // one transaction
        encoder.write_compact_string("payments-tx").unwrap();
        encoder.write_i64(99);
        encoder.write_compact_string("Ongoing").unwrap();
        encoder.write_empty_tagged_fields();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_transactions_error_response(error_code: i16) -> Vec<u8> {
        let mut response = describe_transactions_response();
        response[10..12].copy_from_slice(&error_code.to_be_bytes());
        response
    }

    fn write_topic_metadata(encoder: &mut Encoder, name: &str, partitions: &[i32]) {
        write_topic_metadata_with_error(encoder, name, partitions, 0);
    }

    fn write_topic_metadata_with_error(
        encoder: &mut Encoder,
        name: &str,
        partitions: &[i32],
        topic_error: i16,
    ) {
        encoder.write_i16(topic_error); // topic error
        encoder.write_string(name).unwrap();
        encoder.write_bool(false);
        encoder
            .write_array(Some(partitions), |encoder, partition| {
                encoder.write_i16(0); // partition error
                encoder.write_i32(*partition);
                encoder.write_i32(1); // leader
                encoder.write_array(Some(&[1]), |encoder, broker| {
                    encoder.write_i32(*broker);
                    Ok(())
                })?;
                encoder.write_array(Some(&[1]), |encoder, broker| {
                    encoder.write_i32(*broker);
                    Ok(())
                })
            })
            .unwrap();
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

    fn describe_acls_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 4, // throttle time
            0, 0, // success
            0xff, 0xff, // null error message
            0, 0, 0, 1, // resource count
            2, // topic resource
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
            3,    // literal pattern
            0, 0, 0, 1, // ACL count
            0, 10, b'U', b's', b'e', b'r', b':', b'a', b'l', b'i', b'c', b'e', // principal
            0, 1, b'*', // host
            3,    // read operation
            3,    // allow permission
        ]
    }

    fn create_acls_response() -> Vec<u8> {
        vec![
            0, 0, 0, 2, // correlation ID
            0, 0, 0, 5, // throttle time
            0, 0, 0, 2, // result count
            0, 0, // success
            0xff, 0xff, // null error message
            0, 29, // cluster authorization failed
            0, 7, b'd', b'e', b'n', b'i', b'e', b'd', b'!', // error message
        ]
    }

    fn delete_acls_response() -> Vec<u8> {
        vec![
            0, 0, 0, 3, // correlation ID
            0, 0, 0, 6, // throttle time
            0, 0, 0, 1, // filter result count
            0, 0, // filter success
            0xff, 0xff, // null filter error message
            0, 0, 0, 1, // matching ACL count
            0, 0, // ACL deletion success
            0xff, 0xff, // null ACL error message
            2,    // topic resource
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // resource name
            3,    // literal pattern
            0, 10, b'U', b's', b'e', b'r', b':', b'a', b'l', b'i', b'c', b'e', // principal
            0, 1, b'*', // host
            3,    // read operation
            3,    // allow permission
        ]
    }

    fn describe_client_quotas_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i32(7); // throttle time
        encoder.write_i16(0); // success
        encoder.write_nullable_string(None).unwrap();
        encoder.write_i32(1); // entry count
        encoder.write_i32(1); // entity count
        encoder.write_string("user").unwrap();
        encoder.write_nullable_string(Some("alice")).unwrap();
        encoder.write_i32(1); // value count
        encoder.write_string("producer_byte_rate").unwrap();
        encoder.write_f64(1024.5);
        encoder.into_bytes()
    }

    fn alter_client_quotas_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(2); // correlation ID
        encoder.write_i32(5); // throttle time
        encoder.write_i32(1); // result count
        encoder.write_i16(0); // success
        encoder.write_nullable_string(None).unwrap();
        encoder.write_i32(1); // entity count
        encoder.write_string("user").unwrap();
        encoder.write_nullable_string(Some("alice")).unwrap();
        encoder.into_bytes()
    }

    fn describe_user_scram_credentials_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(3); // throttle time
        encoder.write_i16(0); // success
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_unsigned_varint(2); // one user
        encoder.write_compact_string("alice").unwrap();
        encoder.write_i16(0); // success
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_unsigned_varint(3); // two credentials
        encoder.write_i8(1); // SCRAM-SHA-256
        encoder.write_i32(4096);
        encoder.write_empty_tagged_fields();
        encoder.write_i8(2); // SCRAM-SHA-512
        encoder.write_i32(8192);
        encoder.write_empty_tagged_fields();
        encoder.write_empty_tagged_fields();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_user_scram_credentials_retryable_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(0); // throttle time
        encoder.write_i16(41); // not controller
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_unsigned_varint(1); // no user results
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn alter_user_scram_credentials_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder.write_unsigned_varint(2); // one result
        encoder.write_compact_string("alice").unwrap();
        encoder.write_i16(0); // success
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_empty_tagged_fields();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn alter_partition_reassignments_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder.write_i16(0); // top-level success
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_unsigned_varint(2); // one topic
        encoder.write_compact_string("orders").unwrap();
        encoder.write_unsigned_varint(2); // one partition
        encoder.write_i32(0); // partition index
        encoder.write_i16(0); // success
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_empty_tagged_fields(); // partition tags
        encoder.write_empty_tagged_fields(); // topic tags
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn list_partition_reassignments_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(5); // throttle time
        encoder.write_i16(0); // success
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_unsigned_varint(2); // one topic
        encoder.write_compact_string("orders").unwrap();
        encoder.write_unsigned_varint(2); // one partition
        encoder.write_i32(0); // partition index
        encoder
            .write_array(Some(&[1, 2, 3]), |encoder, replica| {
                encoder.write_i32(*replica);
                Ok(())
            })
            .unwrap();
        encoder
            .write_array(Some(&[3]), |encoder, replica| {
                encoder.write_i32(*replica);
                Ok(())
            })
            .unwrap();
        encoder
            .write_array(Some(&[1]), |encoder, replica| {
                encoder.write_i32(*replica);
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields(); // partition tags
        encoder.write_empty_tagged_fields(); // topic tags
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn list_partition_reassignments_retryable_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(0); // throttle time
        encoder.write_i16(41); // not controller
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_unsigned_varint(1); // no topic results
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
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

    fn classic_alter_configs_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 7, // throttle time
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

    fn find_group_coordinator_response(port: u16) -> Vec<u8> {
        let mut response = vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 0, // throttle time
            0, 0, // success
            0xff, 0xff, // null error message
            0, 0, 0, 1, // node ID
            0, 9, b'1', b'2', b'7', b'.', b'0', b'.', b'0', b'.', b'1', // host
        ];
        response.extend_from_slice(&i32::from(port).to_be_bytes());
        response
    }

    fn find_group_coordinator_error_response(error_code: i16) -> Vec<u8> {
        let mut response = find_group_coordinator_response(0);
        response[8..10].copy_from_slice(&error_code.to_be_bytes());
        response
    }

    fn describe_groups_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 4, // throttle time
            0, 0, 0, 1, // group count
            0, 0, // success
            0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u', b'p', 0, 6,
            b'S', b't', b'a', b'b', b'l', b'e', // state
            0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // protocol type
            0, 5, b'r', b'a', b'n', b'g', b'e', // protocol
            0, 0, 0, 1, // member count
            0, 8, b'm', b'e', b'm', b'b', b'e', b'r', b'-', b'1', // member ID
            0, 8, b'c', b'l', b'i', b'e', b'n', b't', b'-', b'1', // client ID
            0, 10, b'/', b'1', b'2', b'7', b'.', b'0', b'.', b'0', b'.', b'1', // client host
            0, 0, 0, 2, 1, 2, // member metadata
            0, 0, 0, 3, 3, 4, 5, // member assignment
        ]
    }

    fn list_groups_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 7, // throttle time
            0, 0, // success
            0, 0, 0, 2, // group count
            0, 12, b'o', b'r', b'd', b'e', b'r', b's', b'-', b'g', b'r', b'o', b'u', b'p', 0, 8,
            b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r', // consumer group
            0, 15, b'c', b'o', b'n', b'n', b'e', b'c', b't', b'-', b'c', b'l', b'u', b's', b't',
            b'e', b'r', 0, 7, b'c', b'o', b'n', b'n', b'e', b'c', b't', // connect group
        ]
    }

    fn delete_groups_response() -> Vec<u8> {
        delete_groups_response_with_error(68)
    }

    fn delete_groups_response_with_error(error_code: i16) -> Vec<u8> {
        vec![
            0,
            0,
            0,
            1, // correlation ID
            0,
            0,
            0,
            5, // throttle time
            0,
            0,
            0,
            1, // result count
            0,
            12,
            b'o',
            b'r',
            b'd',
            b'e',
            b'r',
            b's',
            b'-',
            b'g',
            b'r',
            b'o',
            b'u',
            b'p',
            (error_code >> 8) as u8,
            error_code as u8,
        ]
    }

    fn offset_delete_retryable_response(error_code: i16) -> Vec<u8> {
        vec![
            0,
            0,
            0,
            1, // correlation ID
            (error_code >> 8) as u8,
            error_code as u8,
            0,
            0,
            0,
            0, // throttle time
            0,
            0,
            0,
            0, // no topic results
        ]
    }

    fn offset_delete_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, // top-level success
            0, 0, 0, 5, // throttle time
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 0, 0, 2, // partition count
            0, 0, 0, 0, // partition 0
            0, 0, // success
            0, 0, 0, 2, // partition 2
            0, 86, // group subscribed to topic
        ]
    }

    fn offset_delete_success_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, // top-level success
            0, 0, 0, 5, // throttle time
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 0, 0, 1, // partition count
            0, 0, 0, 0, // partition 0
            0, 0, // success
        ]
    }

    fn offset_fetch_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 0, 0, 2, // partition count
            0, 0, 0, 0, // partition 0
            0, 0, 0, 0, 0, 0, 0, 42, // committed offset
            0xff, 0xff, // null metadata
            0, 0, // success
            0, 0, 0, 2, // partition 2
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // no offset
            0xff, 0xff, // null metadata
            0, 3, // unknown topic or partition
            0, 0, // top-level success
        ]
    }

    fn offset_fetch_error_response(error_code: i16) -> Vec<u8> {
        let mut response = offset_fetch_response();
        let last = response.len();
        response[last - 2..].copy_from_slice(&error_code.to_be_bytes());
        response
    }

    fn offset_fetch_v9_response(group_error_code: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // response correlation ID
        encoder.write_empty_tagged_fields(); // flexible response header
        encoder.write_i32(12); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders-group")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("orders")?;
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_i32(0);
                        encoder.write_i64(42);
                        encoder.write_i32(-1);
                        encoder.write_compact_nullable_string(Some("processed"))?;
                        encoder.write_i16(0);
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i16(group_error_code);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn offset_commit_response() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation ID
            0, 0, 0, 1, // topic count
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0, 0, 0, 2, // partition count
            0, 0, 0, 0, 0, 0, // partition 0, success
            0, 0, 0, 2, 0, 0, // partition 2, success
        ]
    }

    fn offset_commit_error_response(error_code: i16) -> Vec<u8> {
        let mut response = offset_commit_response();
        let last = response.len();
        response[last - 2..].copy_from_slice(&error_code.to_be_bytes());
        response
    }

    fn offset_commit_v9_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // response correlation ID
        encoder.write_empty_tagged_fields(); // flexible response header
        encoder.write_i32(12); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(0);
                    encoder.write_i16(0);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }
}
