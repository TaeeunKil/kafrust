use std::collections::{BTreeMap, BTreeSet};
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
use kafrust_protocol::api::alter_replica_log_dirs::{
    AlterReplicaLogDir, AlterReplicaLogDirTopicResult, AlterReplicaLogDirsResponse,
};
use kafrust_protocol::api::alter_user_scram_credentials::{
    AlterUserScramCredentialsDeletionV0, AlterUserScramCredentialsUpsertionV0,
};
use kafrust_protocol::api::api_versions::ApiVersionsResponseV3;
use kafrust_protocol::api::consumer_group_describe::{
    ConsumerGroupDescribeAssignment, ConsumerGroupDescribeResponseV0,
    ConsumerGroupDescribeResponseV1, ConsumerGroupDescribeTopicPartitions, DescribedConsumerGroup,
    DescribedConsumerGroupMember,
};
use kafrust_protocol::api::create_acls::CreateAclsCreationV1;
use kafrust_protocol::api::create_partitions::{
    CreatePartitionsAssignmentV0, CreatePartitionsTopicResultV0, CreatePartitionsTopicV0,
};
use kafrust_protocol::api::create_topics::{
    CreateTopicsAssignmentV2, CreateTopicsConfigV2, CreateTopicsTopicResultV2, CreateTopicsTopicV2,
};
use kafrust_protocol::api::delegation_token::{
    CreateDelegationTokenRequest, CreateDelegationTokenResponse,
    DelegationTokenPrincipal as ProtocolDelegationTokenPrincipal, DescribeDelegationTokenResponse,
    DescribedDelegationToken, RenewDelegationTokenResponse,
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
use kafrust_protocol::api::describe_cluster::DescribeClusterResponse;
use kafrust_protocol::api::describe_configs::{
    DescribeConfigsEntryV1, DescribeConfigsEntryV4, DescribeConfigsResourceV1,
    DescribeConfigsResourceV4, DescribeConfigsResultV1, DescribeConfigsResultV4,
    DescribeConfigsSynonymV1, DescribeConfigsSynonymV4,
};
use kafrust_protocol::api::describe_groups::{DescribeGroupsGroupV1, DescribeGroupsMemberV1};
use kafrust_protocol::api::describe_log_dirs::{DescribeLogDirsResponse, DescribeLogDirsTopic};
use kafrust_protocol::api::describe_producers::{
    DescribeProducersActiveProducerV0, DescribeProducersPartitionResponseV0,
    DescribeProducersTopicResponseV0,
};
use kafrust_protocol::api::describe_quorum::DescribeQuorumResponse;
use kafrust_protocol::api::describe_share_group_offsets::{
    DescribeShareGroupOffsetsGroup, DescribeShareGroupOffsetsGroupResultV0,
    DescribeShareGroupOffsetsGroupResultV1, DescribeShareGroupOffsetsTopic,
    DescribeShareGroupOffsetsTopicResultV0, DescribeShareGroupOffsetsTopicResultV1,
};
use kafrust_protocol::api::describe_topic_partitions::DescribeTopicPartitionsResponseV0;
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
use kafrust_protocol::api::find_coordinator::CoordinatorType;
use kafrust_protocol::api::incremental_alter_configs::{
    IncrementalAlterConfigsEntryV0, IncrementalAlterConfigsResourceResponseV0,
    IncrementalAlterConfigsResourceV0,
};
use kafrust_protocol::api::list_config_resources::ListedConfigResourceV1;
use kafrust_protocol::api::list_groups::{
    ListGroupsResponseV1, ListGroupsResponseV4, ListGroupsResponseV5, ListedGroupV1, ListedGroupV4,
    ListedGroupV5, API_KEY as LIST_GROUPS_API_KEY,
};
use kafrust_protocol::api::list_partition_reassignments::ListPartitionReassignmentsTopicV0;
use kafrust_protocol::api::list_transactions::ListedTransactionV0;
use kafrust_protocol::api::metadata::{
    BrokerMetadata, MetadataRequestTopicV12, MetadataResponseV1, TopicMetadata,
};
use kafrust_protocol::api::offset_commit::{
    OffsetCommitPartition, OffsetCommitPartitionV10, OffsetCommitPartitionV9,
    OffsetCommitResponseV10, OffsetCommitTopic, OffsetCommitTopicResponse, OffsetCommitTopicV10,
    OffsetCommitTopicV9,
};
use kafrust_protocol::api::offset_delete::{
    OffsetDeleteRequestPartitionV0, OffsetDeleteRequestTopicV0, OffsetDeleteResponsePartitionV0,
    OffsetDeleteResponseTopicV0,
};
use kafrust_protocol::api::offset_fetch::{
    OffsetFetchGroupResponse, OffsetFetchResponseV10, OffsetFetchTopic, OffsetFetchTopicResponse,
    OffsetFetchTopicV10, OffsetFetchTopicV9,
};
use kafrust_protocol::api::raft_voter::{
    AddRaftVoterResponse, RaftVoterListener as ProtocolRaftVoterListener,
};
use kafrust_protocol::api::share_group_describe::{
    DescribedShareGroup, DescribedShareGroupMember, ShareGroupDescribeAssignment,
    ShareGroupDescribeTopicPartitions,
};
use kafrust_protocol::api::share_group_offsets::{
    AlterShareGroupOffsetsPartitionV0, AlterShareGroupOffsetsResponseV0,
    AlterShareGroupOffsetsTopicResultV0, AlterShareGroupOffsetsTopicV0,
    DeleteShareGroupOffsetsResponseV0, DeleteShareGroupOffsetsTopicResultV0,
    DeleteShareGroupOffsetsTopicV0,
};
use kafrust_protocol::api::share_group_state::{
    DeleteShareGroupStateTopic, InitializeShareGroupStatePartition, InitializeShareGroupStateTopic,
    ReadShareGroupStatePartition, ReadShareGroupStateResponseV0,
    ReadShareGroupStateSummaryResponseV0, ReadShareGroupStateSummaryResponseV1,
    ReadShareGroupStateSummaryTopicResult as ProtocolShareGroupStateSummaryTopicResult,
    ReadShareGroupStateTopic, ShareGroupStateBatch as ProtocolShareGroupStateBatch,
    ShareGroupStatePartitionResult as ProtocolShareGroupStatePartitionResult,
    ShareGroupStateResultResponse,
    ShareGroupStateTopicResult as ProtocolShareGroupStateTopicResult,
    WriteShareGroupStatePartitionV0, WriteShareGroupStatePartitionV1, WriteShareGroupStateTopicV0,
    WriteShareGroupStateTopicV1,
};
use kafrust_protocol::api::streams_group_describe::{
    DescribedStreamsGroup as ProtocolDescribedStreamsGroup,
    DescribedStreamsGroupMember as ProtocolDescribedStreamsGroupMember,
    StreamsGroupAssignment as ProtocolStreamsGroupAssignment,
    StreamsGroupEndpoint as ProtocolStreamsGroupEndpoint,
    StreamsGroupKeyValue as ProtocolStreamsGroupKeyValue,
    StreamsGroupSubtopology as ProtocolStreamsGroupSubtopology,
    StreamsGroupTask as ProtocolStreamsGroupTask,
    StreamsGroupTaskOffset as ProtocolStreamsGroupTaskOffset,
    StreamsGroupTopic as ProtocolStreamsGroupTopic,
    StreamsGroupTopicConfig as ProtocolStreamsGroupTopicConfig,
    StreamsGroupTopology as ProtocolStreamsGroupTopology,
};
use kafrust_protocol::api::unregister_broker::UnregisterBrokerResponseV0;
use kafrust_protocol::api::update_features::UpdateFeaturesResponseV0;

use crate::client::{format_share_partition_coordinator_key, Client};
use crate::config::ClientConfig;
use crate::error::{BrokerErrorKind, Error, Result};
use crate::metrics::ClientMetrics;
use crate::scram::{derive_salted_password, ScramHash};
use rand::RngCore;

const ADMIN_COORDINATOR_MAX_RETRIES: u32 = 5;
const ADMIN_COORDINATOR_RETRY_BACKOFF_BASE: Duration = Duration::from_millis(50);
const ADMIN_COORDINATOR_MAX_RETRY_BACKOFF: Duration = Duration::from_millis(800);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShareStateResource {
    topic_id: [u8; 16],
    partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ShareStateCoordinatorEndpoint {
    host: String,
    port: i32,
}

struct MemberOffsetFetchV10Request {
    group_id: String,
    member_id: Option<String>,
    member_epoch: i32,
    topics: Vec<OffsetFetchTopicV10>,
    topic_names: BTreeMap<[u8; 16], String>,
    require_stable: bool,
}

struct MemberOffsetCommitV10Request {
    group_id: String,
    member_id: String,
    member_epoch: i32,
    group_instance_id: Option<String>,
    topics: Vec<OffsetCommitTopicV10>,
    topic_names: BTreeMap<[u8; 16], String>,
}

enum ListGroupsResponse {
    V1(ListGroupsResponseV1),
    V4(ListGroupsResponseV4),
    V5(ListGroupsResponseV5),
}

impl ListGroupsResponse {
    fn error_code(&self) -> i16 {
        match self {
            Self::V1(response) => response.error_code,
            Self::V4(response) => response.error_code,
            Self::V5(response) => response.error_code,
        }
    }

    fn throttle_time_ms(&self) -> i32 {
        match self {
            Self::V1(response) => response.throttle_time_ms,
            Self::V4(response) => response.throttle_time_ms,
            Self::V5(response) => response.throttle_time_ms,
        }
    }

    fn api_version(&self) -> i16 {
        match self {
            Self::V1(_) => 1,
            Self::V4(_) => 4,
            Self::V5(_) => 5,
        }
    }

    fn into_group_listings(
        self,
        coordinator_id: i32,
        throttle_time: Duration,
    ) -> Vec<GroupListing> {
        let api_version = self.api_version();
        match self {
            Self::V1(response) => response
                .groups
                .into_iter()
                .map(|group| {
                    GroupListing::from_protocol_v1(
                        group,
                        coordinator_id,
                        throttle_time,
                        api_version,
                    )
                })
                .collect(),
            Self::V4(response) => response
                .groups
                .into_iter()
                .map(|group| {
                    GroupListing::from_protocol_v4(
                        group,
                        coordinator_id,
                        throttle_time,
                        api_version,
                    )
                })
                .collect(),
            Self::V5(response) => response
                .groups
                .into_iter()
                .map(|group| {
                    GroupListing::from_protocol_v5(
                        group,
                        coordinator_id,
                        throttle_time,
                        api_version,
                    )
                })
                .collect(),
        }
    }
}

fn admin_mutation_error(client: &Client, operation: &'static str, error: Error) -> Error {
    if client.last_request_may_have_been_transmitted()
        && matches!(
            error,
            Error::Io(_)
                | Error::RequestTimedOut { .. }
                | Error::ResponseTooLarge { .. }
                | Error::Protocol(_)
        )
    {
        Error::AdminMutationOutcomeUnknown { operation }
    } else {
        error
    }
}

/// Kafka administration client.
///
/// Each controller-scoped operation uses explicitly configured KRaft controller
/// bootstrap servers when present. Otherwise it discovers the controller
/// through cluster metadata before opening the controller connection.
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

    /// Validates and returns this admin client without opening a broker
    /// connection.
    pub fn build_config(self) -> Result<Self> {
        self.validate()?;
        Ok(self)
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

    async fn resolve_topic_ids_from_metadata(
        &self,
        coordinator: &mut Client,
        requested_names: Option<&[String]>,
    ) -> Result<Option<BTreeMap<String, [u8; 16]>>> {
        if !coordinator.supports_metadata_v12().await? {
            return Ok(None);
        }

        let request_topics = requested_names.map(|names| {
            names
                .iter()
                .map(|name| MetadataRequestTopicV12 {
                    topic_id: [0; 16],
                    name: Some(name.clone()),
                })
                .collect::<Vec<_>>()
        });
        let response = coordinator.metadata_v12(request_topics).await?;
        if response.topics.iter().any(|topic| topic.error_code != 0) {
            return Ok(None);
        }

        let mut topic_ids = BTreeMap::new();
        let mut names_by_topic_id = BTreeMap::new();
        for topic in response.topics {
            let Some(name) = topic.name else {
                continue;
            };
            if topic.topic_id == [0; 16] {
                continue;
            }
            if let Some(previous) = names_by_topic_id.insert(topic.topic_id, name.clone()) {
                if previous != name {
                    return Ok(None);
                }
            }
            if let Some(previous) = topic_ids.insert(name, topic.topic_id) {
                if previous != topic.topic_id {
                    return Ok(None);
                }
            }
        }

        if requested_names
            .is_some_and(|names| names.iter().any(|name| !topic_ids.contains_key(name)))
        {
            return Ok(None);
        }
        Ok(Some(topic_ids))
    }

    async fn offset_fetch_topics_v10_with_metadata(
        &self,
        coordinator: &mut Client,
        topics: Option<&[ConsumerGroupOffsetQuery]>,
    ) -> Result<Option<(Option<Vec<OffsetFetchTopicV10>>, BTreeMap<[u8; 16], String>)>> {
        let Some(topics) = topics else {
            let topic_ids = self
                .resolve_topic_ids_from_metadata(coordinator, None)
                .await?;
            return Ok(topic_ids.map(|topic_ids| {
                (
                    None,
                    topic_ids
                        .into_iter()
                        .map(|(name, topic_id)| (topic_id, name))
                        .collect(),
                )
            }));
        };

        if let Some(request_topics) = offset_fetch_topics_v10(Some(topics)) {
            return Ok(Some((
                Some(request_topics),
                topic_names_by_id_from_queries(topics),
            )));
        }

        let mut topic_ids_by_name = BTreeMap::new();
        let mut missing_names = BTreeSet::new();
        for topic in topics {
            if let Some(topic_id) = nonzero_topic_id(topic.topic_id) {
                if let Some(previous) = topic_ids_by_name.insert(topic.topic.clone(), topic_id) {
                    if previous != topic_id {
                        return Ok(None);
                    }
                }
            } else {
                missing_names.insert(topic.topic.clone());
            }
        }
        let missing_names = missing_names
            .into_iter()
            .filter(|name| !topic_ids_by_name.contains_key(name))
            .collect::<Vec<_>>();
        if !missing_names.is_empty() {
            let Some(resolved) = self
                .resolve_topic_ids_from_metadata(coordinator, Some(&missing_names))
                .await?
            else {
                return Ok(None);
            };
            for (name, topic_id) in resolved {
                if let Some(previous) = topic_ids_by_name.insert(name, topic_id) {
                    if previous != topic_id {
                        return Ok(None);
                    }
                }
            }
        }

        let mut topics_by_id = BTreeMap::<[u8; 16], Vec<i32>>::new();
        let mut topic_names = BTreeMap::new();
        for topic in topics {
            let Some(&topic_id) = topic_ids_by_name.get(&topic.topic) else {
                return Ok(None);
            };
            if let Some(previous) = topic_names.insert(topic_id, topic.topic.clone()) {
                if previous != topic.topic {
                    return Ok(None);
                }
            }
            topics_by_id
                .entry(topic_id)
                .or_default()
                .extend(topic.partitions.iter().copied());
        }
        let request_topics = topics_by_id
            .into_iter()
            .map(|(topic_id, partition_indexes)| OffsetFetchTopicV10 {
                topic_id,
                partition_indexes,
            })
            .collect();
        Ok(Some((Some(request_topics), topic_names)))
    }

    async fn offset_commit_topics_v10_with_metadata(
        &self,
        coordinator: &mut Client,
        offsets: &[ConsumerGroupOffset],
    ) -> Result<Option<(Vec<OffsetCommitTopicV10>, BTreeMap<[u8; 16], String>)>> {
        if let Some(topics) = offset_commit_topics_v10(offsets) {
            return Ok(Some((topics, topic_names_by_id_from_offsets(offsets))));
        }

        if offsets.is_empty() {
            return Ok(None);
        }

        let mut topic_ids_by_name = BTreeMap::new();
        let mut missing_names = BTreeSet::new();
        for offset in offsets {
            if let Some(topic_id) = nonzero_topic_id(offset.topic_id) {
                if let Some(previous) = topic_ids_by_name.insert(offset.topic.clone(), topic_id) {
                    if previous != topic_id {
                        return Ok(None);
                    }
                }
            } else {
                missing_names.insert(offset.topic.clone());
            }
        }
        let missing_names = missing_names
            .into_iter()
            .filter(|name| !topic_ids_by_name.contains_key(name))
            .collect::<Vec<_>>();
        if !missing_names.is_empty() {
            let Some(resolved) = self
                .resolve_topic_ids_from_metadata(coordinator, Some(&missing_names))
                .await?
            else {
                return Ok(None);
            };
            for (name, topic_id) in resolved {
                if let Some(previous) = topic_ids_by_name.insert(name, topic_id) {
                    if previous != topic_id {
                        return Ok(None);
                    }
                }
            }
        }

        let mut topics_by_id = BTreeMap::<[u8; 16], Vec<OffsetCommitPartitionV10>>::new();
        let mut topic_names = BTreeMap::new();
        for offset in offsets {
            let Some(&topic_id) = topic_ids_by_name.get(&offset.topic) else {
                return Ok(None);
            };
            if let Some(previous) = topic_names.insert(topic_id, offset.topic.clone()) {
                if previous != offset.topic {
                    return Ok(None);
                }
            }
            topics_by_id
                .entry(topic_id)
                .or_default()
                .push(OffsetCommitPartitionV10 {
                    partition_index: offset.partition,
                    committed_offset: offset.offset,
                    committed_leader_epoch: offset.leader_epoch,
                    committed_metadata: offset.metadata.clone(),
                });
        }
        let topics = topics_by_id
            .into_iter()
            .map(|(topic_id, partitions)| OffsetCommitTopicV10 {
                topic_id,
                partitions,
            })
            .collect();
        Ok(Some((topics, topic_names)))
    }

    /// Describes the Kafka cluster brokers and active controller.
    #[tracing::instrument(level = "debug", name = "kafka.admin.describe_cluster", skip_all, err)]
    pub async fn describe_cluster(&self) -> Result<ClusterDescription> {
        let metadata = self.metadata_with_admin_retries(Some(Vec::new())).await?;

        Ok(ClusterDescription::from_metadata(metadata))
    }

    /// Describes the cluster through Kafka's dedicated DescribeCluster API.
    ///
    /// Kafka 3.7 and newer brokers advertise API 60. The v1 path preserves
    /// endpoint type and the v0 fallback remains available for brokers that
    /// only advertise the original flexible version. If API 60 is absent,
    /// this method falls back to Metadata so older deployments retain the
    /// established cluster description behavior.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_cluster_with_options",
        skip_all,
        err
    )]
    pub async fn describe_cluster_with_options(
        &self,
        options: DescribeClusterOptions,
    ) -> Result<ClusterDescription> {
        let mut retry = 0;
        loop {
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
            let api_versions = match client
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(response) => response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if api_versions.error_code != 0 {
                return Err(client.broker_error(
                    api_versions.error_code,
                    "describe cluster capabilities".to_owned(),
                ));
            }

            let Some(api_version) = api_versions.highest_supported_version(60, 1) else {
                let metadata = self
                    .metadata_with_admin_retries_from(Some(Vec::new()), &mut retry)
                    .await?;
                return Ok(ClusterDescription::from_metadata(metadata));
            };
            if options.endpoint_type.is_some() && api_version < 1 {
                return Err(Error::Unsupported(
                    "DescribeCluster endpoint selection requires v1",
                ));
            }
            let endpoint_type = options
                .endpoint_type
                .unwrap_or(DescribeClusterEndpointType::Brokers)
                .code();
            let response = if api_version >= 1 {
                client
                    .describe_cluster_v1(
                        options.include_cluster_authorized_operations,
                        endpoint_type,
                    )
                    .await
            } else {
                client
                    .describe_cluster_v0(options.include_cluster_authorized_operations)
                    .await
            };
            match response {
                Ok(response)
                    if response.error_code != 0
                        && retry < self.max_retries
                        && is_retryable_admin_read_code(response.error_code) =>
                {
                    retry += 1;
                    self.config.record_broker_error();
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Ok(response) if response.error_code != 0 => {
                    return Err(
                        client.broker_error(response.error_code, "describe cluster".to_owned())
                    );
                }
                Ok(response) => return Ok(ClusterDescription::from_describe_cluster(response)),
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Describes broker-supported and cluster-finalized Kafka features.
    ///
    /// Kafka exposes this metadata through the tagged fields of ApiVersions
    /// v3+, so this method performs a capability handshake against one broker
    /// and returns the typed feature view without issuing a second request.
    #[tracing::instrument(level = "debug", name = "kafka.admin.describe_features", skip_all, err)]
    pub async fn describe_features(&self) -> Result<FeatureMetadata> {
        let mut retry = 0;
        loop {
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
            let response = match client
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(response) => response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if response.error_code != 0 {
                return Err(
                    client.broker_error(response.error_code, "describe Kafka features".to_owned())
                );
            }
            return Ok(FeatureMetadata::from_protocol(response));
        }
    }

    /// Updates finalized Kafka feature levels through UpdateFeatures v1 when
    /// advertised, falling back to v0 when the requested operation is
    /// representable there.
    ///
    /// Kafka routes this mutation to the active controller. The request is
    /// sent once after controller discovery; a transport error after send is
    /// reported as [`Error::AdminMutationOutcomeUnknown`] because retrying
    /// could apply a feature change twice or leave the caller unsure which
    /// feature levels were persisted.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.update_features",
        skip_all,
        fields(update_count = updates.len()),
        err
    )]
    pub async fn update_features(
        &self,
        updates: &[FeatureUpdate],
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResult> {
        let mut controller_client = self.controller_client_with_retries().await?;
        let api_versions = controller_client
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        let api_version = api_versions
            .highest_supported_version(57, 1)
            .or_else(|| api_versions.highest_supported_version(57, 0))
            .ok_or(Error::Unsupported(
                "broker does not advertise UpdateFeatures v0 or v1",
            ))?;
        let response = match api_version {
            1 => controller_client
                .update_features_v1(
                    duration_millis_i32(options.timeout),
                    updates.iter().map(FeatureUpdate::as_protocol_v1).collect(),
                    options.validate_only,
                )
                .await
                .map_err(|error| {
                    admin_mutation_error(&controller_client, "UpdateFeatures", error)
                })?,
            0 => {
                if options.validate_only {
                    return Err(Error::Unsupported(
                        "UpdateFeatures validate_only requires v1",
                    ));
                }
                let updates = updates
                    .iter()
                    .map(FeatureUpdate::as_protocol_v0)
                    .collect::<Option<Vec<_>>>()
                    .ok_or(Error::Unsupported(
                        "UpdateFeatures unsafe downgrade requires v1",
                    ))?;
                controller_client
                    .update_features_v0(duration_millis_i32(options.timeout), updates)
                    .await
                    .map_err(|error| {
                        admin_mutation_error(&controller_client, "UpdateFeatures", error)
                    })?
            }
            _ => unreachable!("negotiated UpdateFeatures version exceeds client support"),
        };

        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        for result in &response.results {
            if result.error_code != 0 {
                self.config.record_broker_error();
            }
        }
        Ok(UpdateFeaturesResult::from_protocol(response))
    }

    /// Adds a voter to the active KRaft controller quorum.
    ///
    /// The request is sent once after controller discovery. A transport
    /// failure after transmission is returned as
    /// [`Error::AdminMutationOutcomeUnknown`] because the voter may already
    /// have been committed by the controller.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.add_raft_voter",
        skip_all,
        fields(voter_id = options.voter_id, ack_when_committed = options.ack_when_committed),
        err
    )]
    pub async fn add_raft_voter(&self, options: AddRaftVoterOptions) -> Result<RaftVoterResult> {
        let mut controller_client = self.controller_client_with_retries().await?;
        let api_versions = controller_client
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        let api_version = api_versions
            .highest_supported_version(80, 1)
            .or_else(|| api_versions.highest_supported_version(80, 0))
            .ok_or(Error::Unsupported(
                "broker does not advertise AddRaftVoter v0 or v1",
            ))?;
        if api_version == 0 && options.ack_when_committed {
            return Err(Error::Unsupported(
                "AddRaftVoter ack_when_committed requires v1",
            ));
        }
        let listeners = options
            .listeners
            .iter()
            .map(ProtocolRaftVoterListener::from)
            .collect();
        let response = match api_version {
            1 => controller_client
                .add_raft_voter_v1(
                    options.cluster_id.clone(),
                    duration_millis_i32(options.timeout),
                    options.voter_id,
                    options.voter_directory_id,
                    listeners,
                    options.ack_when_committed,
                )
                .await
                .map_err(|error| admin_mutation_error(&controller_client, "AddRaftVoter", error))?,
            0 => controller_client
                .add_raft_voter_v0(
                    options.cluster_id.clone(),
                    duration_millis_i32(options.timeout),
                    options.voter_id,
                    options.voter_directory_id,
                    listeners,
                )
                .await
                .map_err(|error| admin_mutation_error(&controller_client, "AddRaftVoter", error))?,
            _ => unreachable!("negotiated AddRaftVoter version exceeds client support"),
        };
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        Ok(RaftVoterResult::from_protocol(response, api_version))
    }

    /// Removes a voter from the active KRaft controller quorum.
    ///
    /// The request is sent once after controller discovery. A transport
    /// failure after transmission is returned as
    /// [`Error::AdminMutationOutcomeUnknown`] because the voter may already
    /// have been removed by the controller.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.remove_raft_voter",
        skip_all,
        fields(voter_id = options.voter_id),
        err
    )]
    pub async fn remove_raft_voter(
        &self,
        options: RemoveRaftVoterOptions,
    ) -> Result<RaftVoterResult> {
        let mut controller_client = self.controller_client_with_retries().await?;
        let api_versions = controller_client
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        let api_version =
            api_versions
                .highest_supported_version(81, 0)
                .ok_or(Error::Unsupported(
                    "broker does not advertise RemoveRaftVoter v0",
                ))?;
        let response = controller_client
            .remove_raft_voter_v0(
                options.cluster_id,
                options.voter_id,
                options.voter_directory_id,
            )
            .await
            .map_err(|error| admin_mutation_error(&controller_client, "RemoveRaftVoter", error))?;
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        Ok(RaftVoterResult::from_protocol(response, api_version))
    }

    /// Unregisters a broker through the active KRaft controller.
    ///
    /// Kafka sends this controller mutation once after controller discovery.
    /// A transport failure after transmission is returned as
    /// [`Error::AdminMutationOutcomeUnknown`] because the controller may
    /// already have removed the broker registration.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.unregister_broker",
        skip_all,
        fields(broker_id),
        err
    )]
    pub async fn unregister_broker(&self, broker_id: i32) -> Result<UnregisterBrokerResult> {
        let mut controller_client = self.controller_client_with_retries().await?;
        let api_versions = controller_client
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        let api_version =
            api_versions
                .highest_supported_version(64, 0)
                .ok_or(Error::Unsupported(
                    "broker does not advertise UnregisterBroker v0",
                ))?;
        let response = controller_client
            .unregister_broker_v0(broker_id)
            .await
            .map_err(|error| admin_mutation_error(&controller_client, "UnregisterBroker", error))?;
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        Ok(UnregisterBrokerResult::from_protocol(response, api_version))
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

    /// Describes topic partitions through Kafka's flexible DescribeTopicPartitions v0 API.
    ///
    /// The API is advertised by newer brokers and is not available on Kafka
    /// 3.7-era brokers. Callers targeting older brokers should use
    /// [`Self::list_topics`] and metadata APIs as their compatibility fallback.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_topic_partitions",
        skip_all,
        fields(topic_count = topics.len(), partition_limit = options.response_partition_limit),
        err
    )]
    pub async fn describe_topic_partitions(
        &self,
        topics: &[String],
        options: DescribeTopicPartitionsOptions,
    ) -> Result<DescribeTopicPartitionsResult> {
        let request_topics = topics
            .iter()
            .map(|name| {
                kafrust_protocol::api::describe_topic_partitions::DescribeTopicPartitionsTopicV0 {
                    name: name.clone(),
                }
            })
            .collect::<Vec<_>>();
        let mut retry = 0;
        loop {
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
            let api_versions = match client
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(api_versions) => api_versions,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(version) = api_versions.highest_supported_version(75, 0) else {
                return Err(Error::Unsupported(
                    "broker does not advertise DescribeTopicPartitions v0",
                ));
            };
            if version != 0 {
                return Err(Error::Unsupported(
                    "unsupported DescribeTopicPartitions version",
                ));
            }
            match client
                .describe_topic_partitions_v0(
                    request_topics.clone(),
                    options.response_partition_limit,
                    options.cursor.as_ref().map(|cursor| {
                        kafrust_protocol::api::describe_topic_partitions::
                            DescribeTopicPartitionsCursorV0 {
                            topic_name: cursor.topic_name.clone(),
                            partition_index: cursor.partition_index,
                        }
                    }),
                )
                .await
            {
                Ok(response) => {
                    for topic in &response.topics {
                        if topic.error_code != 0 {
                            self.config.record_broker_error();
                        }
                        for partition in &topic.partitions {
                            if partition.error_code != 0 {
                                self.config.record_broker_error();
                            }
                        }
                    }
                    return Ok(DescribeTopicPartitionsResult::from_protocol(response));
                }
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Describes a Kafka metadata quorum and its replica state.
    ///
    /// Kafka 3.7 brokers may advertise an earlier DescribeQuorum response
    /// version; newer brokers can additionally return replica directory UUIDs,
    /// error messages, and controller listener endpoints through v2.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_quorum",
        skip_all,
        fields(topic_count = topics.len()),
        err
    )]
    pub async fn describe_quorum(
        &self,
        topics: &[DescribeQuorumTopic],
    ) -> Result<DescribeQuorumResult> {
        let request_topics = topics
            .iter()
            .map(
                |topic| kafrust_protocol::api::describe_quorum::DescribeQuorumTopic {
                    name: topic.name.clone(),
                    partition_indexes: topic.partition_indexes.clone(),
                },
            )
            .collect::<Vec<_>>();
        let mut retry = 0;
        loop {
            let connection = if self.config.controller_bootstrap_servers_ref().is_empty() {
                self.config.clone().connect().await
            } else {
                self.config.connect_controller().await
            };
            let mut client = match connection {
                Ok(client) => client,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
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
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(version) = api_versions.highest_supported_version(55, 2) else {
                return Err(Error::Unsupported(
                    "broker does not advertise DescribeQuorum",
                ));
            };
            let response = match version {
                // Kafka 3.7 advertises v1, but the request wire shape is
                // identical to v0. Use v0 below the v2 response additions so
                // older controller listeners receive the original request
                // shape while preserving the version-appropriate response.
                0 | 1 => client.describe_quorum_v0(request_topics.clone()).await,
                _ => client.describe_quorum_v2(request_topics.clone()).await,
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
                Ok(response) => {
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
                    return Ok(DescribeQuorumResult::from_protocol(response));
                }
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
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
        let result = client
            .create_acls_v1(bindings.iter().map(AclBinding::as_protocol).collect())
            .await;
        let response =
            result.map_err(|error| admin_mutation_error(&client, "CreateAcls", error))?;
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
        let result = client
            .delete_acls_v1(filters.iter().map(AclFilter::as_protocol).collect())
            .await;
        let response =
            result.map_err(|error| admin_mutation_error(&client, "DeleteAcls", error))?;
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
        let result = client
            .alter_client_quotas_v0(
                alterations
                    .iter()
                    .map(ClientQuotaAlteration::as_protocol)
                    .collect(),
                validate_only,
            )
            .await;
        let response =
            result.map_err(|error| admin_mutation_error(&client, "AlterClientQuotas", error))?;
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
        let result = client
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
            .await;
        let response = result
            .map_err(|error| admin_mutation_error(&client, "AlterUserScramCredentials", error))?;
        for result in &response.results {
            if result.error_code != 0 {
                self.config.record_broker_error();
            }
        }
        Ok(AlterUserScramCredentialsResult::from_protocol(response))
    }

    /// Creates a delegation token through the active controller.
    ///
    /// Connection, controller, and ApiVersions discovery may be retried before
    /// transmission. The token mutation itself is single-attempt because a
    /// transport failure after transmission leaves the broker outcome
    /// ambiguous. The returned HMAC is required for renewal or expiry and is
    /// never included in tracing fields or `Debug` output.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.create_delegation_token",
        skip_all,
        fields(renewer_count = options.renewers.len()),
        err
    )]
    pub async fn create_delegation_token(
        &self,
        options: CreateDelegationTokenOptions,
    ) -> Result<CreatedDelegationToken> {
        let (mut client, version) = self
            .controller_client_with_api_version(
                kafrust_protocol::api::delegation_token::CREATE_API_KEY,
                3,
                "broker does not advertise CreateDelegationToken v1 or newer",
            )
            .await?;
        if options.owner.is_some() && version < 3 {
            return Err(Error::Unsupported(
                "delegation token owner selection requires CreateDelegationToken v3",
            ));
        }
        let request = CreateDelegationTokenRequest {
            correlation_id: 0,
            client_id: None,
            owner: options
                .owner
                .as_ref()
                .map(DelegationTokenPrincipal::as_protocol),
            renewers: options
                .renewers
                .iter()
                .map(DelegationTokenPrincipal::as_protocol)
                .collect(),
            max_lifetime_ms: options.max_lifetime_ms,
        };
        let result = match version {
            1 => client.create_delegation_token_v1(request).await,
            2 | 3 => client.create_delegation_token_v2(request, version).await,
            _ => {
                return Err(Error::Unsupported(
                    "unsupported CreateDelegationToken version",
                ))
            }
        };
        let response = result
            .map_err(|error| admin_mutation_error(&client, "CreateDelegationToken", error))?;
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        Ok(CreatedDelegationToken::from_protocol(response))
    }

    /// Describes delegation tokens visible to the authenticated principal.
    ///
    /// `None` asks Kafka for every token permitted by the broker. This is a
    /// read operation and preserves the HMAC values needed by administrative
    /// renewal or expiry; callers must protect those bytes like credentials.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_delegation_tokens",
        skip_all,
        fields(owner_count = owners.map_or(0, <[DelegationTokenPrincipal]>::len)),
        err
    )]
    pub async fn describe_delegation_tokens(
        &self,
        owners: Option<&[DelegationTokenPrincipal]>,
    ) -> Result<DescribeDelegationTokensResult> {
        let owners = owners.map(|owners| {
            owners
                .iter()
                .map(DelegationTokenPrincipal::as_protocol)
                .collect::<Vec<_>>()
        });
        let mut retry = 0;
        let response = loop {
            let (mut client, version) = self
                .controller_client_with_api_version(
                    kafrust_protocol::api::delegation_token::DESCRIBE_API_KEY,
                    3,
                    "broker does not advertise DescribeDelegationToken v1 or newer",
                )
                .await?;
            let response = match version {
                1 => client.describe_delegation_token_v1(owners.clone()).await,
                _ => {
                    client
                        .describe_delegation_token_v2(owners.clone(), version)
                        .await
                }
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
        Ok(DescribeDelegationTokensResult::from_protocol(response))
    }

    /// Renews one delegation token through the active controller.
    ///
    /// The HMAC is treated as credential material and is not logged. A
    /// transport failure after transmission is returned without replaying the
    /// mutation.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.renew_delegation_token",
        skip_all,
        fields(renew_period = ?renew_period),
        err
    )]
    pub async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        renew_period: Duration,
    ) -> Result<DelegationTokenOperationResult> {
        let (mut client, version) = self
            .controller_client_with_api_version(
                kafrust_protocol::api::delegation_token::RENEW_API_KEY,
                2,
                "broker does not advertise RenewDelegationToken v1 or newer",
            )
            .await?;
        let renew_period_ms = duration_millis_i64(renew_period);
        let result = match version {
            1 => {
                client
                    .renew_delegation_token_v1(hmac.to_vec(), renew_period_ms)
                    .await
            }
            _ => {
                client
                    .renew_delegation_token_v2(hmac.to_vec(), renew_period_ms)
                    .await
            }
        };
        let response =
            result.map_err(|error| admin_mutation_error(&client, "RenewDelegationToken", error))?;
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        Ok(DelegationTokenOperationResult::from_protocol(response))
    }

    /// Expires one delegation token through the active controller.
    ///
    /// A zero period requests immediate expiry according to Kafka's protocol.
    /// The HMAC is treated as credential material and is never logged.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.expire_delegation_token",
        skip_all,
        fields(expiry_time_period = ?expiry_time_period),
        err
    )]
    pub async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        expiry_time_period: Duration,
    ) -> Result<DelegationTokenOperationResult> {
        let (mut client, version) = self
            .controller_client_with_api_version(
                kafrust_protocol::api::delegation_token::EXPIRE_API_KEY,
                2,
                "broker does not advertise ExpireDelegationToken v1 or newer",
            )
            .await?;
        let expiry_time_period_ms = duration_millis_i64(expiry_time_period);
        let result = match version {
            1 => {
                client
                    .expire_delegation_token_v1(hmac.to_vec(), expiry_time_period_ms)
                    .await
            }
            _ => {
                client
                    .expire_delegation_token_v2(hmac.to_vec(), expiry_time_period_ms)
                    .await
            }
        };
        let response = result
            .map_err(|error| admin_mutation_error(&client, "ExpireDelegationToken", error))?;
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        Ok(DelegationTokenOperationResult::from_protocol(response))
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
                let response = controller_client
                    .elect_leaders_v0(topics, timeout_ms)
                    .await
                    .map_err(|error| {
                        admin_mutation_error(&controller_client, "ElectLeaders", error)
                    })?;
                ElectLeadersResult::from_protocol_v0(response)
            }
            1 => {
                let response = controller_client
                    .elect_leaders_v1(election_type.as_i8(), topics, timeout_ms)
                    .await
                    .map_err(|error| {
                        admin_mutation_error(&controller_client, "ElectLeaders", error)
                    })?;
                ElectLeadersResult::from_protocol_v1(response)
            }
            _ => {
                let response = controller_client
                    .elect_leaders_v2(election_type.as_i8(), topics, timeout_ms)
                    .await
                    .map_err(|error| {
                        admin_mutation_error(&controller_client, "ElectLeaders", error)
                    })?;
                ElectLeadersResult::from_protocol_v2(response)
            }
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
        let result = controller_client
            .alter_partition_reassignments_v0(
                duration_millis_i32(options.timeout),
                reassignments
                    .iter()
                    .map(PartitionReassignment::as_protocol)
                    .collect(),
            )
            .await;
        let response = result.map_err(|error| {
            admin_mutation_error(&controller_client, "AlterPartitionReassignments", error)
        })?;

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

    /// Describes configurations for Kafka topics.
    ///
    /// The default path uses DescribeConfigs v1 for compatibility with Kafka
    /// 3.7-era brokers. Setting [`DescribeConfigsOptions::include_documentation`]
    /// requests DescribeConfigs v4 on a broker that advertises it and retains
    /// Kafka's configuration type and documentation metadata.
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
        if options.include_documentation {
            return self.describe_topic_configs_v4(resources, options).await;
        }
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

    async fn describe_topic_configs_v4(
        &self,
        resources: &[TopicConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<DescribeConfigsResult> {
        let request_resources = resources
            .iter()
            .map(TopicConfigResource::as_protocol_v4)
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
            let api_versions = match client
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(response) => response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if api_versions.error_code != 0 {
                return Err(client.broker_error(
                    api_versions.error_code,
                    "describe topic config capabilities".to_owned(),
                ));
            }
            if api_versions.highest_supported_version(32, 4).is_none() {
                return Err(Error::Unsupported(
                    "broker does not advertise DescribeConfigs v4",
                ));
            }
            match client
                .describe_configs_v4(
                    request_resources.clone(),
                    options.include_synonyms,
                    options.include_documentation,
                )
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
                .map(ConfigResourceResult::from_v4_protocol)
                .collect(),
        })
    }

    /// Lists Kafka configuration resources through API 74.
    ///
    /// Kafka 4.1 added v1 for discovering topic, broker, group, client metrics,
    /// and broker-logger resources that can be inspected with DescribeConfigs.
    /// An empty type filter requests all resource types supported by the
    /// broker. On Kafka 3.9-era brokers, an exact `ClientMetrics` filter uses
    /// the compatible v0 operation; broader filters return
    /// `Error::Unsupported` rather than pretending v0 can list resource types
    /// it cannot represent.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.list_config_resources",
        skip_all,
        fields(resource_type_count = options.resource_types.len()),
        err
    )]
    pub async fn list_config_resources(
        &self,
        options: ListConfigResourcesOptions,
    ) -> Result<ListConfigResourcesResult> {
        let resource_types = options
            .resource_types
            .iter()
            .map(|resource_type| resource_type.code())
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
            let api_versions = match client
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(response) => response,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let api_version = api_versions
                .highest_supported_version(74, 1)
                .filter(|version| *version >= 1)
                .or_else(|| api_versions.highest_supported_version(74, 0))
                .ok_or(Error::Unsupported(
                    "broker does not advertise ListConfigResources v0 or v1",
                ))?;
            let response = match api_version {
                1 => client
                    .list_config_resources_v1(resource_types.clone())
                    .await
                    .map(|response| ListConfigResourcesResult::from_protocol(response, 1)),
                0 if options.resource_types == [ConfigResourceType::ClientMetrics] => client
                    .list_config_resources_v0()
                    .await
                    .map(|response| ListConfigResourcesResult::from_protocol_v0(response, 0)),
                0 => {
                    return Err(Error::Unsupported(
                        "ListConfigResources v0 only lists client metrics resources",
                    ));
                }
                _ => unreachable!("negotiated ListConfigResources version exceeds support"),
            };
            match response {
                Ok(response)
                    if retry < self.max_retries
                        && is_retryable_admin_read_code(response.error_code) =>
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

        if response.error_code != 0 {
            self.config.record_broker_error();
        }

        Ok(response)
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
        let result = client
            .incremental_alter_configs_v0(
                resources
                    .iter()
                    .map(TopicConfigAlteration::as_protocol)
                    .collect(),
                options.validate_only,
            )
            .await;
        let response = result
            .map_err(|error| admin_mutation_error(&client, "IncrementalAlterConfigs", error))?;

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
        let result = client
            .alter_configs_v1(
                resources
                    .iter()
                    .map(TopicConfigUpdate::as_protocol)
                    .collect(),
                options.validate_only,
            )
            .await;
        let response =
            result.map_err(|error| admin_mutation_error(&client, "AlterConfigs", error))?;

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

    /// Describes KIP-848 consumer groups through ConsumerGroupDescribe.
    ///
    /// This is the modern group protocol equivalent of
    /// [`Self::describe_consumer_groups`], which uses the classic
    /// DescribeGroups API. Kafka 3.8+ brokers advertise this API for groups
    /// using the consumer protocol and expose group and assignment epochs.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_consumer_groups_modern",
        skip_all,
        fields(group_count = group_ids.len(), include_authorized_operations),
        err
    )]
    pub async fn describe_consumer_groups_modern(
        &self,
        group_ids: &[String],
        include_authorized_operations: bool,
    ) -> Result<Vec<ModernConsumerGroupDescription>> {
        let mut descriptions = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let mut retry = 0;
            let (throttle_time_ms, group) = loop {
                let mut coordinator = self.group_coordinator_client(group_id).await?;
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let Some(version) = api_versions
                    .highest_supported_version(69, 1)
                    .filter(|version| *version >= 0)
                else {
                    return Err(Error::Unsupported(
                        "broker does not advertise ConsumerGroupDescribe v0",
                    ));
                };
                let response = match version {
                    0 => coordinator
                        .consumer_group_describe_v0(
                            vec![group_id.clone()],
                            include_authorized_operations,
                        )
                        .await
                        .map(|response: ConsumerGroupDescribeResponseV0| {
                            (response.throttle_time_ms, response.groups)
                        }),
                    1 => coordinator
                        .consumer_group_describe_v1(
                            vec![group_id.clone()],
                            include_authorized_operations,
                        )
                        .await
                        .map(|response: ConsumerGroupDescribeResponseV1| {
                            (response.throttle_time_ms, response.groups)
                        }),
                    _ => {
                        return Err(Error::Unsupported(
                            "unsupported ConsumerGroupDescribe version",
                        ))
                    }
                };
                let (throttle_time_ms, groups) = match response {
                    Ok(response) => response,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let group = groups.into_iter().find(|group| group.group_id == *group_id);
                let Some(group) = group else {
                    return Err(Error::MissingGroupDescription {
                        group_id: group_id.clone(),
                    });
                };
                if retry < self.max_retries && is_retryable_admin_coordinator_code(group.error_code)
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break (throttle_time_ms, group);
            };
            if group.error_code != 0 {
                self.config.record_broker_error();
            }
            descriptions.push(ModernConsumerGroupDescription::from_protocol(
                group,
                Duration::from_millis(nonnegative_i32_to_u64(throttle_time_ms)),
            ));
        }
        Ok(descriptions)
    }

    /// Describes KIP-932 share groups through ShareGroupDescribe.
    ///
    /// Share groups are coordinator-owned, so each group is resolved and
    /// queried independently. Kafka 4.1+ exposes the stable v1 wire shape.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_share_groups",
        skip_all,
        fields(group_count = group_ids.len(), include_authorized_operations),
        err
    )]
    pub async fn describe_share_groups(
        &self,
        group_ids: &[String],
        include_authorized_operations: bool,
    ) -> Result<Vec<ShareGroupDescription>> {
        let mut descriptions = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let mut retry = 0;
            let (throttle_time_ms, group) = loop {
                let mut coordinator = self.share_group_coordinator_client(group_id).await?;
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let Some(version) = api_versions
                    .highest_supported_version(77, 1)
                    .filter(|version| *version >= 1)
                else {
                    return Err(Error::Unsupported(
                        "broker does not advertise ShareGroupDescribe v1",
                    ));
                };
                let response = match version {
                    1 => {
                        coordinator
                            .share_group_describe_v1(
                                vec![group_id.clone()],
                                include_authorized_operations,
                            )
                            .await
                    }
                    _ => return Err(Error::Unsupported("unsupported ShareGroupDescribe version")),
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let throttle_time_ms = response.throttle_time_ms;
                let group = response
                    .groups
                    .into_iter()
                    .find(|group| group.group_id == *group_id);
                let Some(group) = group else {
                    return Err(Error::MissingGroupDescription {
                        group_id: group_id.clone(),
                    });
                };
                if retry < self.max_retries && is_retryable_admin_coordinator_code(group.error_code)
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break (throttle_time_ms, group);
            };
            if group.error_code != 0 {
                self.config.record_broker_error();
            }
            descriptions.push(ShareGroupDescription::from_protocol(
                group,
                Duration::from_millis(nonnegative_i32_to_u64(throttle_time_ms)),
            ));
        }
        Ok(descriptions)
    }

    /// Describes Kafka Streams groups through StreamsGroupDescribe.
    ///
    /// Streams groups are coordinator-owned. Each requested group is resolved
    /// independently so a coordinator movement for one group does not cause a
    /// response for another group to be attributed to the wrong coordinator.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.describe_streams_groups",
        skip_all,
        fields(group_count = group_ids.len(), include_authorized_operations),
        err
    )]
    pub async fn describe_streams_groups(
        &self,
        group_ids: &[String],
        include_authorized_operations: bool,
    ) -> Result<Vec<StreamsGroupDescription>> {
        let mut descriptions = Vec::with_capacity(group_ids.len());
        for group_id in group_ids {
            let mut retry = 0;
            let (throttle_time_ms, group) = loop {
                let mut coordinator = self.group_coordinator_client(group_id).await?;
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if api_versions.highest_supported_version(89, 0).is_none() {
                    return Err(Error::Unsupported(
                        "broker does not advertise StreamsGroupDescribe v0",
                    ));
                }
                let response = match coordinator
                    .streams_group_describe_v0(
                        vec![group_id.clone()],
                        include_authorized_operations,
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let throttle_time_ms = response.throttle_time_ms;
                let group = response
                    .groups
                    .into_iter()
                    .find(|group| group.group_id == *group_id);
                let Some(group) = group else {
                    return Err(Error::MissingGroupDescription {
                        group_id: group_id.clone(),
                    });
                };
                if retry < self.max_retries && is_retryable_admin_coordinator_code(group.error_code)
                {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break (throttle_time_ms, group);
            };
            if group.error_code != 0 {
                self.config.record_broker_error();
            }
            descriptions.push(StreamsGroupDescription::from_protocol(
                group,
                Duration::from_millis(nonnegative_i32_to_u64(throttle_time_ms)),
            ));
        }
        Ok(descriptions)
    }

    /// Initializes share-group state for the selected topic partitions.
    ///
    /// The request is routed to the share-group coordinator. A transmitted
    /// request whose response is lost is reported as an unknown mutation
    /// outcome and is never replayed automatically.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.initialize_share_group_state",
        skip_all,
        fields(group_id, topic_count = topics.len()),
        err
    )]
    pub async fn initialize_share_group_state(
        &self,
        group_id: &str,
        topics: &[ShareGroupStateInitializeTopic],
    ) -> Result<ShareGroupStateResult> {
        let resources = topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| ShareStateResource {
                        topic_id: topic.topic_id,
                        partition: partition.partition,
                    })
            })
            .collect::<Vec<_>>();
        let routes = self
            .share_state_coordinator_routes(group_id, &resources)
            .await?;
        let mut results = Vec::new();
        for (endpoint, route_resources) in routes {
            let protocol_topics = topics
                .iter()
                .filter_map(|topic| {
                    let partitions = topic
                        .partitions
                        .iter()
                        .filter(|partition| {
                            route_resources.contains(&ShareStateResource {
                                topic_id: topic.topic_id,
                                partition: partition.partition,
                            })
                        })
                        .map(|partition| InitializeShareGroupStatePartition {
                            partition: partition.partition,
                            state_epoch: partition.state_epoch,
                            start_offset: partition.start_offset,
                        })
                        .collect::<Vec<_>>();
                    (!partitions.is_empty()).then_some(InitializeShareGroupStateTopic {
                        topic_id: topic.topic_id,
                        partitions,
                    })
                })
                .collect::<Vec<_>>();
            let mut retry = 0;
            let response = loop {
                let mut coordinator = match self
                    .config
                    .connect_broker(format!("{}:{}", endpoint.host, endpoint.port))
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if api_versions.highest_supported_version(83, 0).is_none() {
                    return Err(Error::Unsupported(
                        "broker does not advertise InitializeShareGroupState v0",
                    ));
                }
                let response = coordinator
                    .initialize_share_group_state_v0(group_id, protocol_topics.clone())
                    .await
                    .map_err(|error| {
                        admin_mutation_error(&coordinator, "InitializeShareGroupState", error)
                    })?;
                let retryable = response.results.iter().any(|topic| {
                    topic
                        .partitions
                        .iter()
                        .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
                });
                if retry < self.max_retries && retryable {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break response;
            };
            results.extend(response.results);
        }
        let response = ShareGroupStateResultResponse { results };
        if response.results.iter().any(|topic| {
            topic
                .partitions
                .iter()
                .any(|partition| partition.error_code != 0)
        }) {
            self.config.record_broker_error();
        }
        Ok(ShareGroupStateResult::from_protocol(response))
    }

    /// Reads the complete delivery state for selected share partitions.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.read_share_group_state",
        skip_all,
        fields(group_id, topic_count = topics.len()),
        err
    )]
    pub async fn read_share_group_state(
        &self,
        group_id: &str,
        topics: &[ShareGroupStateReadTopic],
    ) -> Result<ReadShareGroupStateResult> {
        let resources = topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| ShareStateResource {
                        topic_id: topic.topic_id,
                        partition: partition.partition,
                    })
            })
            .collect::<Vec<_>>();
        let routes = self
            .share_state_coordinator_routes(group_id, &resources)
            .await?;
        let mut results = Vec::new();
        for (endpoint, route_resources) in routes {
            let protocol_topics = topics
                .iter()
                .filter_map(|topic| {
                    let partitions = topic
                        .partitions
                        .iter()
                        .filter(|partition| {
                            route_resources.contains(&ShareStateResource {
                                topic_id: topic.topic_id,
                                partition: partition.partition,
                            })
                        })
                        .map(|partition| ReadShareGroupStatePartition {
                            partition: partition.partition,
                            leader_epoch: partition.leader_epoch,
                        })
                        .collect::<Vec<_>>();
                    (!partitions.is_empty()).then_some(ReadShareGroupStateTopic {
                        topic_id: topic.topic_id,
                        partitions,
                    })
                })
                .collect::<Vec<_>>();
            let mut retry = 0;
            let response = loop {
                let mut coordinator = match self
                    .config
                    .connect_broker(format!("{}:{}", endpoint.host, endpoint.port))
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if api_versions.highest_supported_version(84, 0).is_none() {
                    return Err(Error::Unsupported(
                        "broker does not advertise ReadShareGroupState v0",
                    ));
                }
                let response = match coordinator
                    .read_share_group_state_v0(group_id, protocol_topics.clone())
                    .await
                {
                    Ok(response) => response,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let retryable = response.results.iter().any(|topic| {
                    topic
                        .partitions
                        .iter()
                        .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
                });
                if retry < self.max_retries && retryable {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break response;
            };
            results.extend(response.results);
        }
        let response = ReadShareGroupStateResponseV0 { results };
        if response.results.iter().any(|topic| {
            topic
                .partitions
                .iter()
                .any(|partition| partition.error_code != 0)
        }) {
            self.config.record_broker_error();
        }
        Ok(ReadShareGroupStateResult::from_protocol(response))
    }

    /// Writes share-group delivery state, preferring v1 when available.
    ///
    /// `delivery_complete_count` requires WriteShareGroupState v1. If a
    /// broker advertises only v0, a request that contains this field fails
    /// explicitly instead of silently losing it.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.write_share_group_state",
        skip_all,
        fields(group_id, topic_count = topics.len()),
        err
    )]
    pub async fn write_share_group_state(
        &self,
        group_id: &str,
        topics: &[ShareGroupStateWriteTopic],
    ) -> Result<ShareGroupStateResult> {
        let resources = topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| ShareStateResource {
                        topic_id: topic.topic_id,
                        partition: partition.partition,
                    })
            })
            .collect::<Vec<_>>();
        let routes = self
            .share_state_coordinator_routes(group_id, &resources)
            .await?;
        let mut results = Vec::new();
        for (endpoint, route_resources) in routes {
            let route_topics = topics
                .iter()
                .filter_map(|topic| {
                    let partitions = topic
                        .partitions
                        .iter()
                        .filter(|partition| {
                            route_resources.contains(&ShareStateResource {
                                topic_id: topic.topic_id,
                                partition: partition.partition,
                            })
                        })
                        .collect::<Vec<_>>();
                    (!partitions.is_empty()).then_some((topic, partitions))
                })
                .collect::<Vec<_>>();
            let requires_v1 = route_topics.iter().any(|(_, partitions)| {
                partitions
                    .iter()
                    .any(|partition| partition.delivery_complete_count.is_some())
            });
            let mut retry = 0;
            let response = loop {
                let mut coordinator = match self
                    .config
                    .connect_broker(format!("{}:{}", endpoint.host, endpoint.port))
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let Some(version) = api_versions.highest_supported_version(85, 1) else {
                    return Err(Error::Unsupported(
                        "broker does not advertise WriteShareGroupState",
                    ));
                };
                let response = match version {
                    1 => {
                        let protocol_topics = route_topics
                            .iter()
                            .map(|(topic, partitions)| WriteShareGroupStateTopicV1 {
                                topic_id: topic.topic_id,
                                partitions: partitions
                                    .iter()
                                    .map(|partition| WriteShareGroupStatePartitionV1 {
                                        partition: partition.partition,
                                        state_epoch: partition.state_epoch,
                                        leader_epoch: partition.leader_epoch,
                                        start_offset: partition.start_offset,
                                        delivery_complete_count: partition
                                            .delivery_complete_count
                                            .unwrap_or(0),
                                        state_batches: partition
                                            .state_batches
                                            .iter()
                                            .map(ProtocolShareGroupStateBatch::from)
                                            .collect(),
                                    })
                                    .collect(),
                            })
                            .collect();
                        coordinator
                            .write_share_group_state_v1(group_id, protocol_topics)
                            .await
                            .map_err(|error| {
                                admin_mutation_error(&coordinator, "WriteShareGroupState", error)
                            })?
                    }
                    0 if !requires_v1 => {
                        let protocol_topics = route_topics
                            .iter()
                            .map(|(topic, partitions)| WriteShareGroupStateTopicV0 {
                                topic_id: topic.topic_id,
                                partitions: partitions
                                    .iter()
                                    .map(|partition| WriteShareGroupStatePartitionV0 {
                                        partition: partition.partition,
                                        state_epoch: partition.state_epoch,
                                        leader_epoch: partition.leader_epoch,
                                        start_offset: partition.start_offset,
                                        state_batches: partition
                                            .state_batches
                                            .iter()
                                            .map(ProtocolShareGroupStateBatch::from)
                                            .collect(),
                                    })
                                    .collect(),
                            })
                            .collect();
                        coordinator
                            .write_share_group_state_v0(group_id, protocol_topics)
                            .await
                            .map_err(|error| {
                                admin_mutation_error(&coordinator, "WriteShareGroupState", error)
                            })?
                    }
                    0 => {
                        return Err(Error::Unsupported(
                            "WriteShareGroupState v1 is required for delivery_complete_count",
                        ));
                    }
                    _ => {
                        return Err(Error::Unsupported(
                            "unsupported WriteShareGroupState version",
                        ))
                    }
                };
                let retryable = response.results.iter().any(|topic| {
                    topic
                        .partitions
                        .iter()
                        .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
                });
                if retry < self.max_retries && retryable {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break response;
            };
            results.extend(response.results);
        }
        let response = ShareGroupStateResultResponse { results };
        if response.results.iter().any(|topic| {
            topic
                .partitions
                .iter()
                .any(|partition| partition.error_code != 0)
        }) {
            self.config.record_broker_error();
        }
        Ok(ShareGroupStateResult::from_protocol(response))
    }

    /// Deletes share-group state for selected topic partitions.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_share_group_state",
        skip_all,
        fields(group_id, topic_count = topics.len()),
        err
    )]
    pub async fn delete_share_group_state(
        &self,
        group_id: &str,
        topics: &[ShareGroupStateDeleteTopic],
    ) -> Result<ShareGroupStateResult> {
        let resources = topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| ShareStateResource {
                        topic_id: topic.topic_id,
                        partition: *partition,
                    })
            })
            .collect::<Vec<_>>();
        let routes = self
            .share_state_coordinator_routes(group_id, &resources)
            .await?;
        let mut results = Vec::new();
        for (endpoint, route_resources) in routes {
            let protocol_topics = topics
                .iter()
                .filter_map(|topic| {
                    let partitions = topic
                        .partitions
                        .iter()
                        .copied()
                        .filter(|partition| {
                            route_resources.contains(&ShareStateResource {
                                topic_id: topic.topic_id,
                                partition: *partition,
                            })
                        })
                        .collect::<Vec<_>>();
                    (!partitions.is_empty()).then_some(DeleteShareGroupStateTopic {
                        topic_id: topic.topic_id,
                        partitions,
                    })
                })
                .collect::<Vec<_>>();
            let mut retry = 0;
            let response = loop {
                let mut coordinator = match self
                    .config
                    .connect_broker(format!("{}:{}", endpoint.host, endpoint.port))
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if api_versions.highest_supported_version(86, 0).is_none() {
                    return Err(Error::Unsupported(
                        "broker does not advertise DeleteShareGroupState v0",
                    ));
                }
                let response = coordinator
                    .delete_share_group_state_v0(group_id, protocol_topics.clone())
                    .await
                    .map_err(|error| {
                        admin_mutation_error(&coordinator, "DeleteShareGroupState", error)
                    })?;
                let retryable = response.results.iter().any(|topic| {
                    topic
                        .partitions
                        .iter()
                        .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
                });
                if retry < self.max_retries && retryable {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break response;
            };
            results.extend(response.results);
        }
        let response = ShareGroupStateResultResponse { results };
        if response.results.iter().any(|topic| {
            topic
                .partitions
                .iter()
                .any(|partition| partition.error_code != 0)
        }) {
            self.config.record_broker_error();
        }
        Ok(ShareGroupStateResult::from_protocol(response))
    }

    /// Reads the compact summary of share-group state, preferring v1 when
    /// available so delivery completion counts are retained.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.read_share_group_state_summary",
        skip_all,
        fields(group_id, topic_count = topics.len()),
        err
    )]
    pub async fn read_share_group_state_summary(
        &self,
        group_id: &str,
        topics: &[ShareGroupStateReadTopic],
    ) -> Result<ReadShareGroupStateSummaryResult> {
        let resources = topics
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| ShareStateResource {
                        topic_id: topic.topic_id,
                        partition: partition.partition,
                    })
            })
            .collect::<Vec<_>>();
        let routes = self
            .share_state_coordinator_routes(group_id, &resources)
            .await?;
        let mut results = Vec::new();
        for (endpoint, route_resources) in routes {
            let protocol_topics = topics
                .iter()
                .filter_map(|topic| {
                    let partitions = topic
                        .partitions
                        .iter()
                        .filter(|partition| {
                            route_resources.contains(&ShareStateResource {
                                topic_id: topic.topic_id,
                                partition: partition.partition,
                            })
                        })
                        .map(|partition| ReadShareGroupStatePartition {
                            partition: partition.partition,
                            leader_epoch: partition.leader_epoch,
                        })
                        .collect::<Vec<_>>();
                    (!partitions.is_empty()).then_some(ReadShareGroupStateTopic {
                        topic_id: topic.topic_id,
                        partitions,
                    })
                })
                .collect::<Vec<_>>();
            let mut retry = 0;
            let response = loop {
                let mut coordinator = match self
                    .config
                    .connect_broker(format!("{}:{}", endpoint.host, endpoint.port))
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let api_versions = match coordinator
                    .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                    .await
                {
                    Ok(api_versions) => api_versions,
                    Err(error)
                        if retry < self.max_retries
                            && is_retryable_admin_coordinator_error(&error) =>
                    {
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let Some(version) = api_versions.highest_supported_version(87, 1) else {
                    return Err(Error::Unsupported(
                        "broker does not advertise ReadShareGroupStateSummary",
                    ));
                };
                let response = match version {
                    1 => coordinator
                        .read_share_group_state_summary_v1(group_id, protocol_topics.clone())
                        .await
                        .map(ReadShareGroupStateSummaryResponse::V1),
                    0 => coordinator
                        .read_share_group_state_summary_v0(group_id, protocol_topics.clone())
                        .await
                        .map(ReadShareGroupStateSummaryResponse::V0),
                    _ => {
                        return Err(Error::Unsupported(
                            "unsupported ReadShareGroupStateSummary version",
                        ))
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let retryable = response.has_retryable_error();
                if retry < self.max_retries && retryable {
                    self.config.record_broker_error();
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                break response;
            };
            results.extend(response.into_topics());
        }
        let response = ReadShareGroupStateSummaryResult { topics: results };
        if response.topics.iter().any(|topic| {
            topic
                .partitions
                .iter()
                .any(|partition| partition.error_code != 0)
        }) {
            self.config.record_broker_error();
        }
        Ok(response)
    }

    /// Sets share-group offsets through AlterShareGroupOffsets.
    ///
    /// Kafka requires the share group to be empty for this operation. Results
    /// are preserved at both the top-level and partition levels. A transport
    /// failure after transmission is returned as
    /// [`Error::AdminMutationOutcomeUnknown`] and is never replayed.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_share_group_offsets",
        skip_all,
        fields(group_id, offset_count = offsets.len()),
        err
    )]
    pub async fn alter_share_group_offsets(
        &self,
        group_id: &str,
        offsets: &[ShareGroupOffset],
    ) -> Result<AlterShareGroupOffsetsResult> {
        let mut topics = BTreeMap::new();
        for offset in offsets {
            topics
                .entry(offset.topic.clone())
                .or_insert_with(Vec::new)
                .push(AlterShareGroupOffsetsPartitionV0 {
                    partition_index: offset.partition,
                    start_offset: offset.offset,
                });
        }
        let topics: Vec<AlterShareGroupOffsetsTopicV0> = topics
            .into_iter()
            .map(|(topic_name, partitions)| AlterShareGroupOffsetsTopicV0 {
                topic_name,
                partitions,
            })
            .collect();

        let mut retry = 0;
        let response = loop {
            let mut coordinator = self.share_group_coordinator_client(group_id).await?;
            let api_versions = match coordinator
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(api_versions) => api_versions,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if api_versions.highest_supported_version(91, 0).is_none() {
                return Err(Error::Unsupported(
                    "broker does not advertise AlterShareGroupOffsets v0",
                ));
            }
            let response = coordinator
                .alter_share_group_offsets_v0(group_id, topics.clone())
                .await
                .map_err(|error| {
                    admin_mutation_error(&coordinator, "AlterShareGroupOffsets", error)
                })?;
            let retryable = is_retryable_admin_coordinator_code(response.error_code)
                || response.responses.iter().any(|topic| {
                    topic
                        .partitions
                        .iter()
                        .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
                });
            if retry < self.max_retries && retryable {
                self.config.record_broker_error();
                retry += 1;
                self.config.record_retry();
                tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                continue;
            }
            break response;
        };
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        for topic in &response.responses {
            if topic
                .partitions
                .iter()
                .any(|partition| partition.error_code != 0)
            {
                self.config.record_broker_error();
            }
        }
        Ok(AlterShareGroupOffsetsResult::from_protocol(response))
    }

    /// Deletes share-group offsets for the selected topics.
    ///
    /// Kafka requires the share group to be empty for this operation. A
    /// transmitted request is not replayed after an ambiguous transport
    /// failure.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_share_group_offsets",
        skip_all,
        fields(group_id, topic_count = topics.len()),
        err
    )]
    pub async fn delete_share_group_offsets(
        &self,
        group_id: &str,
        topics: &[String],
    ) -> Result<DeleteShareGroupOffsetsResult> {
        let topics: Vec<DeleteShareGroupOffsetsTopicV0> = topics
            .iter()
            .cloned()
            .map(|topic_name| DeleteShareGroupOffsetsTopicV0 { topic_name })
            .collect();
        let mut retry = 0;
        let response = loop {
            let mut coordinator = self.share_group_coordinator_client(group_id).await?;
            let api_versions = match coordinator
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(api_versions) => api_versions,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if api_versions.highest_supported_version(92, 0).is_none() {
                return Err(Error::Unsupported(
                    "broker does not advertise DeleteShareGroupOffsets v0",
                ));
            }
            let response = coordinator
                .delete_share_group_offsets_v0(group_id, topics.clone())
                .await
                .map_err(|error| {
                    admin_mutation_error(&coordinator, "DeleteShareGroupOffsets", error)
                })?;
            if retry < self.max_retries && is_retryable_admin_coordinator_code(response.error_code)
            {
                self.config.record_broker_error();
                retry += 1;
                self.config.record_retry();
                tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                continue;
            }
            break response;
        };
        if response.error_code != 0 {
            self.config.record_broker_error();
        }
        for topic in &response.responses {
            if topic.error_code != 0 {
                self.config.record_broker_error();
            }
        }
        Ok(DeleteShareGroupOffsetsResult::from_protocol(response))
    }

    /// Lists share-group partition offsets through DescribeShareGroupOffsets.
    ///
    /// Kafka 4.1 introduced API v0 and Kafka 4.2 added the partition `lag`
    /// field in v1. Pass `None` to request every topic-partition known to the
    /// share group, or provide topic and partition filters. The operation is
    /// coordinator-routed and retries coordinator movement, but does not
    /// retry topic-level authorization or partition errors.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.list_share_group_offsets",
        skip_all,
        fields(group_id, has_topic_filter = topics.is_some()),
        err
    )]
    pub async fn list_share_group_offsets(
        &self,
        group_id: &str,
        topics: Option<&[ShareGroupOffsetQuery]>,
    ) -> Result<ListShareGroupOffsetsResult> {
        let request_topics = topics.map(|topics| {
            topics
                .iter()
                .map(ShareGroupOffsetQuery::as_protocol)
                .collect::<Vec<_>>()
        });
        let mut retry = 0;
        let result = loop {
            let mut coordinator = self.share_group_coordinator_client(group_id).await?;
            let api_versions = match coordinator
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(api_versions) => api_versions,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(version) = api_versions
                .highest_supported_version(90, 1)
                .filter(|version| *version <= 1)
            else {
                return Err(Error::Unsupported(
                    "broker does not advertise DescribeShareGroupOffsets v0",
                ));
            };
            let group = match version {
                1 => {
                    let response = match coordinator
                        .describe_share_group_offsets_v1(vec![DescribeShareGroupOffsetsGroup {
                            group_id: group_id.to_owned(),
                            topics: request_topics.clone(),
                        }])
                        .await
                    {
                        Ok(response) => response,
                        Err(error)
                            if retry < self.max_retries
                                && is_retryable_admin_coordinator_error(&error) =>
                        {
                            retry += 1;
                            self.config.record_retry();
                            tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let group = response
                        .groups
                        .into_iter()
                        .find(|group| group.group_id == group_id)
                        .ok_or_else(|| Error::MissingGroupDescription {
                            group_id: group_id.to_owned(),
                        })?;
                    if retry < self.max_retries
                        && is_retryable_admin_coordinator_code(group.error_code)
                    {
                        self.config.record_broker_error();
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    ListShareGroupOffsetsResult::from_protocol_v1(
                        group,
                        Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
                    )
                }
                0 => {
                    let response = match coordinator
                        .describe_share_group_offsets_v0(vec![DescribeShareGroupOffsetsGroup {
                            group_id: group_id.to_owned(),
                            topics: request_topics.clone(),
                        }])
                        .await
                    {
                        Ok(response) => response,
                        Err(error)
                            if retry < self.max_retries
                                && is_retryable_admin_coordinator_error(&error) =>
                        {
                            retry += 1;
                            self.config.record_retry();
                            tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let group = response
                        .groups
                        .into_iter()
                        .find(|group| group.group_id == group_id)
                        .ok_or_else(|| Error::MissingGroupDescription {
                            group_id: group_id.to_owned(),
                        })?;
                    if retry < self.max_retries
                        && is_retryable_admin_coordinator_code(group.error_code)
                    {
                        self.config.record_broker_error();
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        continue;
                    }
                    ListShareGroupOffsetsResult::from_protocol_v0(
                        group,
                        Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
                    )
                }
                _ => {
                    return Err(Error::Unsupported(
                        "unsupported DescribeShareGroupOffsets version",
                    ))
                }
            };
            break group;
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

    /// Deletes share groups through Kafka's coordinator-routed DeleteGroups API.
    ///
    /// Kafka uses the same API key for classic and share-group deletion; this
    /// name makes the intended group type explicit at the application boundary.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.delete_share_groups",
        skip_all,
        fields(group_count = group_ids.len()),
        err
    )]
    pub async fn delete_share_groups(
        &self,
        group_ids: &[String],
    ) -> Result<Vec<DeleteShareGroupResult>> {
        self.delete_consumer_groups(group_ids).await
    }

    /// Lists Kafka groups by querying every broker in the cluster.
    ///
    /// ListGroups is broker-scoped because each group coordinator only reports
    /// the groups it owns. Results are sorted by group ID and deduplicated.
    /// The request version is negotiated per broker; use
    /// [`Self::list_groups_with_options`] when a broker-side state or type
    /// filter is required.
    #[tracing::instrument(level = "debug", name = "kafka.admin.list_groups", skip_all, err)]
    pub async fn list_groups(&self) -> Result<Vec<GroupListing>> {
        self.list_groups_with_options(ListGroupsOptions::default())
            .await
    }

    /// Lists Kafka groups with broker-negotiated state and type filters.
    ///
    /// Brokers advertising ListGroups v5 receive both filters and return the
    /// group state and type. Brokers advertising v4 receive the state filter
    /// and return group state. Older brokers fall back to v1 when no modern
    /// filter was requested; a requested filter that the broker cannot
    /// represent returns [`Error::Unsupported`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.list_groups_with_options",
        skip_all,
        err
    )]
    pub async fn list_groups_with_options(
        &self,
        options: ListGroupsOptions,
    ) -> Result<Vec<GroupListing>> {
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
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if api_versions.error_code != 0 {
                    return Err(client.broker_error(
                        api_versions.error_code,
                        "list groups capabilities".to_owned(),
                    ));
                }

                let Some(version) = api_versions
                    .highest_supported_version(LIST_GROUPS_API_KEY, 5)
                    .filter(|version| *version >= 1)
                else {
                    return Err(Error::Unsupported(
                        "broker does not advertise ListGroups v1 or newer",
                    ));
                };
                if !options.states.is_empty() && version < 4 {
                    return Err(Error::Unsupported(
                        "ListGroups state filters require v4 or newer",
                    ));
                }
                if !options.types.is_empty() && version < 5 {
                    return Err(Error::Unsupported(
                        "ListGroups type filters require v5 or newer",
                    ));
                }

                let response = match version {
                    5 => client
                        .list_groups_v5(options.states.clone(), options.types.clone())
                        .await
                        .map(ListGroupsResponse::V5),
                    4 => client
                        .list_groups_v4(options.states.clone())
                        .await
                        .map(ListGroupsResponse::V4),
                    _ => client.list_groups_v1().await.map(ListGroupsResponse::V1),
                };
                match response {
                    Ok(response)
                        if retry < self.max_retries
                            && is_retryable_admin_read_code(response.error_code()) =>
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

            if response.error_code() != 0 {
                return Err(self.config.broker_error(
                    response.error_code(),
                    format!("list groups on broker {}", broker.node_id),
                ));
            }
            let throttle_time =
                Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms()));
            for group in response.into_group_listings(broker.node_id, throttle_time) {
                groups.insert(group.group_id.clone(), group);
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

    /// Moves selected replica logs to broker-local directories.
    ///
    /// AlterReplicaLogDirs is a broker-local mutation, so `broker_id` is
    /// required and the assignments are sent only to that broker. Connection
    /// and ApiVersions discovery may be retried before transmission. Once the
    /// request is sent, a transport failure is returned without replaying the
    /// mutation because the broker may already have started the move.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.admin.alter_replica_log_dirs",
        skip_all,
        fields(broker_id, assignment_count = assignments.len()),
        err
    )]
    pub async fn alter_replica_log_dirs(
        &self,
        broker_id: i32,
        assignments: &[ReplicaLogDirAssignment],
    ) -> Result<AlterReplicaLogDirsResult> {
        let metadata = self.metadata_with_admin_retries(Some(Vec::new())).await?;
        let broker = metadata
            .brokers
            .iter()
            .find(|broker| broker.node_id == broker_id)
            .ok_or(Error::MissingBroker { node_id: broker_id })?;
        let endpoint = format!("{}:{}", broker.host, broker.port);
        let dirs = group_replica_log_dir_assignments(assignments);

        let mut retry = 0;
        let (mut client, version) = loop {
            let mut client = match self.config.connect_broker(endpoint.clone()).await {
                Ok(client) => client,
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
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
                Err(error) if retry < self.max_retries && is_retryable_admin_read_error(&error) => {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(version) = api_versions
                .highest_supported_version(34, 2)
                .filter(|version| *version >= 1)
            else {
                return Err(Error::Unsupported(
                    "broker does not advertise AlterReplicaLogDirs v1 or newer",
                ));
            };
            break (client, version);
        };

        let result = match version {
            1 => client.alter_replica_log_dirs_v1(dirs).await,
            _ => client.alter_replica_log_dirs_v2(dirs).await,
        };
        let response =
            result.map_err(|error| admin_mutation_error(&client, "AlterReplicaLogDirs", error))?;
        for topic in &response.results {
            if topic
                .partitions
                .iter()
                .any(|partition| partition.error_code != 0)
            {
                self.config.record_broker_error();
            }
        }
        Ok(AlterReplicaLogDirsResult::from_protocol(
            broker_id, response,
        ))
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
                let result = coordinator.delete_groups_v1(vec![group_id.clone()]).await;
                match result
                    .map_err(|error| admin_mutation_error(&coordinator, "DeleteGroups", error))
                {
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
            let result = coordinator
                .offset_delete_v0(group_id, request_topics.clone())
                .await;
            match result.map_err(|error| admin_mutation_error(&coordinator, "OffsetDelete", error))
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

    /// Lists committed offsets through the KIP-848 member-aware OffsetFetch API.
    ///
    /// The member ID may be omitted for an unjoined consumer-protocol request;
    /// joined members should pass the current ID and member epoch from
    /// [`ConsumerGroup::metadata`](crate::ConsumerGroup::metadata). A fresh
    /// metadata snapshot is required after every rejoin. `require_stable`
    /// requests that Kafka wait for unstable transactional offsets. Kafrust
    /// uses OffsetFetch v10 when the coordinator and Metadata v12 advertise
    /// the required capability, resolving names to UUIDs automatically. A
    /// caller may provide UUIDs through [`ConsumerGroupOffsetQuery::topic_id`]
    /// to avoid that metadata lookup. Otherwise it uses the name-based v9
    /// fallback.
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
        let mut fallback_coordinator = {
            let mut coordinator = self.group_coordinator_client(group_id).await?;
            if coordinator.supports_offset_fetch_v10().await? {
                let v10_topics = self
                    .offset_fetch_topics_v10_with_metadata(&mut coordinator, topics)
                    .await?;
                if let Some((request_topics_v10, topic_names)) = v10_topics {
                    tracing::debug!(
                        group_id,
                        api_version = 10,
                        "using member-aware Admin OffsetFetch"
                    );
                    return self
                        .list_consumer_group_offsets_with_member_v10(
                            MemberOffsetFetchV10Request {
                                group_id: group_id.to_owned(),
                                member_id: member_id.map(str::to_owned),
                                member_epoch,
                                topics: request_topics_v10.unwrap_or_default(),
                                topic_names,
                                require_stable,
                            },
                            coordinator,
                        )
                        .await;
                }
            }
            Some(coordinator)
        };

        let request_topics = topics.map(|topics| {
            topics
                .iter()
                .map(ConsumerGroupOffsetQuery::as_protocol_v9)
                .collect::<Vec<_>>()
        });
        let member_id = member_id.map(str::to_owned);
        let mut retry = 0;
        let response = loop {
            let mut coordinator = match fallback_coordinator.take() {
                Some(coordinator) => coordinator,
                None => self.group_coordinator_client(group_id).await?,
            };
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
    /// the typed result instead of being collapsed into one boolean. A
    /// transport failure after transmission returns an ambiguous mutation
    /// outcome rather than replaying the commit, because another actor may
    /// have advanced the group's committed offset in the meantime.
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
            let result = coordinator
                .offset_commit_v2(group_id, -1, "", -1, topics.clone())
                .await;
            match result {
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
                Err(error) => {
                    return Err(admin_mutation_error(&coordinator, "OffsetCommit", error))
                }
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

    /// Alters committed offsets through the KIP-848 member-aware OffsetCommit
    /// API.
    ///
    /// `member_id`, `member_epoch`, and `group_instance_id` must describe the
    /// current joined member when the broker enforces consumer-protocol
    /// membership. Retryable broker responses are retried with the same offset
    /// values; a transport failure after transmission is returned as an
    /// ambiguous mutation outcome rather than replayed. Kafrust uses
    /// OffsetCommit v10 when the coordinator and Metadata v12 advertise the
    /// required capability, resolving topic names to UUIDs automatically. A
    /// caller may provide UUIDs through [`ConsumerGroupOffset::topic_id`] to
    /// avoid that metadata lookup; otherwise the name-based v9 fallback is
    /// retained.
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
        let mut fallback_coordinator = {
            let mut coordinator = self.group_coordinator_client(group_id).await?;
            if coordinator.supports_offset_commit_v10().await? {
                let v10_topics = self
                    .offset_commit_topics_v10_with_metadata(&mut coordinator, offsets)
                    .await?;
                if let Some((topics_v10, topic_names)) = v10_topics {
                    tracing::debug!(
                        group_id,
                        api_version = 10,
                        "using member-aware Admin OffsetCommit"
                    );
                    return self
                        .alter_consumer_group_offsets_with_member_v10(
                            MemberOffsetCommitV10Request {
                                group_id: group_id.to_owned(),
                                member_id: member_id.to_owned(),
                                member_epoch,
                                group_instance_id: group_instance_id.map(str::to_owned),
                                topics: topics_v10,
                                topic_names,
                            },
                            coordinator,
                        )
                        .await;
                }
            }
            Some(coordinator)
        };

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
            let mut coordinator = match fallback_coordinator.take() {
                Some(coordinator) => coordinator,
                None => self.group_coordinator_client(group_id).await?,
            };
            let result = coordinator
                .offset_commit_v9(
                    group_id,
                    member_epoch,
                    member_id,
                    group_instance_id.clone(),
                    topics.clone(),
                )
                .await;
            match result {
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
                Err(error) => {
                    return Err(admin_mutation_error(&coordinator, "OffsetCommit", error))
                }
            }
        };

        record_offset_commit_v9_errors(&self.config, &response);
        Ok(AlterConsumerGroupOffsetsResult::from_protocol_v9(
            group_id,
            response.throttle_time_ms,
            response.topics,
        ))
    }

    async fn list_consumer_group_offsets_with_member_v10(
        &self,
        request: MemberOffsetFetchV10Request,
        mut coordinator: Client,
    ) -> Result<ListConsumerGroupOffsetsResult> {
        let mut retry = 0;
        let response = loop {
            match coordinator
                .offset_fetch_v10(
                    &request.group_id,
                    request.member_id.clone(),
                    request.member_epoch,
                    Some(request.topics.clone()),
                    request.require_stable,
                )
                .await
            {
                Ok(response)
                    if retry < self.max_retries && is_retryable_offset_fetch_v10(&response) =>
                {
                    record_offset_fetch_v10_errors(&self.config, &response);
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    coordinator = self.group_coordinator_client(&request.group_id).await?;
                }
                Ok(response) => break response,
                Err(error)
                    if retry < self.max_retries && is_retryable_admin_coordinator_error(&error) =>
                {
                    retry += 1;
                    self.config.record_retry();
                    tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                    coordinator = self.group_coordinator_client(&request.group_id).await?;
                }
                Err(error) => return Err(error),
            }
        };

        record_offset_fetch_v10_errors(&self.config, &response);
        let throttle_time_ms = response.throttle_time_ms;
        let group = response
            .groups
            .into_iter()
            .find(|group| group.group_id == request.group_id)
            .ok_or_else(|| Error::MissingGroupDescription {
                group_id: request.group_id.clone(),
            })?;
        let topics =
            group
                .topics
                .into_iter()
                .map(|topic| {
                    let name = request.topic_names.get(&topic.topic_id).cloned().ok_or(
                        Error::Unsupported("offset fetch response contained an unknown topic UUID"),
                    )?;
                    Ok(OffsetFetchTopicResponse {
                        name,
                        partitions: topic.partitions,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        Ok(ListConsumerGroupOffsetsResult::from_protocol_v9(
            &request.group_id,
            throttle_time_ms,
            OffsetFetchGroupResponse {
                group_id: group.group_id,
                topics,
                error_code: group.error_code,
            },
        ))
    }

    async fn alter_consumer_group_offsets_with_member_v10(
        &self,
        request: MemberOffsetCommitV10Request,
        mut coordinator: Client,
    ) -> Result<AlterConsumerGroupOffsetsResult> {
        let mut retry = 0;
        let response = loop {
            let result = coordinator
                .offset_commit_v10(
                    &request.group_id,
                    request.member_epoch,
                    &request.member_id,
                    request.group_instance_id.clone(),
                    request.topics.clone(),
                )
                .await;
            match result {
                Ok(response) => {
                    if retry < self.max_retries && is_retryable_offset_commit_v10(&response) {
                        record_offset_commit_v10_errors(&self.config, &response);
                        retry += 1;
                        self.config.record_retry();
                        tokio::time::sleep(admin_coordinator_retry_backoff(retry)).await;
                        coordinator = self.group_coordinator_client(&request.group_id).await?;
                    } else {
                        break response;
                    }
                }
                Err(error) => {
                    return Err(admin_mutation_error(&coordinator, "OffsetCommit", error))
                }
            }
        };

        record_offset_commit_v10_errors(&self.config, &response);
        let topics =
            response
                .topics
                .into_iter()
                .map(|topic| {
                    let name = request.topic_names.get(&topic.topic_id).cloned().ok_or(
                        Error::Unsupported(
                            "offset commit response contained an unknown topic UUID",
                        ),
                    )?;
                    Ok(OffsetCommitTopicResponse {
                        name,
                        partitions: topic.partitions,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        Ok(AlterConsumerGroupOffsetsResult::from_protocol_v9(
            &request.group_id,
            response.throttle_time_ms,
            topics,
        ))
    }

    async fn group_coordinator_client(&self, group_id: &str) -> Result<Client> {
        self.coordinator_client(group_id, CoordinatorType::Group)
            .await
    }

    async fn share_group_coordinator_client(&self, group_id: &str) -> Result<Client> {
        // Share-group membership and share-group admin APIs are owned by the
        // ordinary group coordinator. Share-partition state uses the separate
        // v6 lookup below.
        self.coordinator_client(group_id, CoordinatorType::Group)
            .await
    }

    async fn share_state_coordinator_routes(
        &self,
        group_id: &str,
        resources: &[ShareStateResource],
    ) -> Result<BTreeMap<ShareStateCoordinatorEndpoint, BTreeSet<ShareStateResource>>> {
        if resources.is_empty() {
            return Err(Error::Unsupported(
                "Share Group State requires at least one partition",
            ));
        }
        let mut retry = 0;
        loop {
            let result = async {
                let mut bootstrap = self.config.clone().connect().await?;
                let coordinator_resources = resources
                    .iter()
                    .map(|resource| (resource.topic_id, resource.partition))
                    .collect::<Vec<_>>();
                let response = bootstrap
                    .find_share_partition_coordinators(group_id, &coordinator_resources)
                    .await?;
                let mut routes = BTreeMap::<
                    ShareStateCoordinatorEndpoint,
                    BTreeSet<ShareStateResource>,
                >::new();
                for resource in resources {
                    let key = format_share_partition_coordinator_key(
                        group_id,
                        resource.topic_id,
                        resource.partition,
                    );
                    let coordinator = response
                        .coordinators
                        .iter()
                        .find(|coordinator| coordinator.coordinator_key == key)
                        .ok_or(Error::Unsupported(
                            "FindCoordinator v6 returned no share-partition result",
                        ))?;
                    if coordinator.error_code != 0 {
                        self.config.record_broker_error();
                        return Err(Error::Broker {
                            code: coordinator.error_code,
                            context: format!(
                                "find share partition coordinator for group {group_id}, partition {}",
                                resource.partition
                            ),
                        });
                    }
                    routes
                        .entry(ShareStateCoordinatorEndpoint {
                            host: coordinator.host.clone(),
                            port: coordinator.port,
                        })
                        .or_default()
                        .insert(*resource);
                }
                Ok(routes)
            }
            .await;
            match result {
                Ok(routes) => return Ok(routes),
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

    async fn coordinator_client(
        &self,
        group_id: &str,
        coordinator_type: CoordinatorType,
    ) -> Result<Client> {
        let mut retry = 0;
        loop {
            match self
                .coordinator_client_once(group_id, coordinator_type)
                .await
            {
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

    async fn coordinator_client_once(
        &self,
        group_id: &str,
        coordinator_type: CoordinatorType,
    ) -> Result<Client> {
        let mut bootstrap = self.config.clone().connect().await?;
        let coordinator = match coordinator_type {
            CoordinatorType::Group => bootstrap.find_group_coordinator(group_id).await?,
            CoordinatorType::Share => bootstrap.find_share_group_coordinator(group_id).await?,
            CoordinatorType::Transaction => {
                bootstrap.find_transaction_coordinator(group_id).await?
            }
        };
        if coordinator.error_code != 0 {
            self.config.record_broker_error();
            return Err(Error::Broker {
                code: coordinator.error_code,
                context: format!("find coordinator for group {group_id}"),
            });
        }
        self.config
            .connect_broker(format!("{}:{}", coordinator.host, coordinator.port))
            .await
    }

    async fn controller_client(&self) -> Result<Client> {
        if !self.config.controller_bootstrap_servers_ref().is_empty() {
            return self.config.connect_controller().await;
        }
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

    // ApiVersions discovery is safe to retry because the controller-scoped
    // operation has not been transmitted yet. The returned client is used for
    // exactly one subsequent token request.
    async fn controller_client_with_api_version(
        &self,
        api_key: i16,
        max_version: i16,
        operation: &'static str,
    ) -> Result<(Client, i16)> {
        let mut retry = 0;
        loop {
            let mut client = match self.controller_client().await {
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
            let api_versions = match client
                .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
                .await
            {
                Ok(api_versions) => api_versions,
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
            let Some(version) = api_versions
                .highest_supported_version(api_key, max_version)
                .filter(|version| *version >= 1)
            else {
                return Err(Error::Unsupported(operation));
            };
            return Ok((client, version));
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
        let result = controller_client
            .create_topics_v2(
                topics.iter().map(NewTopic::as_protocol).collect(),
                duration_millis_i32(options.timeout),
                options.validate_only,
            )
            .await;
        let response = result
            .map_err(|error| admin_mutation_error(&controller_client, "CreateTopics", error))?;

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
        let result = controller_client
            .create_partitions_v0(
                topics.iter().map(NewPartitions::as_protocol).collect(),
                duration_millis_i32(options.timeout),
                options.validate_only,
            )
            .await;
        let response = result
            .map_err(|error| admin_mutation_error(&controller_client, "CreatePartitions", error))?;

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
        let result = controller_client
            .delete_topics_v3(topic_names.to_vec(), duration_millis_i32(options.timeout))
            .await;
        let response = result
            .map_err(|error| admin_mutation_error(&controller_client, "DeleteTopics", error))?;

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

/// A Kafka principal used as a delegation-token owner or renewer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationTokenPrincipal {
    principal_type: String,
    principal_name: String,
}

impl DelegationTokenPrincipal {
    /// Creates a principal such as `User:alice`.
    pub fn new(principal_type: impl Into<String>, principal_name: impl Into<String>) -> Self {
        Self {
            principal_type: principal_type.into(),
            principal_name: principal_name.into(),
        }
    }

    /// Returns the Kafka principal type.
    pub fn principal_type(&self) -> &str {
        &self.principal_type
    }

    /// Returns the Kafka principal name.
    pub fn principal_name(&self) -> &str {
        &self.principal_name
    }

    fn as_protocol(&self) -> ProtocolDelegationTokenPrincipal {
        ProtocolDelegationTokenPrincipal {
            principal_type: self.principal_type.clone(),
            principal_name: self.principal_name.clone(),
        }
    }

    fn from_protocol(principal: ProtocolDelegationTokenPrincipal) -> Self {
        Self {
            principal_type: principal.principal_type,
            principal_name: principal.principal_name,
        }
    }
}

/// Options for creating one Kafka delegation token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDelegationTokenOptions {
    owner: Option<DelegationTokenPrincipal>,
    renewers: Vec<DelegationTokenPrincipal>,
    max_lifetime_ms: i64,
}

impl CreateDelegationTokenOptions {
    /// Creates options using the broker's configured maximum lifetime.
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects an explicit owner. Kafka uses the authenticated principal when
    /// no owner is provided.
    pub fn owner(mut self, owner: DelegationTokenPrincipal) -> Self {
        self.owner = Some(owner);
        self
    }

    /// Adds a principal that may renew the created token.
    pub fn renewer(mut self, renewer: DelegationTokenPrincipal) -> Self {
        self.renewers.push(renewer);
        self
    }

    /// Sets the maximum token lifetime. The default `-1` delegates the value
    /// to Kafka's server-side configuration.
    pub fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime_ms = duration_millis_i64(lifetime);
        self
    }
}

impl Default for CreateDelegationTokenOptions {
    fn default() -> Self {
        Self {
            owner: None,
            renewers: Vec::new(),
            max_lifetime_ms: -1,
        }
    }
}

/// A created delegation token and its credential HMAC.
#[derive(Clone, PartialEq, Eq)]
pub struct CreatedDelegationToken {
    owner: DelegationTokenPrincipal,
    requester: Option<DelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: Vec<u8>,
    error_code: i16,
    throttle_time: Duration,
}

impl fmt::Debug for CreatedDelegationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreatedDelegationToken")
            .field("owner", &self.owner)
            .field("requester", &self.requester)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac_len", &self.hmac.len())
            .field("error_code", &self.error_code)
            .field("throttle_time", &self.throttle_time)
            .finish()
    }
}

impl CreatedDelegationToken {
    /// Returns the token owner.
    pub fn owner(&self) -> &DelegationTokenPrincipal {
        &self.owner
    }

    /// Returns the requester when the broker negotiated CreateDelegationToken
    /// v3 or newer.
    pub fn requester(&self) -> Option<&DelegationTokenPrincipal> {
        self.requester.as_ref()
    }

    /// Returns the issue timestamp in Unix milliseconds.
    pub fn issue_timestamp_ms(&self) -> i64 {
        self.issue_timestamp_ms
    }

    /// Returns the expiry timestamp in Unix milliseconds.
    pub fn expiry_timestamp_ms(&self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Returns the maximum token timestamp in Unix milliseconds.
    pub fn max_timestamp_ms(&self) -> i64 {
        self.max_timestamp_ms
    }

    /// Returns Kafka's token identifier.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the HMAC required by renew and expire operations.
    pub fn hmac(&self) -> &[u8] {
        &self.hmac
    }

    /// Returns Kafka's raw response error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns whether Kafka created the token successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(response: CreateDelegationTokenResponse) -> Self {
        Self {
            owner: DelegationTokenPrincipal::from_protocol(response.owner),
            requester: response
                .requester
                .map(DelegationTokenPrincipal::from_protocol),
            issue_timestamp_ms: response.issue_timestamp_ms,
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            max_timestamp_ms: response.max_timestamp_ms,
            token_id: response.token_id,
            hmac: response.hmac,
            error_code: response.error_code,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
        }
    }
}

/// Result returned by delegation-token renew and expire operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelegationTokenOperationResult {
    error_code: i16,
    expiry_timestamp_ms: i64,
    throttle_time: Duration,
}

impl DelegationTokenOperationResult {
    /// Returns Kafka's raw response error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the resulting expiry timestamp in Unix milliseconds.
    pub fn expiry_timestamp_ms(&self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns whether Kafka accepted the operation.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(response: RenewDelegationTokenResponse) -> Self {
        Self {
            error_code: response.error_code,
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
        }
    }
}

/// Result returned by [`AdminClient::describe_delegation_tokens`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDelegationTokensResult {
    error_code: i16,
    throttle_time: Duration,
    tokens: Vec<DescribedDelegationTokenResult>,
}

impl DescribeDelegationTokensResult {
    /// Returns Kafka's top-level response error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the tokens visible to the authenticated principal.
    pub fn tokens(&self) -> &[DescribedDelegationTokenResult] {
        &self.tokens
    }

    /// Returns whether Kafka returned the token listing successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(response: DescribeDelegationTokenResponse) -> Self {
        Self {
            error_code: response.error_code,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            tokens: response
                .tokens
                .into_iter()
                .map(DescribedDelegationTokenResult::from_protocol)
                .collect(),
        }
    }
}

/// One token returned by DescribeDelegationToken.
#[derive(Clone, PartialEq, Eq)]
pub struct DescribedDelegationTokenResult {
    owner: DelegationTokenPrincipal,
    requester: Option<DelegationTokenPrincipal>,
    issue_timestamp_ms: i64,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
    token_id: String,
    hmac: Vec<u8>,
    renewers: Vec<DelegationTokenPrincipal>,
}

impl fmt::Debug for DescribedDelegationTokenResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescribedDelegationTokenResult")
            .field("owner", &self.owner)
            .field("requester", &self.requester)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac_len", &self.hmac.len())
            .field("renewers", &self.renewers)
            .finish()
    }
}

impl DescribedDelegationTokenResult {
    /// Returns the token owner.
    pub fn owner(&self) -> &DelegationTokenPrincipal {
        &self.owner
    }

    /// Returns the requester when the broker negotiated DescribeDelegationToken
    /// v3 or newer.
    pub fn requester(&self) -> Option<&DelegationTokenPrincipal> {
        self.requester.as_ref()
    }

    /// Returns the issue timestamp in Unix milliseconds.
    pub fn issue_timestamp_ms(&self) -> i64 {
        self.issue_timestamp_ms
    }

    /// Returns the expiry timestamp in Unix milliseconds.
    pub fn expiry_timestamp_ms(&self) -> i64 {
        self.expiry_timestamp_ms
    }

    /// Returns the maximum token timestamp in Unix milliseconds.
    pub fn max_timestamp_ms(&self) -> i64 {
        self.max_timestamp_ms
    }

    /// Returns Kafka's token identifier.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the HMAC required by renew and expire operations.
    pub fn hmac(&self) -> &[u8] {
        &self.hmac
    }

    /// Returns the principals allowed to renew this token.
    pub fn renewers(&self) -> &[DelegationTokenPrincipal] {
        &self.renewers
    }

    fn from_protocol(token: DescribedDelegationToken) -> Self {
        Self {
            owner: DelegationTokenPrincipal::from_protocol(token.owner),
            requester: token.requester.map(DelegationTokenPrincipal::from_protocol),
            issue_timestamp_ms: token.issue_timestamp_ms,
            expiry_timestamp_ms: token.expiry_timestamp_ms,
            max_timestamp_ms: token.max_timestamp_ms,
            token_id: token.token_id,
            hmac: token.hmac,
            renewers: token
                .renewers
                .into_iter()
                .map(DelegationTokenPrincipal::from_protocol)
                .collect(),
        }
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

/// A replica directory movement submitted to AlterReplicaLogDirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaLogDirAssignment {
    topic: String,
    partition_index: i32,
    destination_log_dir: String,
}

impl ReplicaLogDirAssignment {
    /// Creates a movement for one topic partition.
    pub fn new(
        topic: impl Into<String>,
        partition_index: i32,
        destination_log_dir: impl Into<String>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition_index,
            destination_log_dir: destination_log_dir.into(),
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the broker-local destination directory.
    pub fn destination_log_dir(&self) -> &str {
        &self.destination_log_dir
    }
}

fn group_replica_log_dir_assignments(
    assignments: &[ReplicaLogDirAssignment],
) -> Vec<AlterReplicaLogDir> {
    let mut grouped = BTreeMap::<String, BTreeMap<String, Vec<i32>>>::new();
    for assignment in assignments {
        grouped
            .entry(assignment.destination_log_dir.clone())
            .or_default()
            .entry(assignment.topic.clone())
            .or_default()
            .push(assignment.partition_index);
    }
    grouped
        .into_iter()
        .map(|(path, topics)| AlterReplicaLogDir {
            path,
            topics: topics
                .into_iter()
                .map(|(name, partitions)| {
                    kafrust_protocol::api::alter_replica_log_dirs::AlterReplicaLogDirTopic {
                        name,
                        partitions,
                    }
                })
                .collect(),
        })
        .collect()
}

/// Result returned by [`AdminClient::alter_replica_log_dirs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsResult {
    broker_id: i32,
    throttle_time: Duration,
    topics: Vec<AlterReplicaLogDirsTopicResult>,
}

impl AlterReplicaLogDirsResult {
    /// Returns the broker that accepted the movement request.
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns topic-level partition outcomes.
    pub fn topics(&self) -> &[AlterReplicaLogDirsTopicResult] {
        &self.topics
    }

    /// Returns whether every returned partition succeeded.
    pub fn is_success(&self) -> bool {
        self.topics
            .iter()
            .all(AlterReplicaLogDirsTopicResult::is_success)
    }

    /// Returns whether any partition movement failed.
    pub fn has_errors(&self) -> bool {
        !self.is_success()
    }

    fn from_protocol(broker_id: i32, response: AlterReplicaLogDirsResponse) -> Self {
        Self {
            broker_id,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .results
                .into_iter()
                .map(AlterReplicaLogDirsTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// Per-topic outcomes returned by AlterReplicaLogDirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsTopicResult {
    name: String,
    partitions: Vec<AlterReplicaLogDirsPartitionResult>,
}

impl AlterReplicaLogDirsTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns partition outcomes in broker response order.
    pub fn partitions(&self) -> &[AlterReplicaLogDirsPartitionResult] {
        &self.partitions
    }

    /// Returns whether all returned partitions succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(AlterReplicaLogDirsPartitionResult::is_success)
    }

    fn from_protocol(result: AlterReplicaLogDirTopicResult) -> Self {
        Self {
            name: result.name,
            partitions: result
                .partitions
                .into_iter()
                .map(AlterReplicaLogDirsPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// One partition outcome returned by AlterReplicaLogDirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterReplicaLogDirsPartitionResult {
    partition_index: i32,
    error_code: i16,
}

impl AlterReplicaLogDirsPartitionResult {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns Kafka's raw partition error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns whether Kafka accepted this partition movement.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's broker error classification, when present.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    fn from_protocol(
        result: kafrust_protocol::api::alter_replica_log_dirs::AlterReplicaLogDirPartitionResult,
    ) -> Self {
        Self {
            partition_index: result.partition_index,
            error_code: result.error_code,
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

    /// Returns whether the broker and every returned log directory succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0 && self.log_dirs.iter().all(LogDirectoryResult::is_success)
    }

    fn from_protocol(broker_id: i32, response: DescribeLogDirsResponse) -> Self {
        Self {
            broker_id,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            log_dirs: response
                .results
                .into_iter()
                .map(LogDirectoryResult::from_protocol)
                .collect(),
        }
    }
}

/// One broker log directory returned by DescribeLogDirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirectoryResult {
    error_code: i16,
    path: String,
    topics: Vec<LogDirectoryTopicResult>,
    total_bytes: i64,
    usable_bytes: i64,
    is_cordoned: bool,
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

    /// Returns total bytes on this log directory's volume, or `-1` when the
    /// negotiated broker version does not expose volume capacity.
    pub fn total_bytes(&self) -> i64 {
        self.total_bytes
    }

    /// Returns usable bytes on this log directory's volume, or `-1` when
    /// unavailable.
    pub fn usable_bytes(&self) -> i64 {
        self.usable_bytes
    }

    /// Returns whether this log directory's volume is cordoned.
    pub fn is_cordoned(&self) -> bool {
        self.is_cordoned
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
            total_bytes: result.total_bytes,
            usable_bytes: result.usable_bytes,
            is_cordoned: result.is_cordoned,
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
    group_state: Option<String>,
    group_type: Option<String>,
    coordinator_id: i32,
    throttle_time: Duration,
    api_version: i16,
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

    /// Returns the broker-reported group state when ListGroups v4 or newer
    /// was used, such as `Stable` or `Empty`.
    pub fn group_state(&self) -> Option<&str> {
        self.group_state.as_deref()
    }

    /// Returns the broker-reported group type when ListGroups v5 was used,
    /// such as `consumer`, `classic`, or `share`.
    pub fn group_type(&self) -> Option<&str> {
        self.group_type.as_deref()
    }

    /// Returns the broker ID that coordinates this group.
    pub fn coordinator_id(&self) -> i32 {
        self.coordinator_id
    }

    /// Returns the coordinator's throttle time for the ListGroups request.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the negotiated ListGroups API version used for this result.
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    fn from_protocol_v1(
        group: ListedGroupV1,
        coordinator_id: i32,
        throttle_time: Duration,
        api_version: i16,
    ) -> Self {
        Self {
            group_id: group.group_id,
            protocol_type: group.protocol_type,
            group_state: None,
            group_type: None,
            coordinator_id,
            throttle_time,
            api_version,
        }
    }

    fn from_protocol_v4(
        group: ListedGroupV4,
        coordinator_id: i32,
        throttle_time: Duration,
        api_version: i16,
    ) -> Self {
        Self {
            group_id: group.group_id,
            protocol_type: group.protocol_type,
            group_state: Some(group.group_state),
            group_type: None,
            coordinator_id,
            throttle_time,
            api_version,
        }
    }

    fn from_protocol_v5(
        group: ListedGroupV5,
        coordinator_id: i32,
        throttle_time: Duration,
        api_version: i16,
    ) -> Self {
        Self {
            group_id: group.group_id,
            protocol_type: group.protocol_type,
            group_state: Some(group.group_state),
            group_type: Some(group.group_type),
            coordinator_id,
            throttle_time,
            api_version,
        }
    }
}

/// Options for broker-negotiated ListGroups queries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListGroupsOptions {
    states: Vec<String>,
    types: Vec<String>,
}

impl ListGroupsOptions {
    /// Creates an option set that asks for every group.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a broker-side group-state filter.
    pub fn state(mut self, state: impl Into<String>) -> Self {
        self.states.push(state.into());
        self
    }

    /// Replaces the broker-side group-state filter.
    pub fn states(mut self, states: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.states = states.into_iter().map(Into::into).collect();
        self
    }

    /// Adds a broker-side group-type filter.
    pub fn group_type(mut self, group_type: impl Into<String>) -> Self {
        self.types.push(group_type.into());
        self
    }

    /// Replaces the broker-side group-type filter.
    pub fn group_types(mut self, group_types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.types = group_types.into_iter().map(Into::into).collect();
        self
    }

    /// Returns the requested group-state filter.
    pub fn states_ref(&self) -> &[String] {
        &self.states
    }

    /// Returns the requested group-type filter.
    pub fn group_types_ref(&self) -> &[String] {
        &self.types
    }
}

/// Outcome for one group in [`AdminClient::delete_consumer_groups`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerGroupResult {
    group_id: String,
    error_code: i16,
    throttle_time: Duration,
}

/// Outcome for one share group in [`AdminClient::delete_share_groups`].
pub type DeleteShareGroupResult = DeleteConsumerGroupResult;

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

/// One listener endpoint advertised by a KRaft voter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterListener {
    name: String,
    host: String,
    port: u16,
}

impl RaftVoterListener {
    /// Creates a listener endpoint from its Kafka listener name, host, and port.
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            host: host.into(),
            port,
        }
    }

    /// Returns the Kafka listener name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the listener host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the listener port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl From<&RaftVoterListener> for ProtocolRaftVoterListener {
    fn from(listener: &RaftVoterListener) -> Self {
        Self {
            name: listener.name.clone(),
            host: listener.host.clone(),
            port: listener.port,
        }
    }
}

/// Options for adding a voter to the KRaft controller quorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRaftVoterOptions {
    cluster_id: Option<String>,
    timeout: Duration,
    voter_id: i32,
    voter_directory_id: [u8; 16],
    listeners: Vec<RaftVoterListener>,
    ack_when_committed: bool,
}

impl AddRaftVoterOptions {
    /// Creates options with a 60-second controller timeout and no listeners.
    pub fn new(voter_id: i32, voter_directory_id: [u8; 16]) -> Self {
        Self {
            cluster_id: None,
            timeout: Duration::from_secs(60),
            voter_id,
            voter_directory_id,
            listeners: Vec::new(),
            ack_when_committed: false,
        }
    }

    /// Sets the expected Kafka cluster ID, when known.
    pub fn cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Sets the controller-side voter addition timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Adds one listener endpoint to the voter registration.
    pub fn listener(mut self, listener: RaftVoterListener) -> Self {
        self.listeners.push(listener);
        self
    }

    /// Requests that v1 wait until the voter addition is committed.
    pub fn ack_when_committed(mut self, ack_when_committed: bool) -> Self {
        self.ack_when_committed = ack_when_committed;
        self
    }

    /// Returns the configured cluster ID.
    pub fn cluster_id_ref(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Returns the controller-side timeout.
    pub fn request_timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the voter broker ID.
    pub fn voter_id(&self) -> i32 {
        self.voter_id
    }

    /// Returns the voter directory UUID.
    pub fn voter_directory_id(&self) -> [u8; 16] {
        self.voter_directory_id
    }

    /// Returns the listener endpoints.
    pub fn listeners(&self) -> &[RaftVoterListener] {
        &self.listeners
    }

    /// Returns whether the request waits for the committed quorum change.
    pub fn ack_when_committed_ref(&self) -> bool {
        self.ack_when_committed
    }
}

/// Options for removing a voter from the KRaft controller quorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveRaftVoterOptions {
    cluster_id: Option<String>,
    voter_id: i32,
    voter_directory_id: [u8; 16],
}

impl RemoveRaftVoterOptions {
    /// Creates options for a voter identified by broker ID and directory UUID.
    pub fn new(voter_id: i32, voter_directory_id: [u8; 16]) -> Self {
        Self {
            cluster_id: None,
            voter_id,
            voter_directory_id,
        }
    }

    /// Sets the expected Kafka cluster ID.
    pub fn cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Returns the configured cluster ID.
    pub fn cluster_id_ref(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Returns the voter broker ID.
    pub fn voter_id(&self) -> i32 {
        self.voter_id
    }

    /// Returns the voter directory UUID.
    pub fn voter_directory_id(&self) -> [u8; 16] {
        self.voter_directory_id
    }
}

/// Outcome returned by AddRaftVoter and RemoveRaftVoter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterResult {
    api_version: i16,
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
}

impl RaftVoterResult {
    /// Returns the negotiated API version.
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a broker error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns whether Kafka accepted the voter mutation.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(response: AddRaftVoterResponse, api_version: i16) -> Self {
        Self {
            api_version,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
        }
    }
}

/// Outcome returned by [`AdminClient::unregister_broker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterBrokerResult {
    api_version: i16,
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
}

impl UnregisterBrokerResult {
    /// Returns the negotiated API version.
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a broker error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns whether Kafka accepted the broker-unregistration request.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(response: UnregisterBrokerResponseV0, api_version: i16) -> Self {
        Self {
            api_version,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
        }
    }
}

/// The Kafka UpdateFeatures v1 operation to apply to a finalized feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureUpgradeType {
    /// Raise or retain the feature level without allowing a downgrade.
    Upgrade,
    /// Allow a downgrade that Kafka considers safe.
    SafeDowngrade,
    /// Allow a downgrade that Kafka considers unsafe.
    UnsafeDowngrade,
}

/// One finalized Kafka feature level change submitted to
/// [`AdminClient::update_features`]. A level below one requests removal of a
/// finalized feature according to Kafka's UpdateFeatures contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdate {
    feature: String,
    max_version_level: i16,
    upgrade_type: FeatureUpgradeType,
}

impl FeatureUpdate {
    /// Creates a feature update with downgrade protection enabled by default.
    pub fn new(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FeatureUpgradeType::Upgrade,
        }
    }

    /// Allows Kafka to lower or remove the currently finalized feature level.
    pub fn allow_downgrade(mut self, allow_downgrade: bool) -> Self {
        self.upgrade_type = if allow_downgrade {
            FeatureUpgradeType::SafeDowngrade
        } else {
            FeatureUpgradeType::Upgrade
        };
        self
    }

    /// Sets Kafka's v1 three-way feature upgrade operation.
    pub fn upgrade_type(mut self, upgrade_type: FeatureUpgradeType) -> Self {
        self.upgrade_type = upgrade_type;
        self
    }

    /// Returns the Kafka feature name.
    pub fn feature(&self) -> &str {
        &self.feature
    }

    /// Returns the requested maximum finalized version level.
    pub fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Returns whether Kafka may lower or remove the current level.
    pub fn downgrade_allowed(&self) -> bool {
        !matches!(self.upgrade_type, FeatureUpgradeType::Upgrade)
    }

    /// Returns the requested Kafka v1 operation.
    pub fn upgrade_type_ref(&self) -> FeatureUpgradeType {
        self.upgrade_type
    }

    fn as_protocol_v0(&self) -> Option<kafrust_protocol::api::update_features::FeatureUpdateV0> {
        if matches!(self.upgrade_type, FeatureUpgradeType::UnsafeDowngrade) {
            return None;
        }
        Some(kafrust_protocol::api::update_features::FeatureUpdateV0 {
            feature: self.feature.clone(),
            max_version_level: self.max_version_level,
            allow_downgrade: self.downgrade_allowed(),
        })
    }

    fn as_protocol_v1(&self) -> kafrust_protocol::api::update_features::FeatureUpdateV1 {
        let upgrade_type = match self.upgrade_type {
            FeatureUpgradeType::Upgrade => 1,
            FeatureUpgradeType::SafeDowngrade => 2,
            FeatureUpgradeType::UnsafeDowngrade => 3,
        };
        kafrust_protocol::api::update_features::FeatureUpdateV1 {
            feature: self.feature.clone(),
            max_version_level: self.max_version_level,
            upgrade_type,
        }
    }
}

/// Options for a controller-routed UpdateFeatures request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateFeaturesOptions {
    timeout: Duration,
    validate_only: bool,
}

impl UpdateFeaturesOptions {
    /// Creates options with Kafka's 60-second default request timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the broker-side feature update timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the configured broker request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.timeout
    }

    /// Validates the requested changes without finalizing them when the
    /// broker supports UpdateFeatures v1.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Returns whether the request is validation-only.
    pub fn validate_only_ref(&self) -> bool {
        self.validate_only
    }
}

impl Default for UpdateFeaturesOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            validate_only: false,
        }
    }
}

/// Top-level and per-feature outcomes returned by UpdateFeatures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateFeaturesResult {
    throttle_time: Duration,
    error_code: i16,
    error_message: Option<String>,
    results: Vec<FeatureUpdateResult>,
}

impl UpdateFeaturesResult {
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

    /// Returns whether the top-level feature update request succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns per-feature outcomes retained by Kafka for UpdateFeatures.
    pub fn results(&self) -> &[FeatureUpdateResult] {
        &self.results
    }

    fn from_protocol(response: UpdateFeaturesResponseV0) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            error_message: response.error_message,
            results: response
                .results
                .into_iter()
                .map(FeatureUpdateResult::from_protocol)
                .collect(),
        }
    }
}

/// Outcome for one feature in an UpdateFeatures response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureUpdateResult {
    feature: String,
    error_code: i16,
    error_message: Option<String>,
}

impl FeatureUpdateResult {
    /// Returns the Kafka feature name.
    pub fn feature(&self) -> &str {
        &self.feature
    }

    /// Returns the feature-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the feature-level Kafka error message, when present.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka accepted this feature update.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(
        result: kafrust_protocol::api::update_features::FeatureUpdateResultV0,
    ) -> Self {
        Self {
            feature: result.feature,
            error_code: result.error_code,
            error_message: result.error_message,
        }
    }
}

/// A Kafka feature version range supported by one broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedFeature {
    name: String,
    min_version: i16,
    max_version: i16,
}

impl SupportedFeature {
    /// Returns the Kafka feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the minimum version level supported by the broker.
    pub fn min_version(&self) -> i16 {
        self.min_version
    }

    /// Returns the maximum version level supported by the broker.
    pub fn max_version(&self) -> i16 {
        self.max_version
    }
}

/// A cluster-wide finalized Kafka feature version range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedFeature {
    name: String,
    min_version_level: i16,
    max_version_level: i16,
}

impl FinalizedFeature {
    /// Returns the Kafka feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the finalized minimum version level.
    pub fn min_version_level(&self) -> i16 {
        self.min_version_level
    }

    /// Returns the finalized maximum version level.
    pub fn max_version_level(&self) -> i16 {
        self.max_version_level
    }
}

/// Broker-supported and cluster-finalized feature metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureMetadata {
    supported_features: Vec<SupportedFeature>,
    finalized_features_epoch: i64,
    finalized_features: Vec<FinalizedFeature>,
    zk_migration_ready: bool,
}

impl FeatureMetadata {
    /// Returns the feature ranges supported by the broker serving the request.
    pub fn supported_features(&self) -> &[SupportedFeature] {
        &self.supported_features
    }

    /// Returns the finalized-feature metadata epoch, or `-1` if Kafka reported
    /// that the epoch is unknown.
    pub fn finalized_features_epoch(&self) -> i64 {
        self.finalized_features_epoch
    }

    /// Returns cluster-wide finalized feature ranges.
    pub fn finalized_features(&self) -> &[FinalizedFeature] {
        &self.finalized_features
    }

    /// Returns whether the broker reports that ZooKeeper migration is ready.
    pub fn zk_migration_ready(&self) -> bool {
        self.zk_migration_ready
    }

    fn from_protocol(response: ApiVersionsResponseV3) -> Self {
        Self {
            supported_features: response
                .supported_features
                .into_iter()
                .map(|feature| SupportedFeature {
                    name: feature.name,
                    min_version: feature.min_version,
                    max_version: feature.max_version,
                })
                .collect(),
            finalized_features_epoch: response.finalized_features_epoch,
            finalized_features: response
                .finalized_features
                .into_iter()
                .map(|feature| FinalizedFeature {
                    name: feature.name,
                    min_version_level: feature.min_version_level,
                    max_version_level: feature.max_version_level,
                })
                .collect(),
            zk_migration_ready: response.zk_migration_ready,
        }
    }
}

/// Kafka endpoint set selected by DescribeCluster v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeClusterEndpointType {
    /// Return broker endpoints.
    Brokers,
    /// Return controller endpoints.
    Controllers,
    /// Preserve an endpoint type introduced by a newer broker.
    Other(i8),
}

impl DescribeClusterEndpointType {
    /// Returns Kafka's raw endpoint type code.
    pub fn code(self) -> i8 {
        match self {
            Self::Brokers => 1,
            Self::Controllers => 2,
            Self::Other(code) => code,
        }
    }

    /// Preserves an endpoint type received from Kafka.
    pub fn from_code(code: i8) -> Self {
        match code {
            1 => Self::Brokers,
            2 => Self::Controllers,
            code => Self::Other(code),
        }
    }
}

/// Options for Kafka's dedicated DescribeCluster API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DescribeClusterOptions {
    include_cluster_authorized_operations: bool,
    endpoint_type: Option<DescribeClusterEndpointType>,
}

impl DescribeClusterOptions {
    /// Creates options that request broker endpoints without ACL bitfields.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests the cluster authorized-operations bitfield.
    pub fn include_cluster_authorized_operations(mut self, include: bool) -> Self {
        self.include_cluster_authorized_operations = include;
        self
    }

    /// Selects the endpoint set for DescribeCluster v1.
    pub fn endpoint_type(mut self, endpoint_type: DescribeClusterEndpointType) -> Self {
        self.endpoint_type = Some(endpoint_type);
        self
    }

    /// Returns whether cluster authorized operations were requested.
    pub fn includes_cluster_authorized_operations(&self) -> bool {
        self.include_cluster_authorized_operations
    }

    /// Returns the requested endpoint set, or `None` for the v0-compatible
    /// default broker endpoint selection.
    pub fn endpoint_type_ref(&self) -> Option<DescribeClusterEndpointType> {
        self.endpoint_type
    }
}

/// Kafka cluster metadata returned by [`AdminClient::describe_cluster`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDescription {
    controller_id: i32,
    brokers: Vec<BrokerDescription>,
    cluster_id: Option<String>,
    endpoint_type: Option<DescribeClusterEndpointType>,
    cluster_authorized_operations: Option<i32>,
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

    /// Returns the Kafka cluster ID when the dedicated DescribeCluster path
    /// supplied it.
    pub fn cluster_id(&self) -> Option<&str> {
        self.cluster_id.as_deref()
    }

    /// Returns the endpoint set represented by this response.
    pub fn endpoint_type(&self) -> Option<DescribeClusterEndpointType> {
        self.endpoint_type
    }

    /// Returns Kafka's cluster authorized-operations bitfield when requested.
    pub fn cluster_authorized_operations(&self) -> Option<i32> {
        self.cluster_authorized_operations
    }

    /// Returns the active controller broker when it is present in metadata.
    pub fn controller(&self) -> Option<&BrokerDescription> {
        self.brokers
            .iter()
            .find(|broker| broker.id == self.controller_id)
    }

    fn from_metadata(metadata: MetadataResponseV1) -> Self {
        Self {
            controller_id: metadata.controller_id,
            brokers: metadata
                .brokers
                .into_iter()
                .map(BrokerDescription::from_protocol)
                .collect(),
            cluster_id: None,
            endpoint_type: None,
            cluster_authorized_operations: None,
        }
    }

    fn from_describe_cluster(response: DescribeClusterResponse) -> Self {
        Self {
            controller_id: response.controller_id,
            brokers: response
                .brokers
                .into_iter()
                .map(|broker| BrokerDescription {
                    id: broker.node_id,
                    host: broker.host,
                    port: broker.port,
                    rack: broker.rack,
                })
                .collect(),
            cluster_id: Some(response.cluster_id),
            endpoint_type: response
                .endpoint_type
                .map(DescribeClusterEndpointType::from_code),
            cluster_authorized_operations: Some(response.cluster_authorized_operations),
        }
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

/// Cursor for paging through DescribeTopicPartitions results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsCursor {
    topic_name: String,
    partition_index: i32,
}

impl DescribeTopicPartitionsCursor {
    /// Creates a cursor starting at the given topic and partition.
    pub fn new(topic_name: impl Into<String>, partition_index: i32) -> Self {
        Self {
            topic_name: topic_name.into(),
            partition_index,
        }
    }

    /// Returns the topic name at the cursor.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the partition index at the cursor.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }
}

/// Options controlling a DescribeTopicPartitions request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsOptions {
    response_partition_limit: i32,
    cursor: Option<DescribeTopicPartitionsCursor>,
}

impl Default for DescribeTopicPartitionsOptions {
    fn default() -> Self {
        Self {
            response_partition_limit: 2_000,
            cursor: None,
        }
    }
}

impl DescribeTopicPartitionsOptions {
    /// Creates options using Kafka's default partition limit of 2000.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of partitions returned by the broker.
    pub fn with_response_partition_limit(mut self, limit: i32) -> Self {
        self.response_partition_limit = limit;
        self
    }

    /// Sets the starting cursor for a paged request.
    pub fn with_cursor(mut self, cursor: DescribeTopicPartitionsCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Returns the requested response partition limit.
    pub fn response_partition_limit(&self) -> i32 {
        self.response_partition_limit
    }

    /// Returns the optional paging cursor.
    pub fn cursor(&self) -> Option<&DescribeTopicPartitionsCursor> {
        self.cursor.as_ref()
    }
}

/// One partition returned by DescribeTopicPartitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsPartition {
    error_code: i16,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: Vec<i32>,
    isr_nodes: Vec<i32>,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_elr: Option<Vec<i32>>,
    offline_replicas: Vec<i32>,
}

impl DescribeTopicPartitionsPartition {
    /// Returns Kafka's partition-level error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a partition error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the current leader broker ID.
    pub fn leader_id(&self) -> i32 {
        self.leader_id
    }

    /// Returns the leader epoch.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns all replica broker IDs.
    pub fn replica_nodes(&self) -> &[i32] {
        &self.replica_nodes
    }

    /// Returns in-sync replica broker IDs.
    pub fn isr_nodes(&self) -> &[i32] {
        &self.isr_nodes
    }

    /// Returns Kafka's eligible leader replica set when present.
    pub fn eligible_leader_replicas(&self) -> Option<&[i32]> {
        self.eligible_leader_replicas.as_deref()
    }

    /// Returns Kafka's last known eligible leader replica set when present.
    pub fn last_known_elr(&self) -> Option<&[i32]> {
        self.last_known_elr.as_deref()
    }

    /// Returns replicas Kafka currently marks offline.
    pub fn offline_replicas(&self) -> &[i32] {
        &self.offline_replicas
    }

    /// Returns whether Kafka reported no partition-level error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }
}

/// One topic returned by DescribeTopicPartitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsTopic {
    error_code: i16,
    name: Option<String>,
    topic_id: [u8; 16],
    is_internal: bool,
    partitions: Vec<DescribeTopicPartitionsPartition>,
    topic_authorized_operations: i32,
}

impl DescribeTopicPartitionsTopic {
    /// Returns Kafka's topic-level error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns kafrust's classification for a topic error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns the optional topic name included by Kafka.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    /// Returns whether Kafka marks this as an internal topic.
    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    /// Returns partition descriptions included in this response page.
    pub fn partitions(&self) -> &[DescribeTopicPartitionsPartition] {
        &self.partitions
    }

    /// Returns Kafka's topic authorized-operations bitfield.
    pub fn topic_authorized_operations(&self) -> i32 {
        self.topic_authorized_operations
    }

    /// Returns whether Kafka reported no topic-level error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }
}

/// Result returned by [`AdminClient::describe_topic_partitions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicPartitionsResult {
    throttle_time: Duration,
    topics: Vec<DescribeTopicPartitionsTopic>,
    next_cursor: Option<DescribeTopicPartitionsCursor>,
}

impl DescribeTopicPartitionsResult {
    /// Returns the broker throttle duration.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns topic descriptions in the response page.
    pub fn topics(&self) -> &[DescribeTopicPartitionsTopic] {
        &self.topics
    }

    /// Returns Kafka's next page cursor, when more partitions are available.
    pub fn next_cursor(&self) -> Option<&DescribeTopicPartitionsCursor> {
        self.next_cursor.as_ref()
    }

    /// Returns whether all topic and partition entries succeeded.
    pub fn is_success(&self) -> bool {
        self.topics.iter().all(|topic| {
            topic.is_success()
                && topic
                    .partitions
                    .iter()
                    .all(DescribeTopicPartitionsPartition::is_success)
        })
    }

    fn from_protocol(response: DescribeTopicPartitionsResponseV0) -> Self {
        Self {
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .topics
                .into_iter()
                .map(|topic| DescribeTopicPartitionsTopic {
                    error_code: topic.error_code,
                    name: topic.name,
                    topic_id: topic.topic_id,
                    is_internal: topic.is_internal,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition| DescribeTopicPartitionsPartition {
                            error_code: partition.error_code,
                            partition_index: partition.partition_index,
                            leader_id: partition.leader_id,
                            leader_epoch: partition.leader_epoch,
                            replica_nodes: partition.replica_nodes,
                            isr_nodes: partition.isr_nodes,
                            eligible_leader_replicas: partition.eligible_leader_replicas,
                            last_known_elr: partition.last_known_elr,
                            offline_replicas: partition.offline_replicas,
                        })
                        .collect(),
                    topic_authorized_operations: topic.topic_authorized_operations,
                })
                .collect(),
            next_cursor: response
                .next_cursor
                .map(|cursor| DescribeTopicPartitionsCursor {
                    topic_name: cursor.topic_name,
                    partition_index: cursor.partition_index,
                }),
        }
    }
}

/// One topic and partition selection for [`AdminClient::describe_quorum`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumTopic {
    name: String,
    partition_indexes: Vec<i32>,
}

impl DescribeQuorumTopic {
    /// Creates a quorum query for one topic with no selected partitions.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partition_indexes: Vec::new(),
        }
    }

    /// Adds a partition index to this topic query.
    pub fn partition(mut self, partition_index: i32) -> Self {
        self.partition_indexes.push(partition_index);
        self
    }

    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the selected partition indexes.
    pub fn partition_indexes(&self) -> &[i32] {
        &self.partition_indexes
    }
}

/// Replica state returned by DescribeQuorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumReplicaState {
    replica_id: i32,
    replica_directory_id: Option<[u8; 16]>,
    log_end_offset: i64,
    last_fetch_timestamp: Option<i64>,
    last_caught_up_timestamp: Option<i64>,
}

impl DescribeQuorumReplicaState {
    /// Returns the replica broker ID.
    pub fn replica_id(&self) -> i32 {
        self.replica_id
    }

    /// Returns the replica directory UUID when supplied by v2.
    pub fn replica_directory_id(&self) -> Option<[u8; 16]> {
        self.replica_directory_id
    }

    /// Returns the replica's last known log end offset.
    pub fn log_end_offset(&self) -> i64 {
        self.log_end_offset
    }

    /// Returns the last fetch timestamp for v1 and newer responses.
    pub fn last_fetch_timestamp(&self) -> Option<i64> {
        self.last_fetch_timestamp
    }

    /// Returns the last caught-up timestamp for v1 and newer responses.
    pub fn last_caught_up_timestamp(&self) -> Option<i64> {
        self.last_caught_up_timestamp
    }
}

/// One partition returned by DescribeQuorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumPartitionResult {
    partition_index: i32,
    error_code: i16,
    error_message: Option<String>,
    leader_id: i32,
    leader_epoch: i32,
    high_watermark: i64,
    current_voters: Vec<DescribeQuorumReplicaState>,
    observers: Vec<DescribeQuorumReplicaState>,
}

impl DescribeQuorumPartitionResult {
    /// Returns the partition index.
    pub fn partition_index(&self) -> i32 {
        self.partition_index
    }

    /// Returns the partition error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a partition error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns the current quorum leader ID.
    pub fn leader_id(&self) -> i32 {
        self.leader_id
    }

    /// Returns the current quorum leader epoch.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns the partition high watermark.
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    /// Returns current voter replica state.
    pub fn current_voters(&self) -> &[DescribeQuorumReplicaState] {
        &self.current_voters
    }

    /// Returns observer replica state.
    pub fn observers(&self) -> &[DescribeQuorumReplicaState] {
        &self.observers
    }

    /// Returns whether Kafka reported no partition error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }
}

/// One topic returned by DescribeQuorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumTopicResult {
    name: String,
    partitions: Vec<DescribeQuorumPartitionResult>,
}

impl DescribeQuorumTopicResult {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns partition quorum state.
    pub fn partitions(&self) -> &[DescribeQuorumPartitionResult] {
        &self.partitions
    }
}

/// One listener endpoint returned by DescribeQuorum v2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumListener {
    name: String,
    host: String,
    port: u16,
}

impl DescribeQuorumListener {
    /// Returns the listener name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the listener host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the listener port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// One controller node returned by DescribeQuorum v2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumNode {
    node_id: i32,
    listeners: Vec<DescribeQuorumListener>,
}

impl DescribeQuorumNode {
    /// Returns the controller node ID.
    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Returns advertised controller listeners.
    pub fn listeners(&self) -> &[DescribeQuorumListener] {
        &self.listeners
    }
}

/// Result returned by [`AdminClient::describe_quorum`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeQuorumResult {
    api_version: i16,
    error_code: i16,
    error_message: Option<String>,
    topics: Vec<DescribeQuorumTopicResult>,
    nodes: Vec<DescribeQuorumNode>,
}

impl DescribeQuorumResult {
    /// Returns the negotiated DescribeQuorum version.
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    /// Returns the top-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the top-level broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns kafrust's classification for a top-level error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns topic quorum results.
    pub fn topics(&self) -> &[DescribeQuorumTopicResult] {
        &self.topics
    }

    /// Returns controller node endpoints included by v2.
    pub fn nodes(&self) -> &[DescribeQuorumNode] {
        &self.nodes
    }

    /// Returns whether all top-level and partition errors are zero.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
            && self.topics.iter().all(|topic| {
                topic
                    .partitions
                    .iter()
                    .all(DescribeQuorumPartitionResult::is_success)
            })
    }

    fn from_protocol(response: DescribeQuorumResponse) -> Self {
        Self {
            api_version: response.api_version,
            error_code: response.error_code,
            error_message: response.error_message,
            topics: response
                .topics
                .into_iter()
                .map(|topic| DescribeQuorumTopicResult {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition| DescribeQuorumPartitionResult {
                            partition_index: partition.partition_index,
                            error_code: partition.error_code,
                            error_message: partition.error_message,
                            leader_id: partition.leader_id,
                            leader_epoch: partition.leader_epoch,
                            high_watermark: partition.high_watermark,
                            current_voters: partition
                                .current_voters
                                .into_iter()
                                .map(|replica| DescribeQuorumReplicaState {
                                    replica_id: replica.replica_id,
                                    replica_directory_id: replica.replica_directory_id,
                                    log_end_offset: replica.log_end_offset,
                                    last_fetch_timestamp: replica.last_fetch_timestamp,
                                    last_caught_up_timestamp: replica.last_caught_up_timestamp,
                                })
                                .collect(),
                            observers: partition
                                .observers
                                .into_iter()
                                .map(|replica| DescribeQuorumReplicaState {
                                    replica_id: replica.replica_id,
                                    replica_directory_id: replica.replica_directory_id,
                                    log_end_offset: replica.log_end_offset,
                                    last_fetch_timestamp: replica.last_fetch_timestamp,
                                    last_caught_up_timestamp: replica.last_caught_up_timestamp,
                                })
                                .collect(),
                        })
                        .collect(),
                })
                .collect(),
            nodes: response
                .nodes
                .into_iter()
                .map(|node| DescribeQuorumNode {
                    node_id: node.node_id,
                    listeners: node
                        .listeners
                        .into_iter()
                        .map(|listener| DescribeQuorumListener {
                            name: listener.name,
                            host: listener.host,
                            port: listener.port,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// Kafka resource types accepted by ListConfigResources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigResourceType {
    /// Topic configuration resources.
    Topic,
    /// Broker configuration resources.
    Broker,
    /// Broker logger configuration resources.
    BrokerLogger,
    /// Client metrics configuration resources.
    ClientMetrics,
    /// Consumer group configuration resources.
    Group,
    /// A resource type introduced by a newer broker.
    Other(i8),
}

impl ConfigResourceType {
    /// Returns Kafka's raw resource type code.
    pub const fn code(self) -> i8 {
        match self {
            Self::Topic => 2,
            Self::Broker => 4,
            Self::BrokerLogger => 8,
            Self::ClientMetrics => 16,
            Self::Group => 32,
            Self::Other(code) => code,
        }
    }

    /// Converts a broker resource type code without discarding unknown values.
    pub const fn from_code(code: i8) -> Self {
        match code {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
            16 => Self::ClientMetrics,
            32 => Self::Group,
            code => Self::Other(code),
        }
    }
}

/// Options for one ListConfigResources operation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListConfigResourcesOptions {
    resource_types: Vec<ConfigResourceType>,
}

impl ListConfigResourcesOptions {
    /// Creates an option set that asks for all resource types supported by Kafka.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a resource type to the broker-side filter.
    pub fn resource_type(mut self, resource_type: ConfigResourceType) -> Self {
        self.resource_types.push(resource_type);
        self
    }

    /// Replaces the broker-side resource type filter.
    pub fn resource_types(
        mut self,
        resource_types: impl IntoIterator<Item = ConfigResourceType>,
    ) -> Self {
        self.resource_types = resource_types.into_iter().collect();
        self
    }

    /// Returns the requested resource type filter, or an empty slice for all types.
    pub fn resource_types_ref(&self) -> &[ConfigResourceType] {
        &self.resource_types
    }
}

/// Complete response from one ListConfigResources operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConfigResourcesResult {
    api_version: i16,
    throttle_time: Duration,
    error_code: i16,
    resources: Vec<ListedConfigResource>,
}

impl ListConfigResourcesResult {
    /// Returns the negotiated API 74 version.
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns the top-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns whether Kafka listed the resources successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns kafrust's classification for a top-level error code.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        (self.error_code != 0).then(|| BrokerErrorKind::from_code(self.error_code))
    }

    /// Returns resources in broker response order.
    pub fn resources(&self) -> &[ListedConfigResource] {
        &self.resources
    }

    /// Consumes this response and returns its resources.
    pub fn into_resources(self) -> Vec<ListedConfigResource> {
        self.resources
    }

    fn from_protocol(
        response: kafrust_protocol::api::list_config_resources::ListConfigResourcesResponseV1,
        api_version: i16,
    ) -> Self {
        Self {
            api_version,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            resources: response
                .resources
                .into_iter()
                .map(ListedConfigResource::from_protocol)
                .collect(),
        }
    }

    fn from_protocol_v0(
        response: kafrust_protocol::api::list_config_resources::ListConfigResourcesResponseV0,
        api_version: i16,
    ) -> Self {
        Self {
            api_version,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            error_code: response.error_code,
            resources: response
                .resources
                .into_iter()
                .map(|resource| ListedConfigResource {
                    name: resource.name,
                    resource_type: ConfigResourceType::ClientMetrics,
                })
                .collect(),
        }
    }
}

/// One configuration resource returned by ListConfigResources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedConfigResource {
    name: String,
    resource_type: ConfigResourceType,
}

impl ListedConfigResource {
    /// Returns the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the typed resource kind.
    pub fn resource_type(&self) -> ConfigResourceType {
        self.resource_type
    }

    /// Returns Kafka's raw resource type code.
    pub fn resource_type_code(&self) -> i8 {
        self.resource_type.code()
    }

    fn from_protocol(resource: ListedConfigResourceV1) -> Self {
        Self {
            name: resource.resource_name,
            resource_type: ConfigResourceType::from_code(resource.resource_type),
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

    fn as_protocol_v4(&self) -> DescribeConfigsResourceV4 {
        DescribeConfigsResourceV4 {
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
    include_documentation: bool,
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

    /// Selects whether Kafka should return configuration type and documentation.
    /// This requires DescribeConfigs v4, available on Kafka 4.0 and newer.
    pub fn include_documentation(mut self, include_documentation: bool) -> Self {
        self.include_documentation = include_documentation;
        self
    }

    /// Returns whether configuration synonyms were requested.
    pub fn includes_synonyms(&self) -> bool {
        self.include_synonyms
    }

    /// Returns whether configuration documentation was requested.
    pub fn includes_documentation(&self) -> bool {
        self.include_documentation
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

    fn from_v4_protocol(result: DescribeConfigsResultV4) -> Self {
        Self {
            resource_type: result.resource_type,
            name: result.resource_name,
            error_code: result.error_code,
            error_message: result.error_message,
            entries: result
                .configs
                .into_iter()
                .map(ConfigEntry::from_v4_protocol)
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
    config_type: Option<i8>,
    documentation: Option<String>,
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

    /// Returns Kafka's raw configuration type code when DescribeConfigs v4
    /// supplied it.
    pub fn config_type(&self) -> Option<i8> {
        self.config_type
    }

    /// Returns Kafka's configuration documentation when requested.
    pub fn documentation(&self) -> Option<&str> {
        self.documentation.as_deref()
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
            config_type: None,
            documentation: None,
        }
    }

    fn from_v4_protocol(entry: DescribeConfigsEntryV4) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            read_only: entry.read_only,
            source: ConfigSource::from_code(entry.config_source),
            is_sensitive: entry.is_sensitive,
            synonyms: entry
                .synonyms
                .into_iter()
                .map(ConfigSynonym::from_v4_protocol)
                .collect(),
            config_type: Some(entry.config_type),
            documentation: entry.documentation,
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

    fn from_v4_protocol(synonym: DescribeConfigsSynonymV4) -> Self {
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

/// Description returned by Kafka's modern ConsumerGroupDescribe API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModernConsumerGroupDescription {
    group_id: String,
    error_code: i16,
    error_message: Option<String>,
    state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
    members: Vec<ModernConsumerGroupMember>,
    authorized_operations: i32,
    throttle_time: Duration,
}

impl ModernConsumerGroupDescription {
    /// Returns the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns Kafka's current group state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns the group's current epoch.
    pub fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns the group's target assignment epoch.
    pub fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Returns the selected server-side assignor.
    pub fn assignor_name(&self) -> &str {
        &self.assignor_name
    }

    /// Returns current members in broker response order.
    pub fn members(&self) -> &[ModernConsumerGroupMember] {
        &self.members
    }

    /// Returns Kafka's authorized-operations bitfield, or its sentinel value
    /// when the request did not ask for authorization details.
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Returns whether Kafka described this group successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw group error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's optional group error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the coordinator's throttle time for this request.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    fn from_protocol(group: DescribedConsumerGroup, throttle_time: Duration) -> Self {
        Self {
            group_id: group.group_id,
            error_code: group.error_code,
            error_message: group.error_message,
            state: group.group_state,
            group_epoch: group.group_epoch,
            assignment_epoch: group.assignment_epoch,
            assignor_name: group.assignor_name,
            members: group
                .members
                .into_iter()
                .map(ModernConsumerGroupMember::from_protocol)
                .collect(),
            authorized_operations: group.authorized_operations,
            throttle_time,
        }
    }
}

/// One member returned by Kafka's modern ConsumerGroupDescribe API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModernConsumerGroupMember {
    member_id: String,
    instance_id: Option<String>,
    rack_id: Option<String>,
    member_epoch: i32,
    client_id: String,
    client_host: String,
    subscribed_topic_names: Vec<String>,
    subscribed_topic_regex: Option<String>,
    assignment: ModernConsumerGroupAssignment,
    target_assignment: ModernConsumerGroupAssignment,
    member_type: i8,
}

impl ModernConsumerGroupMember {
    /// Returns the member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the static group instance ID, when configured.
    pub fn instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }

    /// Returns the rack ID, when configured.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns the member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns the topics explicitly subscribed by the member.
    pub fn subscribed_topic_names(&self) -> &[String] {
        &self.subscribed_topic_names
    }

    /// Returns the subscription regex, when configured.
    pub fn subscribed_topic_regex(&self) -> Option<&str> {
        self.subscribed_topic_regex.as_deref()
    }

    /// Returns the member's current assignment.
    pub fn assignment(&self) -> &ModernConsumerGroupAssignment {
        &self.assignment
    }

    /// Returns the member's target assignment.
    pub fn target_assignment(&self) -> &ModernConsumerGroupAssignment {
        &self.target_assignment
    }

    /// Returns -1 for unknown, 0 for classic, and 1 for consumer-protocol
    /// members.
    pub fn member_type(&self) -> i8 {
        self.member_type
    }

    fn from_protocol(member: DescribedConsumerGroupMember) -> Self {
        Self {
            member_id: member.member_id,
            instance_id: member.instance_id,
            rack_id: member.rack_id,
            member_epoch: member.member_epoch,
            client_id: member.client_id,
            client_host: member.client_host,
            subscribed_topic_names: member.subscribed_topic_names,
            subscribed_topic_regex: member.subscribed_topic_regex,
            assignment: ModernConsumerGroupAssignment::from_protocol(member.assignment),
            target_assignment: ModernConsumerGroupAssignment::from_protocol(
                member.target_assignment,
            ),
            member_type: member.member_type,
        }
    }
}

/// Topic-partition assignment returned by ConsumerGroupDescribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModernConsumerGroupAssignment {
    topic_partitions: Vec<ModernConsumerGroupTopicPartitions>,
}

impl ModernConsumerGroupAssignment {
    /// Returns assigned topic-partition entries.
    pub fn topic_partitions(&self) -> &[ModernConsumerGroupTopicPartitions] {
        &self.topic_partitions
    }

    fn from_protocol(assignment: ConsumerGroupDescribeAssignment) -> Self {
        Self {
            topic_partitions: assignment
                .topic_partitions
                .into_iter()
                .map(ModernConsumerGroupTopicPartitions::from_protocol)
                .collect(),
        }
    }
}

/// One topic's partitions in a modern consumer-group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModernConsumerGroupTopicPartitions {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl ModernConsumerGroupTopicPartitions {
    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns the topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns assigned partition indexes.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    fn from_protocol(topic: ConsumerGroupDescribeTopicPartitions) -> Self {
        Self {
            topic_id: topic.topic_id,
            topic_name: topic.topic_name,
            partitions: topic.partitions,
        }
    }
}

/// Description returned by Kafka's stable ShareGroupDescribe API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupDescription {
    group_id: String,
    error_code: i16,
    error_message: Option<String>,
    state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    assignor_name: String,
    members: Vec<ShareGroupMember>,
    authorized_operations: i32,
    throttle_time: Duration,
}

impl ShareGroupDescription {
    /// Returns the share group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns Kafka's current share group state string.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns the group's current epoch.
    pub fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns the group's assignment epoch.
    pub fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Returns the selected server-side assignor.
    pub fn assignor_name(&self) -> &str {
        &self.assignor_name
    }

    /// Returns current members in broker response order.
    pub fn members(&self) -> &[ShareGroupMember] {
        &self.members
    }

    /// Returns Kafka's authorized-operations bitfield.
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Returns whether Kafka described this share group successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw group error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's optional group error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the coordinator's throttle time for this request.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    fn from_protocol(group: DescribedShareGroup, throttle_time: Duration) -> Self {
        Self {
            group_id: group.group_id,
            error_code: group.error_code,
            error_message: group.error_message,
            state: group.group_state,
            group_epoch: group.group_epoch,
            assignment_epoch: group.assignment_epoch,
            assignor_name: group.assignor_name,
            members: group
                .members
                .into_iter()
                .map(ShareGroupMember::from_protocol)
                .collect(),
            authorized_operations: group.authorized_operations,
            throttle_time,
        }
    }
}

/// One member returned by Kafka's ShareGroupDescribe API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMember {
    member_id: String,
    rack_id: Option<String>,
    member_epoch: i32,
    client_id: String,
    client_host: String,
    subscribed_topic_names: Vec<String>,
    assignment: ShareGroupAssignment,
}

impl ShareGroupMember {
    /// Returns the member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the rack ID, when configured.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns the member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the member's client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the member's client host.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns the topics explicitly subscribed by the member.
    pub fn subscribed_topic_names(&self) -> &[String] {
        &self.subscribed_topic_names
    }

    /// Returns the member's current assignment.
    pub fn assignment(&self) -> &ShareGroupAssignment {
        &self.assignment
    }

    fn from_protocol(member: DescribedShareGroupMember) -> Self {
        Self {
            member_id: member.member_id,
            rack_id: member.rack_id,
            member_epoch: member.member_epoch,
            client_id: member.client_id,
            client_host: member.client_host,
            subscribed_topic_names: member.subscribed_topic_names,
            assignment: ShareGroupAssignment::from_protocol(member.assignment),
        }
    }
}

/// Topic-partition assignment returned by ShareGroupDescribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupAssignment {
    topic_partitions: Vec<ShareGroupTopicPartitions>,
}

impl ShareGroupAssignment {
    /// Returns assigned topic-partition entries.
    pub fn topic_partitions(&self) -> &[ShareGroupTopicPartitions] {
        &self.topic_partitions
    }

    fn from_protocol(assignment: ShareGroupDescribeAssignment) -> Self {
        Self {
            topic_partitions: assignment
                .topic_partitions
                .into_iter()
                .map(ShareGroupTopicPartitions::from_protocol)
                .collect(),
        }
    }
}

/// One topic's partitions in a share-group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupTopicPartitions {
    topic_id: [u8; 16],
    topic_name: String,
    partitions: Vec<i32>,
}

impl ShareGroupTopicPartitions {
    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns the topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns assigned partition indexes.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    fn from_protocol(topic: ShareGroupDescribeTopicPartitions) -> Self {
        Self {
            topic_id: topic.topic_id,
            topic_name: topic.topic_name,
            partitions: topic.partitions,
        }
    }
}

/// Description returned by Kafka's StreamsGroupDescribe API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupDescription {
    group_id: String,
    error_code: i16,
    error_message: Option<String>,
    state: String,
    group_epoch: i32,
    assignment_epoch: i32,
    topology: Option<StreamsGroupTopology>,
    members: Vec<StreamsGroupMember>,
    authorized_operations: i32,
    throttle_time: Duration,
}

impl StreamsGroupDescription {
    /// Returns the Streams group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns Kafka's current Streams group state.
    pub fn state(&self) -> &str {
        &self.state
    }

    /// Returns the current group epoch.
    pub fn group_epoch(&self) -> i32 {
        self.group_epoch
    }

    /// Returns the current assignment epoch.
    pub fn assignment_epoch(&self) -> i32 {
        self.assignment_epoch
    }

    /// Returns the initialized topology, when the broker included one.
    pub fn topology(&self) -> Option<&StreamsGroupTopology> {
        self.topology.as_ref()
    }

    /// Returns Streams group members in broker response order.
    pub fn members(&self) -> &[StreamsGroupMember] {
        &self.members
    }

    /// Returns Kafka's authorized-operations bitfield.
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Returns whether Kafka described this group successfully.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns Kafka's raw group error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns Kafka's optional group error message.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the coordinator's throttle duration for this request.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    fn from_protocol(group: ProtocolDescribedStreamsGroup, throttle_time: Duration) -> Self {
        Self {
            group_id: group.group_id,
            error_code: group.error_code,
            error_message: group.error_message,
            state: group.group_state,
            group_epoch: group.group_epoch,
            assignment_epoch: group.assignment_epoch,
            topology: group.topology.map(StreamsGroupTopology::from_protocol),
            members: group
                .members
                .into_iter()
                .map(StreamsGroupMember::from_protocol)
                .collect(),
            authorized_operations: group.authorized_operations,
            throttle_time,
        }
    }
}

/// The topology currently initialized for a Streams group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTopology {
    epoch: i32,
    subtopologies: Option<Vec<StreamsGroupSubtopology>>,
}

impl StreamsGroupTopology {
    /// Returns the topology epoch.
    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Returns the configured subtopologies, preserving Kafka's nullable value.
    pub fn subtopologies(&self) -> Option<&[StreamsGroupSubtopology]> {
        self.subtopologies.as_deref()
    }

    fn from_protocol(topology: ProtocolStreamsGroupTopology) -> Self {
        Self {
            epoch: topology.epoch,
            subtopologies: topology.subtopologies.map(|subtopologies| {
                subtopologies
                    .into_iter()
                    .map(StreamsGroupSubtopology::from_protocol)
                    .collect()
            }),
        }
    }
}

/// One subtopology in a Streams group topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupSubtopology {
    subtopology_id: String,
    source_topics: Vec<String>,
    repartition_sink_topics: Vec<String>,
    state_changelog_topics: Vec<StreamsGroupTopic>,
    repartition_source_topics: Vec<StreamsGroupTopic>,
}

impl StreamsGroupSubtopology {
    /// Returns the stable subtopology identifier.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns source topics read by this subtopology.
    pub fn source_topics(&self) -> &[String] {
        &self.source_topics
    }

    /// Returns repartition sink topic names.
    pub fn repartition_sink_topics(&self) -> &[String] {
        &self.repartition_sink_topics
    }

    /// Returns automatically managed state changelog topics.
    pub fn state_changelog_topics(&self) -> &[StreamsGroupTopic] {
        &self.state_changelog_topics
    }

    /// Returns automatically managed repartition source topics.
    pub fn repartition_source_topics(&self) -> &[StreamsGroupTopic] {
        &self.repartition_source_topics
    }

    fn from_protocol(subtopology: ProtocolStreamsGroupSubtopology) -> Self {
        Self {
            subtopology_id: subtopology.subtopology_id,
            source_topics: subtopology.source_topics,
            repartition_sink_topics: subtopology.repartition_sink_topics,
            state_changelog_topics: subtopology
                .state_changelog_topics
                .into_iter()
                .map(StreamsGroupTopic::from_protocol)
                .collect(),
            repartition_source_topics: subtopology
                .repartition_source_topics
                .into_iter()
                .map(StreamsGroupTopic::from_protocol)
                .collect(),
        }
    }
}

/// A topic managed by a Streams group topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTopic {
    name: String,
    partitions: i32,
    replication_factor: i16,
    topic_configs: Vec<StreamsGroupTopicConfig>,
}

impl StreamsGroupTopic {
    /// Returns the topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the configured partition count.
    pub fn partitions(&self) -> i32 {
        self.partitions
    }

    /// Returns the configured replication factor.
    pub fn replication_factor(&self) -> i16 {
        self.replication_factor
    }

    /// Returns topic-level configurations.
    pub fn topic_configs(&self) -> &[StreamsGroupTopicConfig] {
        &self.topic_configs
    }

    fn from_protocol(topic: ProtocolStreamsGroupTopic) -> Self {
        Self {
            name: topic.name,
            partitions: topic.partitions,
            replication_factor: topic.replication_factor,
            topic_configs: topic
                .topic_configs
                .into_iter()
                .map(StreamsGroupTopicConfig::from_protocol)
                .collect(),
        }
    }
}

/// One configuration entry for a Streams group-managed topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTopicConfig {
    key: String,
    value: String,
}

impl StreamsGroupTopicConfig {
    /// Returns the configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the configuration value.
    pub fn value(&self) -> &str {
        &self.value
    }

    fn from_protocol(config: ProtocolStreamsGroupTopicConfig) -> Self {
        Self {
            key: config.key,
            value: config.value,
        }
    }
}

/// One member returned by StreamsGroupDescribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupMember {
    member_id: String,
    member_epoch: i32,
    instance_id: Option<String>,
    rack_id: Option<String>,
    client_id: String,
    client_host: String,
    topology_epoch: i32,
    process_id: String,
    user_endpoint: Option<StreamsGroupEndpoint>,
    client_tags: Vec<StreamsGroupKeyValue>,
    task_offsets: Vec<StreamsGroupTaskOffset>,
    task_end_offsets: Vec<StreamsGroupTaskOffset>,
    assignment: StreamsGroupAssignment,
    target_assignment: StreamsGroupAssignment,
    is_classic: bool,
}

impl StreamsGroupMember {
    /// Returns the member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the static instance ID, when configured.
    pub fn instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }

    /// Returns the rack ID, when configured.
    pub fn rack_id(&self) -> Option<&str> {
        self.rack_id.as_deref()
    }

    /// Returns the Kafka client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the client host reported by Kafka.
    pub fn client_host(&self) -> &str {
        &self.client_host
    }

    /// Returns the member's topology epoch.
    pub fn topology_epoch(&self) -> i32 {
        self.topology_epoch
    }

    /// Returns the Streams process ID.
    pub fn process_id(&self) -> &str {
        &self.process_id
    }

    /// Returns the Interactive Queries endpoint, when configured.
    pub fn user_endpoint(&self) -> Option<&StreamsGroupEndpoint> {
        self.user_endpoint.as_ref()
    }

    /// Returns rack-aware client tags.
    pub fn client_tags(&self) -> &[StreamsGroupKeyValue] {
        &self.client_tags
    }

    /// Returns cumulative changelog offsets reported by the member.
    pub fn task_offsets(&self) -> &[StreamsGroupTaskOffset] {
        &self.task_offsets
    }

    /// Returns cumulative changelog end offsets reported by the member.
    pub fn task_end_offsets(&self) -> &[StreamsGroupTaskOffset] {
        &self.task_end_offsets
    }

    /// Returns the member's current task assignment.
    pub fn assignment(&self) -> &StreamsGroupAssignment {
        &self.assignment
    }

    /// Returns the member's target task assignment.
    pub fn target_assignment(&self) -> &StreamsGroupAssignment {
        &self.target_assignment
    }

    /// Returns whether this is a classic member pending upgrade.
    pub fn is_classic(&self) -> bool {
        self.is_classic
    }

    fn from_protocol(member: ProtocolDescribedStreamsGroupMember) -> Self {
        Self {
            member_id: member.member_id,
            member_epoch: member.member_epoch,
            instance_id: member.instance_id,
            rack_id: member.rack_id,
            client_id: member.client_id,
            client_host: member.client_host,
            topology_epoch: member.topology_epoch,
            process_id: member.process_id,
            user_endpoint: member
                .user_endpoint
                .map(StreamsGroupEndpoint::from_protocol),
            client_tags: member
                .client_tags
                .into_iter()
                .map(StreamsGroupKeyValue::from_protocol)
                .collect(),
            task_offsets: member
                .task_offsets
                .into_iter()
                .map(StreamsGroupTaskOffset::from_protocol)
                .collect(),
            task_end_offsets: member
                .task_end_offsets
                .into_iter()
                .map(StreamsGroupTaskOffset::from_protocol)
                .collect(),
            assignment: StreamsGroupAssignment::from_protocol(member.assignment),
            target_assignment: StreamsGroupAssignment::from_protocol(member.target_assignment),
            is_classic: member.is_classic,
        }
    }
}

/// A host and port exposed by a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupEndpoint {
    host: String,
    port: u16,
}

impl StreamsGroupEndpoint {
    /// Returns the endpoint host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the endpoint port.
    pub fn port(&self) -> u16 {
        self.port
    }

    fn from_protocol(endpoint: ProtocolStreamsGroupEndpoint) -> Self {
        Self {
            host: endpoint.host,
            port: endpoint.port,
        }
    }
}

/// A key/value entry attached to a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupKeyValue {
    key: String,
    value: String,
}

impl StreamsGroupKeyValue {
    /// Returns the key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the value.
    pub fn value(&self) -> &str {
        &self.value
    }

    fn from_protocol(entry: ProtocolStreamsGroupKeyValue) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
        }
    }
}

/// A cumulative changelog offset reported by a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTaskOffset {
    subtopology_id: String,
    partition: i32,
    offset: i64,
}

impl StreamsGroupTaskOffset {
    /// Returns the subtopology identifier.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the cumulative offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    fn from_protocol(offset: ProtocolStreamsGroupTaskOffset) -> Self {
        Self {
            subtopology_id: offset.subtopology_id,
            partition: offset.partition,
            offset: offset.offset,
        }
    }
}

/// Current or target task assignment for a Streams group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupAssignment {
    active_tasks: Vec<StreamsGroupTask>,
    standby_tasks: Vec<StreamsGroupTask>,
    warmup_tasks: Vec<StreamsGroupTask>,
}

impl StreamsGroupAssignment {
    /// Returns active tasks.
    pub fn active_tasks(&self) -> &[StreamsGroupTask] {
        &self.active_tasks
    }

    /// Returns standby tasks.
    pub fn standby_tasks(&self) -> &[StreamsGroupTask] {
        &self.standby_tasks
    }

    /// Returns warm-up tasks.
    pub fn warmup_tasks(&self) -> &[StreamsGroupTask] {
        &self.warmup_tasks
    }

    fn from_protocol(assignment: ProtocolStreamsGroupAssignment) -> Self {
        Self {
            active_tasks: assignment
                .active_tasks
                .into_iter()
                .map(StreamsGroupTask::from_protocol)
                .collect(),
            standby_tasks: assignment
                .standby_tasks
                .into_iter()
                .map(StreamsGroupTask::from_protocol)
                .collect(),
            warmup_tasks: assignment
                .warmup_tasks
                .into_iter()
                .map(StreamsGroupTask::from_protocol)
                .collect(),
        }
    }
}

/// One Streams task assignment identified by subtopology and partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupTask {
    subtopology_id: String,
    partitions: Vec<i32>,
}

impl StreamsGroupTask {
    /// Returns the subtopology identifier.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns assigned partition indexes.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    fn from_protocol(task: ProtocolStreamsGroupTask) -> Self {
        Self {
            subtopology_id: task.subtopology_id,
            partitions: task.partitions,
        }
    }
}

/// One partition supplied to InitializeShareGroupState.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupStateInitializePartition {
    partition: i32,
    state_epoch: i32,
    start_offset: i64,
}

impl ShareGroupStateInitializePartition {
    /// Creates an initialization entry for one share partition.
    pub fn new(partition: i32, state_epoch: i32, start_offset: i64) -> Self {
        Self {
            partition,
            state_epoch,
            start_offset,
        }
    }

    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the state epoch.
    pub fn state_epoch(&self) -> i32 {
        self.state_epoch
    }

    /// Returns the share-partition start offset.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }
}

/// One topic supplied to InitializeShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateInitializeTopic {
    topic_id: [u8; 16],
    partitions: Vec<ShareGroupStateInitializePartition>,
}

impl ShareGroupStateInitializeTopic {
    /// Creates an initialization request entry for one topic UUID.
    pub fn new(
        topic_id: [u8; 16],
        partitions: impl IntoIterator<Item = ShareGroupStateInitializePartition>,
    ) -> Self {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns initialization entries in request order.
    pub fn partitions(&self) -> &[ShareGroupStateInitializePartition] {
        &self.partitions
    }
}

/// One partition filter supplied to ReadShareGroupState or its summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupStateReadPartition {
    partition: i32,
    leader_epoch: i32,
}

impl ShareGroupStateReadPartition {
    /// Creates a read entry for one share partition.
    pub fn new(partition: i32, leader_epoch: i32) -> Self {
        Self {
            partition,
            leader_epoch,
        }
    }

    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the leader epoch supplied to Kafka.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }
}

/// One topic filter supplied to ReadShareGroupState or its summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateReadTopic {
    topic_id: [u8; 16],
    partitions: Vec<ShareGroupStateReadPartition>,
}

impl ShareGroupStateReadTopic {
    /// Creates a read request entry for one topic UUID.
    pub fn new(
        topic_id: [u8; 16],
        partitions: impl IntoIterator<Item = ShareGroupStateReadPartition>,
    ) -> Self {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns partition filters in request order.
    pub fn partitions(&self) -> &[ShareGroupStateReadPartition] {
        &self.partitions
    }
}

/// One delivery-state batch supplied to or returned by share-group state APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupStateBatch {
    first_offset: i64,
    last_offset: i64,
    delivery_state: i8,
    delivery_count: i16,
}

impl ShareGroupStateBatch {
    /// Creates one state batch using Kafka's delivery-state numeric value.
    pub fn new(
        first_offset: i64,
        last_offset: i64,
        delivery_state: i8,
        delivery_count: i16,
    ) -> Self {
        Self {
            first_offset,
            last_offset,
            delivery_state,
            delivery_count,
        }
    }

    /// Returns the first offset in the batch.
    pub fn first_offset(&self) -> i64 {
        self.first_offset
    }

    /// Returns the last offset in the batch.
    pub fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Returns Kafka's delivery-state numeric value.
    pub fn delivery_state(&self) -> i8 {
        self.delivery_state
    }

    /// Returns the delivery count.
    pub fn delivery_count(&self) -> i16 {
        self.delivery_count
    }
}

impl From<&ShareGroupStateBatch> for ProtocolShareGroupStateBatch {
    fn from(batch: &ShareGroupStateBatch) -> Self {
        Self {
            first_offset: batch.first_offset,
            last_offset: batch.last_offset,
            delivery_state: batch.delivery_state,
            delivery_count: batch.delivery_count,
        }
    }
}

/// One partition supplied to WriteShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateWritePartition {
    partition: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: Option<i32>,
    state_batches: Vec<ShareGroupStateBatch>,
}

impl ShareGroupStateWritePartition {
    /// Creates a write entry for one share partition.
    pub fn new(
        partition: i32,
        state_epoch: i32,
        leader_epoch: i32,
        start_offset: i64,
        state_batches: impl IntoIterator<Item = ShareGroupStateBatch>,
    ) -> Self {
        Self {
            partition,
            state_epoch,
            leader_epoch,
            start_offset,
            delivery_complete_count: None,
            state_batches: state_batches.into_iter().collect(),
        }
    }

    /// Adds the Kafka 4.3 delivery-completion count carried by Write v1.
    pub fn with_delivery_complete_count(mut self, count: i32) -> Self {
        self.delivery_complete_count = Some(count);
        self
    }

    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the share-partition state epoch.
    pub fn state_epoch(&self) -> i32 {
        self.state_epoch
    }

    /// Returns the leader epoch.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns the share-partition start offset.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Returns the optional v1 delivery-completion count.
    pub fn delivery_complete_count(&self) -> Option<i32> {
        self.delivery_complete_count
    }

    /// Returns state batches in request order.
    pub fn state_batches(&self) -> &[ShareGroupStateBatch] {
        &self.state_batches
    }
}

/// One topic supplied to WriteShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateWriteTopic {
    topic_id: [u8; 16],
    partitions: Vec<ShareGroupStateWritePartition>,
}

impl ShareGroupStateWriteTopic {
    /// Creates a write request entry for one topic UUID.
    pub fn new(
        topic_id: [u8; 16],
        partitions: impl IntoIterator<Item = ShareGroupStateWritePartition>,
    ) -> Self {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns partition writes in request order.
    pub fn partitions(&self) -> &[ShareGroupStateWritePartition] {
        &self.partitions
    }
}

/// One topic supplied to DeleteShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateDeleteTopic {
    topic_id: [u8; 16],
    partitions: Vec<i32>,
}

impl ShareGroupStateDeleteTopic {
    /// Creates a deletion request entry for one topic UUID.
    pub fn new(topic_id: [u8; 16], partitions: impl IntoIterator<Item = i32>) -> Self {
        Self {
            topic_id,
            partitions: partitions.into_iter().collect(),
        }
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns partition indexes in request order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// Result of Initialize, Write, or Delete share-group state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateResult {
    topics: Vec<ShareGroupStateTopicResult>,
}

impl ShareGroupStateResult {
    /// Returns topic results in broker response order.
    pub fn topics(&self) -> &[ShareGroupStateTopicResult] {
        &self.topics
    }

    /// Returns whether every returned partition succeeded.
    pub fn is_success(&self) -> bool {
        self.topics
            .iter()
            .all(ShareGroupStateTopicResult::is_success)
    }

    fn from_protocol(response: ShareGroupStateResultResponse) -> Self {
        Self {
            topics: response
                .results
                .into_iter()
                .map(ShareGroupStateTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// One topic result returned by Initialize, Write, or Delete share state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStateTopicResult {
    topic_id: [u8; 16],
    partitions: Vec<ShareGroupStatePartitionResult>,
}

impl ShareGroupStateTopicResult {
    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns partition results in broker response order.
    pub fn partitions(&self) -> &[ShareGroupStatePartitionResult] {
        &self.partitions
    }

    /// Returns whether every partition in this topic succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(ShareGroupStatePartitionResult::is_success)
    }

    fn from_protocol(topic: ProtocolShareGroupStateTopicResult) -> Self {
        Self {
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .into_iter()
                .map(ShareGroupStatePartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// One partition result returned by a share-group state mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupStatePartitionResult {
    partition: i32,
    error_code: i16,
    error_message: Option<String>,
}

impl ShareGroupStatePartitionResult {
    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka accepted this partition operation.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(partition: ProtocolShareGroupStatePartitionResult) -> Self {
        Self {
            partition: partition.partition,
            error_code: partition.error_code,
            error_message: partition.error_message,
        }
    }
}

/// Complete ReadShareGroupState result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateResult {
    topics: Vec<ReadShareGroupStateTopicResult>,
}

impl ReadShareGroupStateResult {
    /// Returns topic results in broker response order.
    pub fn topics(&self) -> &[ReadShareGroupStateTopicResult] {
        &self.topics
    }

    /// Returns whether every returned partition succeeded.
    pub fn is_success(&self) -> bool {
        self.topics
            .iter()
            .all(ReadShareGroupStateTopicResult::is_success)
    }

    fn from_protocol(response: ReadShareGroupStateResponseV0) -> Self {
        Self {
            topics: response
                .results
                .into_iter()
                .map(ReadShareGroupStateTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// One topic returned by ReadShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateTopicResult {
    topic_id: [u8; 16],
    partitions: Vec<ReadShareGroupStatePartitionResult>,
}

impl ReadShareGroupStateTopicResult {
    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns partition state in broker response order.
    pub fn partitions(&self) -> &[ReadShareGroupStatePartitionResult] {
        &self.partitions
    }

    /// Returns whether every partition in this topic succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(ReadShareGroupStatePartitionResult::is_success)
    }

    fn from_protocol(
        topic: kafrust_protocol::api::share_group_state::ReadShareGroupStateTopicResult,
    ) -> Self {
        Self {
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .into_iter()
                .map(ReadShareGroupStatePartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// One partition returned by ReadShareGroupState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStatePartitionResult {
    partition: i32,
    error_code: i16,
    error_message: Option<String>,
    state_epoch: i32,
    start_offset: i64,
    state_batches: Vec<ShareGroupStateBatch>,
}

impl ReadShareGroupStatePartitionResult {
    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the share-partition state epoch.
    pub fn state_epoch(&self) -> i32 {
        self.state_epoch
    }

    /// Returns the share-partition start offset.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Returns delivery-state batches in broker response order.
    pub fn state_batches(&self) -> &[ShareGroupStateBatch] {
        &self.state_batches
    }

    /// Returns whether Kafka returned this partition without an error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(
        partition: kafrust_protocol::api::share_group_state::ReadShareGroupStatePartitionResult,
    ) -> Self {
        Self {
            partition: partition.partition,
            error_code: partition.error_code,
            error_message: partition.error_message,
            state_epoch: partition.state_epoch,
            start_offset: partition.start_offset,
            state_batches: partition
                .state_batches
                .into_iter()
                .map(ShareGroupStateBatch::from_protocol)
                .collect(),
        }
    }
}

impl ShareGroupStateBatch {
    fn from_protocol(batch: ProtocolShareGroupStateBatch) -> Self {
        Self {
            first_offset: batch.first_offset,
            last_offset: batch.last_offset,
            delivery_state: batch.delivery_state,
            delivery_count: batch.delivery_count,
        }
    }
}

/// Complete ReadShareGroupStateSummary result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryResult {
    topics: Vec<ReadShareGroupStateSummaryTopicResult>,
}

impl ReadShareGroupStateSummaryResult {
    /// Returns topic results in broker response order.
    pub fn topics(&self) -> &[ReadShareGroupStateSummaryTopicResult] {
        &self.topics
    }

    /// Returns whether every returned partition succeeded.
    pub fn is_success(&self) -> bool {
        self.topics
            .iter()
            .all(ReadShareGroupStateSummaryTopicResult::is_success)
    }
}

/// One topic returned by ReadShareGroupStateSummary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryTopicResult {
    topic_id: [u8; 16],
    partitions: Vec<ReadShareGroupStateSummaryPartitionResult>,
}

impl ReadShareGroupStateSummaryTopicResult {
    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns partition summaries in broker response order.
    pub fn partitions(&self) -> &[ReadShareGroupStateSummaryPartitionResult] {
        &self.partitions
    }

    /// Returns whether every partition in this topic succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(ReadShareGroupStateSummaryPartitionResult::is_success)
    }

    fn from_protocol(topic: ProtocolShareGroupStateSummaryTopicResult) -> Self {
        Self {
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .into_iter()
                .map(ReadShareGroupStateSummaryPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// One partition returned by ReadShareGroupStateSummary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadShareGroupStateSummaryPartitionResult {
    partition: i32,
    error_code: i16,
    error_message: Option<String>,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: Option<i32>,
}

impl ReadShareGroupStateSummaryPartitionResult {
    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the share-partition state epoch.
    pub fn state_epoch(&self) -> i32 {
        self.state_epoch
    }

    /// Returns the leader epoch.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns the share-partition start offset.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Returns the v1 delivery-completion count, when supplied by Kafka.
    pub fn delivery_complete_count(&self) -> Option<i32> {
        self.delivery_complete_count
    }

    /// Returns whether Kafka returned this partition without an error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(
        partition: kafrust_protocol::api::share_group_state::
            ReadShareGroupStateSummaryPartitionResult,
    ) -> Self {
        Self {
            partition: partition.partition,
            error_code: partition.error_code,
            error_message: partition.error_message,
            state_epoch: partition.state_epoch,
            leader_epoch: partition.leader_epoch,
            start_offset: partition.start_offset,
            delivery_complete_count: partition.delivery_complete_count,
        }
    }
}

enum ReadShareGroupStateSummaryResponse {
    V0(ReadShareGroupStateSummaryResponseV0),
    V1(ReadShareGroupStateSummaryResponseV1),
}

impl ReadShareGroupStateSummaryResponse {
    fn into_topics(self) -> Vec<ReadShareGroupStateSummaryTopicResult> {
        match self {
            Self::V0(response) => response
                .results
                .into_iter()
                .map(ReadShareGroupStateSummaryTopicResult::from_protocol)
                .collect(),
            Self::V1(response) => response
                .results
                .into_iter()
                .map(ReadShareGroupStateSummaryTopicResult::from_protocol)
                .collect(),
        }
    }

    fn has_retryable_error(&self) -> bool {
        match self {
            Self::V0(response) => response.results.iter().any(|topic| {
                topic
                    .partitions
                    .iter()
                    .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
            }),
            Self::V1(response) => response.results.iter().any(|topic| {
                topic
                    .partitions
                    .iter()
                    .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
            }),
        }
    }
}

/// One share-group offset to set administratively.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffset {
    topic: String,
    partition: i32,
    offset: i64,
}

impl ShareGroupOffset {
    /// Creates a share-group offset update.
    pub fn new(topic: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
        }
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the share-partition start offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

/// A topic and partition filter for share-group offset inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetQuery {
    topic: String,
    partitions: Vec<i32>,
}

impl ShareGroupOffsetQuery {
    /// Creates a share-group offset query for one topic.
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

    fn as_protocol(&self) -> DescribeShareGroupOffsetsTopic {
        DescribeShareGroupOffsetsTopic {
            topic_name: self.topic.clone(),
            partitions: self.partitions.clone(),
        }
    }
}

/// Complete result from DescribeShareGroupOffsets for one share group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListShareGroupOffsetsResult {
    group_id: String,
    error_code: i16,
    error_message: Option<String>,
    throttle_time: Duration,
    topics: Vec<ShareGroupOffsetTopicResult>,
}

impl ListShareGroupOffsetsResult {
    /// Returns the share group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns whether Kafka accepted the group query and every partition.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
            && self
                .topics
                .iter()
                .all(ShareGroupOffsetTopicResult::is_success)
    }

    /// Returns the group-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the group-level error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns topic results in broker response order.
    pub fn topics(&self) -> &[ShareGroupOffsetTopicResult] {
        &self.topics
    }

    fn from_protocol_v0(
        group: DescribeShareGroupOffsetsGroupResultV0,
        throttle_time: Duration,
    ) -> Self {
        Self {
            group_id: group.group_id,
            error_code: group.error_code,
            error_message: group.error_message,
            throttle_time,
            topics: group
                .topics
                .into_iter()
                .map(ShareGroupOffsetTopicResult::from_protocol_v0)
                .collect(),
        }
    }

    fn from_protocol_v1(
        group: DescribeShareGroupOffsetsGroupResultV1,
        throttle_time: Duration,
    ) -> Self {
        Self {
            group_id: group.group_id,
            error_code: group.error_code,
            error_message: group.error_message,
            throttle_time,
            topics: group
                .topics
                .into_iter()
                .map(ShareGroupOffsetTopicResult::from_protocol_v1)
                .collect(),
        }
    }
}

/// Result for one topic returned by DescribeShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetTopicResult {
    topic_name: String,
    topic_id: [u8; 16],
    partitions: Vec<ShareGroupOffsetPartitionResult>,
}

impl ShareGroupOffsetTopicResult {
    /// Returns the topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns partition results in broker response order.
    pub fn partitions(&self) -> &[ShareGroupOffsetPartitionResult] {
        &self.partitions
    }

    /// Returns whether every returned partition succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(ShareGroupOffsetPartitionResult::is_success)
    }

    fn from_protocol_v0(topic: DescribeShareGroupOffsetsTopicResultV0) -> Self {
        Self {
            topic_name: topic.topic_name,
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .into_iter()
                .map(ShareGroupOffsetPartitionResult::from_protocol_v0)
                .collect(),
        }
    }

    fn from_protocol_v1(topic: DescribeShareGroupOffsetsTopicResultV1) -> Self {
        Self {
            topic_name: topic.topic_name,
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .into_iter()
                .map(ShareGroupOffsetPartitionResult::from_protocol_v1)
                .collect(),
        }
    }
}

/// Result for one partition returned by DescribeShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupOffsetPartitionResult {
    partition: i32,
    start_offset: i64,
    leader_epoch: i32,
    lag: Option<i64>,
    error_code: i16,
    error_message: Option<String>,
}

impl ShareGroupOffsetPartitionResult {
    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the share-partition start offset.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Returns the leader epoch, or Kafka's sentinel when unavailable.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns the partition lag when the broker supplied it.
    pub fn lag(&self) -> Option<i64> {
        self.lag
    }

    /// Returns the partition-level Kafka error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the partition-level error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka returned this partition without an error.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol_v0(
        partition: kafrust_protocol::api::describe_share_group_offsets::
            DescribeShareGroupOffsetsPartitionV0,
    ) -> Self {
        Self {
            partition: partition.partition_index,
            start_offset: partition.start_offset,
            leader_epoch: partition.leader_epoch,
            lag: None,
            error_code: partition.error_code,
            error_message: partition.error_message,
        }
    }

    fn from_protocol_v1(
        partition: kafrust_protocol::api::describe_share_group_offsets::
            DescribeShareGroupOffsetsPartitionV1,
    ) -> Self {
        Self {
            partition: partition.partition_index,
            start_offset: partition.start_offset,
            leader_epoch: partition.leader_epoch,
            lag: (partition.lag >= 0).then_some(partition.lag),
            error_code: partition.error_code,
            error_message: partition.error_message,
        }
    }
}

/// Complete result from AlterShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsResult {
    error_code: i16,
    error_message: Option<String>,
    throttle_time: Duration,
    topics: Vec<AlterShareGroupOffsetsTopicResult>,
}

impl AlterShareGroupOffsetsResult {
    /// Returns the top-level broker error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the top-level broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-topic offset results.
    pub fn topics(&self) -> &[AlterShareGroupOffsetsTopicResult] {
        &self.topics
    }

    /// Returns whether the top-level and all partition results succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
            && self
                .topics
                .iter()
                .all(AlterShareGroupOffsetsTopicResult::is_success)
    }

    fn from_protocol(response: AlterShareGroupOffsetsResponseV0) -> Self {
        Self {
            error_code: response.error_code,
            error_message: response.error_message,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .responses
                .into_iter()
                .map(AlterShareGroupOffsetsTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// Result for one topic returned by AlterShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsTopicResult {
    topic_name: String,
    topic_id: [u8; 16],
    partitions: Vec<AlterShareGroupOffsetsPartitionResult>,
}

impl AlterShareGroupOffsetsTopicResult {
    /// Returns the topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns per-partition results.
    pub fn partitions(&self) -> &[AlterShareGroupOffsetsPartitionResult] {
        &self.partitions
    }

    /// Returns whether all partitions in this topic succeeded.
    pub fn is_success(&self) -> bool {
        self.partitions
            .iter()
            .all(AlterShareGroupOffsetsPartitionResult::is_success)
    }

    fn from_protocol(topic: AlterShareGroupOffsetsTopicResultV0) -> Self {
        Self {
            topic_name: topic.topic_name,
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .into_iter()
                .map(AlterShareGroupOffsetsPartitionResult::from_protocol)
                .collect(),
        }
    }
}

/// Result for one partition returned by AlterShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterShareGroupOffsetsPartitionResult {
    partition: i32,
    error_code: i16,
    error_message: Option<String>,
}

impl AlterShareGroupOffsetsPartitionResult {
    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the partition error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the partition error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka accepted this partition update.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(
        partition: kafrust_protocol::api::share_group_offsets::AlterShareGroupOffsetsPartitionResultV0,
    ) -> Self {
        Self {
            partition: partition.partition_index,
            error_code: partition.error_code,
            error_message: partition.error_message,
        }
    }
}

/// Complete result from DeleteShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsResult {
    error_code: i16,
    error_message: Option<String>,
    throttle_time: Duration,
    topics: Vec<DeleteShareGroupOffsetsTopicResult>,
}

impl DeleteShareGroupOffsetsResult {
    /// Returns the top-level broker error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the top-level broker error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the broker throttle time.
    pub fn throttle_time(&self) -> Duration {
        self.throttle_time
    }

    /// Returns per-topic deletion results.
    pub fn topics(&self) -> &[DeleteShareGroupOffsetsTopicResult] {
        &self.topics
    }

    /// Returns whether the top-level and all topic results succeeded.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
            && self
                .topics
                .iter()
                .all(DeleteShareGroupOffsetsTopicResult::is_success)
    }

    fn from_protocol(response: DeleteShareGroupOffsetsResponseV0) -> Self {
        Self {
            error_code: response.error_code,
            error_message: response.error_message,
            throttle_time: Duration::from_millis(nonnegative_i32_to_u64(response.throttle_time_ms)),
            topics: response
                .responses
                .into_iter()
                .map(DeleteShareGroupOffsetsTopicResult::from_protocol)
                .collect(),
        }
    }
}

/// Result for one topic returned by DeleteShareGroupOffsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteShareGroupOffsetsTopicResult {
    topic_name: String,
    topic_id: [u8; 16],
    error_code: i16,
    error_message: Option<String>,
}

impl DeleteShareGroupOffsetsTopicResult {
    /// Returns the topic name.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the Kafka topic UUID.
    pub fn topic_id(&self) -> &[u8; 16] {
        &self.topic_id
    }

    /// Returns the topic error code.
    pub fn error_code(&self) -> i16 {
        self.error_code
    }

    /// Returns the topic error message, when supplied.
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns whether Kafka deleted this topic's share offsets.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    fn from_protocol(topic: DeleteShareGroupOffsetsTopicResultV0) -> Self {
        Self {
            topic_name: topic.topic_name,
            topic_id: topic.topic_id,
            error_code: topic.error_code,
            error_message: topic.error_message,
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

fn offset_fetch_topics_v10(
    topics: Option<&[ConsumerGroupOffsetQuery]>,
) -> Option<Vec<OffsetFetchTopicV10>> {
    topics?
        .iter()
        .map(ConsumerGroupOffsetQuery::as_protocol_v10)
        .collect()
}

fn nonzero_topic_id(topic_id: Option<[u8; 16]>) -> Option<[u8; 16]> {
    topic_id.filter(|topic_id| *topic_id != [0; 16])
}

fn topic_names_by_id_from_queries(
    topics: &[ConsumerGroupOffsetQuery],
) -> BTreeMap<[u8; 16], String> {
    topics
        .iter()
        .filter_map(|topic| topic.topic_id.map(|id| (id, topic.topic.clone())))
        .collect()
}

fn is_retryable_offset_fetch_v10(response: &OffsetFetchResponseV10) -> bool {
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

fn offset_commit_topics_v10(offsets: &[ConsumerGroupOffset]) -> Option<Vec<OffsetCommitTopicV10>> {
    if offsets.is_empty() {
        return None;
    }
    let mut topics = BTreeMap::<[u8; 16], Vec<OffsetCommitPartitionV10>>::new();
    for offset in offsets {
        let topic_id = offset.topic_id?;
        if topic_id == [0; 16] {
            return None;
        }
        topics
            .entry(topic_id)
            .or_default()
            .push(OffsetCommitPartitionV10 {
                partition_index: offset.partition,
                committed_offset: offset.offset,
                committed_leader_epoch: offset.leader_epoch,
                committed_metadata: offset.metadata.clone(),
            });
    }
    Some(
        topics
            .into_iter()
            .map(|(topic_id, partitions)| OffsetCommitTopicV10 {
                topic_id,
                partitions,
            })
            .collect(),
    )
}

fn topic_names_by_id_from_offsets(offsets: &[ConsumerGroupOffset]) -> BTreeMap<[u8; 16], String> {
    offsets
        .iter()
        .filter_map(|offset| offset.topic_id.map(|id| (id, offset.topic.clone())))
        .collect()
}

fn is_retryable_offset_commit_v10(response: &OffsetCommitResponseV10) -> bool {
    response.topics.iter().any(|topic| {
        topic
            .partitions
            .iter()
            .any(|partition| is_retryable_admin_coordinator_code(partition.error_code))
    })
}

fn record_offset_fetch_v10_errors(config: &ClientConfig, response: &OffsetFetchResponseV10) {
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

fn record_offset_commit_v10_errors(config: &ClientConfig, response: &OffsetCommitResponseV10) {
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
    topic_id: Option<[u8; 16]>,
}

impl ConsumerGroupOffsetQuery {
    /// Creates an offset query for one topic.
    pub fn new(topic: impl Into<String>, partitions: impl IntoIterator<Item = i32>) -> Self {
        Self {
            topic: topic.into(),
            partitions: partitions.into_iter().collect(),
            topic_id: None,
        }
    }

    /// Associates the stable Kafka topic UUID used by OffsetFetch v10.
    ///
    /// A zero UUID is ignored for explicit version selection. The member-aware
    /// Admin method may resolve the topic name through Metadata v12; if that
    /// capability is unavailable, it uses the name-based OffsetFetch v9 path.
    pub fn topic_id(mut self, topic_id: [u8; 16]) -> Self {
        self.topic_id = Some(topic_id);
        self
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns partition indexes in request order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }

    /// Returns the optional topic UUID used for OffsetFetch v10.
    pub fn topic_id_ref(&self) -> Option<[u8; 16]> {
        self.topic_id
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

    fn as_protocol_v10(&self) -> Option<OffsetFetchTopicV10> {
        let topic_id = self.topic_id?;
        (topic_id != [0; 16]).then_some(OffsetFetchTopicV10 {
            topic_id,
            partition_indexes: self.partitions.clone(),
        })
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
    topic_id: Option<[u8; 16]>,
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
            topic_id: None,
        }
    }

    /// Associates the stable Kafka topic UUID used by OffsetCommit v10.
    ///
    /// A zero UUID is ignored for explicit version selection. The member-aware
    /// Admin method may resolve the topic name through Metadata v12; if that
    /// capability is unavailable, it uses the name-based OffsetCommit v9 path.
    pub fn topic_id(mut self, topic_id: [u8; 16]) -> Self {
        self.topic_id = Some(topic_id);
        self
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

    /// Returns the optional topic UUID used for OffsetCommit v10.
    pub fn topic_id_ref(&self) -> Option<[u8; 16]> {
        self.topic_id
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
        AclFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
        AddRaftVoterOptions, AdminClient, AlterConfigsOptions, ClientQuotaAlteration,
        ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent, ClientQuotaMatchType,
        ConfigAlterOperationKind, ConfigResourceType, ConfigSource, ConsumerGroupOffset,
        ConsumerGroupOffsetDelete, ConsumerGroupOffsetQuery, CreateDelegationTokenOptions,
        CreatePartitionsOptions, CreateTopicsOptions, DelegationTokenPrincipal,
        DeleteRecordsOptions, DeleteRecordsTopic, DeleteTopicsOptions, DescribeClusterEndpointType,
        DescribeClusterOptions, DescribeConfigsOptions, DescribeProducersTopic,
        DescribeQuorumTopic, DescribeTopicPartitionsCursor, DescribeTopicPartitionsOptions,
        ElectLeadersOptions, ElectionType, LeaderElection, ListConfigResourcesOptions,
        ListGroupsOptions, ListTransactionsOptions, LogDirTopic, NewPartitions, NewTopic,
        PartitionReassignment, PartitionReassignmentOptions, PartitionReassignmentQuery,
        RaftVoterListener, RemoveRaftVoterOptions, ReplicaLogDirAssignment,
        ScramCredentialDeletion, ScramCredentialMechanism, ScramCredentialUpsertion,
        ShareGroupStateBatch, ShareGroupStateDeleteTopic, ShareGroupStateInitializePartition,
        ShareGroupStateInitializeTopic, ShareGroupStateReadPartition, ShareGroupStateReadTopic,
        ShareGroupStateWritePartition, ShareGroupStateWriteTopic, TopicConfigAlteration,
        TopicConfigResource, TopicConfigUpdate,
    };
    use crate::{BrokerErrorKind, Client, ClientConfig, ClientMetrics, Error};
    use kafrust_protocol::codec::DecodeLimits;
    use kafrust_protocol::codec::Encoder;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn bind_test_listener() -> (TcpListener, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        (listener, address)
    }

    async fn serve_multi_route_share_state(listener: TcpListener, topic_id: [u8; 16]) {
        for _ in 0..4 {
            let (mut coordinator, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_state()).await;

            let request = read_frame(&mut coordinator).await;
            assert!(request.windows(16).any(|window| window == topic_id));
            assert!(!request.windows(16).any(|window| window
                == if topic_id == [7; 16] {
                    [8; 16]
                } else {
                    [7; 16]
                }));
            let api_key = i16::from_be_bytes([request[0], request[1]]);
            let response = match api_key {
                84 => read_share_group_state_response_for(topic_id, 0),
                85 | 86 => share_group_state_result_response_for(topic_id, 0),
                87 => share_group_state_summary_response_for(topic_id, 0),
                _ => return,
            };
            write_frame(&mut coordinator, &response).await;
        }
    }

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

    #[tokio::test]
    async fn classifies_admin_mutation_when_response_is_lost_after_transmission() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let mut size = [0u8; 4];
            broker_stream.read_exact(&mut size).await.unwrap();
            drop(broker_stream);
        });
        let mut client = Client::from_stream_with_metrics(
            Box::new(client_stream),
            Some("kafrust-admin-ambiguity-test".to_owned()),
            Some(Duration::from_secs(1)),
            crate::client::DEFAULT_MAX_RESPONSE_BYTES,
            DecodeLimits::default(),
            ClientMetrics::new(),
        );

        let transport_error = client.api_versions().await.unwrap_err();
        let error = super::admin_mutation_error(&client, "CreateTopics", transport_error);

        assert!(matches!(
            error,
            Error::AdminMutationOutcomeUnknown {
                operation: "CreateTopics"
            }
        ));
        broker.await.unwrap();
    }

    #[test]
    fn preserves_pretransmission_admin_errors() {
        let error = super::admin_mutation_error(
            &Client::from_stream(
                Box::new(tokio::io::duplex(1).0),
                Some("kafrust-admin-error-test".to_owned()),
                Some(Duration::from_secs(1)),
            ),
            "CreateTopics",
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "not transmitted",
            )),
        );

        assert!(matches!(
            error,
            Error::Io(error) if error.kind() == std::io::ErrorKind::ConnectionRefused
        ));
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
    async fn adds_raft_voter_through_the_dedicated_controller() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_raft_voter(1, 0)).await;

            let add_request = read_frame(&mut connection).await;
            assert_eq!(&add_request[0..4], &[0, 80, 0, 1]);
            assert!(add_request
                .windows(b"cluster".len())
                .any(|bytes| bytes == b"cluster"));
            assert!(add_request
                .windows(b"CONTROLLER".len())
                .any(|bytes| bytes == b"CONTROLLER"));
            write_frame(&mut connection, &raft_voter_response(12, 0)).await;
        });
        let admin = AdminClient::new(
            ClientConfig::new(["bootstrap:9092"])
                .controller_bootstrap_servers([addr.to_string()])
                .request_timeout_ms(1_000),
        );

        let result = admin
            .add_raft_voter(
                AddRaftVoterOptions::new(4, [9; 16])
                    .cluster_id("cluster")
                    .timeout(Duration::from_secs(60))
                    .listener(RaftVoterListener::new("CONTROLLER", "controller", 9093))
                    .ack_when_committed(true),
            )
            .await
            .unwrap();

        assert_eq!(result.api_version(), 1);
        assert_eq!(result.throttle_time(), Duration::from_millis(12));
        assert!(result.is_success());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn removes_raft_voter_through_the_dedicated_controller() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_raft_voter(0, 0)).await;

            let remove_request = read_frame(&mut connection).await;
            assert_eq!(&remove_request[0..4], &[0, 81, 0, 0]);
            assert!(remove_request
                .windows(b"cluster".len())
                .any(|bytes| bytes == b"cluster"));
            write_frame(&mut connection, &raft_voter_response(0, 0)).await;
        });
        let admin = AdminClient::new(
            ClientConfig::new(["bootstrap:9092"])
                .controller_bootstrap_servers([addr.to_string()])
                .request_timeout_ms(1_000),
        );

        let result = admin
            .remove_raft_voter(RemoveRaftVoterOptions::new(2, [3; 16]).cluster_id("cluster"))
            .await
            .unwrap();

        assert_eq!(result.api_version(), 0);
        assert!(result.is_success());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unregisters_broker_through_the_dedicated_controller() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_unregister_broker()).await;

            let unregister_request = read_frame(&mut connection).await;
            assert_eq!(&unregister_request[0..4], &[0, 64, 0, 0]);
            assert_eq!(
                &unregister_request[unregister_request.len() - 5..],
                &[0, 0, 0, 4, 0]
            );
            write_frame(&mut connection, &unregister_broker_response(12, 0)).await;
        });
        let admin = AdminClient::new(
            ClientConfig::new(["bootstrap:9092"])
                .controller_bootstrap_servers([addr.to_string()])
                .request_timeout_ms(1_000),
        );

        let result = admin.unregister_broker(4).await.unwrap();

        assert_eq!(result.api_version(), 0);
        assert_eq!(result.throttle_time(), Duration::from_millis(12));
        assert!(result.is_success());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_cluster_with_dedicated_api_and_preserves_authorization_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_describe_cluster()).await;

            let describe_request = read_frame(&mut connection).await;
            assert_eq!(&describe_request[0..4], &[0, 60, 0, 1]);
            write_frame(&mut connection, &describe_cluster_response()).await;
        });
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .client_id("kafrust-admin-test")
                .request_timeout_ms(1_000),
        );

        let cluster = admin
            .describe_cluster_with_options(
                DescribeClusterOptions::new()
                    .include_cluster_authorized_operations(true)
                    .endpoint_type(DescribeClusterEndpointType::Controllers),
            )
            .await
            .unwrap();

        assert_eq!(cluster.cluster_id(), Some("cluster"));
        assert_eq!(
            cluster.endpoint_type(),
            Some(DescribeClusterEndpointType::Controllers)
        );
        assert_eq!(cluster.cluster_authorized_operations(), Some(7));
        assert_eq!(cluster.controller_id(), 1);
        assert_eq!(cluster.brokers()[0].rack(), Some("rack-a"));
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
    async fn describes_topic_partitions_with_cursor_and_partition_state() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(
                &mut connection,
                &api_versions_with_describe_topic_partitions(),
            )
            .await;

            let describe_request = read_frame(&mut connection).await;
            assert_eq!(&describe_request[0..4], &[0, 75, 0, 0]);
            assert!(describe_request.windows(6).any(|bytes| bytes == b"orders"));
            write_frame(&mut connection, &describe_topic_partitions_response()).await;
        });
        let admin = AdminClient::new(ClientConfig::new([addr.to_string()]));

        let result = admin
            .describe_topic_partitions(
                &["orders".to_owned()],
                DescribeTopicPartitionsOptions::new()
                    .with_response_partition_limit(10)
                    .with_cursor(DescribeTopicPartitionsCursor::new("orders", 0)),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(4));
        assert_eq!(result.topics().len(), 1);
        let topic = &result.topics()[0];
        assert_eq!(topic.name(), Some("orders"));
        assert_eq!(topic.topic_id(), [7; 16]);
        assert!(topic.is_success());
        assert_eq!(topic.partitions().len(), 1);
        assert_eq!(topic.partitions()[0].leader_id(), 1);
        assert_eq!(topic.partitions()[0].replica_nodes(), &[1]);
        assert_eq!(topic.partitions()[0].offline_replicas(), &[]);
        assert_eq!(
            result
                .next_cursor()
                .map(DescribeTopicPartitionsCursor::topic_name),
            Some("orders")
        );
        assert!(result.is_success());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_describe_topic_partitions_when_broker_does_not_advertise_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_describe_log_dirs(5)).await;
        });
        let admin = AdminClient::new(ClientConfig::new([addr.to_string()]));

        let error = admin
            .describe_topic_partitions(
                &["orders".to_owned()],
                DescribeTopicPartitionsOptions::new(),
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, Error::Unsupported(message) if message.contains("DescribeTopicPartitions"))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_quorum_with_v2_replica_state_and_nodes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_describe_quorum(2)).await;

            let describe_request = read_frame(&mut connection).await;
            assert_eq!(&describe_request[0..4], &[0, 55, 0, 2]);
            assert!(describe_request
                .windows("__cluster_metadata".len())
                .any(|bytes| bytes == b"__cluster_metadata"));
            write_frame(&mut connection, &describe_quorum_v2_response()).await;
        });
        let admin = AdminClient::new(
            ClientConfig::new(["broker:9092"]).controller_bootstrap_servers([addr.to_string()]),
        );

        let result = admin
            .describe_quorum(&[DescribeQuorumTopic::new("__cluster_metadata").partition(0)])
            .await
            .unwrap();

        assert_eq!(result.api_version(), 2);
        assert!(result.is_success());
        assert_eq!(result.topics().len(), 1);
        let partition = &result.topics()[0].partitions()[0];
        assert_eq!(partition.leader_id(), 1);
        assert_eq!(partition.high_watermark(), 42);
        assert_eq!(partition.current_voters()[0].replica_id(), 1);
        assert_eq!(
            partition.current_voters()[0].replica_directory_id(),
            Some([8; 16])
        );
        assert_eq!(result.nodes()[0].node_id(), 1);
        assert_eq!(result.nodes()[0].listeners()[0].port(), 9093);
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
    async fn lists_config_resources_with_typed_resource_kinds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_list_config_resources(1)).await;

            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 74, 0, 1]);
            write_frame(&mut connection, &list_config_resources_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let result = admin
            .list_config_resources(
                ListConfigResourcesOptions::new()
                    .resource_type(ConfigResourceType::Topic)
                    .resource_type(ConfigResourceType::Group),
            )
            .await
            .unwrap();

        assert_eq!(result.throttle_time(), Duration::from_millis(7));
        assert_eq!(result.api_version(), 1);
        assert!(result.is_success());
        assert_eq!(result.resources().len(), 2);
        assert_eq!(result.resources()[0].name(), "orders");
        assert_eq!(
            result.resources()[0].resource_type(),
            ConfigResourceType::Topic
        );
        assert_eq!(
            result.resources()[1].resource_type(),
            ConfigResourceType::Group
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn falls_back_to_list_client_metrics_resources_v0_for_old_brokers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_list_config_resources(0)).await;

            let request = read_frame(&mut connection).await;
            assert_eq!(&request[0..4], &[0, 74, 0, 0]);
            write_frame(&mut connection, &list_client_metrics_resources_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let result = admin
            .list_config_resources(
                ListConfigResourcesOptions::new().resource_type(ConfigResourceType::ClientMetrics),
            )
            .await
            .unwrap();

        assert_eq!(result.api_version(), 0);
        assert_eq!(result.resources().len(), 1);
        assert_eq!(result.resources()[0].name(), "latency");
        assert_eq!(
            result.resources()[0].resource_type(),
            ConfigResourceType::ClientMetrics
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_non_client_metrics_filter_when_only_v0_is_available() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_list_config_resources(0)).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let error = admin
            .list_config_resources(
                ListConfigResourcesOptions::new().resource_type(ConfigResourceType::Topic),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("ListConfigResources v0 only lists client metrics resources")
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_topic_configs_with_v4_documentation_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut connection).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut connection, &api_versions_with_describe_configs_v4()).await;

            let describe_request = read_frame(&mut connection).await;
            assert_eq!(&describe_request[0..4], &[0, 32, 0, 4]);
            write_frame(&mut connection, &describe_configs_v4_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let result = admin
            .describe_topic_configs(
                &[TopicConfigResource::new("orders")],
                DescribeConfigsOptions::new()
                    .include_synonyms(true)
                    .include_documentation(true),
            )
            .await
            .unwrap();

        let entry = &result.resources()[0].entries()[0];
        assert_eq!(entry.config_type(), Some(7));
        assert_eq!(entry.documentation(), Some("The cleanup policy."));
        assert_eq!(entry.synonyms()[0].source(), ConfigSource::DefaultConfig);
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
    async fn routes_modern_consumer_group_describe_to_coordinator() {
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(
                &mut coordinator,
                &api_versions_with_consumer_group_describe(),
            )
            .await;

            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 69, 0, 1]);
            write_frame(&mut coordinator, &consumer_group_describe_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let descriptions = admin
            .describe_consumer_groups_modern(&["orders-group".to_owned()], true)
            .await
            .unwrap();

        assert_eq!(descriptions.len(), 1);
        let description = &descriptions[0];
        assert_eq!(description.group_id(), "orders-group");
        assert_eq!(description.state(), "Stable");
        assert_eq!(description.group_epoch(), 4);
        assert_eq!(description.assignment_epoch(), 5);
        assert_eq!(description.assignor_name(), "uniform");
        assert_eq!(description.authorized_operations(), -2147483648);
        assert_eq!(description.members().len(), 1);
        let member = &description.members()[0];
        assert_eq!(member.member_id(), "member-1");
        assert_eq!(member.member_type(), 1);
        assert_eq!(
            member.assignment().topic_partitions()[0].topic_name(),
            "orders"
        );
        assert_eq!(
            member.assignment().topic_partitions()[0].partitions(),
            [0, 2]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_share_group_describe_to_coordinator() {
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_describe()).await;

            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 77, 0, 1]);
            write_frame(&mut coordinator, &share_group_describe_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let descriptions = admin
            .describe_share_groups(&["share-orders".to_owned()], true)
            .await
            .unwrap();

        assert_eq!(descriptions.len(), 1);
        let description = &descriptions[0];
        assert_eq!(description.group_id(), "share-orders");
        assert_eq!(description.state(), "Stable");
        assert_eq!(description.group_epoch(), 4);
        assert_eq!(description.assignment_epoch(), 5);
        assert_eq!(description.assignor_name(), "uniform");
        assert_eq!(description.authorized_operations(), -2147483648);
        assert_eq!(description.members().len(), 1);
        let member = &description.members()[0];
        assert_eq!(member.member_id(), "member-1");
        assert_eq!(member.rack_id(), Some("rack-a"));
        assert_eq!(member.member_epoch(), 7);
        assert_eq!(
            member.assignment().topic_partitions()[0].topic_name(),
            "orders"
        );
        assert_eq!(
            member.assignment().topic_partitions()[0].partitions(),
            [0, 2]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_streams_group_describe_to_coordinator() {
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(
                &mut coordinator,
                &api_versions_with_streams_group_describe(),
            )
            .await;

            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 89, 0, 0]);
            write_frame(&mut coordinator, &streams_group_describe_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let descriptions = admin
            .describe_streams_groups(&["streams-orders".to_owned()], true)
            .await
            .unwrap();

        assert_eq!(descriptions.len(), 1);
        let description = &descriptions[0];
        assert_eq!(description.group_id(), "streams-orders");
        assert_eq!(description.state(), "Stable");
        assert_eq!(description.group_epoch(), 4);
        assert_eq!(description.assignment_epoch(), 5);
        assert_eq!(description.authorized_operations(), -2147483648);
        assert!(description.topology().is_none());
        assert!(description.members().is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_share_group_offset_mutations_to_coordinator() {
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_offsets()).await;
            let alter_request = read_frame(&mut coordinator).await;
            assert_eq!(&alter_request[0..4], &[0, 91, 0, 0]);
            write_frame(&mut coordinator, &alter_share_group_offsets_response()).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 1]);
            write_frame(
                &mut bootstrap,
                &find_group_coordinator_response(addr.port()),
            )
            .await;
            let (mut coordinator, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_offsets()).await;
            let delete_request = read_frame(&mut coordinator).await;
            assert_eq!(&delete_request[0..4], &[0, 92, 0, 0]);
            write_frame(&mut coordinator, &delete_share_group_offsets_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let altered = admin
            .alter_share_group_offsets(
                "share-orders",
                &[super::ShareGroupOffset::new("orders", 0, 42)],
            )
            .await
            .unwrap();
        assert!(altered.is_success());
        assert_eq!(altered.topics()[0].topic_name(), "orders");
        assert_eq!(altered.topics()[0].partitions()[0].partition(), 0);

        let deleted = admin
            .delete_share_group_offsets("share-orders", &["orders".to_owned()])
            .await
            .unwrap();
        assert!(deleted.is_success());
        assert_eq!(deleted.topics()[0].topic_name(), "orders");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_share_group_state_and_preserves_v1_fields() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 6]);
            write_frame(
                &mut bootstrap,
                &find_share_partition_coordinator_response(
                    addr.port(),
                    "share-orders:BwcHBwcHBwcHBwcHBwcHBw:0",
                ),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_state()).await;
            let initialize_request = read_frame(&mut coordinator).await;
            assert_eq!(&initialize_request[0..4], &[0, 83, 0, 0]);
            write_frame(&mut coordinator, &share_group_state_result_response()).await;

            let (mut bootstrap, _) = listener.accept().await.unwrap();
            let coordinator_request = read_frame(&mut bootstrap).await;
            assert_eq!(&coordinator_request[0..4], &[0, 10, 0, 6]);
            write_frame(
                &mut bootstrap,
                &find_share_partition_coordinator_response(
                    addr.port(),
                    "share-orders:BwcHBwcHBwcHBwcHBwcHBw:0",
                ),
            )
            .await;

            let (mut coordinator, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_state()).await;
            let write_request = read_frame(&mut coordinator).await;
            assert_eq!(&write_request[0..4], &[0, 85, 0, 1]);
            write_frame(&mut coordinator, &share_group_state_result_response()).await;
        });
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .client_id("kafrust-share-state-test")
                .request_timeout_ms(1_000),
        );

        let initialized = admin
            .initialize_share_group_state(
                "share-orders",
                &[ShareGroupStateInitializeTopic::new(
                    [7; 16],
                    [ShareGroupStateInitializePartition::new(0, 1, 0)],
                )],
            )
            .await
            .unwrap();
        assert!(initialized.is_success());
        assert_eq!(initialized.topics()[0].partitions()[0].partition(), 0);

        let written = admin
            .write_share_group_state(
                "share-orders",
                &[ShareGroupStateWriteTopic::new(
                    [7; 16],
                    [ShareGroupStateWritePartition::new(
                        0,
                        1,
                        2,
                        0,
                        [ShareGroupStateBatch::new(0, 1, 0, 1)],
                    )
                    .with_delivery_complete_count(3)],
                )],
            )
            .await
            .unwrap();
        assert!(written.is_success());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn splits_share_group_state_across_partition_coordinators() {
        let (bootstrap_listener, bootstrap_addr) = bind_test_listener().await;
        let (first_listener, first_addr) = bind_test_listener().await;
        let (second_listener, second_addr) = bind_test_listener().await;
        let first_key = "share-orders:BwcHBwcHBwcHBwcHBwcHBw:0".to_owned();
        let second_key = "share-orders:CAgICAgICAgICAgICAgICA:0".to_owned();

        let bootstrap_key_one = first_key.clone();
        let bootstrap_key_two = second_key.clone();
        let bootstrap_server = tokio::spawn(async move {
            let (mut bootstrap, _) = bootstrap_listener.accept().await.unwrap();
            let request = read_frame(&mut bootstrap).await;
            assert_eq!(&request[0..4], &[0, 10, 0, 6]);
            assert!(request
                .windows(bootstrap_key_one.len())
                .any(|window| window == bootstrap_key_one.as_bytes()));
            assert!(request
                .windows(bootstrap_key_two.len())
                .any(|window| window == bootstrap_key_two.as_bytes()));
            write_frame(
                &mut bootstrap,
                &find_share_partition_coordinators_response(&[
                    (first_addr.port(), bootstrap_key_one.as_str()),
                    (second_addr.port(), bootstrap_key_two.as_str()),
                ]),
            )
            .await;
        });

        let first_server = tokio::spawn(async move {
            let (mut coordinator, _) = first_listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_state()).await;
            let request = read_frame(&mut coordinator).await;
            assert_eq!(&request[0..4], &[0, 83, 0, 0]);
            assert!(request.windows(16).any(|window| window == [7; 16]));
            assert!(!request.windows(16).any(|window| window == [8; 16]));
            write_frame(
                &mut coordinator,
                &share_group_state_result_response_for([7; 16], 0),
            )
            .await;
        });

        let second_server = tokio::spawn(async move {
            let (mut coordinator, _) = second_listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_share_group_state()).await;
            let request = read_frame(&mut coordinator).await;
            assert_eq!(&request[0..4], &[0, 83, 0, 0]);
            assert!(request.windows(16).any(|window| window == [8; 16]));
            assert!(!request.windows(16).any(|window| window == [7; 16]));
            write_frame(
                &mut coordinator,
                &share_group_state_result_response_for([8; 16], 0),
            )
            .await;
        });

        let admin = AdminClient::new(
            ClientConfig::new([bootstrap_addr.to_string()]).request_timeout_ms(1_000),
        );
        let result = admin
            .initialize_share_group_state(
                "share-orders",
                &[
                    ShareGroupStateInitializeTopic::new(
                        [7; 16],
                        [ShareGroupStateInitializePartition::new(0, 1, 0)],
                    ),
                    ShareGroupStateInitializeTopic::new(
                        [8; 16],
                        [ShareGroupStateInitializePartition::new(0, 1, 0)],
                    ),
                ],
            )
            .await
            .unwrap();
        assert!(result.is_success());
        assert_eq!(result.topics().len(), 2);
        assert!(result
            .topics()
            .iter()
            .any(|topic| topic.topic_id() == &[7; 16]));
        assert!(result
            .topics()
            .iter()
            .any(|topic| topic.topic_id() == &[8; 16]));

        bootstrap_server.await.unwrap();
        first_server.await.unwrap();
        second_server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_all_share_group_state_reads_and_mutations_per_coordinator() {
        let (bootstrap_listener, bootstrap_addr) = bind_test_listener().await;
        let (first_listener, first_addr) = bind_test_listener().await;
        let (second_listener, second_addr) = bind_test_listener().await;
        let first_key = "share-orders:BwcHBwcHBwcHBwcHBwcHBw:0".to_owned();
        let second_key = "share-orders:CAgICAgICAgICAgICAgICA:0".to_owned();

        let bootstrap_key_one = first_key.clone();
        let bootstrap_key_two = second_key.clone();
        let bootstrap_server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut bootstrap, _) = bootstrap_listener.accept().await.unwrap();
                let request = read_frame(&mut bootstrap).await;
                assert_eq!(&request[0..4], &[0, 10, 0, 6]);
                assert!(request
                    .windows(bootstrap_key_one.len())
                    .any(|window| window == bootstrap_key_one.as_bytes()));
                assert!(request
                    .windows(bootstrap_key_two.len())
                    .any(|window| window == bootstrap_key_two.as_bytes()));
                write_frame(
                    &mut bootstrap,
                    &find_share_partition_coordinators_response(&[
                        (first_addr.port(), bootstrap_key_one.as_str()),
                        (second_addr.port(), bootstrap_key_two.as_str()),
                    ]),
                )
                .await;
            }
        });
        let first_server = tokio::spawn(serve_multi_route_share_state(first_listener, [7; 16]));
        let second_server = tokio::spawn(serve_multi_route_share_state(second_listener, [8; 16]));
        let admin = AdminClient::new(
            ClientConfig::new([bootstrap_addr.to_string()]).request_timeout_ms(1_000),
        );
        let read_topics = [
            ShareGroupStateReadTopic::new([7; 16], [ShareGroupStateReadPartition::new(0, 2)]),
            ShareGroupStateReadTopic::new([8; 16], [ShareGroupStateReadPartition::new(0, 2)]),
        ];

        let read = admin
            .read_share_group_state("share-orders", &read_topics)
            .await
            .unwrap();
        assert!(read.is_success());
        assert_eq!(read.topics().len(), 2);

        let write = admin
            .write_share_group_state(
                "share-orders",
                &[
                    ShareGroupStateWriteTopic::new(
                        [7; 16],
                        [ShareGroupStateWritePartition::new(
                            0,
                            1,
                            2,
                            0,
                            [ShareGroupStateBatch::new(0, 1, 0, 1)],
                        )
                        .with_delivery_complete_count(3)],
                    ),
                    ShareGroupStateWriteTopic::new(
                        [8; 16],
                        [ShareGroupStateWritePartition::new(
                            0,
                            1,
                            2,
                            0,
                            [ShareGroupStateBatch::new(0, 1, 0, 1)],
                        )
                        .with_delivery_complete_count(3)],
                    ),
                ],
            )
            .await
            .unwrap();
        assert!(write.is_success());
        assert_eq!(write.topics().len(), 2);

        let deleted = admin
            .delete_share_group_state(
                "share-orders",
                &[
                    ShareGroupStateDeleteTopic::new([7; 16], [0]),
                    ShareGroupStateDeleteTopic::new([8; 16], [0]),
                ],
            )
            .await
            .unwrap();
        assert!(deleted.is_success());
        assert_eq!(deleted.topics().len(), 2);

        let summary = admin
            .read_share_group_state_summary("share-orders", &read_topics)
            .await
            .unwrap();
        assert!(summary.is_success());
        assert_eq!(summary.topics().len(), 2);
        assert_eq!(
            summary.topics()[0].partitions()[0].delivery_complete_count(),
            Some(3)
        );

        bootstrap_server.await.unwrap();
        first_server.await.unwrap();
        second_server.await.unwrap();
    }

    #[tokio::test]
    async fn routes_share_group_offset_listing_and_deletion_to_coordinator() {
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(
                &mut coordinator,
                &api_versions_with_share_group_offset_listing(),
            )
            .await;
            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 90, 0, 1]);
            write_frame(
                &mut coordinator,
                &describe_share_group_offsets_v1_response(),
            )
            .await;

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
            write_frame(
                &mut coordinator,
                &delete_groups_response_for_group("share-orders", 0),
            )
            .await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let listed = admin
            .list_share_group_offsets("share-orders", None)
            .await
            .unwrap();
        assert!(listed.is_success());
        assert_eq!(listed.group_id(), "share-orders");
        assert_eq!(listed.topics()[0].topic_name(), "orders");
        assert_eq!(listed.topics()[0].partitions()[0].start_offset(), 42);
        assert_eq!(listed.topics()[0].partitions()[0].lag(), Some(7));

        let deleted = admin
            .delete_share_groups(&["share-orders".to_owned()])
            .await
            .unwrap();
        assert!(deleted[0].is_success());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_modern_consumer_group_describe_after_coordinator_disconnect() {
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(
                &mut coordinator,
                &api_versions_with_consumer_group_describe(),
            )
            .await;
            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 69, 0, 1]);
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(
                &mut coordinator,
                &api_versions_with_consumer_group_describe(),
            )
            .await;
            let describe_request = read_frame(&mut coordinator).await;
            assert_eq!(&describe_request[0..4], &[0, 69, 0, 1]);
            write_frame(&mut coordinator, &consumer_group_describe_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let descriptions = admin
            .describe_consumer_groups_modern(&["orders-group".to_owned()], true)
            .await
            .unwrap();

        assert_eq!(descriptions.len(), 1);
        assert!(descriptions[0].is_success());
        assert_eq!(metrics.snapshot().retries, 1);
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
            let api_versions_request = read_frame(&mut broker).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut broker, &api_versions_with_list_groups(1)).await;
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
    async fn negotiates_list_groups_v5_filters_and_preserves_group_metadata() {
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
            write_frame(&mut broker, &api_versions_with_list_groups(5)).await;

            let list_request = read_frame(&mut broker).await;
            assert_eq!(&list_request[0..4], &[0, 16, 0, 5]);
            write_frame(&mut broker, &list_groups_v5_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let groups = admin
            .list_groups_with_options(
                ListGroupsOptions::new()
                    .state("Stable")
                    .group_type("consumer"),
            )
            .await
            .unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id(), "orders-group");
        assert_eq!(groups[0].group_state(), Some("Stable"));
        assert_eq!(groups[0].group_type(), Some("consumer"));
        assert_eq!(groups[0].api_version(), 5);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn negotiates_list_groups_v4_state_filter_and_preserves_group_state() {
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
            write_frame(&mut broker, &api_versions_with_list_groups(4)).await;

            let list_request = read_frame(&mut broker).await;
            assert_eq!(&list_request[0..4], &[0, 16, 0, 4]);
            assert!(list_request
                .windows(b"Stable".len())
                .any(|window| window == b"Stable"));
            write_frame(&mut broker, &list_groups_v4_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let groups = admin
            .list_groups_with_options(ListGroupsOptions::new().state("Stable"))
            .await
            .unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id(), "orders-group");
        assert_eq!(groups[0].group_state(), Some("Stable"));
        assert_eq!(groups[0].group_type(), None);
        assert_eq!(groups[0].api_version(), 4);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_list_groups_when_broker_does_not_advertise_the_api() {
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
            write_frame(&mut broker, &api_versions_without_list_groups()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let result = admin.list_groups().await;

        assert!(matches!(
            result,
            Err(Error::Unsupported(
                "broker does not advertise ListGroups v1 or newer"
            ))
        ));
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
            let api_versions_request = read_frame(&mut first).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut second).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut second, &api_versions_with_list_groups(1)).await;
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
            let api_versions_request = read_frame(&mut broker).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut broker, &api_versions_with_list_groups(1)).await;
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_list_transactions()).await;
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_list_transactions()).await;
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
            let api_versions_request = read_frame(&mut coordinator).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut coordinator, &api_versions_with_list_transactions()).await;
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
    async fn classifies_consumer_group_offset_commit_disconnect_as_unknown() {
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
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );

        let error = admin
            .alter_consumer_group_offsets(
                "orders-group",
                &[ConsumerGroupOffset::new("orders", 0, 42)],
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::AdminMutationOutcomeUnknown {
                operation: "OffsetCommit"
            }
        ));
        assert_eq!(metrics.snapshot().retries, 0);
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
        assert_eq!(results[0].log_dirs()[0].path(), "/var/lib/kafka");
        assert_eq!(results[0].log_dirs()[0].total_bytes(), 100_000);
        assert_eq!(results[0].log_dirs()[0].usable_bytes(), 90_000);
        assert!(!results[0].log_dirs()[0].is_cordoned());
        assert_eq!(results[0].log_dirs()[0].topics()[0].name(), "orders");
        assert_eq!(
            results[0].log_dirs()[0].topics()[0].partitions()[0].partition_size(),
            4096
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn alters_replica_log_dirs_on_selected_broker_with_negotiated_v2() {
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
            write_frame(&mut broker, &api_versions_with_alter_replica_log_dirs(2)).await;

            let request = read_frame(&mut broker).await;
            assert_eq!(&request[0..4], &[0, 34, 0, 2]);
            assert!(request
                .windows(7)
                .any(|bytes| { bytes == [7, b'o', b'r', b'd', b'e', b'r', b's'] }));
            write_frame(&mut broker, &alter_replica_log_dirs_v2_response()).await;
        });
        let metrics = ClientMetrics::new();
        let admin = AdminClient::new(
            ClientConfig::new([addr.to_string()])
                .request_timeout_ms(1_000)
                .metrics(metrics.clone()),
        );
        let assignments = [ReplicaLogDirAssignment::new(
            "orders",
            0,
            "/var/lib/kafka-2",
        )];

        let result = admin.alter_replica_log_dirs(1, &assignments).await.unwrap();

        assert_eq!(result.broker_id(), 1);
        assert_eq!(result.throttle_time(), Duration::from_millis(11));
        assert!(result.is_success());
        assert_eq!(result.topics()[0].name(), "orders");
        assert_eq!(result.topics()[0].partitions()[0].partition_index(), 0);
        assert_eq!(result.topics()[0].partitions()[0].error_code(), 0);
        assert_eq!(metrics.snapshot().broker_errors, 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn creates_delegation_token_on_controller_with_negotiated_v3() {
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
            write_frame(&mut controller, &api_versions_with_delegation_token(38, 3)).await;

            let request = read_frame(&mut controller).await;
            assert_eq!(&request[0..4], &[0, 38, 0, 3]);
            assert!(request
                .windows(6)
                .any(|bytes| { bytes == [6, b'o', b'w', b'n', b'e', b'r'] }));
            write_frame(&mut controller, &create_delegation_token_v3_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));
        let result = admin
            .create_delegation_token(
                CreateDelegationTokenOptions::new()
                    .owner(DelegationTokenPrincipal::new("User", "owner"))
                    .renewer(DelegationTokenPrincipal::new("User", "renew")),
            )
            .await
            .unwrap();

        assert!(result.is_success());
        assert_eq!(result.owner().principal_name(), "owner");
        assert_eq!(result.requester().unwrap().principal_name(), "requester");
        assert_eq!(result.hmac(), b"secret-hmac");
        assert!(!format!("{result:?}").contains("secret-hmac"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn describes_delegation_tokens_on_controller_with_negotiated_v3() {
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
            write_frame(&mut controller, &api_versions_with_delegation_token(41, 3)).await;

            let request = read_frame(&mut controller).await;
            assert_eq!(&request[0..4], &[0, 41, 0, 3]);
            assert!(request.ends_with(&[0, 0]));
            write_frame(&mut controller, &describe_delegation_token_v3_response()).await;
        });
        let admin =
            AdminClient::new(ClientConfig::new([addr.to_string()]).request_timeout_ms(1_000));

        let result = admin.describe_delegation_tokens(None).await.unwrap();

        assert!(result.is_success());
        assert_eq!(result.tokens().len(), 1);
        let token = &result.tokens()[0];
        assert_eq!(token.owner().principal_name(), "owner");
        assert_eq!(token.requester().unwrap().principal_name(), "requester");
        assert_eq!(token.renewers()[0].principal_name(), "renew");
        assert_eq!(token.hmac(), b"secret-hmac");
        assert!(!format!("{result:?}").contains("secret-hmac"));
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

    fn api_versions_with_describe_cluster() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(60);
        encoder.write_i16(0);
        encoder.write_i16(1);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_cluster_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(0); // throttle time
        encoder.write_i16(0); // top-level error
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_i8(2); // controller endpoint
        encoder.write_compact_string("cluster").unwrap();
        encoder.write_i32(1); // controller ID
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i32(1);
                encoder.write_compact_string("127.0.0.1")?;
                encoder.write_i32(9092);
                encoder.write_compact_nullable_string(Some("rack-a"))?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_i32(7); // cluster authorized operations
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_consumer_group_describe() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(69);
        encoder.write_i16(0);
        encoder.write_i16(1);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_share_group_describe() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(77);
        encoder.write_i16(1);
        encoder.write_i16(1);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_streams_group_describe() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(89);
        encoder.write_i16(0);
        encoder.write_i16(0);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn streams_group_describe_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(Some("ok"))?;
                encoder.write_compact_string("streams-orders")?;
                encoder.write_compact_string("Stable")?;
                encoder.write_i32(4);
                encoder.write_i32(5);
                encoder.write_i8(-1); // nullable topology
                encoder.write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))?;
                encoder.write_i32(-2147483648);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn share_group_describe_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(Some("ok"))?;
                encoder.write_compact_string("share-orders")?;
                encoder.write_compact_string("Stable")?;
                encoder.write_i32(4);
                encoder.write_i32(5);
                encoder.write_compact_string("uniform")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("member-1")?;
                    encoder.write_compact_nullable_string(Some("rack-a"))?;
                    encoder.write_i32(7);
                    encoder.write_compact_string("client-1")?;
                    encoder.write_compact_string("/127.0.0.1")?;
                    encoder
                        .write_compact_array(Some(&["orders".to_owned()]), |encoder, topic| {
                            encoder.write_compact_string(topic)
                        })?;
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_uuid(&[7; 16]);
                        encoder.write_compact_string("orders")?;
                        encoder.write_compact_array(
                            Some(&[0_i32, 2_i32]),
                            |encoder, partition| {
                                encoder.write_i32(*partition);
                                Ok(())
                            },
                        )?;
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i32(-2147483648);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_share_group_offsets() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(3); // two API keys
        for api_key in [91_i16, 92_i16] {
            encoder.write_i16(api_key);
            encoder.write_i16(0);
            encoder.write_i16(0);
            encoder.write_empty_tagged_fields();
        }
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_share_group_state() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(6); // five API keys
        for (api_key, max_version) in [
            (83_i16, 0_i16),
            (84_i16, 0_i16),
            (85_i16, 1_i16),
            (86_i16, 0_i16),
            (87_i16, 1_i16),
        ] {
            encoder.write_i16(api_key);
            encoder.write_i16(0);
            encoder.write_i16(max_version);
            encoder.write_empty_tagged_fields();
        }
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn share_group_state_result_response() -> Vec<u8> {
        share_group_state_result_response_for([7; 16], 0)
    }

    fn share_group_state_result_response_for(topic_id: [u8; 16], partition: i32) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_uuid(&topic_id);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(partition);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
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

    fn read_share_group_state_response_for(topic_id: [u8; 16], partition: i32) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_uuid(&topic_id);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(partition);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i32(1); // state epoch
                    encoder.write_i64(0); // start offset
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_i64(0);
                        encoder.write_i64(1);
                        encoder.write_i8(0);
                        encoder.write_i16(1);
                        encoder.write_empty_tagged_fields();
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

    fn share_group_state_summary_response_for(topic_id: [u8; 16], partition: i32) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_uuid(&topic_id);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(partition);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i32(1); // state epoch
                    encoder.write_i32(2); // leader epoch
                    encoder.write_i64(0); // start offset
                    encoder.write_i32(3); // delivery complete count, v1
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

    fn api_versions_with_share_group_offset_listing() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(90);
        encoder.write_i16(0);
        encoder.write_i16(1);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_share_group_offsets_v1_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(0); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("share-orders")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("orders")?;
                    encoder.write_uuid(&[7; 16]);
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_i32(0);
                        encoder.write_i64(42);
                        encoder.write_i32(3);
                        encoder.write_i64(7);
                        encoder.write_i16(0);
                        encoder.write_compact_nullable_string(None)?;
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn alter_share_group_offsets_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder.write_i16(0);
        encoder.write_compact_nullable_string(None).unwrap();
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_uuid(&[7; 16]);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(0);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
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

    fn delete_share_group_offsets_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder.write_i16(0);
        encoder.write_compact_nullable_string(None).unwrap();
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders")?;
                encoder.write_uuid(&[7; 16]);
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn consumer_group_describe_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(Some("ok"))?;
                encoder.write_compact_string("orders-group")?;
                encoder.write_compact_string("Stable")?;
                encoder.write_i32(4);
                encoder.write_i32(5);
                encoder.write_compact_string("uniform")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("member-1")?;
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_compact_nullable_string(Some("rack-a"))?;
                    encoder.write_i32(7);
                    encoder.write_compact_string("client-1")?;
                    encoder.write_compact_string("/127.0.0.1")?;
                    encoder
                        .write_compact_array(Some(&["orders".to_owned()]), |encoder, topic| {
                            encoder.write_compact_string(topic)
                        })?;
                    encoder.write_compact_nullable_string(None)?;
                    for _ in 0..2 {
                        encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                            encoder.write_uuid(&[7; 16]);
                            encoder.write_compact_string("orders")?;
                            encoder.write_compact_array(
                                Some(&[0_i32, 2_i32]),
                                |encoder, partition| {
                                    encoder.write_i32(*partition);
                                    Ok(())
                                },
                            )?;
                            encoder.write_empty_tagged_fields();
                            Ok(())
                        })?;
                        encoder.write_empty_tagged_fields();
                    }
                    encoder.write_i8(1);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i32(-2147483648);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
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

    fn api_versions_with_delegation_token(api_key: i16, max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(api_key);
        encoder.write_i16(1);
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

    fn api_versions_with_describe_topic_partitions() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(75);
        encoder.write_i16(0);
        encoder.write_i16(0);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_topic_partitions_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(4); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(Some("orders"))?;
                encoder.write_uuid(&[7; 16]);
                encoder.write_bool(false);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i16(0);
                    encoder.write_i32(0);
                    encoder.write_i32(1);
                    encoder.write_i32(8);
                    encoder.write_compact_array(Some(&[1_i32]), |encoder, value| {
                        encoder.write_i32(*value);
                        Ok(())
                    })?;
                    encoder.write_compact_array(Some(&[1_i32]), |encoder, value| {
                        encoder.write_i32(*value);
                        Ok(())
                    })?;
                    encoder.write_compact_array(None, |encoder, value: &i32| {
                        encoder.write_i32(*value);
                        Ok(())
                    })?;
                    encoder.write_compact_array(None, |encoder, value: &i32| {
                        encoder.write_i32(*value);
                        Ok(())
                    })?;
                    encoder.write_compact_array(Some(&[]), |encoder, value: &i32| {
                        encoder.write_i32(*value);
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i32(-2147483648);
                encoder.write_empty_tagged_fields();
                encoder.write_i8(1);
                encoder.write_compact_string("orders")?;
                encoder.write_i32(1);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_describe_quorum(max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(55);
        encoder.write_i16(0);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_raft_voter(add_max_version: i16, remove_max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(3); // two API keys
        for (api_key, max_version) in [(80_i16, add_max_version), (81_i16, remove_max_version)] {
            encoder.write_i16(api_key);
            encoder.write_i16(0);
            encoder.write_i16(max_version);
            encoder.write_empty_tagged_fields();
        }
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_with_unregister_broker() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(64);
        encoder.write_i16(0);
        encoder.write_i16(0);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn raft_voter_response(throttle_time_ms: i32, error_code: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(throttle_time_ms);
        encoder.write_i16(error_code);
        encoder.write_compact_nullable_string(None).unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn unregister_broker_response(throttle_time_ms: i32, error_code: i16) -> Vec<u8> {
        raft_voter_response(throttle_time_ms, error_code)
    }

    fn api_versions_with_list_config_resources(max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(74);
        encoder.write_i16(0);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn list_client_metrics_resources_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(7); // throttle time
        encoder.write_i16(0); // success
        encoder
            .write_compact_array(Some(&["latency"]), |encoder, name| {
                encoder.write_compact_string(name)?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_quorum_v2_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i16(0); // top-level error
        encoder.write_compact_nullable_string(None).unwrap();
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("__cluster_metadata")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_i32(0);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i32(1);
                    encoder.write_i32(4);
                    encoder.write_i64(42);
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_i32(1);
                        encoder.write_uuid(&[8; 16]);
                        encoder.write_i64(42);
                        encoder.write_i64(100);
                        encoder.write_i64(101);
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_compact_array(Some(&[]), |_, ()| Ok(()))?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i32(1);
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("CONTROLLER")?;
                    encoder.write_compact_string("127.0.0.1")?;
                    encoder.write_i16(9093);
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

    fn api_versions_with_alter_replica_log_dirs(max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(34);
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
        encoder.write_i64(100_000);
        encoder.write_i64(90_000);
        encoder.write_bool(false);
        encoder.write_empty_tagged_fields(); // result tags
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn alter_replica_log_dirs_v2_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(11); // throttle time
        encoder.write_unsigned_varint(2); // one topic result
        encoder.write_compact_string("orders").unwrap();
        encoder.write_unsigned_varint(2); // one partition result
        encoder.write_i32(0); // partition index
        encoder.write_i16(0); // success
        encoder.write_empty_tagged_fields(); // partition tags
        encoder.write_empty_tagged_fields(); // topic tags
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn create_delegation_token_v3_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i16(0); // top-level success
        encoder.write_compact_string("User").unwrap();
        encoder.write_compact_string("owner").unwrap();
        encoder.write_compact_string("User").unwrap();
        encoder.write_compact_string("requester").unwrap();
        encoder.write_i64(10);
        encoder.write_i64(20);
        encoder.write_i64(30);
        encoder.write_compact_string("token-1").unwrap();
        encoder.write_compact_bytes(b"secret-hmac").unwrap();
        encoder.write_i32(4); // throttle time
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn describe_delegation_token_v3_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i16(0); // top-level success
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("User")?;
                encoder.write_compact_string("owner")?;
                encoder.write_compact_string("User")?;
                encoder.write_compact_string("requester")?;
                encoder.write_i64(10);
                encoder.write_i64(20);
                encoder.write_i64(30);
                encoder.write_compact_string("token-1")?;
                encoder.write_compact_bytes(b"secret-hmac")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("User")?;
                    encoder.write_compact_string("renew")?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields(); // token tags
                Ok(())
            })
            .unwrap();
        encoder.write_i32(5); // throttle time
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

    fn list_config_resources_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(7); // throttle time
        encoder.write_i16(0); // top-level success
        encoder
            .write_compact_array(Some(&["orders", "group-a"]), |encoder, name| {
                encoder.write_compact_string(name)?;
                encoder.write_i8(if *name == "orders" { 2 } else { 32 });
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
    }

    fn api_versions_with_describe_configs_v4() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(32);
        encoder.write_i16(1);
        encoder.write_i16(4);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn describe_configs_v4_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(2); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(9); // throttle time
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_i8(2);
                encoder.write_compact_string("orders")?;
                encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("cleanup.policy")?;
                    encoder.write_compact_nullable_string(Some("compact"))?;
                    encoder.write_bool(false);
                    encoder.write_i8(1);
                    encoder.write_bool(false);
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_compact_string("cleanup.policy")?;
                        encoder.write_compact_nullable_string(Some("delete"))?;
                        encoder.write_i8(5);
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_i8(7);
                    encoder.write_compact_nullable_string(Some("The cleanup policy."))?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields(); // response tags
        encoder.into_bytes()
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

    fn find_share_partition_coordinator_response(port: u16, key: &str) -> Vec<u8> {
        find_share_partition_coordinators_response(&[(port, key)])
    }

    fn find_share_partition_coordinators_response(entries: &[(u16, &str)]) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(1); // correlation ID
        response.write_empty_tagged_fields(); // response header tags
        response.write_i32(0); // throttle time
        response
            .write_compact_array(Some(entries), |encoder, (port, key)| {
                encoder.write_compact_string(key)?;
                encoder.write_i32(1); // node ID
                encoder.write_compact_string("127.0.0.1")?;
                encoder.write_i32(i32::from(*port));
                encoder.write_i16(0); // success
                encoder.write_compact_nullable_string(None)?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        response.write_empty_tagged_fields(); // response body tags
        response.into_bytes()
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

    fn api_versions_with_list_groups(max_version: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(16);
        encoder.write_i16(0);
        encoder.write_i16(max_version);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn api_versions_without_list_groups() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i16(0); // success
        encoder.write_unsigned_varint(2); // one API key
        encoder.write_i16(3);
        encoder.write_i16(0);
        encoder.write_i16(12);
        encoder.write_empty_tagged_fields();
        encoder.write_i32(0); // throttle time
        encoder.write_empty_tagged_fields();
        encoder.into_bytes()
    }

    fn list_groups_v5_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(7); // throttle time
        encoder.write_i16(0); // success
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders-group")?;
                encoder.write_compact_string("consumer")?;
                encoder.write_compact_string("Stable")?;
                encoder.write_compact_string("consumer")?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields(); // response tagged fields
        encoder.into_bytes()
    }

    fn list_groups_v4_response() -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_empty_tagged_fields(); // response header tags
        encoder.write_i32(7); // throttle time
        encoder.write_i16(0); // success
        encoder
            .write_compact_array(Some(&[()]), |encoder, ()| {
                encoder.write_compact_string("orders-group")?;
                encoder.write_compact_string("consumer")?;
                encoder.write_compact_string("Stable")?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
        encoder.write_empty_tagged_fields(); // response tagged fields
        encoder.into_bytes()
    }

    fn delete_groups_response() -> Vec<u8> {
        delete_groups_response_with_error(68)
    }

    fn delete_groups_response_with_error(error_code: i16) -> Vec<u8> {
        delete_groups_response_for_group("orders-group", error_code)
    }

    fn delete_groups_response_for_group(group_id: &str, error_code: i16) -> Vec<u8> {
        let mut encoder = Encoder::new();
        encoder.write_i32(1); // correlation ID
        encoder.write_i32(5); // throttle time
        encoder.write_i32(1); // result count
        encoder.write_string(group_id).unwrap();
        encoder.write_i16(error_code);
        encoder.into_bytes()
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

    #[test]
    fn maps_api_versions_feature_metadata() {
        let response = kafrust_protocol::api::api_versions::ApiVersionsResponseV3 {
            error_code: 0,
            api_keys: Vec::new(),
            throttle_time_ms: 0,
            supported_features: vec![kafrust_protocol::api::api_versions::SupportedFeature {
                name: "group_coordinator".to_owned(),
                min_version: 1,
                max_version: 3,
            }],
            finalized_features_epoch: 12,
            finalized_features: vec![kafrust_protocol::api::api_versions::FinalizedFeature {
                name: "metadata.version".to_owned(),
                min_version_level: 1,
                max_version_level: 4,
            }],
            zk_migration_ready: true,
            tagged_fields: Vec::new(),
        };

        let metadata = super::FeatureMetadata::from_protocol(response);

        assert_eq!(metadata.finalized_features_epoch(), 12);
        assert_eq!(metadata.supported_features()[0].name(), "group_coordinator");
        assert_eq!(metadata.supported_features()[0].min_version(), 1);
        assert_eq!(metadata.supported_features()[0].max_version(), 3);
        assert_eq!(metadata.finalized_features()[0].name(), "metadata.version");
        assert_eq!(metadata.finalized_features()[0].min_version_level(), 1);
        assert_eq!(metadata.finalized_features()[0].max_version_level(), 4);
        assert!(metadata.zk_migration_ready());
    }

    #[test]
    fn maps_update_features_result() {
        let result = super::UpdateFeaturesResult::from_protocol(super::UpdateFeaturesResponseV0 {
            throttle_time_ms: 12,
            error_code: 0,
            error_message: None,
            results: vec![
                kafrust_protocol::api::update_features::FeatureUpdateResultV0 {
                    feature: "metadata.version".to_owned(),
                    error_code: 0,
                    error_message: None,
                },
            ],
        });

        assert!(result.is_success());
        assert_eq!(result.throttle_time(), Duration::from_millis(12));
        assert_eq!(result.results()[0].feature(), "metadata.version");
        assert!(result.results()[0].is_success());
    }

    #[test]
    fn maps_update_features_upgrade_types_without_v0_loss() {
        let safe = super::FeatureUpdate::new("metadata.version", 21).allow_downgrade(true);
        assert_eq!(
            safe.upgrade_type_ref(),
            super::FeatureUpgradeType::SafeDowngrade
        );
        assert!(safe.as_protocol_v0().is_some());
        assert_eq!(safe.as_protocol_v1().upgrade_type, 2);

        let unsafe_downgrade = super::FeatureUpdate::new("metadata.version", 20)
            .upgrade_type(super::FeatureUpgradeType::UnsafeDowngrade);
        assert_eq!(
            unsafe_downgrade.upgrade_type_ref(),
            super::FeatureUpgradeType::UnsafeDowngrade
        );
        assert!(unsafe_downgrade.as_protocol_v0().is_none());
        assert_eq!(unsafe_downgrade.as_protocol_v1().upgrade_type, 3);

        let options = super::UpdateFeaturesOptions::default().validate_only(true);
        assert!(options.validate_only_ref());
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
