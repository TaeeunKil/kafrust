use base64::Engine as _;
use kafrust_protocol::api::add_offsets_to_txn::{
    AddOffsetsToTxnRequestV0, AddOffsetsToTxnRequestV3, AddOffsetsToTxnResponseV0,
    AddOffsetsToTxnResponseV3, API_KEY as ADD_OFFSETS_TO_TXN_API_KEY,
};
use kafrust_protocol::api::add_partitions_to_txn::{
    AddPartitionsToTxnRequestV0, AddPartitionsToTxnRequestV3, AddPartitionsToTxnResponseV0,
    AddPartitionsToTxnResponseV3, AddPartitionsToTxnTopic,
    API_KEY as ADD_PARTITIONS_TO_TXN_API_KEY,
};
use kafrust_protocol::api::alter_client_quotas::{
    AlterClientQuotasRequestV0, AlterClientQuotasResponseV0,
};
use kafrust_protocol::api::alter_configs::{
    AlterConfigsRequestV1, AlterConfigsResourceV1, AlterConfigsResponseV1,
};
use kafrust_protocol::api::alter_partition_reassignments::{
    AlterPartitionReassignmentsRequestV0, AlterPartitionReassignmentsResponseV0,
};
use kafrust_protocol::api::alter_replica_log_dirs::{
    AlterReplicaLogDir, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
};
use kafrust_protocol::api::alter_user_scram_credentials::{
    AlterUserScramCredentialsRequestV0, AlterUserScramCredentialsResponseV0,
};
use kafrust_protocol::api::api_versions::{
    ApiVersionsRequestV0, ApiVersionsRequestV3, ApiVersionsRequestV4, ApiVersionsRequestV5,
    ApiVersionsResponseV0, ApiVersionsResponseV3, ApiVersionsResponseV4, ApiVersionsResponseV5,
    UNSUPPORTED_VERSION_ERROR_CODE,
};
use kafrust_protocol::api::consumer_group_describe::{
    ConsumerGroupDescribeRequestV0, ConsumerGroupDescribeRequestV1,
    ConsumerGroupDescribeResponseV0, ConsumerGroupDescribeResponseV1,
};
use kafrust_protocol::api::consumer_group_heartbeat::{
    ConsumerGroupHeartbeatRequestV0, ConsumerGroupHeartbeatRequestV1,
    ConsumerGroupHeartbeatResponseV0, ConsumerGroupHeartbeatResponseV1,
    ConsumerGroupHeartbeatTopicPartitions,
};
use kafrust_protocol::api::create_acls::{CreateAclsRequestV1, CreateAclsResponseV1};
use kafrust_protocol::api::create_partitions::{
    CreatePartitionsRequestV0, CreatePartitionsResponseV0, CreatePartitionsTopicV0,
};
use kafrust_protocol::api::create_topics::{
    CreateTopicsRequestV2, CreateTopicsResponseV2, CreateTopicsTopicV2,
};
use kafrust_protocol::api::delegation_token::{
    CreateDelegationTokenRequest, CreateDelegationTokenResponse, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    RenewDelegationTokenRequest, RenewDelegationTokenResponse,
};
use kafrust_protocol::api::delete_acls::{DeleteAclsRequestV1, DeleteAclsResponseV1};
use kafrust_protocol::api::delete_groups::{DeleteGroupsRequestV1, DeleteGroupsResponseV1};
use kafrust_protocol::api::delete_records::{
    DeleteRecordsRequestV1, DeleteRecordsResponseV1, DeleteRecordsTopicV1,
};
use kafrust_protocol::api::delete_topics::{DeleteTopicsRequestV3, DeleteTopicsResponseV3};
use kafrust_protocol::api::describe_acls::{DescribeAclsRequestV1, DescribeAclsResponseV1};
use kafrust_protocol::api::describe_client_quotas::{
    DescribeClientQuotasRequestV0, DescribeClientQuotasResponseV0,
};
use kafrust_protocol::api::describe_cluster::{DescribeClusterRequest, DescribeClusterResponse};
use kafrust_protocol::api::describe_configs::{
    DescribeConfigsRequestV1, DescribeConfigsRequestV4, DescribeConfigsResourceV1,
    DescribeConfigsResourceV4, DescribeConfigsResponseV1, DescribeConfigsResponseV4,
};
use kafrust_protocol::api::describe_groups::{DescribeGroupsRequestV1, DescribeGroupsResponseV1};
use kafrust_protocol::api::describe_log_dirs::{
    DescribeLogDirsRequest, DescribeLogDirsResponse, DescribeLogDirsTopic,
};
use kafrust_protocol::api::describe_producers::{
    DescribeProducersRequestV0, DescribeProducersResponseV0, DescribeProducersTopicV0,
};
use kafrust_protocol::api::describe_quorum::{
    DescribeQuorumRequest, DescribeQuorumResponse, DescribeQuorumTopic,
};
use kafrust_protocol::api::describe_share_group_offsets::{
    DescribeShareGroupOffsetsRequestV0, DescribeShareGroupOffsetsRequestV1,
    DescribeShareGroupOffsetsResponseV0, DescribeShareGroupOffsetsResponseV1,
};
use kafrust_protocol::api::describe_topic_partitions::{
    DescribeTopicPartitionsRequestV0, DescribeTopicPartitionsResponseV0,
    DescribeTopicPartitionsTopicV0,
};
use kafrust_protocol::api::describe_transactions::{
    DescribeTransactionsRequestV0, DescribeTransactionsResponseV0,
};
use kafrust_protocol::api::describe_user_scram_credentials::{
    DescribeUserScramCredentialsRequestV0, DescribeUserScramCredentialsResponseV0,
};
use kafrust_protocol::api::elect_leaders::{
    ElectLeadersRequestV0, ElectLeadersRequestV1, ElectLeadersRequestV2, ElectLeadersResponseV0,
    ElectLeadersResponseV1, ElectLeadersResponseV2, ElectLeadersTopicV0,
};
use kafrust_protocol::api::end_txn::{
    EndTxnRequestV0, EndTxnRequestV3, EndTxnResponseV0, EndTxnResponseV3,
    API_KEY as END_TXN_API_KEY,
};
use kafrust_protocol::api::fetch::{
    FetchForgottenTopicV13, FetchForgottenTopicV14, FetchForgottenTopicV15, FetchForgottenTopicV16,
    FetchForgottenTopicV17, FetchForgottenTopicV18, FetchPartitionV11, FetchPartitionV12,
    FetchPartitionV2, FetchReplicaStateV15, FetchRequestV11, FetchRequestV12, FetchRequestV13,
    FetchRequestV14, FetchRequestV15, FetchRequestV16, FetchRequestV17, FetchRequestV18,
    FetchRequestV4, FetchResponseV11, FetchResponseV12, FetchResponseV13, FetchResponseV14,
    FetchResponseV15, FetchResponseV16, FetchResponseV17, FetchResponseV18, FetchResponseV4,
    FetchTopicV11, FetchTopicV12, FetchTopicV13, FetchTopicV14, FetchTopicV15, FetchTopicV16,
    FetchTopicV17, FetchTopicV18, FetchTopicV2, API_KEY as FETCH_API_KEY,
};
use kafrust_protocol::api::find_coordinator::{
    CoordinatorType, FindCoordinatorRequestV1, FindCoordinatorRequestV6, FindCoordinatorResponseV1,
    FindCoordinatorResponseV6, FindCoordinatorResultV6,
};
use kafrust_protocol::api::heartbeat::{
    HeartbeatRequestV2, HeartbeatRequestV3, HeartbeatResponseV2,
};
use kafrust_protocol::api::incremental_alter_configs::{
    IncrementalAlterConfigsRequestV0, IncrementalAlterConfigsResourceV0,
    IncrementalAlterConfigsResponseV0,
};
use kafrust_protocol::api::init_producer_id::{
    InitProducerIdRequestV0, InitProducerIdRequestV2, InitProducerIdResponseV0,
    InitProducerIdResponseV2, API_KEY as INIT_PRODUCER_ID_API_KEY,
};
use kafrust_protocol::api::join_group::{
    JoinGroupProtocol, JoinGroupRequestV2, JoinGroupRequestV5, JoinGroupResponseV2,
    JoinGroupResponseV5,
};
use kafrust_protocol::api::leave_group::{
    LeaveGroupMemberIdentity, LeaveGroupRequestV3, LeaveGroupResponseV3,
};
use kafrust_protocol::api::list_config_resources::{
    ListConfigResourcesRequestV0, ListConfigResourcesRequestV1, ListConfigResourcesResponseV0,
    ListConfigResourcesResponseV1,
};
use kafrust_protocol::api::list_groups::{
    ListGroupsRequestV1, ListGroupsRequestV4, ListGroupsRequestV5, ListGroupsResponseV1,
    ListGroupsResponseV4, ListGroupsResponseV5,
};
use kafrust_protocol::api::list_offsets::{
    ListOffsetsRequestV1, ListOffsetsResponseV1, ListOffsetsTopicV1,
};
use kafrust_protocol::api::list_partition_reassignments::{
    ListPartitionReassignmentsRequestV0, ListPartitionReassignmentsResponseV0,
};
use kafrust_protocol::api::list_transactions::{
    ListTransactionsRequestV0, ListTransactionsRequestV1, ListTransactionsResponseV0,
    ListTransactionsResponseV1,
};
use kafrust_protocol::api::metadata::{
    MetadataRequestTopicV12, MetadataRequestV1, MetadataRequestV12, MetadataResponseV1,
    MetadataResponseV12, API_KEY as METADATA_API_KEY,
};
use kafrust_protocol::api::offset_commit::{
    OffsetCommitRequestV10, OffsetCommitRequestV2, OffsetCommitRequestV7, OffsetCommitRequestV9,
    OffsetCommitResponseV10, OffsetCommitResponseV2, OffsetCommitResponseV7,
    OffsetCommitResponseV9, OffsetCommitTopic, OffsetCommitTopicV10, OffsetCommitTopicV7,
    OffsetCommitTopicV9, API_KEY as OFFSET_COMMIT_API_KEY,
};
use kafrust_protocol::api::offset_delete::{
    OffsetDeleteRequestTopicV0, OffsetDeleteRequestV0, OffsetDeleteResponseV0,
};
use kafrust_protocol::api::offset_fetch::{
    OffsetFetchRequestV10, OffsetFetchRequestV2, OffsetFetchRequestV9, OffsetFetchResponseV10,
    OffsetFetchResponseV2, OffsetFetchResponseV9, OffsetFetchTopic, OffsetFetchTopicV10,
    OffsetFetchTopicV9, API_KEY as OFFSET_FETCH_API_KEY,
};
use kafrust_protocol::api::offset_for_leader_epoch::{
    OffsetForLeaderEpochRequestV3, OffsetForLeaderEpochResponseV3, OffsetForLeaderEpochTopicV3,
};
use kafrust_protocol::api::produce::{
    MessageSetMessage, ProducePartitionV2, ProducePartitionV3, ProduceRequestV11,
    ProduceRequestV12, ProduceRequestV13, ProduceRequestV2, ProduceRequestV3, ProduceRequestV7,
    ProduceRequestV9, ProduceResponseV11, ProduceResponseV12, ProduceResponseV13,
    ProduceResponseV2, ProduceResponseV7, ProduceResponseV9, ProduceTopicV13, ProduceTopicV2,
    ProduceTopicV3, RecordBatchIdentity, RecordBatchMessage,
};
use kafrust_protocol::api::raft_voter::{
    AddRaftVoterRequestV0, AddRaftVoterRequestV1, AddRaftVoterResponse, RaftVoterListener,
    RemoveRaftVoterRequestV0, RemoveRaftVoterResponse,
};
use kafrust_protocol::api::sasl::{
    SaslAuthenticateRequestV1, SaslAuthenticateRequestV2, SaslAuthenticateResponseV1,
    SaslAuthenticateResponseV2, SaslHandshakeRequestV1, SaslHandshakeResponseV1,
};
use kafrust_protocol::api::share::{
    ShareAcknowledgeRequestV1, ShareAcknowledgeRequestV2, ShareAcknowledgeResponseV1,
    ShareAcknowledgeResponseV2, ShareAcknowledgeTopicV1, ShareFetchRequestV1, ShareFetchRequestV2,
    ShareFetchResponseV1, ShareFetchTopicV1, ShareForgottenTopicV1, ShareGroupHeartbeatRequestV1,
    ShareGroupHeartbeatResponseV1,
};
use kafrust_protocol::api::share_group_describe::{
    ShareGroupDescribeRequestV1, ShareGroupDescribeResponseV1,
};
use kafrust_protocol::api::share_group_offsets::{
    AlterShareGroupOffsetsRequestV0, AlterShareGroupOffsetsResponseV0,
    AlterShareGroupOffsetsTopicV0, DeleteShareGroupOffsetsRequestV0,
    DeleteShareGroupOffsetsResponseV0, DeleteShareGroupOffsetsTopicV0,
};
use kafrust_protocol::api::share_group_state::{
    DeleteShareGroupStateRequestV0, DeleteShareGroupStateResponseV0, DeleteShareGroupStateTopic,
    InitializeShareGroupStateRequestV0, InitializeShareGroupStateResponseV0,
    InitializeShareGroupStateTopic, ReadShareGroupStateRequestV0, ReadShareGroupStateResponseV0,
    ReadShareGroupStateSummaryRequestV0, ReadShareGroupStateSummaryRequestV1,
    ReadShareGroupStateSummaryResponseV0, ReadShareGroupStateSummaryResponseV1,
    ReadShareGroupStateTopic, WriteShareGroupStateRequestV0, WriteShareGroupStateRequestV1,
    WriteShareGroupStateResponseV0, WriteShareGroupStateResponseV1, WriteShareGroupStateTopicV0,
    WriteShareGroupStateTopicV1,
};
use kafrust_protocol::api::streams_group_describe::{
    StreamsGroupDescribeRequestV0, StreamsGroupDescribeResponseV0,
};
use kafrust_protocol::api::streams_group_heartbeat::{
    StreamsGroupHeartbeatEndpoint, StreamsGroupHeartbeatKeyValue, StreamsGroupHeartbeatRequestV0,
    StreamsGroupHeartbeatResponseV0, StreamsGroupHeartbeatTask, StreamsGroupHeartbeatTaskOffset,
    StreamsGroupHeartbeatTopology,
};
use kafrust_protocol::api::sync_group::{
    SyncGroupAssignment, SyncGroupRequestV2, SyncGroupRequestV3, SyncGroupResponseV2,
};
use kafrust_protocol::api::telemetry::{
    GetTelemetrySubscriptionsRequestV0, GetTelemetrySubscriptionsResponseV0,
    PushTelemetryRequestV0, PushTelemetryResponseV0,
};
use kafrust_protocol::api::txn_offset_commit::{
    TxnOffsetCommitRequestV0, TxnOffsetCommitRequestV3, TxnOffsetCommitResponseV0,
    TxnOffsetCommitResponseV3, TxnOffsetCommitTopic, TxnOffsetCommitTopicV3,
};
use kafrust_protocol::api::unregister_broker::{
    UnregisterBrokerRequestV0, UnregisterBrokerResponseV0,
};
use kafrust_protocol::api::update_features::{
    FeatureUpdateV1, UpdateFeaturesRequestV0, UpdateFeaturesRequestV1, UpdateFeaturesResponseV0,
    UpdateFeaturesResponseV1,
};
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use kafrust_protocol::frame::encode_frame;
use kafrust_protocol::header::ResponseHeader;
use std::fmt;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, debug_span, Instrument, Span};

use crate::config::{sasl_oauthbearer_auth_bytes_with_token, SaslCredentials};
use crate::error::{Error, Result};
use crate::metrics::ClientMetrics;

pub(crate) const DEFAULT_MAX_RESPONSE_BYTES: usize = 100 * 1024 * 1024;

/// Low-level Kafka request client over a single broker connection.
///
/// A connection is permanently marked unusable after a request timeout, a
/// transport/framing failure, or cancellation of an in-flight request. This
/// prevents a later request from consuming bytes belonging to an earlier
/// partially completed response; high-level clients create a replacement
/// connection through their retry path.
pub struct Client {
    stream: Box<dyn BrokerStream>,
    client_id: Option<String>,
    next_correlation_id: i32,
    request_timeout: Option<Duration>,
    max_response_bytes: usize,
    decode_limits: DecodeLimits,
    metrics: ClientMetrics,
    api_versions_v3_cache: Option<ApiVersionsResponseV3>,
    sasl_session_lifetime_ms: Option<i64>,
    sasl_credentials: Option<SaslCredentials>,
    sasl_authenticated_at: Option<std::time::Instant>,
    sasl_authentication_in_progress: bool,
    connection_poisoned: bool,
    request_in_flight: bool,
    last_request_state: RequestState,
}

pub(crate) trait BrokerStream: AsyncRead + AsyncWrite + Unpin + Send + Sync {}

impl<T> BrokerStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestState {
    Idle,
    Sent,
    ResponseReceived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchOneRequestV4 {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topic: String,
    pub partition_index: i32,
    pub fetch_offset: i64,
    pub max_partition_bytes: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchOneRequestV11 {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topic: String,
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub max_partition_bytes: i32,
    pub session_id: i32,
    pub session_epoch: i32,
    pub rack_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchOneRequestV12 {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topic: String,
    pub partition_index: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub max_partition_bytes: i32,
    pub session_id: i32,
    pub session_epoch: i32,
    pub rack_id: String,
}

impl Client {
    /// Opens a TCP connection to a Kafka broker.
    pub async fn connect(
        server: impl tokio::net::ToSocketAddrs,
        client_id: Option<String>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(server).await?;
        Ok(Self::from_stream(Box::new(stream), client_id, None))
    }

    pub(crate) async fn connect_with_request_timeout_and_metrics(
        server: impl tokio::net::ToSocketAddrs,
        client_id: Option<String>,
        request_timeout: Duration,
        max_response_bytes: usize,
        decode_limits: DecodeLimits,
        metrics: ClientMetrics,
    ) -> Result<Self> {
        let stream = TcpStream::connect(server).await?;
        Ok(Self::from_stream_with_metrics(
            Box::new(stream),
            client_id,
            Some(request_timeout),
            max_response_bytes,
            decode_limits,
            metrics,
        ))
    }

    pub(crate) fn from_stream(
        stream: Box<dyn BrokerStream>,
        client_id: Option<String>,
        request_timeout: Option<Duration>,
    ) -> Self {
        Self::from_stream_with_metrics(
            stream,
            client_id,
            request_timeout,
            DEFAULT_MAX_RESPONSE_BYTES,
            DecodeLimits::default(),
            ClientMetrics::new(),
        )
    }

    pub(crate) fn from_stream_with_metrics(
        stream: Box<dyn BrokerStream>,
        client_id: Option<String>,
        request_timeout: Option<Duration>,
        max_response_bytes: usize,
        decode_limits: DecodeLimits,
        metrics: ClientMetrics,
    ) -> Self {
        Self {
            stream,
            client_id,
            next_correlation_id: 1,
            request_timeout,
            max_response_bytes,
            decode_limits,
            metrics,
            api_versions_v3_cache: None,
            sasl_session_lifetime_ms: None,
            sasl_credentials: None,
            sasl_authenticated_at: None,
            sasl_authentication_in_progress: false,
            connection_poisoned: false,
            request_in_flight: false,
            last_request_state: RequestState::Idle,
        }
    }

    pub(crate) fn last_request_may_have_been_transmitted(&self) -> bool {
        !matches!(self.last_request_state, RequestState::Idle)
    }

    /// Returns a shared handle to metrics for this client connection.
    pub fn metrics(&self) -> ClientMetrics {
        self.metrics.clone()
    }

    pub(crate) fn broker_error(&self, code: i16, context: String) -> Error {
        self.record_broker_error();
        Error::Broker { code, context }
    }

    pub(crate) fn record_broker_error(&self) {
        self.metrics.record_broker_error();
    }

    /// Sends ApiVersions v0 and decodes the broker response.
    pub async fn api_versions(&mut self) -> Result<ApiVersionsResponseV0> {
        let request = ApiVersionsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ApiVersionsResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends flexible ApiVersions v3 and decodes the broker capability ranges.
    ///
    /// The legacy [`Self::api_versions`] method remains available for callers
    /// that need the fixed v0 response shape. This method reports KIP-511
    /// client software fields and preserves unknown response tags so newer
    /// brokers can add capability metadata without breaking decoding.
    pub async fn api_versions_v3(
        &mut self,
        client_software_name: impl Into<String>,
        client_software_version: impl Into<String>,
    ) -> Result<ApiVersionsResponseV3> {
        let request = ApiVersionsRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            client_software_name: client_software_name.into(),
            client_software_version: client_software_version.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        let response = ApiVersionsResponseV3::decode_body(&mut decoder)?;
        self.api_versions_v3_cache = Some(response.clone());
        Ok(response)
    }

    /// Sends flexible ApiVersions v4 and decodes the broker capability ranges.
    ///
    /// ApiVersions v4 has the same wire shape as v3, but fixes the broker
    /// response semantics for supported feature minimum version zero.
    pub async fn api_versions_v4(
        &mut self,
        client_software_name: impl Into<String>,
        client_software_version: impl Into<String>,
    ) -> Result<ApiVersionsResponseV4> {
        let request = ApiVersionsRequestV4 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            client_software_name: client_software_name.into(),
            client_software_version: client_software_version.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ApiVersionsResponseV4::decode_body(&mut decoder)?)
    }

    /// Sends flexible ApiVersions v5 with optional cluster and node checks.
    ///
    /// Pass `None` and `-1` when the expected identity is not known yet. The
    /// v5 response body is compatible with the v4 response body.
    pub async fn api_versions_v5(
        &mut self,
        client_software_name: impl Into<String>,
        client_software_version: impl Into<String>,
        cluster_id: Option<String>,
        node_id: i32,
    ) -> Result<ApiVersionsResponseV5> {
        let request = ApiVersionsRequestV5 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            client_software_name: client_software_name.into(),
            client_software_version: client_software_version.into(),
            cluster_id,
            node_id,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ApiVersionsResponseV5::decode_body(&mut decoder)?)
    }

    /// Negotiates the newest generally usable flexible ApiVersions shape.
    ///
    /// Kafka 4.x brokers may accept v4, while older brokers can return
    /// `UNSUPPORTED_VERSION`; those connections are retried with v3. The
    /// decoded capability ranges have the same public shape either way.
    pub async fn api_versions_cached(
        &mut self,
        client_software_name: impl Into<String>,
        client_software_version: impl Into<String>,
    ) -> Result<ApiVersionsResponseV4> {
        if let Some(response) = self.api_versions_v3_cache.clone() {
            return Ok(response);
        }

        let client_software_name = client_software_name.into();
        let client_software_version = client_software_version.into();
        let response = self
            .api_versions_v4(&client_software_name, &client_software_version)
            .await?;
        let response = if response.error_code == UNSUPPORTED_VERSION_ERROR_CODE {
            self.api_versions_v3(client_software_name, client_software_version)
                .await?
        } else {
            response
        };
        self.api_versions_v3_cache = Some(response.clone());
        Ok(response)
    }

    /// Returns flexible ApiVersions capabilities cached for this broker connection.
    ///
    /// If this connection has not negotiated capabilities yet, this method
    /// sends ApiVersions v4 once and falls back to v3 when necessary. Use
    /// [`Self::api_versions_v3`] when an explicit v3 request is needed.
    pub async fn api_versions_v3_cached(
        &mut self,
        client_software_name: impl Into<String>,
        client_software_version: impl Into<String>,
    ) -> Result<ApiVersionsResponseV3> {
        if let Some(response) = self.api_versions_v3_cache.clone() {
            return Ok(response);
        }
        self.api_versions_v3(client_software_name, client_software_version)
            .await
    }

    /// Returns the last flexible ApiVersions response negotiated on this connection.
    pub fn cached_api_versions(&self) -> Option<&ApiVersionsResponseV4> {
        self.api_versions_v3_cache.as_ref()
    }

    /// Returns the last flexible ApiVersions response through the legacy name.
    ///
    /// This alias remains for callers that used the pre-v4 API. The cached
    /// response may have been obtained through v4 and fallen back to v3.
    pub fn cached_api_versions_v3(&self) -> Option<&ApiVersionsResponseV3> {
        self.cached_api_versions()
    }

    /// Clears the cached flexible ApiVersions response.
    pub fn clear_api_versions_cache(&mut self) {
        self.api_versions_v3_cache = None;
    }

    /// Clears the cached ApiVersions response through the legacy name.
    pub fn clear_api_versions_v3_cache(&mut self) {
        self.clear_api_versions_cache();
    }

    pub(crate) async fn supports_fetch_v11(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(FETCH_API_KEY, 12)
            .is_some_and(|version| version >= 11))
    }

    pub(crate) async fn supports_fetch_v12(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(FETCH_API_KEY, 12)
            .is_some_and(|version| version >= 12))
    }

    pub(crate) async fn supports_fetch_v13(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(FETCH_API_KEY, 18)
            .is_some_and(|version| version >= 13))
    }

    pub(crate) async fn supports_metadata_v12(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(METADATA_API_KEY, 12)
            .is_some_and(|version| version >= 12))
    }

    pub(crate) async fn supports_offset_fetch_v10(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(OFFSET_FETCH_API_KEY, 10)
            .is_some_and(|version| version >= 10))
    }

    pub(crate) async fn supports_offset_commit_v10(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(OFFSET_COMMIT_API_KEY, 10)
            .is_some_and(|version| version >= 10))
    }

    pub(crate) async fn supports_add_partitions_to_txn_v3(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(ADD_PARTITIONS_TO_TXN_API_KEY, 3)
            .is_some_and(|version| version >= 3))
    }

    pub(crate) async fn supports_add_offsets_to_txn_v3(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(ADD_OFFSETS_TO_TXN_API_KEY, 3)
            .is_some_and(|version| version >= 3))
    }

    pub(crate) async fn supports_init_producer_id_v2(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(INIT_PRODUCER_ID_API_KEY, 2)
            .is_some_and(|version| version >= 2))
    }

    pub(crate) async fn supports_end_txn_v3(&mut self) -> Result<bool> {
        let response = self
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        Ok(response
            .highest_supported_version(END_TXN_API_KEY, 3)
            .is_some_and(|version| version >= 3))
    }

    /// Returns the broker-advertised SASL session lifetime in milliseconds.
    ///
    /// A non-zero value indicates when the broker expects re-authentication on
    /// this connection. Provider-backed OAUTHBEARER connections opportunistically
    /// re-authenticate before requests after half of this lifetime has elapsed;
    /// the client does not run a detached re-authentication task.
    pub fn sasl_session_lifetime_ms(&self) -> Option<i64> {
        self.sasl_session_lifetime_ms
    }

    pub(crate) fn enable_sasl_reauthentication(&mut self, credentials: SaslCredentials) {
        self.sasl_credentials = Some(credentials);
        self.sasl_authenticated_at = Some(std::time::Instant::now());
    }

    async fn maybe_reauthenticate(&mut self) -> Result<()> {
        if self.sasl_authentication_in_progress {
            return Ok(());
        }

        let Some(credentials) = self.sasl_credentials.clone() else {
            return Ok(());
        };
        if !credentials.supports_oauthbearer_reauthentication() {
            return Ok(());
        }

        let Some(session_lifetime_ms) = self.sasl_session_lifetime_ms else {
            return Ok(());
        };
        let Ok(session_lifetime_ms) = u64::try_from(session_lifetime_ms) else {
            return Ok(());
        };
        if session_lifetime_ms == 0 {
            return Ok(());
        }
        let Some(authenticated_at) = self.sasl_authenticated_at else {
            return Ok(());
        };
        let refresh_after = Duration::from_millis(session_lifetime_ms) / 2;
        if authenticated_at.elapsed() < refresh_after {
            return Ok(());
        }

        self.sasl_authentication_in_progress = true;
        let result = async {
            let handshake = self
                .sasl_handshake_v1_unchecked(credentials.mechanism().as_str())
                .await?;
            if handshake.error_code != 0 {
                return Err(self.broker_error(
                    handshake.error_code,
                    "sasl re-authenticate handshake".to_owned(),
                ));
            }
            let token = credentials
                .oauthbearer_token_for_auth(self.request_timeout)
                .await?;
            let response = self
                .sasl_authenticate_v1(sasl_oauthbearer_auth_bytes_with_token(
                    &credentials,
                    &token,
                )?)
                .await?;
            if response.error_code != 0 {
                self.acknowledge_oauthbearer_error().await;
                return Err(self.broker_error(
                    response.error_code,
                    "sasl re-authenticate OAUTHBEARER".to_owned(),
                ));
            }
            if !response.auth_bytes.is_empty() {
                self.acknowledge_oauthbearer_error().await;
                return Err(Error::InvalidSaslResponse {
                    mechanism: "OAUTHBEARER",
                    reason: "broker rejected the OAUTHBEARER token",
                });
            }
            Ok(())
        }
        .await;
        self.sasl_authentication_in_progress = false;
        if result.is_err() {
            // The broker is in the re-authentication exchange after the
            // handshake; do not reuse a connection whose SASL state is partial.
            self.connection_poisoned = true;
        }
        result
    }

    pub(crate) async fn sasl_handshake_v1(
        &mut self,
        mechanism: impl Into<String>,
    ) -> Result<SaslHandshakeResponseV1> {
        let request = SaslHandshakeRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            mechanism: mechanism.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        Self::decode_sasl_handshake_response(&response, self.decode_limits)
    }

    async fn sasl_handshake_v1_unchecked(
        &mut self,
        mechanism: impl Into<String>,
    ) -> Result<SaslHandshakeResponseV1> {
        let request = SaslHandshakeRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            mechanism: mechanism.into(),
        };
        let response = self.send_request_traced(&request.encode()?).await?;
        Self::decode_sasl_handshake_response(&response, self.decode_limits)
    }

    fn decode_sasl_handshake_response(
        response: &[u8],
        decode_limits: DecodeLimits,
    ) -> Result<SaslHandshakeResponseV1> {
        let mut decoder = Decoder::with_limits(response, decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(SaslHandshakeResponseV1::decode_body(&mut decoder)?)
    }

    pub(crate) async fn sasl_authenticate_v1(
        &mut self,
        auth_bytes: Vec<u8>,
    ) -> Result<SaslAuthenticateResponseV1> {
        let request = SaslAuthenticateRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            auth_bytes,
        };
        let response = self.send_request_traced(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        let response = SaslAuthenticateResponseV1::decode_body(&mut decoder)?;
        if response.error_code == 0 && response.auth_bytes.is_empty() {
            self.sasl_session_lifetime_ms = Some(response.session_lifetime_ms);
            self.sasl_authenticated_at = Some(std::time::Instant::now());
        }
        Ok(response)
    }

    pub(crate) async fn sasl_authenticate_v2(
        &mut self,
        auth_bytes: Vec<u8>,
    ) -> Result<SaslAuthenticateResponseV2> {
        let response = self.sasl_authenticate_v2_request(auth_bytes).await?;
        if response.error_code == 0 && response.auth_bytes.is_empty() {
            self.sasl_session_lifetime_ms = Some(response.session_lifetime_ms);
            self.sasl_authenticated_at = Some(std::time::Instant::now());
        }
        Ok(response)
    }

    pub(crate) async fn acknowledge_oauthbearer_error(&mut self) {
        // Kafka's OAUTHBEARER client sends control-A after an error challenge.
        let _ = self.sasl_authenticate_v2_request(vec![1]).await;
    }

    async fn sasl_authenticate_v2_request(
        &mut self,
        auth_bytes: Vec<u8>,
    ) -> Result<SaslAuthenticateResponseV2> {
        let request = SaslAuthenticateRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            auth_bytes,
        };
        let response = self.send_request_traced(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(SaslAuthenticateResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends Metadata v1 for all topics or the provided topic names.
    pub async fn metadata(&mut self, topics: Option<Vec<String>>) -> Result<MetadataResponseV1> {
        let request = MetadataRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(MetadataResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends Metadata v12 and returns topic UUIDs needed by KIP-848 groups.
    pub async fn metadata_v12(
        &mut self,
        topics: Option<Vec<MetadataRequestTopicV12>>,
    ) -> Result<MetadataResponseV12> {
        let request = MetadataRequestV12 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
            allow_auto_topic_creation: false,
            include_topic_authorized_operations: false,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(MetadataResponseV12::decode_body(&mut decoder)?)
    }

    /// Sends DescribeTopicPartitions v0 to the broker represented by this connection.
    pub async fn describe_topic_partitions_v0(
        &mut self,
        topics: Vec<DescribeTopicPartitionsTopicV0>,
        response_partition_limit: i32,
        cursor: Option<
            kafrust_protocol::api::describe_topic_partitions::DescribeTopicPartitionsCursorV0,
        >,
    ) -> Result<DescribeTopicPartitionsResponseV0> {
        let request = DescribeTopicPartitionsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
            response_partition_limit,
            cursor,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeTopicPartitionsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends DescribeQuorum v0 to this broker or controller connection.
    pub async fn describe_quorum_v0(
        &mut self,
        topics: Vec<DescribeQuorumTopic>,
    ) -> Result<DescribeQuorumResponse> {
        self.describe_quorum(0, topics).await
    }

    /// Sends DescribeQuorum v1 to this broker or controller connection.
    pub async fn describe_quorum_v1(
        &mut self,
        topics: Vec<DescribeQuorumTopic>,
    ) -> Result<DescribeQuorumResponse> {
        self.describe_quorum(1, topics).await
    }

    /// Sends DescribeQuorum v2 to this broker or controller connection.
    pub async fn describe_quorum_v2(
        &mut self,
        topics: Vec<DescribeQuorumTopic>,
    ) -> Result<DescribeQuorumResponse> {
        self.describe_quorum(2, topics).await
    }

    /// Sends DescribeCluster v0 to the broker represented by this connection.
    pub async fn describe_cluster_v0(
        &mut self,
        include_cluster_authorized_operations: bool,
    ) -> Result<DescribeClusterResponse> {
        self.describe_cluster(0, include_cluster_authorized_operations, 1)
            .await
    }

    /// Sends DescribeCluster v1 to the broker represented by this connection.
    pub async fn describe_cluster_v1(
        &mut self,
        include_cluster_authorized_operations: bool,
        endpoint_type: i8,
    ) -> Result<DescribeClusterResponse> {
        self.describe_cluster(1, include_cluster_authorized_operations, endpoint_type)
            .await
    }

    async fn describe_cluster(
        &mut self,
        api_version: i16,
        include_cluster_authorized_operations: bool,
        endpoint_type: i8,
    ) -> Result<DescribeClusterResponse> {
        let request = DescribeClusterRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            api_version,
            include_cluster_authorized_operations,
            endpoint_type,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeClusterResponse::decode_body(
            &mut decoder,
            api_version,
        )?)
    }

    async fn describe_quorum(
        &mut self,
        api_version: i16,
        topics: Vec<DescribeQuorumTopic>,
    ) -> Result<DescribeQuorumResponse> {
        let request = DescribeQuorumRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
        };
        let response = self.send_request(&request.encode(api_version)?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeQuorumResponse::decode_body(
            &mut decoder,
            api_version,
        )?)
    }

    /// Sends CreateTopics v2 to the broker represented by this connection.
    ///
    /// Kafka expects this request on the active controller. High-level callers
    /// should prefer [`crate::AdminClient`], which discovers and routes to the
    /// current controller.
    pub async fn create_topics_v2(
        &mut self,
        topics: Vec<CreateTopicsTopicV2>,
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<CreateTopicsResponseV2> {
        let request = CreateTopicsRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
            timeout_ms,
            validate_only,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(CreateTopicsResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends CreatePartitions v0 to the broker represented by this connection.
    ///
    /// Kafka expects this request on the active controller. High-level callers
    /// should prefer [`crate::AdminClient`], which discovers and routes to the
    /// current controller.
    pub async fn create_partitions_v0(
        &mut self,
        topics: Vec<CreatePartitionsTopicV0>,
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<CreatePartitionsResponseV0> {
        let request = CreatePartitionsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
            timeout_ms,
            validate_only,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(CreatePartitionsResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends ElectLeaders v0 to the broker represented by this connection.
    ///
    /// Version 0 only supports preferred leader elections. High-level callers
    /// should prefer [`crate::AdminClient`], which discovers the active
    /// controller and negotiates the highest supported version.
    pub async fn elect_leaders_v0(
        &mut self,
        topics: Option<Vec<ElectLeadersTopicV0>>,
        timeout_ms: i32,
    ) -> Result<ElectLeadersResponseV0> {
        let request = ElectLeadersRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
            timeout_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ElectLeadersResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends ElectLeaders v1 to the broker represented by this connection.
    ///
    /// High-level callers should prefer [`crate::AdminClient`], which
    /// negotiates the version and routes the request to the controller.
    pub async fn elect_leaders_v1(
        &mut self,
        election_type: i8,
        topics: Option<Vec<ElectLeadersTopicV0>>,
        timeout_ms: i32,
    ) -> Result<ElectLeadersResponseV1> {
        let request = ElectLeadersRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            election_type,
            topics,
            timeout_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ElectLeadersResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends ElectLeaders v2 to the broker represented by this connection.
    ///
    /// Version 2 uses Kafka's flexible request and response schemas. High-level
    /// callers should prefer [`crate::AdminClient`], which negotiates the
    /// version and routes the request to the controller.
    pub async fn elect_leaders_v2(
        &mut self,
        election_type: i8,
        topics: Option<Vec<ElectLeadersTopicV0>>,
        timeout_ms: i32,
    ) -> Result<ElectLeadersResponseV2> {
        let request = ElectLeadersRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            election_type,
            topics,
            timeout_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ElectLeadersResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends DeleteTopics v3 to the broker represented by this connection.
    ///
    /// Kafka expects this request on the active controller. High-level callers
    /// should prefer [`crate::AdminClient`], which discovers and routes to the
    /// current controller.
    pub async fn delete_topics_v3(
        &mut self,
        topic_names: Vec<String>,
        timeout_ms: i32,
    ) -> Result<DeleteTopicsResponseV3> {
        let request = DeleteTopicsRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topic_names,
            timeout_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DeleteTopicsResponseV3::decode_body(&mut decoder)?)
    }

    /// Sends DeleteRecords v1 to the broker represented by this connection.
    ///
    /// Kafka expects this request on a broker that can serve the target
    /// partitions. High-level callers should prefer [`crate::AdminClient`],
    /// which discovers and routes the request through a bootstrap connection.
    pub async fn delete_records_v1(
        &mut self,
        topics: Vec<DeleteRecordsTopicV1>,
        timeout_ms: i32,
    ) -> Result<DeleteRecordsResponseV1> {
        let request = DeleteRecordsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
            timeout_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DeleteRecordsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DescribeProducers v0 to the broker represented by this connection.
    ///
    /// Kafka expects each requested partition on its current leader. High-level
    /// callers should prefer [`crate::AdminClient`], which performs metadata
    /// discovery and groups requests by leader.
    pub async fn describe_producers_v0(
        &mut self,
        topics: Vec<DescribeProducersTopicV0>,
    ) -> Result<DescribeProducersResponseV0> {
        let request = DescribeProducersRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeProducersResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends DescribeTransactions v0 to the broker represented by this connection.
    ///
    /// The connection must be the transaction coordinator for the requested
    /// transactional IDs. High-level callers should prefer [`crate::AdminClient`].
    pub async fn describe_transactions_v0(
        &mut self,
        transactional_ids: Vec<String>,
    ) -> Result<DescribeTransactionsResponseV0> {
        let request = DescribeTransactionsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_ids,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeTransactionsResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends ListTransactions v0 to the broker represented by this connection.
    ///
    /// The broker returns the transactions for the coordinator shard it owns.
    /// High-level callers should prefer [`crate::AdminClient`], which queries
    /// all metadata brokers and aggregates the shards.
    pub async fn list_transactions_v0(
        &mut self,
        state_filters: Vec<String>,
        producer_id_filters: Vec<i64>,
    ) -> Result<ListTransactionsResponseV0> {
        let request = ListTransactionsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            state_filters,
            producer_id_filters,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ListTransactionsResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends ListTransactions v1 with a broker-side duration filter.
    ///
    /// The response schema is shared with ListTransactions v0. Callers should
    /// negotiate API support through [`Self::api_versions_cached`] before
    /// selecting this method.
    pub async fn list_transactions_v1(
        &mut self,
        state_filters: Vec<String>,
        producer_id_filters: Vec<i64>,
        duration_filter_ms: i64,
    ) -> Result<ListTransactionsResponseV1> {
        let request = ListTransactionsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            state_filters,
            producer_id_filters,
            duration_filter_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ListTransactionsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DescribeConfigs v1 to the broker represented by this connection.
    pub async fn describe_configs_v1(
        &mut self,
        resources: Vec<DescribeConfigsResourceV1>,
        include_synonyms: bool,
    ) -> Result<DescribeConfigsResponseV1> {
        let request = DescribeConfigsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            resources,
            include_synonyms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DescribeConfigsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DescribeConfigs v4 with optional configuration documentation.
    pub async fn describe_configs_v4(
        &mut self,
        resources: Vec<DescribeConfigsResourceV4>,
        include_synonyms: bool,
        include_documentation: bool,
    ) -> Result<DescribeConfigsResponseV4> {
        let request = DescribeConfigsRequestV4 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            resources,
            include_synonyms,
            include_documentation,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeConfigsResponseV4::decode_body(&mut decoder)?)
    }

    /// Sends Kafka API 74 v0, the older ListClientMetricsResources operation.
    pub async fn list_config_resources_v0(&mut self) -> Result<ListConfigResourcesResponseV0> {
        let request = ListConfigResourcesRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ListConfigResourcesResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends Kafka ListConfigResources v1 to the broker represented by this connection.
    ///
    /// An empty resource-type filter asks Kafka for all supported configuration
    /// resource types. Version 1 is available on Kafka 4.1 and newer brokers.
    pub async fn list_config_resources_v1(
        &mut self,
        resource_types: Vec<i8>,
    ) -> Result<ListConfigResourcesResponseV1> {
        let request = ListConfigResourcesRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            resource_types,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ListConfigResourcesResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DescribeLogDirs v1 to this broker.
    ///
    /// Kafka 4.0 removed v0, so high-level callers should negotiate a
    /// supported version before selecting this low-level method.
    pub async fn describe_log_dirs_v1(
        &mut self,
        topics: Option<Vec<DescribeLogDirsTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        let request = DescribeLogDirsRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
        };
        let response = self.send_request(&request.encode_v1()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DescribeLogDirsResponse::decode_body_v1(&mut decoder)?)
    }

    /// Sends DescribeLogDirs v2 to this broker using flexible encoding.
    pub async fn describe_log_dirs_v2(
        &mut self,
        topics: Option<Vec<DescribeLogDirsTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        self.describe_log_dirs_flexible(2, topics).await
    }

    /// Sends DescribeLogDirs v3 to this broker using flexible encoding.
    pub async fn describe_log_dirs_v3(
        &mut self,
        topics: Option<Vec<DescribeLogDirsTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        self.describe_log_dirs_flexible(3, topics).await
    }

    /// Sends DescribeLogDirs v4 to this broker using flexible encoding.
    pub async fn describe_log_dirs_v4(
        &mut self,
        topics: Option<Vec<DescribeLogDirsTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        self.describe_log_dirs_flexible(4, topics).await
    }

    /// Sends DescribeLogDirs v5 to this broker using flexible encoding.
    pub async fn describe_log_dirs_v5(
        &mut self,
        topics: Option<Vec<DescribeLogDirsTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        self.describe_log_dirs_flexible(5, topics).await
    }

    async fn describe_log_dirs_flexible(
        &mut self,
        api_version: i16,
        topics: Option<Vec<DescribeLogDirsTopic>>,
    ) -> Result<DescribeLogDirsResponse> {
        let request = DescribeLogDirsRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            topics,
        };
        let response = self.send_request(&request.encode_v2(api_version)?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(match api_version {
            2 => DescribeLogDirsResponse::decode_body_v2(&mut decoder)?,
            3 => DescribeLogDirsResponse::decode_body_v3(&mut decoder)?,
            4 => DescribeLogDirsResponse::decode_body_v4(&mut decoder)?,
            5 => DescribeLogDirsResponse::decode_body_v5(&mut decoder)?,
            _ => return Err(Error::Unsupported("unsupported DescribeLogDirs version")),
        })
    }

    /// Sends AlterReplicaLogDirs v1 to this broker.
    ///
    /// This is a mutating broker-local request. High-level callers should
    /// select the target broker and negotiate the version before sending it.
    pub async fn alter_replica_log_dirs_v1(
        &mut self,
        dirs: Vec<AlterReplicaLogDir>,
    ) -> Result<AlterReplicaLogDirsResponse> {
        let request = AlterReplicaLogDirsRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            dirs,
        };
        let response = self.send_request(&request.encode_v1()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(AlterReplicaLogDirsResponse::decode_body_v1(&mut decoder)?)
    }

    /// Sends AlterReplicaLogDirs v2 to this broker using flexible encoding.
    pub async fn alter_replica_log_dirs_v2(
        &mut self,
        dirs: Vec<AlterReplicaLogDir>,
    ) -> Result<AlterReplicaLogDirsResponse> {
        let request = AlterReplicaLogDirsRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            dirs,
        };
        let response = self.send_request(&request.encode_v2(2)?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AlterReplicaLogDirsResponse::decode_body_v2(&mut decoder)?)
    }

    /// Sends DescribeAcls v1 to the broker represented by this connection.
    // Kafka defines seven independent ACL filter fields; keeping them explicit
    // makes the low-level wire boundary auditable and mirrors the protocol.
    #[allow(clippy::too_many_arguments)]
    pub async fn describe_acls_v1(
        &mut self,
        resource_type_filter: i8,
        resource_name_filter: Option<String>,
        pattern_type_filter: i8,
        principal_filter: Option<String>,
        host_filter: Option<String>,
        operation: i8,
        permission_type: i8,
    ) -> Result<DescribeAclsResponseV1> {
        let request = DescribeAclsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            resource_type_filter,
            resource_name_filter,
            pattern_type_filter,
            principal_filter,
            host_filter,
            operation,
            permission_type,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DescribeAclsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DescribeClientQuotas v0 to the broker represented by this connection.
    pub async fn describe_client_quotas_v0(
        &mut self,
        components: Vec<
            kafrust_protocol::api::describe_client_quotas::DescribeClientQuotasComponentV0,
        >,
        strict: bool,
    ) -> Result<DescribeClientQuotasResponseV0> {
        let request = DescribeClientQuotasRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            components,
            strict,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DescribeClientQuotasResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends AlterClientQuotas v0 to the broker represented by this connection.
    pub async fn alter_client_quotas_v0(
        &mut self,
        entries: Vec<kafrust_protocol::api::alter_client_quotas::AlterClientQuotasEntryV0>,
        validate_only: bool,
    ) -> Result<AlterClientQuotasResponseV0> {
        let request = AlterClientQuotasRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            entries,
            validate_only,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(AlterClientQuotasResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends DescribeUserScramCredentials v0 to the broker represented by this connection.
    pub async fn describe_user_scram_credentials_v0(
        &mut self,
        users: Option<Vec<String>>,
    ) -> Result<DescribeUserScramCredentialsResponseV0> {
        let request = DescribeUserScramCredentialsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            users,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeUserScramCredentialsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends AlterUserScramCredentials v0 to the broker represented by this connection.
    pub async fn alter_user_scram_credentials_v0(
        &mut self,
        deletions: Vec<kafrust_protocol::api::alter_user_scram_credentials::
            AlterUserScramCredentialsDeletionV0>,
        upsertions: Vec<kafrust_protocol::api::alter_user_scram_credentials::
            AlterUserScramCredentialsUpsertionV0>,
    ) -> Result<AlterUserScramCredentialsResponseV0> {
        let request = AlterUserScramCredentialsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            deletions,
            upsertions,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AlterUserScramCredentialsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends CreateDelegationToken v1 to the broker represented by this
    /// connection.
    pub async fn create_delegation_token_v1(
        &mut self,
        request: CreateDelegationTokenRequest,
    ) -> Result<CreateDelegationTokenResponse> {
        let request = CreateDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            ..request
        };
        let response = self.send_request(&request.encode_v1()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(CreateDelegationTokenResponse::decode_body_v1(&mut decoder)?)
    }

    /// Sends CreateDelegationToken v2 or v3 using flexible encoding.
    pub async fn create_delegation_token_v2(
        &mut self,
        request: CreateDelegationTokenRequest,
        api_version: i16,
    ) -> Result<CreateDelegationTokenResponse> {
        let request = CreateDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            ..request
        };
        let response = self.send_request(&request.encode_v2(api_version)?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(CreateDelegationTokenResponse::decode_body_v2(
            &mut decoder,
            api_version,
        )?)
    }

    /// Sends RenewDelegationToken v1 to the broker represented by this
    /// connection.
    pub async fn renew_delegation_token_v1(
        &mut self,
        hmac: Vec<u8>,
        renew_period_ms: i64,
    ) -> Result<RenewDelegationTokenResponse> {
        let request = RenewDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            hmac,
            renew_period_ms,
        };
        let response = self.send_request(&request.encode_v1()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(RenewDelegationTokenResponse::decode_body_v1(&mut decoder)?)
    }

    /// Sends RenewDelegationToken v2 using flexible encoding.
    pub async fn renew_delegation_token_v2(
        &mut self,
        hmac: Vec<u8>,
        renew_period_ms: i64,
    ) -> Result<RenewDelegationTokenResponse> {
        let request = RenewDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            hmac,
            renew_period_ms,
        };
        let response = self.send_request(&request.encode_v2()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(RenewDelegationTokenResponse::decode_body_v2(&mut decoder)?)
    }

    /// Sends ExpireDelegationToken v1 to the broker represented by this
    /// connection.
    pub async fn expire_delegation_token_v1(
        &mut self,
        hmac: Vec<u8>,
        expiry_time_period_ms: i64,
    ) -> Result<ExpireDelegationTokenResponse> {
        let request = ExpireDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            hmac,
            expiry_time_period_ms,
        };
        let response = self.send_request(&request.encode_v1()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ExpireDelegationTokenResponse::decode_body_v1(&mut decoder)?)
    }

    /// Sends ExpireDelegationToken v2 using flexible encoding.
    pub async fn expire_delegation_token_v2(
        &mut self,
        hmac: Vec<u8>,
        expiry_time_period_ms: i64,
    ) -> Result<ExpireDelegationTokenResponse> {
        let request = ExpireDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            hmac,
            expiry_time_period_ms,
        };
        let response = self.send_request(&request.encode_v2()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ExpireDelegationTokenResponse::decode_body_v2(&mut decoder)?)
    }

    /// Sends DescribeDelegationToken v1 to the broker represented by this
    /// connection.
    pub async fn describe_delegation_token_v1(
        &mut self,
        owners: Option<Vec<kafrust_protocol::api::delegation_token::DelegationTokenPrincipal>>,
    ) -> Result<DescribeDelegationTokenResponse> {
        let request = DescribeDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            owners,
        };
        let response = self.send_request(&request.encode_v1()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DescribeDelegationTokenResponse::decode_body_v1(
            &mut decoder,
        )?)
    }

    /// Sends DescribeDelegationToken v2 or v3 using flexible encoding.
    pub async fn describe_delegation_token_v2(
        &mut self,
        owners: Option<Vec<kafrust_protocol::api::delegation_token::DelegationTokenPrincipal>>,
        api_version: i16,
    ) -> Result<DescribeDelegationTokenResponse> {
        let request = DescribeDelegationTokenRequest {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            owners,
        };
        let response = self.send_request(&request.encode_v2(api_version)?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeDelegationTokenResponse::decode_body_v2(
            &mut decoder,
            api_version,
        )?)
    }

    /// Sends AlterPartitionReassignments v0 to the broker represented by this connection.
    pub async fn alter_partition_reassignments_v0(
        &mut self,
        timeout_ms: i32,
        topics: Vec<kafrust_protocol::api::alter_partition_reassignments::
            AlterPartitionReassignmentsTopicV0>,
    ) -> Result<AlterPartitionReassignmentsResponseV0> {
        let request = AlterPartitionReassignmentsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            timeout_ms,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AlterPartitionReassignmentsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends UpdateFeatures v0 to the active controller represented by this
    /// connection. High-level callers should prefer [`crate::AdminClient`],
    /// which discovers the active controller and applies mutation outcome
    /// handling.
    pub async fn update_features_v0(
        &mut self,
        timeout_ms: i32,
        updates: Vec<kafrust_protocol::api::update_features::FeatureUpdateV0>,
    ) -> Result<UpdateFeaturesResponseV0> {
        let request = UpdateFeaturesRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            timeout_ms,
            updates,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(UpdateFeaturesResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends UpdateFeatures v1 to the active controller represented by this
    /// connection. High-level callers should prefer [`crate::AdminClient`],
    /// which negotiates the broker capability and preserves mutation outcome
    /// handling.
    pub async fn update_features_v1(
        &mut self,
        timeout_ms: i32,
        updates: Vec<FeatureUpdateV1>,
        validate_only: bool,
    ) -> Result<UpdateFeaturesResponseV1> {
        let request = UpdateFeaturesRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            timeout_ms,
            updates,
            validate_only,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(UpdateFeaturesResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends AddRaftVoter v0 to the active KRaft controller represented by
    /// this connection. High-level callers should prefer
    /// [`crate::AdminClient`], which discovers the controller and preserves
    /// mutation outcome handling.
    pub async fn add_raft_voter_v0(
        &mut self,
        cluster_id: Option<String>,
        timeout_ms: i32,
        voter_id: i32,
        voter_directory_id: [u8; 16],
        listeners: Vec<RaftVoterListener>,
    ) -> Result<AddRaftVoterResponse> {
        let request = AddRaftVoterRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            timeout_ms,
            voter_id,
            voter_directory_id,
            listeners,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AddRaftVoterResponse::decode_body(&mut decoder)?)
    }

    /// Sends AddRaftVoter v1 to the active KRaft controller represented by
    /// this connection.
    pub async fn add_raft_voter_v1(
        &mut self,
        cluster_id: Option<String>,
        timeout_ms: i32,
        voter_id: i32,
        voter_directory_id: [u8; 16],
        listeners: Vec<RaftVoterListener>,
        ack_when_committed: bool,
    ) -> Result<AddRaftVoterResponse> {
        let request = AddRaftVoterRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            timeout_ms,
            voter_id,
            voter_directory_id,
            listeners,
            ack_when_committed,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AddRaftVoterResponse::decode_body(&mut decoder)?)
    }

    /// Sends RemoveRaftVoter v0 to the active KRaft controller represented by
    /// this connection.
    pub async fn remove_raft_voter_v0(
        &mut self,
        cluster_id: Option<String>,
        voter_id: i32,
        voter_directory_id: [u8; 16],
    ) -> Result<RemoveRaftVoterResponse> {
        let request = RemoveRaftVoterRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            voter_id,
            voter_directory_id,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(RemoveRaftVoterResponse::decode_body(&mut decoder)?)
    }

    /// Sends UnregisterBroker v0 to the active KRaft controller represented by
    /// this connection. High-level callers should prefer [`crate::AdminClient`],
    /// which discovers the controller and preserves mutation outcome handling.
    pub async fn unregister_broker_v0(
        &mut self,
        broker_id: i32,
    ) -> Result<UnregisterBrokerResponseV0> {
        let request = UnregisterBrokerRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            broker_id,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(UnregisterBrokerResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends ListPartitionReassignments v0 to the broker represented by this connection.
    pub async fn list_partition_reassignments_v0(
        &mut self,
        timeout_ms: i32,
        topics: Option<Vec<kafrust_protocol::api::list_partition_reassignments::
            ListPartitionReassignmentsTopicV0>>,
    ) -> Result<ListPartitionReassignmentsResponseV0> {
        let request = ListPartitionReassignmentsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            timeout_ms,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ListPartitionReassignmentsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends CreateAcls v1 to the broker represented by this connection.
    pub async fn create_acls_v1(
        &mut self,
        creations: Vec<kafrust_protocol::api::create_acls::CreateAclsCreationV1>,
    ) -> Result<CreateAclsResponseV1> {
        let request = CreateAclsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            creations,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(CreateAclsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DeleteAcls v1 to the broker represented by this connection.
    pub async fn delete_acls_v1(
        &mut self,
        filters: Vec<kafrust_protocol::api::delete_acls::DeleteAclsFilterV1>,
    ) -> Result<DeleteAclsResponseV1> {
        let request = DeleteAclsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            filters,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DeleteAclsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DescribeGroups v1 to this group coordinator connection.
    pub async fn describe_groups_v1(
        &mut self,
        group_ids: Vec<String>,
    ) -> Result<DescribeGroupsResponseV1> {
        let request = DescribeGroupsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_ids,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DescribeGroupsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends ConsumerGroupDescribe v0 to this group coordinator connection.
    pub async fn consumer_group_describe_v0(
        &mut self,
        group_ids: Vec<String>,
        include_authorized_operations: bool,
    ) -> Result<ConsumerGroupDescribeResponseV0> {
        let request = ConsumerGroupDescribeRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_ids,
            include_authorized_operations,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ConsumerGroupDescribeResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends ConsumerGroupDescribe v1 to this group coordinator connection.
    pub async fn consumer_group_describe_v1(
        &mut self,
        group_ids: Vec<String>,
        include_authorized_operations: bool,
    ) -> Result<ConsumerGroupDescribeResponseV1> {
        let request = ConsumerGroupDescribeRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_ids,
            include_authorized_operations,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ConsumerGroupDescribeResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends the stable KIP-932 ShareGroupDescribe v1 request.
    pub async fn share_group_describe_v1(
        &mut self,
        group_ids: Vec<String>,
        include_authorized_operations: bool,
    ) -> Result<ShareGroupDescribeResponseV1> {
        let request = ShareGroupDescribeRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_ids,
            include_authorized_operations,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ShareGroupDescribeResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends StreamsGroupDescribe v0 to this Streams group coordinator.
    pub async fn streams_group_describe_v0(
        &mut self,
        group_ids: Vec<String>,
        include_authorized_operations: bool,
    ) -> Result<StreamsGroupDescribeResponseV0> {
        let request = StreamsGroupDescribeRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_ids,
            include_authorized_operations,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(StreamsGroupDescribeResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends StreamsGroupHeartbeat v0 to a Streams group coordinator.
    #[allow(clippy::too_many_arguments)]
    pub async fn streams_group_heartbeat_v0(
        &mut self,
        group_id: impl Into<String>,
        member_id: impl Into<String>,
        member_epoch: i32,
        endpoint_information_epoch: i32,
        instance_id: Option<String>,
        rack_id: Option<String>,
        rebalance_timeout_ms: i32,
        topology: Option<StreamsGroupHeartbeatTopology>,
        active_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
        standby_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
        warmup_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
        process_id: Option<String>,
        user_endpoint: Option<StreamsGroupHeartbeatEndpoint>,
        client_tags: Option<Vec<StreamsGroupHeartbeatKeyValue>>,
        task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
        task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
        shutdown_application: bool,
    ) -> Result<StreamsGroupHeartbeatResponseV0> {
        let request = StreamsGroupHeartbeatRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch,
            endpoint_information_epoch,
            instance_id,
            rack_id,
            rebalance_timeout_ms,
            topology,
            active_tasks,
            standby_tasks,
            warmup_tasks,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            shutdown_application,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(StreamsGroupHeartbeatResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends InitializeShareGroupState v0 to this share-group coordinator.
    pub async fn initialize_share_group_state_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<InitializeShareGroupStateTopic>,
    ) -> Result<InitializeShareGroupStateResponseV0> {
        let request = InitializeShareGroupStateRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(InitializeShareGroupStateResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends ReadShareGroupState v0 to this share-group coordinator.
    pub async fn read_share_group_state_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<ReadShareGroupStateTopic>,
    ) -> Result<ReadShareGroupStateResponseV0> {
        let request = ReadShareGroupStateRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ReadShareGroupStateResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends WriteShareGroupState v0 to this share-group coordinator.
    pub async fn write_share_group_state_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<WriteShareGroupStateTopicV0>,
    ) -> Result<WriteShareGroupStateResponseV0> {
        let request = WriteShareGroupStateRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(WriteShareGroupStateResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends WriteShareGroupState v1, including delivery completion counts.
    pub async fn write_share_group_state_v1(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<WriteShareGroupStateTopicV1>,
    ) -> Result<WriteShareGroupStateResponseV1> {
        let request = WriteShareGroupStateRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(WriteShareGroupStateResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends DeleteShareGroupState v0 to this share-group coordinator.
    pub async fn delete_share_group_state_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<DeleteShareGroupStateTopic>,
    ) -> Result<DeleteShareGroupStateResponseV0> {
        let request = DeleteShareGroupStateRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DeleteShareGroupStateResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends ReadShareGroupStateSummary v0 to this share-group coordinator.
    pub async fn read_share_group_state_summary_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<ReadShareGroupStateTopic>,
    ) -> Result<ReadShareGroupStateSummaryResponseV0> {
        let request = ReadShareGroupStateSummaryRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ReadShareGroupStateSummaryResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends ReadShareGroupStateSummary v1, including delivery completion counts.
    pub async fn read_share_group_state_summary_v1(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<ReadShareGroupStateTopic>,
    ) -> Result<ReadShareGroupStateSummaryResponseV1> {
        let request = ReadShareGroupStateSummaryRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ReadShareGroupStateSummaryResponseV1::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends AlterShareGroupOffsets v0 to this share-group coordinator.
    pub async fn alter_share_group_offsets_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<AlterShareGroupOffsetsTopicV0>,
    ) -> Result<AlterShareGroupOffsetsResponseV0> {
        let request = AlterShareGroupOffsetsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AlterShareGroupOffsetsResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends DeleteShareGroupOffsets v0 to this share-group coordinator.
    pub async fn delete_share_group_offsets_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<DeleteShareGroupOffsetsTopicV0>,
    ) -> Result<DeleteShareGroupOffsetsResponseV0> {
        let request = DeleteShareGroupOffsetsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DeleteShareGroupOffsetsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends DescribeShareGroupOffsets v0 to a share-group coordinator.
    pub async fn describe_share_group_offsets_v0(
        &mut self,
        groups: Vec<
            kafrust_protocol::api::describe_share_group_offsets::DescribeShareGroupOffsetsGroup,
        >,
    ) -> Result<DescribeShareGroupOffsetsResponseV0> {
        let request = DescribeShareGroupOffsetsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            groups,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeShareGroupOffsetsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends DescribeShareGroupOffsets v1, including partition lag.
    pub async fn describe_share_group_offsets_v1(
        &mut self,
        groups: Vec<
            kafrust_protocol::api::describe_share_group_offsets::DescribeShareGroupOffsetsGroup,
        >,
    ) -> Result<DescribeShareGroupOffsetsResponseV1> {
        let request = DescribeShareGroupOffsetsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            groups,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(DescribeShareGroupOffsetsResponseV1::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends ListGroups v1 to the broker represented by this connection.
    pub async fn list_groups_v1(&mut self) -> Result<ListGroupsResponseV1> {
        let request = ListGroupsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ListGroupsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends ListGroups v4 with a group-state filter to the broker represented
    /// by this connection.
    pub async fn list_groups_v4(
        &mut self,
        states_filter: Vec<String>,
    ) -> Result<ListGroupsResponseV4> {
        let request = ListGroupsRequestV4 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            states_filter,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ListGroupsResponseV4::decode_body(&mut decoder)?)
    }

    /// Sends ListGroups v5 with group-state and group-type filters to the
    /// broker represented by this connection.
    pub async fn list_groups_v5(
        &mut self,
        states_filter: Vec<String>,
        types_filter: Vec<String>,
    ) -> Result<ListGroupsResponseV5> {
        let request = ListGroupsRequestV5 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            states_filter,
            types_filter,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ListGroupsResponseV5::decode_body(&mut decoder)?)
    }

    /// Sends DeleteGroups v1 to this group coordinator connection.
    pub async fn delete_groups_v1(
        &mut self,
        group_ids: Vec<String>,
    ) -> Result<DeleteGroupsResponseV1> {
        let request = DeleteGroupsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_ids,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(DeleteGroupsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends OffsetDelete v0 to this group coordinator connection.
    pub async fn offset_delete_v0(
        &mut self,
        group_id: impl Into<String>,
        topics: Vec<OffsetDeleteRequestTopicV0>,
    ) -> Result<OffsetDeleteResponseV0> {
        let request = OffsetDeleteRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(OffsetDeleteResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends IncrementalAlterConfigs v0 to this broker.
    pub async fn incremental_alter_configs_v0(
        &mut self,
        resources: Vec<IncrementalAlterConfigsResourceV0>,
        validate_only: bool,
    ) -> Result<IncrementalAlterConfigsResponseV0> {
        let request = IncrementalAlterConfigsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            resources,
            validate_only,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(IncrementalAlterConfigsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Sends classic AlterConfigs v1 to this broker.
    ///
    /// Kafka AlterConfigs v1 is the non-flexible compatibility path. It
    /// replaces the complete dynamic configuration map for each resource;
    /// callers should use a null value to remove a key.
    pub async fn alter_configs_v1(
        &mut self,
        resources: Vec<AlterConfigsResourceV1>,
        validate_only: bool,
    ) -> Result<AlterConfigsResponseV1> {
        let request = AlterConfigsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            resources,
            validate_only,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(AlterConfigsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends InitProducerId v0 for an idempotent or transactional producer session.
    pub async fn init_producer_id_v0(
        &mut self,
        transactional_id: Option<String>,
        transaction_timeout_ms: i32,
    ) -> Result<InitProducerIdResponseV0> {
        let request = InitProducerIdRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            transaction_timeout_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(InitProducerIdResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends flexible InitProducerId v2 for an idempotent or transactional
    /// producer session.
    pub async fn init_producer_id_v2(
        &mut self,
        transactional_id: Option<String>,
        transaction_timeout_ms: i32,
    ) -> Result<InitProducerIdResponseV2> {
        let request = InitProducerIdRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            transaction_timeout_ms,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(InitProducerIdResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends EndTxn v0 to commit or abort a transactional producer session.
    pub async fn end_txn_v0(
        &mut self,
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<EndTxnResponseV0> {
        let request = EndTxnRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            committed,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(EndTxnResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends flexible EndTxn v3 to commit or abort a transactional producer
    /// session.
    pub async fn end_txn_v3(
        &mut self,
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<EndTxnResponseV3> {
        let request = EndTxnRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            committed,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(EndTxnResponseV3::decode_body(&mut decoder)?)
    }

    /// Sends FindCoordinator v1 for a consumer group ID.
    pub async fn find_group_coordinator(
        &mut self,
        group_id: impl Into<String>,
    ) -> Result<FindCoordinatorResponseV1> {
        self.find_coordinator_v1(group_id.into(), CoordinatorType::Group)
            .await
    }

    /// Sends FindCoordinator v1 for a share group ID.
    pub async fn find_share_group_coordinator(
        &mut self,
        group_id: impl Into<String>,
    ) -> Result<FindCoordinatorResponseV1> {
        self.find_coordinator_v1(group_id.into(), CoordinatorType::Share)
            .await
    }

    /// Sends FindCoordinator v6 for one KIP-932 share-partition resource.
    ///
    /// Share-group membership uses the ordinary group coordinator. Durable
    /// share-partition state uses a separate coordinator selected by the
    /// `group:topic-id:partition` key and the Share coordinator type.
    pub async fn find_share_partition_coordinator(
        &mut self,
        group_id: impl AsRef<str>,
        topic_id: [u8; 16],
        partition: i32,
    ) -> Result<FindCoordinatorResultV6> {
        let coordinator_key =
            format_share_partition_coordinator_key(group_id.as_ref(), topic_id, partition);
        let response = self
            .find_share_partition_coordinators(group_id, &[(topic_id, partition)])
            .await?;
        response
            .coordinators
            .into_iter()
            .find(|coordinator| coordinator.coordinator_key == coordinator_key)
            .ok_or(Error::Unsupported(
                "FindCoordinator v6 returned no share-partition result",
            ))
    }

    /// Sends FindCoordinator v6 for multiple KIP-932 share-partition resources.
    ///
    /// Kafka may assign different share partitions in one request to different
    /// brokers. Callers that send a multi-partition Share Group State request
    /// must use the returned per-key coordinator results to split the request.
    pub async fn find_share_partition_coordinators(
        &mut self,
        group_id: impl AsRef<str>,
        resources: &[([u8; 16], i32)],
    ) -> Result<FindCoordinatorResponseV6> {
        if resources.is_empty() {
            return Err(Error::Unsupported(
                "FindCoordinator v6 requires at least one share partition",
            ));
        }
        let group_id = group_id.as_ref();
        let coordinator_keys = resources
            .iter()
            .map(|(topic_id, partition)| {
                format_share_partition_coordinator_key(group_id, *topic_id, *partition)
            })
            .collect();
        let request = FindCoordinatorRequestV6 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            coordinator_type: CoordinatorType::Share,
            coordinator_keys,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FindCoordinatorResponseV6::decode_body(&mut decoder)?)
    }

    /// Sends FindCoordinator v1 for a transactional ID.
    pub async fn find_transaction_coordinator(
        &mut self,
        transactional_id: impl Into<String>,
    ) -> Result<FindCoordinatorResponseV1> {
        self.find_coordinator_v1(transactional_id.into(), CoordinatorType::Transaction)
            .await
    }

    async fn find_coordinator_v1(
        &mut self,
        coordinator_key: String,
        coordinator_type: CoordinatorType,
    ) -> Result<FindCoordinatorResponseV1> {
        let request = FindCoordinatorRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            coordinator_key,
            coordinator_type,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(FindCoordinatorResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends AddPartitionsToTxn v0 for a transactional producer.
    pub async fn add_partitions_to_txn_v0(
        &mut self,
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<AddPartitionsToTxnTopic>,
    ) -> Result<AddPartitionsToTxnResponseV0> {
        let request = AddPartitionsToTxnRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(AddPartitionsToTxnResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends flexible AddPartitionsToTxn v3 for a transactional producer.
    pub async fn add_partitions_to_txn_v3(
        &mut self,
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<AddPartitionsToTxnTopic>,
    ) -> Result<AddPartitionsToTxnResponseV3> {
        let request = AddPartitionsToTxnRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AddPartitionsToTxnResponseV3::decode_body(&mut decoder)?)
    }

    /// Sends AddOffsetsToTxn v0 to bind a consumer group to a transaction.
    pub async fn add_offsets_to_txn_v0(
        &mut self,
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        group_id: impl Into<String>,
    ) -> Result<AddOffsetsToTxnResponseV0> {
        let request = AddOffsetsToTxnRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            group_id: group_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(AddOffsetsToTxnResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends flexible AddOffsetsToTxn v3 to bind a consumer group to a transaction.
    pub async fn add_offsets_to_txn_v3(
        &mut self,
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        group_id: impl Into<String>,
    ) -> Result<AddOffsetsToTxnResponseV3> {
        let request = AddOffsetsToTxnRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            group_id: group_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(AddOffsetsToTxnResponseV3::decode_body(&mut decoder)?)
    }

    /// Sends TxnOffsetCommit v0 for offsets included in a transaction.
    pub async fn txn_offset_commit_v0(
        &mut self,
        transactional_id: impl Into<String>,
        group_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<TxnOffsetCommitTopic>,
    ) -> Result<TxnOffsetCommitResponseV0> {
        let request = TxnOffsetCommitRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            group_id: group_id.into(),
            producer_id,
            producer_epoch,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(TxnOffsetCommitResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends TxnOffsetCommit v3 with consumer-group generation fencing.
    #[allow(clippy::too_many_arguments)]
    pub async fn txn_offset_commit_v3(
        &mut self,
        transactional_id: impl Into<String>,
        group_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        generation_id: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
        topics: Vec<TxnOffsetCommitTopicV3>,
    ) -> Result<TxnOffsetCommitResponseV3> {
        let request = TxnOffsetCommitRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id: transactional_id.into(),
            group_id: group_id.into(),
            producer_id,
            producer_epoch,
            generation_id,
            member_id: member_id.into(),
            group_instance_id,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(TxnOffsetCommitResponseV3::decode_body(&mut decoder)?)
    }

    /// Sends OffsetFetch v2 for a consumer group.
    pub async fn offset_fetch_v2(
        &mut self,
        group_id: impl Into<String>,
        topics: Option<Vec<OffsetFetchTopic>>,
    ) -> Result<OffsetFetchResponseV2> {
        let request = OffsetFetchRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(OffsetFetchResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends OffsetFetch v9 for Kafka's KIP-848 consumer group protocol.
    pub async fn offset_fetch_v9(
        &mut self,
        group_id: impl Into<String>,
        member_id: Option<String>,
        member_epoch: i32,
        topics: Option<Vec<OffsetFetchTopicV9>>,
    ) -> Result<OffsetFetchResponseV9> {
        self.offset_fetch_v9_with_require_stable(group_id, member_id, member_epoch, topics, false)
            .await
    }

    /// Sends OffsetFetch v9 with an explicit stable-offset requirement.
    ///
    /// `require_stable` asks Kafka to wait for unstable transactional offsets
    /// before returning them, as defined by the OffsetFetch protocol.
    pub async fn offset_fetch_v9_with_require_stable(
        &mut self,
        group_id: impl Into<String>,
        member_id: Option<String>,
        member_epoch: i32,
        topics: Option<Vec<OffsetFetchTopicV9>>,
        require_stable: bool,
    ) -> Result<OffsetFetchResponseV9> {
        let request = OffsetFetchRequestV9 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            member_id,
            member_epoch,
            topics,
            require_stable,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(OffsetFetchResponseV9::decode_body(&mut decoder)?)
    }

    /// Sends OffsetFetch v10 with topic UUIDs for Kafka 4.x.
    pub async fn offset_fetch_v10(
        &mut self,
        group_id: impl Into<String>,
        member_id: Option<String>,
        member_epoch: i32,
        topics: Option<Vec<OffsetFetchTopicV10>>,
        require_stable: bool,
    ) -> Result<OffsetFetchResponseV10> {
        let request = OffsetFetchRequestV10 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            member_id,
            member_epoch,
            topics,
            require_stable,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(OffsetFetchResponseV10::decode_body(&mut decoder)?)
    }

    /// Sends ListOffsets v1 to resolve timestamp-based partition offsets.
    pub async fn list_offsets_v1(
        &mut self,
        topics: Vec<ListOffsetsTopicV1>,
    ) -> Result<ListOffsetsResponseV1> {
        let request = ListOffsetsRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            replica_id: -1,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ListOffsetsResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends OffsetForLeaderEpoch v3 for partition log recovery metadata.
    pub async fn offset_for_leader_epoch_v3(
        &mut self,
        topics: Vec<OffsetForLeaderEpochTopicV3>,
    ) -> Result<OffsetForLeaderEpochResponseV3> {
        let request = OffsetForLeaderEpochRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            replica_id: -1,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(OffsetForLeaderEpochResponseV3::decode_body(&mut decoder)?)
    }

    /// Sends JoinGroup v2 using the provided group protocol metadata.
    pub async fn join_group_v2(
        &mut self,
        group_id: impl Into<String>,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: impl Into<String>,
        protocol_type: impl Into<String>,
        protocols: Vec<JoinGroupProtocol>,
    ) -> Result<JoinGroupResponseV2> {
        let request = JoinGroupRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id: member_id.into(),
            protocol_type: protocol_type.into(),
            protocols,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(JoinGroupResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends JoinGroup v5 with an optional static group instance ID.
    #[allow(clippy::too_many_arguments)]
    pub async fn join_group_v5(
        &mut self,
        group_id: impl Into<String>,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
        protocol_type: impl Into<String>,
        protocols: Vec<JoinGroupProtocol>,
    ) -> Result<JoinGroupResponseV5> {
        let request = JoinGroupRequestV5 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id: member_id.into(),
            group_instance_id,
            protocol_type: protocol_type.into(),
            protocols,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(JoinGroupResponseV5::decode_body(&mut decoder)?)
    }

    /// Sends SyncGroup v2 using the provided member assignments.
    pub async fn sync_group_v2(
        &mut self,
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
        assignments: Vec<SyncGroupAssignment>,
    ) -> Result<SyncGroupResponseV2> {
        let request = SyncGroupRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
            assignments,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(SyncGroupResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends SyncGroup v3 with an optional static group instance ID.
    pub async fn sync_group_v3(
        &mut self,
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
        assignments: Vec<SyncGroupAssignment>,
    ) -> Result<SyncGroupResponseV2> {
        let request = SyncGroupRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
            group_instance_id,
            assignments,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(SyncGroupResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends Heartbeat v2 for a joined consumer group member.
    pub async fn heartbeat_v2(
        &mut self,
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
    ) -> Result<HeartbeatResponseV2> {
        let request = HeartbeatRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(HeartbeatResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends Heartbeat v3 with an optional static group instance ID.
    pub async fn heartbeat_v3(
        &mut self,
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
    ) -> Result<HeartbeatResponseV2> {
        let request = HeartbeatRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
            group_instance_id,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(HeartbeatResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends KIP-848 ConsumerGroupHeartbeat v0.
    #[allow(clippy::too_many_arguments)]
    pub async fn consumer_group_heartbeat_v0(
        &mut self,
        group_id: impl Into<String>,
        member_id: impl Into<String>,
        member_epoch: i32,
        instance_id: Option<String>,
        rack_id: Option<String>,
        rebalance_timeout_ms: i32,
        subscribed_topic_names: Option<Vec<String>>,
        server_assignor: Option<String>,
        topic_partitions: Option<Vec<ConsumerGroupHeartbeatTopicPartitions>>,
    ) -> Result<ConsumerGroupHeartbeatResponseV0> {
        let request = ConsumerGroupHeartbeatRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch,
            instance_id,
            rack_id,
            rebalance_timeout_ms,
            subscribed_topic_names,
            server_assignor,
            topic_partitions,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ConsumerGroupHeartbeatResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends KIP-848 ConsumerGroupHeartbeat v1 with an optional topic regex.
    #[allow(clippy::too_many_arguments)]
    pub async fn consumer_group_heartbeat_v1(
        &mut self,
        group_id: impl Into<String>,
        member_id: impl Into<String>,
        member_epoch: i32,
        instance_id: Option<String>,
        rack_id: Option<String>,
        rebalance_timeout_ms: i32,
        subscribed_topic_names: Option<Vec<String>>,
        subscribed_topic_regex: Option<String>,
        server_assignor: Option<String>,
        topic_partitions: Option<Vec<ConsumerGroupHeartbeatTopicPartitions>>,
    ) -> Result<ConsumerGroupHeartbeatResponseV1> {
        let request = ConsumerGroupHeartbeatRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch,
            instance_id,
            rack_id,
            rebalance_timeout_ms,
            subscribed_topic_names,
            subscribed_topic_regex,
            server_assignor,
            topic_partitions,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ConsumerGroupHeartbeatResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends the stable KIP-932 ShareGroupHeartbeat v1 request.
    #[allow(clippy::too_many_arguments)]
    pub async fn share_group_heartbeat_v1(
        &mut self,
        group_id: impl Into<String>,
        member_id: impl Into<String>,
        member_epoch: i32,
        rack_id: Option<String>,
        subscribed_topic_names: Option<Vec<String>>,
    ) -> Result<ShareGroupHeartbeatResponseV1> {
        let request = ShareGroupHeartbeatRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch,
            rack_id,
            subscribed_topic_names,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ShareGroupHeartbeatResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends the stable KIP-932 ShareFetch v1 request.
    #[allow(clippy::too_many_arguments)]
    pub async fn share_fetch_v1(
        &mut self,
        group_id: Option<String>,
        member_id: Option<String>,
        share_session_epoch: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        max_records: i32,
        batch_size: i32,
        topics: Vec<ShareFetchTopicV1>,
        forgotten_topics: Vec<ShareForgottenTopicV1>,
    ) -> Result<ShareFetchResponseV1> {
        let request = ShareFetchRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id,
            member_id,
            share_session_epoch,
            max_wait_ms,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            topics,
            forgotten_topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ShareFetchResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends ShareFetch v2 with the KIP-1206 acquire mode field.
    ///
    /// The response remains decoded with the stable ShareFetch v1 response
    /// schema because KIP-1206 only adds a request field.
    #[allow(clippy::too_many_arguments)]
    pub async fn share_fetch_v2(
        &mut self,
        group_id: Option<String>,
        member_id: Option<String>,
        share_session_epoch: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        max_records: i32,
        batch_size: i32,
        share_acquire_mode: i8,
        topics: Vec<ShareFetchTopicV1>,
        forgotten_topics: Vec<ShareForgottenTopicV1>,
    ) -> Result<ShareFetchResponseV1> {
        self.share_fetch_v2_with_renew(
            group_id,
            member_id,
            share_session_epoch,
            max_wait_ms,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            share_acquire_mode,
            false,
            topics,
            forgotten_topics,
        )
        .await
    }

    /// Sends ShareFetch v2 with optional KIP-1222 renewal semantics.
    ///
    /// A renewal fetch must use zero wait, byte, and record limits. The
    /// high-level share consumer enforces those limits before calling this
    /// method.
    #[allow(clippy::too_many_arguments)]
    pub async fn share_fetch_v2_with_renew(
        &mut self,
        group_id: Option<String>,
        member_id: Option<String>,
        share_session_epoch: i32,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        max_records: i32,
        batch_size: i32,
        share_acquire_mode: i8,
        is_renew_ack: bool,
        topics: Vec<ShareFetchTopicV1>,
        forgotten_topics: Vec<ShareForgottenTopicV1>,
    ) -> Result<ShareFetchResponseV1> {
        let request = ShareFetchRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id,
            member_id,
            share_session_epoch,
            max_wait_ms,
            min_bytes,
            max_bytes,
            max_records,
            batch_size,
            share_acquire_mode,
            is_renew_ack,
            topics,
            forgotten_topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ShareFetchResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends the stable KIP-932 ShareAcknowledge v1 request.
    pub async fn share_acknowledge_v1(
        &mut self,
        group_id: Option<String>,
        member_id: Option<String>,
        share_session_epoch: i32,
        topics: Vec<ShareAcknowledgeTopicV1>,
    ) -> Result<ShareAcknowledgeResponseV1> {
        let request = ShareAcknowledgeRequestV1 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id,
            member_id,
            share_session_epoch,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ShareAcknowledgeResponseV1::decode_body(&mut decoder)?)
    }

    /// Sends ShareAcknowledge v2 with KIP-1222 renewal support.
    pub async fn share_acknowledge_v2(
        &mut self,
        group_id: Option<String>,
        member_id: Option<String>,
        share_session_epoch: i32,
        is_renew_ack: bool,
        topics: Vec<ShareAcknowledgeTopicV1>,
    ) -> Result<ShareAcknowledgeResponseV2> {
        let request = ShareAcknowledgeRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id,
            member_id,
            share_session_epoch,
            is_renew_ack,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ShareAcknowledgeResponseV2::decode_body(&mut decoder)?)
    }

    /// Requests the broker's KIP-714 telemetry subscription.
    pub async fn get_telemetry_subscriptions_v0(
        &mut self,
        client_instance_id: [u8; 16],
    ) -> Result<GetTelemetrySubscriptionsResponseV0> {
        let request = GetTelemetrySubscriptionsRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            client_instance_id,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(GetTelemetrySubscriptionsResponseV0::decode_body(
            &mut decoder,
        )?)
    }

    /// Pushes an OpenTelemetry MetricsData payload through KIP-714.
    pub async fn push_telemetry_v0(
        &mut self,
        client_instance_id: [u8; 16],
        subscription_id: i32,
        terminating: bool,
        compression_type: i8,
        metrics: Vec<u8>,
    ) -> Result<PushTelemetryResponseV0> {
        let request = PushTelemetryRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            client_instance_id,
            subscription_id,
            terminating,
            compression_type,
            metrics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(PushTelemetryResponseV0::decode_body(&mut decoder)?)
    }

    /// Sends LeaveGroup v3 for one or more dynamic or static group members.
    pub async fn leave_group_v3(
        &mut self,
        group_id: impl Into<String>,
        members: Vec<LeaveGroupMemberIdentity>,
    ) -> Result<LeaveGroupResponseV3> {
        let request = LeaveGroupRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            members,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(LeaveGroupResponseV3::decode_body(&mut decoder)?)
    }

    /// Sends OffsetCommit v2 for a joined consumer group member.
    pub async fn offset_commit_v2(
        &mut self,
        group_id: impl Into<String>,
        generation_id_or_member_epoch: i32,
        member_id: impl Into<String>,
        retention_time_ms: i64,
        topics: Vec<OffsetCommitTopic>,
    ) -> Result<OffsetCommitResponseV2> {
        let request = OffsetCommitRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id_or_member_epoch,
            member_id: member_id.into(),
            retention_time_ms,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(OffsetCommitResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends OffsetCommit v7 with static-member fencing and leader epochs.
    pub async fn offset_commit_v7(
        &mut self,
        group_id: impl Into<String>,
        generation_id_or_member_epoch: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
        topics: Vec<OffsetCommitTopicV7>,
    ) -> Result<OffsetCommitResponseV7> {
        let request = OffsetCommitRequestV7 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id_or_member_epoch,
            member_id: member_id.into(),
            group_instance_id,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(OffsetCommitResponseV7::decode_body(&mut decoder)?)
    }

    /// Sends OffsetCommit v9 for Kafka's KIP-848 consumer group protocol.
    pub async fn offset_commit_v9(
        &mut self,
        group_id: impl Into<String>,
        member_epoch: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
        topics: Vec<OffsetCommitTopicV9>,
    ) -> Result<OffsetCommitResponseV9> {
        let request = OffsetCommitRequestV9 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id_or_member_epoch: member_epoch,
            member_id: member_id.into(),
            group_instance_id,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(OffsetCommitResponseV9::decode_body(&mut decoder)?)
    }

    /// Sends OffsetCommit v10 with topic UUIDs for Kafka 4.x.
    pub async fn offset_commit_v10(
        &mut self,
        group_id: impl Into<String>,
        member_epoch: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
        topics: Vec<OffsetCommitTopicV10>,
    ) -> Result<OffsetCommitResponseV10> {
        let request = OffsetCommitRequestV10 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            group_id: group_id.into(),
            generation_id_or_member_epoch: member_epoch,
            member_id: member_id.into(),
            group_instance_id,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(OffsetCommitResponseV10::decode_body(&mut decoder)?)
    }

    pub(crate) async fn fetch_one_v4(
        &mut self,
        request: FetchOneRequestV4,
    ) -> Result<FetchResponseV4> {
        let request = FetchRequestV4 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            replica_id: request.replica_id,
            max_wait_ms: request.max_wait_ms,
            min_bytes: request.min_bytes,
            max_bytes: request.max_bytes,
            isolation_level: request.isolation_level,
            topics: vec![FetchTopicV2 {
                name: request.topic,
                partitions: vec![FetchPartitionV2 {
                    partition_index: request.partition_index,
                    fetch_offset: request.fetch_offset,
                    max_bytes: request.max_partition_bytes,
                }],
            }],
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(FetchResponseV4::decode_body(&mut decoder)?)
    }

    pub(crate) async fn fetch_one_v11(
        &mut self,
        request: FetchOneRequestV11,
    ) -> Result<FetchResponseV11> {
        let request = FetchRequestV11 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            replica_id: request.replica_id,
            max_wait_ms: request.max_wait_ms,
            min_bytes: request.min_bytes,
            max_bytes: request.max_bytes,
            isolation_level: request.isolation_level,
            session_id: request.session_id,
            session_epoch: request.session_epoch,
            topics: vec![FetchTopicV11 {
                name: request.topic,
                partitions: vec![FetchPartitionV11 {
                    partition_index: request.partition_index,
                    current_leader_epoch: request.current_leader_epoch,
                    fetch_offset: request.fetch_offset,
                    log_start_offset: -1,
                    max_bytes: request.max_partition_bytes,
                }],
            }],
            forgotten_topics: Vec::new(),
            rack_id: request.rack_id,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(FetchResponseV11::decode_body(&mut decoder)?)
    }

    pub(crate) async fn fetch_one_v12(
        &mut self,
        request: FetchOneRequestV12,
    ) -> Result<FetchResponseV12> {
        let request = FetchRequestV12 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            replica_id: request.replica_id,
            max_wait_ms: request.max_wait_ms,
            min_bytes: request.min_bytes,
            max_bytes: request.max_bytes,
            isolation_level: request.isolation_level,
            session_id: request.session_id,
            session_epoch: request.session_epoch,
            topics: vec![FetchTopicV12 {
                name: request.topic,
                partitions: vec![FetchPartitionV12 {
                    partition_index: request.partition_index,
                    current_leader_epoch: request.current_leader_epoch,
                    fetch_offset: request.fetch_offset,
                    last_fetched_epoch: request.last_fetched_epoch,
                    log_start_offset: -1,
                    max_bytes: request.max_partition_bytes,
                }],
            }],
            forgotten_topics: Vec::new(),
            rack_id: request.rack_id,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FetchResponseV12::decode_body(&mut decoder)?)
    }

    /// Sends Fetch v13 with topic UUIDs for Kafka 4.x.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_v13(
        &mut self,
        cluster_id: Option<String>,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopicV13>,
        forgotten_topics: Vec<FetchForgottenTopicV13>,
        rack_id: impl Into<String>,
    ) -> Result<FetchResponseV13> {
        let request = FetchRequestV13 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            replica_id: -1,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id: rack_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FetchResponseV13::decode_body(&mut decoder)?)
    }

    /// Sends Fetch v14 with topic UUIDs and tiered-storage error semantics.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_v14(
        &mut self,
        cluster_id: Option<String>,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopicV14>,
        forgotten_topics: Vec<FetchForgottenTopicV14>,
        rack_id: impl Into<String>,
    ) -> Result<FetchResponseV14> {
        let request = FetchRequestV14 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            replica_id: -1,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id: rack_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FetchResponseV14::decode_body(&mut decoder)?)
    }

    /// Sends Fetch v15 with the Kafka 4.x replica-state tagged struct.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_v15(
        &mut self,
        cluster_id: Option<String>,
        replica_state: Option<FetchReplicaStateV15>,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopicV15>,
        forgotten_topics: Vec<FetchForgottenTopicV15>,
        rack_id: impl Into<String>,
    ) -> Result<FetchResponseV15> {
        let request = FetchRequestV15 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id: rack_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FetchResponseV15::decode_body(&mut decoder)?)
    }

    /// Sends Fetch v16, whose request shape is identical to v15.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_v16(
        &mut self,
        cluster_id: Option<String>,
        replica_state: Option<FetchReplicaStateV15>,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopicV16>,
        forgotten_topics: Vec<FetchForgottenTopicV16>,
        rack_id: impl Into<String>,
    ) -> Result<FetchResponseV16> {
        let request = FetchRequestV16 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id: rack_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FetchResponseV16::decode_body(&mut decoder)?)
    }

    /// Sends Fetch v17 with follower directory-ID tagged fields.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_v17(
        &mut self,
        cluster_id: Option<String>,
        replica_state: Option<FetchReplicaStateV15>,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopicV17>,
        forgotten_topics: Vec<FetchForgottenTopicV17>,
        rack_id: impl Into<String>,
    ) -> Result<FetchResponseV17> {
        let request = FetchRequestV17 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id: rack_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FetchResponseV17::decode_body(&mut decoder)?)
    }

    /// Sends Fetch v18 with directory-ID and follower high-watermark fields.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_v18(
        &mut self,
        cluster_id: Option<String>,
        replica_state: Option<FetchReplicaStateV15>,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
        session_id: i32,
        session_epoch: i32,
        topics: Vec<FetchTopicV18>,
        forgotten_topics: Vec<FetchForgottenTopicV18>,
        rack_id: impl Into<String>,
    ) -> Result<FetchResponseV18> {
        let request = FetchRequestV18 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            cluster_id,
            replica_state,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id: rack_id.into(),
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(FetchResponseV18::decode_body(&mut decoder)?)
    }

    /// Sends Produce v2 for pre-built topic partition payloads.
    pub async fn produce_v2(
        &mut self,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV2>,
    ) -> Result<ProduceResponseV2> {
        let request = ProduceRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            acks,
            timeout_ms,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ProduceResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends Produce v2 without waiting for a broker response.
    pub async fn produce_v2_no_response(
        &mut self,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV2>,
    ) -> Result<()> {
        let request = ProduceRequestV2 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            acks,
            timeout_ms,
            topics,
        };
        self.send_request_no_response(&request.encode()?).await
    }

    /// Sends Produce v2 for one topic partition.
    pub async fn produce_one_v2(
        &mut self,
        acks: i16,
        timeout_ms: i32,
        topic: String,
        partition_index: i32,
        records: Vec<MessageSetMessage>,
    ) -> Result<ProduceResponseV2> {
        self.produce_v2(
            acks,
            timeout_ms,
            vec![ProduceTopicV2 {
                name: topic,
                partitions: vec![ProducePartitionV2 {
                    partition_index,
                    records,
                }],
            }],
        )
        .await
    }

    /// Sends Produce v3 for pre-built record batch topic partition payloads.
    pub async fn produce_v3(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<ProduceResponseV2> {
        let request = ProduceRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            acks,
            timeout_ms,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ProduceResponseV2::decode_body(&mut decoder)?)
    }

    /// Sends Produce v3 without waiting for a broker response.
    pub async fn produce_v3_no_response(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<()> {
        let request = ProduceRequestV3 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            acks,
            timeout_ms,
            topics,
        };
        self.send_request_no_response(&request.encode()?).await
    }

    /// Sends Produce v3 for one topic partition using RecordBatch records.
    pub async fn produce_one_v3(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topic: String,
        partition_index: i32,
        records: Vec<RecordBatchMessage>,
    ) -> Result<ProduceResponseV2> {
        self.produce_v3(
            transactional_id,
            acks,
            timeout_ms,
            vec![ProduceTopicV3 {
                name: topic,
                partitions: vec![ProducePartitionV3 {
                    partition_index,
                    compression: kafrust_protocol::record_batch::RecordBatchCompression::None,
                    identity: RecordBatchIdentity::NON_IDEMPOTENT,
                    records,
                }],
            }],
        )
        .await
    }

    /// Sends Produce v7 for pre-built record batch topic partition payloads.
    pub async fn produce_v7(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<ProduceResponseV7> {
        let request = ProduceRequestV7 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            acks,
            timeout_ms,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(ProduceResponseV7::decode_body(&mut decoder)?)
    }

    /// Sends Produce v7 without waiting for a broker response.
    pub async fn produce_v7_no_response(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<()> {
        let request = ProduceRequestV7 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            acks,
            timeout_ms,
            topics,
        };
        self.send_request_no_response(&request.encode()?).await
    }

    /// Sends flexible Produce v9 for pre-built record batch topic partitions.
    pub async fn produce_v9(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<ProduceResponseV9> {
        self.produce_flexible(9, transactional_id, acks, timeout_ms, topics)
            .await
    }

    /// Sends flexible Produce v9 without waiting for a broker response.
    pub async fn produce_v9_no_response(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<()> {
        self.produce_flexible_no_response(9, transactional_id, acks, timeout_ms, topics)
            .await
    }

    /// Sends flexible Produce v11 for pre-built record batch topic partitions.
    pub async fn produce_v11(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<ProduceResponseV11> {
        self.produce_flexible(11, transactional_id, acks, timeout_ms, topics)
            .await
    }

    /// Sends flexible Produce v11 without waiting for a broker response.
    pub async fn produce_v11_no_response(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<()> {
        self.produce_flexible_no_response(11, transactional_id, acks, timeout_ms, topics)
            .await
    }

    /// Sends flexible Produce v12 for pre-built record batch topic partitions.
    pub async fn produce_v12(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<ProduceResponseV12> {
        self.produce_flexible(12, transactional_id, acks, timeout_ms, topics)
            .await
    }

    /// Sends flexible Produce v12 without waiting for a broker response.
    pub async fn produce_v12_no_response(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<()> {
        self.produce_flexible_no_response(12, transactional_id, acks, timeout_ms, topics)
            .await
    }

    /// Sends topic-ID based flexible Produce v13 for pre-built record batches.
    pub async fn produce_v13(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV13>,
    ) -> Result<ProduceResponseV13> {
        self.produce_topic_id_flexible(transactional_id, acks, timeout_ms, topics)
            .await
    }

    /// Sends topic-ID based flexible Produce v13 without waiting for a response.
    pub async fn produce_v13_no_response(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV13>,
    ) -> Result<()> {
        self.produce_topic_id_flexible_no_response(transactional_id, acks, timeout_ms, topics)
            .await
    }

    pub(crate) async fn produce_flexible(
        &mut self,
        api_version: i16,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<ProduceResponseV9> {
        let request = if api_version >= 12 {
            ProduceRequestV12 {
                correlation_id: self.next_correlation_id(),
                client_id: self.client_id.clone(),
                transactional_id,
                acks,
                timeout_ms,
                topics,
            }
            .encode()?
        } else if api_version >= 11 {
            ProduceRequestV11 {
                correlation_id: self.next_correlation_id(),
                client_id: self.client_id.clone(),
                transactional_id,
                acks,
                timeout_ms,
                topics,
            }
            .encode()?
        } else {
            ProduceRequestV9 {
                correlation_id: self.next_correlation_id(),
                client_id: self.client_id.clone(),
                transactional_id,
                acks,
                timeout_ms,
                topics,
            }
            .encode()?
        };
        let response = self.send_request(&request).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ProduceResponseV9::decode_body(&mut decoder)?)
    }

    pub(crate) async fn produce_flexible_no_response(
        &mut self,
        api_version: i16,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV3>,
    ) -> Result<()> {
        let request = if api_version >= 12 {
            ProduceRequestV12 {
                correlation_id: self.next_correlation_id(),
                client_id: self.client_id.clone(),
                transactional_id,
                acks,
                timeout_ms,
                topics,
            }
            .encode()?
        } else if api_version >= 11 {
            ProduceRequestV11 {
                correlation_id: self.next_correlation_id(),
                client_id: self.client_id.clone(),
                transactional_id,
                acks,
                timeout_ms,
                topics,
            }
            .encode()?
        } else {
            ProduceRequestV9 {
                correlation_id: self.next_correlation_id(),
                client_id: self.client_id.clone(),
                transactional_id,
                acks,
                timeout_ms,
                topics,
            }
            .encode()?
        };
        self.send_request_no_response(&request).await
    }

    async fn produce_topic_id_flexible(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV13>,
    ) -> Result<ProduceResponseV13> {
        let request = ProduceRequestV13 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            acks,
            timeout_ms,
            topics,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v1(&mut decoder)?;
        Ok(ProduceResponseV13::decode_body(&mut decoder)?)
    }

    async fn produce_topic_id_flexible_no_response(
        &mut self,
        transactional_id: Option<String>,
        acks: i16,
        timeout_ms: i32,
        topics: Vec<ProduceTopicV13>,
    ) -> Result<()> {
        let request = ProduceRequestV13 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            transactional_id,
            acks,
            timeout_ms,
            topics,
        };
        self.send_request_no_response(&request.encode()?).await
    }

    async fn send_request_no_response(&mut self, request: &[u8]) -> Result<()> {
        self.last_request_state = RequestState::Idle;
        self.ensure_connection_usable()?;
        if let Err(error) = self.maybe_reauthenticate().await {
            self.last_request_state = RequestState::Idle;
            return Err(error);
        }
        self.last_request_state = RequestState::Idle;
        self.send_request_no_response_traced(request).await
    }

    async fn send_request_no_response_traced(&mut self, request: &[u8]) -> Result<()> {
        let trace = RequestTrace::from_request(request);
        let span = RequestTrace::span(trace);
        let mut span_guard = RequestSpanGuard::new(span.clone());
        let metrics = self.metrics.start_request(request.len());

        async {
            RequestTrace::log_start(trace);
            let result = if let Some(timeout) = self.request_timeout {
                match tokio::time::timeout(
                    timeout,
                    self.send_request_no_response_unbounded(request),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(Error::RequestTimedOut {
                        timeout_ms: duration_millis(timeout),
                    }),
                }
            } else {
                self.send_request_no_response_unbounded(request).await
            };

            self.poison_connection_for_request_error(&result);

            RequestTrace::log_finish_no_response(trace, &result);
            span_guard.finish_no_response(&result);
            match &result {
                Ok(()) => metrics.succeed(0),
                Err(error) => metrics.fail(matches!(error, Error::RequestTimedOut { .. })),
            }
            result
        }
        .instrument(span)
        .await
    }

    async fn send_request(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        self.last_request_state = RequestState::Idle;
        self.ensure_connection_usable()?;
        if let Err(error) = self.maybe_reauthenticate().await {
            self.last_request_state = RequestState::Idle;
            return Err(error);
        }
        self.last_request_state = RequestState::Idle;
        self.send_request_traced(request).await
    }

    async fn send_request_traced(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        let trace = RequestTrace::from_request(request);
        let span = RequestTrace::span(trace);
        let mut span_guard = RequestSpanGuard::new(span.clone());
        let metrics = self.metrics.start_request(request.len());

        async {
            RequestTrace::log_start(trace);
            let result = if let Some(timeout) = self.request_timeout {
                match tokio::time::timeout(timeout, self.send_request_unbounded(request)).await {
                    Ok(result) => result,
                    Err(_) => Err(Error::RequestTimedOut {
                        timeout_ms: duration_millis(timeout),
                    }),
                }
            } else {
                self.send_request_unbounded(request).await
            };

            self.poison_connection_for_request_error(&result);

            RequestTrace::log_finish(trace, &result);
            span_guard.finish(&result);
            match &result {
                Ok(response) => metrics.succeed(response.len()),
                Err(error) => metrics.fail(matches!(error, Error::RequestTimedOut { .. })),
            }
            result
        }
        .instrument(span)
        .await
    }

    async fn send_request_no_response_unbounded(&mut self, request: &[u8]) -> Result<()> {
        self.request_in_flight = true;
        let result = self.send_request_no_response_unbounded_inner(request).await;
        self.request_in_flight = false;
        result
    }

    async fn send_request_no_response_unbounded_inner(&mut self, request: &[u8]) -> Result<()> {
        let frame = encode_frame(request)?;
        self.last_request_state = RequestState::Sent;
        if let Err(error) = self.stream.write_all(&frame).await {
            self.connection_poisoned = true;
            return Err(error.into());
        }
        RequestTrace::log_written(RequestTrace::from_request(request));
        if let Err(error) = self.stream.flush().await {
            self.connection_poisoned = true;
            return Err(error.into());
        }
        RequestTrace::log_sent(RequestTrace::from_request(request));
        Ok(())
    }

    async fn send_request_unbounded(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        self.request_in_flight = true;
        let result = self.send_request_unbounded_inner(request).await;
        self.request_in_flight = false;
        result
    }

    async fn send_request_unbounded_inner(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        let frame = encode_frame(request)?;
        self.last_request_state = RequestState::Sent;
        if let Err(error) = self.stream.write_all(&frame).await {
            self.connection_poisoned = true;
            return Err(error.into());
        }
        RequestTrace::log_written(RequestTrace::from_request(request));
        if let Err(error) = self.stream.flush().await {
            self.connection_poisoned = true;
            return Err(error.into());
        }
        RequestTrace::log_sent(RequestTrace::from_request(request));

        let mut size = [0u8; 4];
        if let Err(error) = self.stream.read_exact(&mut size).await {
            self.connection_poisoned = true;
            return Err(error.into());
        }
        let size = i32::from_be_bytes(size);
        if size < 0 {
            self.connection_poisoned = true;
            return Err(Error::Protocol(kafrust_protocol::Error::NegativeLength {
                kind: "response frame",
                length: size,
            }));
        }

        let size = match usize::try_from(size) {
            Ok(size) => size,
            Err(_) => {
                self.connection_poisoned = true;
                return Err(Error::Protocol(kafrust_protocol::Error::LengthOverflow(
                    "response frame",
                )));
            }
        };
        if size > self.max_response_bytes {
            self.connection_poisoned = true;
            return Err(Error::ResponseTooLarge {
                size,
                max: self.max_response_bytes,
            });
        }

        let mut response = vec![0; size];
        if let Err(error) = self.stream.read_exact(&mut response).await {
            self.connection_poisoned = true;
            return Err(error.into());
        }
        self.last_request_state = RequestState::ResponseReceived;
        Ok(response)
    }

    fn ensure_connection_usable(&self) -> Result<()> {
        if self.connection_poisoned || self.request_in_flight {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "broker connection is unusable after a prior request failure",
            )));
        }
        Ok(())
    }

    pub(crate) fn poison_connection(&mut self) {
        self.connection_poisoned = true;
    }

    #[cfg(test)]
    pub(crate) fn is_connection_poisoned(&self) -> bool {
        self.connection_poisoned || self.request_in_flight
    }

    fn poison_connection_for_request_error<T>(&mut self, result: &Result<T>) {
        if matches!(
            result,
            Err(Error::Io(_))
                | Err(Error::RequestTimedOut { .. })
                | Err(Error::ResponseTooLarge { .. })
        ) {
            self.connection_poisoned = true;
        }
    }

    fn next_correlation_id(&mut self) -> i32 {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1).max(1);
        correlation_id
    }
}

fn format_topic_id(topic_id: [u8; 16]) -> String {
    // Kafka's Uuid::toString uses URL-safe base64 without padding, not the
    // conventional RFC-4122 hyphenated representation.
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(topic_id)
}

pub(crate) fn format_share_partition_coordinator_key(
    group_id: &str,
    topic_id: [u8; 16],
    partition: i32,
) -> String {
    format!("{group_id}:{}:{partition}", format_topic_id(topic_id))
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("client_id", &self.client_id)
            .field("next_correlation_id", &self.next_correlation_id)
            .field("request_timeout", &self.request_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("decode_limits", &self.decode_limits)
            .field("connection_poisoned", &self.connection_poisoned)
            .field("request_in_flight", &self.request_in_flight)
            .field("last_request_state", &self.last_request_state)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestTrace {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    request_bytes: usize,
}

impl RequestTrace {
    fn from_request(request: &[u8]) -> Option<Self> {
        if request.len() < 8 {
            return None;
        }

        Some(Self {
            api_key: i16::from_be_bytes([request[0], request[1]]),
            api_version: i16::from_be_bytes([request[2], request[3]]),
            correlation_id: i32::from_be_bytes([request[4], request[5], request[6], request[7]]),
            request_bytes: request.len(),
        })
    }

    fn span(trace: Option<Self>) -> Span {
        match trace {
            Some(trace) => debug_span!(
                "kafka.request",
                api_key = trace.api_key,
                api_version = trace.api_version,
                correlation_id = trace.correlation_id,
                request_bytes = trace.request_bytes,
                outcome = tracing::field::Empty,
                response_bytes = tracing::field::Empty,
                elapsed_ms = tracing::field::Empty,
            ),
            None => Span::none(),
        }
    }

    fn log_start(trace: Option<Self>) {
        if let Some(trace) = trace {
            debug!(
                api_key = trace.api_key,
                api_version = trace.api_version,
                correlation_id = trace.correlation_id,
                request_bytes = trace.request_bytes,
                "sending kafka request"
            );
        }
    }

    fn log_sent(trace: Option<Self>) {
        if let Some(trace) = trace {
            debug!(
                api_key = trace.api_key,
                api_version = trace.api_version,
                correlation_id = trace.correlation_id,
                request_bytes = trace.request_bytes,
                "kafka request sent"
            );
        }
    }

    fn log_written(trace: Option<Self>) {
        if let Some(trace) = trace {
            debug!(
                api_key = trace.api_key,
                api_version = trace.api_version,
                correlation_id = trace.correlation_id,
                request_bytes = trace.request_bytes,
                "kafka request written"
            );
        }
    }

    fn log_finish(trace: Option<Self>, result: &Result<Vec<u8>>) {
        match (trace, result) {
            (Some(trace), Ok(response)) => {
                debug!(
                    api_key = trace.api_key,
                    api_version = trace.api_version,
                    correlation_id = trace.correlation_id,
                    response_bytes = response.len(),
                    "received kafka response"
                );
            }
            (Some(trace), Err(error)) => {
                debug!(
                    api_key = trace.api_key,
                    api_version = trace.api_version,
                    correlation_id = trace.correlation_id,
                    error = %error,
                    "kafka request failed"
                );
            }
            (None, _) => {}
        }
    }

    fn log_finish_no_response(trace: Option<Self>, result: &Result<()>) {
        match (trace, result) {
            (Some(trace), Ok(())) => {
                debug!(
                    api_key = trace.api_key,
                    api_version = trace.api_version,
                    correlation_id = trace.correlation_id,
                    "kafka request sent without response"
                );
            }
            (Some(trace), Err(error)) => {
                debug!(
                    api_key = trace.api_key,
                    api_version = trace.api_version,
                    correlation_id = trace.correlation_id,
                    error = %error,
                    "kafka request failed"
                );
            }
            (None, _) => {}
        }
    }

    fn record_finish(span: &Span, started_at: Instant, result: &Result<Vec<u8>>) {
        span.record("elapsed_ms", duration_millis(started_at.elapsed()));
        match result {
            Ok(response) => {
                span.record("outcome", "success");
                span.record("response_bytes", response.len());
            }
            Err(_) => {
                span.record("outcome", "error");
            }
        }
    }

    fn record_finish_no_response(span: &Span, started_at: Instant, result: &Result<()>) {
        span.record("elapsed_ms", duration_millis(started_at.elapsed()));
        match result {
            Ok(()) => {
                span.record("outcome", "sent");
                span.record("response_bytes", 0usize);
            }
            Err(_) => {
                span.record("outcome", "error");
            }
        }
    }
}

struct RequestSpanGuard {
    span: Span,
    started_at: Instant,
    completed: bool,
}

impl RequestSpanGuard {
    fn new(span: Span) -> Self {
        Self {
            span,
            started_at: Instant::now(),
            completed: false,
        }
    }

    fn finish(&mut self, result: &Result<Vec<u8>>) {
        RequestTrace::record_finish(&self.span, self.started_at, result);
        self.completed = true;
    }

    fn finish_no_response(&mut self, result: &Result<()>) {
        RequestTrace::record_finish_no_response(&self.span, self.started_at, result);
        self.completed = true;
    }
}

impl Drop for RequestSpanGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.span
            .record("elapsed_ms", duration_millis(self.started_at.elapsed()));
        self.span.record("outcome", "cancelled");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        format_topic_id, AddPartitionsToTxnTopic, Client, RaftVoterListener, RequestTrace,
        TxnOffsetCommitTopic, DEFAULT_MAX_RESPONSE_BYTES,
    };
    use crate::config::SaslCredentials;
    use crate::{ClientMetrics, Error};
    use kafrust_protocol::api::fetch::{FetchPartitionV12, FetchTopicV13};
    use kafrust_protocol::api::offset_commit::{OffsetCommitPartitionV10, OffsetCommitTopicV10};
    use kafrust_protocol::api::offset_fetch::OffsetFetchTopicV10;
    use kafrust_protocol::api::offset_for_leader_epoch::{
        OffsetForLeaderEpochPartitionV3, OffsetForLeaderEpochTopicV3,
    };
    use kafrust_protocol::api::txn_offset_commit::TxnOffsetCommitPartition;
    use kafrust_protocol::api::update_features::{FeatureUpdateV0, FeatureUpdateV1};
    use kafrust_protocol::codec::Encoder;
    use kafrust_protocol::codec::{DecodeLimits, Decoder};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn formats_topic_id_like_kafka_uuid() {
        assert_eq!(format_topic_id([7; 16]), "BwcHBwcHBwcHBwcHBwcHBw");
    }

    #[tokio::test]
    async fn times_out_when_broker_does_not_respond() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut size = [0u8; 4];
            socket.read_exact(&mut size).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let mut client = Client::connect_with_request_timeout_and_metrics(
            addr,
            Some("kafrust-timeout-test".to_owned()),
            Duration::from_millis(5),
            DEFAULT_MAX_RESPONSE_BYTES,
            DecodeLimits::default(),
            ClientMetrics::new(),
        )
        .await
        .unwrap();

        let error = client.api_versions().await.unwrap_err();

        assert!(matches!(error, Error::RequestTimedOut { timeout_ms: 5 }));
        assert!(client.last_request_may_have_been_transmitted());
        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.requests_started, 1);
        assert_eq!(metrics.requests_failed, 1);
        assert_eq!(metrics.requests_timed_out, 1);
        assert_eq!(metrics.in_flight_requests, 0);
        assert!(metrics.request_bytes > 0);
        assert!(metrics.max_latency >= Duration::from_millis(5));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn does_not_reuse_connection_after_request_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut size = [0u8; 4];
            socket.read_exact(&mut size).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let mut client = Client::connect_with_request_timeout_and_metrics(
            addr,
            Some("kafrust-timeout-reuse-test".to_owned()),
            Duration::from_millis(5),
            DEFAULT_MAX_RESPONSE_BYTES,
            DecodeLimits::default(),
            ClientMetrics::new(),
        )
        .await
        .unwrap();

        assert!(matches!(
            client.api_versions().await,
            Err(Error::RequestTimedOut { timeout_ms: 5 })
        ));
        assert!(client.last_request_may_have_been_transmitted());
        let error = tokio::time::timeout(Duration::from_millis(20), client.api_versions())
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error, Error::Io(error) if error.kind() == std::io::ErrorKind::NotConnected)
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn does_not_reuse_connection_after_canceled_request() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let _request = read_test_frame(&mut broker_stream).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-cancel-reuse-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(5), client.api_versions())
                .await
                .is_err()
        );
        assert!(client.is_connection_poisoned());
        let error = client.api_versions().await.unwrap_err();
        assert!(
            matches!(error, Error::Io(error) if error.kind() == std::io::ErrorKind::NotConnected)
        );

        broker.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_response_frame_before_allocating_over_limit() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let _request = read_test_frame(&mut broker_stream).await;
            broker_stream
                .write_all(&32_i32.to_be_bytes())
                .await
                .unwrap();
            broker_stream.flush().await.unwrap();
        });
        let metrics = ClientMetrics::new();
        let mut client = Client::from_stream_with_metrics(
            Box::new(client_stream),
            Some("kafrust-response-limit-test".to_owned()),
            Some(Duration::from_secs(1)),
            8,
            DecodeLimits::default(),
            metrics.clone(),
        );

        let error = client.api_versions().await.unwrap_err();

        assert!(matches!(
            error,
            Error::ResponseTooLarge { size: 32, max: 8 }
        ));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests_failed, 1);
        assert_eq!(snapshot.response_bytes, 0);
        assert_eq!(snapshot.in_flight_requests, 0);
        let reuse_error = client.api_versions().await.unwrap_err();
        assert!(matches!(
            reuse_error,
            Error::Io(error) if error.kind() == std::io::ErrorKind::NotConnected
        ));
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_request_without_waiting_for_response() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..8], &[0, 18, 0, 0, 0, 0, 0, 1]);
        });
        let metrics = ClientMetrics::new();
        let mut client = Client::from_stream_with_metrics(
            Box::new(client_stream),
            Some("kafrust-no-response-test".to_owned()),
            Some(Duration::from_secs(1)),
            DEFAULT_MAX_RESPONSE_BYTES,
            DecodeLimits::default(),
            metrics.clone(),
        );

        client
            .send_request_no_response(&[0, 18, 0, 0, 0, 0, 0, 1])
            .await
            .unwrap();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests_started, 1);
        assert_eq!(snapshot.requests_succeeded, 1);
        assert_eq!(snapshot.response_bytes, 0);
        assert_eq!(snapshot.in_flight_requests, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_topic_uuid_offset_fetch_and_commit_v10() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(2048);
        let broker = tokio::spawn(async move {
            let fetch_request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&fetch_request[0..4], &[0, 9, 0, 10]);

            let mut fetch_response = Encoder::new();
            fetch_response.write_i32(1);
            fetch_response.write_empty_tagged_fields();
            fetch_response.write_i32(0);
            fetch_response
                .write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_compact_string("orders-group")?;
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_uuid(&[9; 16]);
                        encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                            encoder.write_i32(0);
                            encoder.write_i64(42);
                            encoder.write_i32(7);
                            encoder.write_compact_nullable_string(Some("processed"))?;
                            encoder.write_i16(0);
                            encoder.write_empty_tagged_fields();
                            Ok(())
                        })?;
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_i16(0);
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })
                .unwrap();
            fetch_response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &fetch_response.into_bytes()).await;

            let commit_request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&commit_request[0..4], &[0, 8, 0, 10]);

            let mut commit_response = Encoder::new();
            commit_response.write_i32(2);
            commit_response.write_empty_tagged_fields();
            commit_response.write_i32(2);
            commit_response
                .write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_uuid(&[9; 16]);
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
            commit_response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &commit_response.into_bytes()).await;
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-v10-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let fetch = client
            .offset_fetch_v10(
                "orders-group",
                Some("member-a".to_owned()),
                7,
                Some(vec![OffsetFetchTopicV10 {
                    topic_id: [9; 16],
                    partition_indexes: vec![0],
                }]),
                true,
            )
            .await
            .unwrap();
        assert_eq!(fetch.groups[0].topics[0].topic_id, [9; 16]);
        assert_eq!(fetch.groups[0].topics[0].partitions[0].committed_offset, 42);

        let commit = client
            .offset_commit_v10(
                "orders-group",
                7,
                "member-a",
                None,
                vec![OffsetCommitTopicV10 {
                    topic_id: [9; 16],
                    partitions: vec![OffsetCommitPartitionV10 {
                        partition_index: 0,
                        committed_offset: 43,
                        committed_leader_epoch: 7,
                        committed_metadata: None,
                    }],
                }],
            )
            .await
            .unwrap();
        assert_eq!(commit.throttle_time_ms, 2);
        assert_eq!(commit.topics[0].topic_id, [9; 16]);
        assert_eq!(commit.topics[0].partitions[0].error_code, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_topic_uuid_fetch_v13_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(2048);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 1, 0, 13]);
            assert!(request.windows(16).any(|bytes| bytes == [7; 16]));

            let mut response = Encoder::new();
            response.write_i32(1); // response correlation id
            response.write_empty_tagged_fields();
            response.write_i32(0); // throttle time
            response.write_i16(0); // top-level error
            response.write_i32(3); // fetch session id
            response
                .write_compact_array(Some(&[()]), |encoder, ()| {
                    encoder.write_uuid(&[7; 16]);
                    encoder.write_compact_array(Some(&[()]), |encoder, ()| {
                        encoder.write_i32(0);
                        encoder.write_i16(0);
                        encoder.write_i64(43);
                        encoder.write_i64(42);
                        encoder.write_i64(40);
                        encoder.write_compact_array(Some(&[] as &[()]), |_encoder, ()| Ok(()))?;
                        encoder.write_i32(-1);
                        encoder.write_compact_nullable_bytes(Some(&[]))?;
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-fetch-v13-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let response = client
            .fetch_v13(
                Some("cluster-a".to_owned()),
                500,
                1,
                1_048_576,
                1,
                3,
                2,
                vec![FetchTopicV13 {
                    topic_id: [7; 16],
                    partitions: vec![FetchPartitionV12 {
                        partition_index: 0,
                        current_leader_epoch: 4,
                        fetch_offset: 42,
                        last_fetched_epoch: 3,
                        log_start_offset: -1,
                        max_bytes: 1_048_576,
                    }],
                }],
                Vec::new(),
                "rack-a",
            )
            .await
            .unwrap();

        assert_eq!(response.session_id, 3);
        assert_eq!(response.responses[0].topic_id, [7; 16]);
        assert_eq!(response.responses[0].partitions[0].high_watermark, 43);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_response_array_before_allocating_over_limit() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let _request = read_test_frame(&mut broker_stream).await;
            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                0, 0, 0, 2, // api key count
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });
        let mut client = Client::from_stream_with_metrics(
            Box::new(client_stream),
            Some("kafrust-array-limit-test".to_owned()),
            Some(Duration::from_secs(1)),
            DEFAULT_MAX_RESPONSE_BYTES,
            DecodeLimits::new().with_max_array_elements(1),
            ClientMetrics::new(),
        );

        let error = client.api_versions().await.unwrap_err();

        assert!(matches!(
            error,
            Error::Protocol(kafrust_protocol::Error::LimitExceeded {
                kind: "api versions",
                actual: 2,
                max: 1,
            })
        ));
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_request_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let mut request_size = [0u8; 4];
            broker_stream.read_exact(&mut request_size).await.unwrap();
            let request_size = usize::try_from(i32::from_be_bytes(request_size)).unwrap();
            let mut request = vec![0u8; request_size];
            broker_stream.read_exact(&mut request).await.unwrap();

            assert_eq!(&request[0..2], &[0, 18]);
            assert_eq!(&request[2..4], &[0, 0]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                0, 0, 0, 1, // api key count
                0, 18, 0, 0, 0, 4, // ApiVersions min/max
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client.api_versions().await.unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.highest_supported_version(18, 4), Some(4));
        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.requests_started, 1);
        assert_eq!(metrics.requests_succeeded, 1);
        assert_eq!(metrics.requests_failed, 0);
        assert_eq!(metrics.response_bytes, 16);
        assert_eq!(metrics.in_flight_requests, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_offset_for_leader_epoch_v3_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..2], &[0, 23]);
            assert_eq!(&request[2..4], &[0, 3]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);

            let mut body = Encoder::new();
            body.write_i32(0);
            body.write_i32(1);
            body.write_string("orders").unwrap();
            body.write_i32(1);
            body.write_i16(0);
            body.write_i32(2);
            body.write_i32(8);
            body.write_i64(42);
            let mut response = vec![0, 0, 0, 1];
            response.extend(body.into_bytes());
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-offset-epoch-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let response = client
            .offset_for_leader_epoch_v3(vec![OffsetForLeaderEpochTopicV3 {
                name: "orders".to_owned(),
                partitions: vec![OffsetForLeaderEpochPartitionV3 {
                    partition_index: 2,
                    current_leader_epoch: 8,
                    leader_epoch: 7,
                }],
            }])
            .await
            .unwrap();

        assert_eq!(response.topics[0].partitions[0].end_offset, 42);
        assert_eq!(response.topics[0].partitions[0].leader_epoch, 8);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_trailing_data_plane_response_bytes_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..2], &[0, 23]);
            assert_eq!(&request[2..4], &[0, 3]);

            let mut body = Encoder::new();
            body.write_i32(0);
            body.write_i32(0);
            let mut response = vec![0, 0, 0, 1];
            response.extend(body.into_bytes());
            response.push(0xa5);
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-trailing-response-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let error = client
            .offset_for_leader_epoch_v3(Vec::new())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Protocol(kafrust_protocol::Error::TrailingBytes { remaining: 1 })
        ));
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_flexible_api_versions_v3_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let mut request_size = [0u8; 4];
            broker_stream.read_exact(&mut request_size).await.unwrap();
            let request_size = usize::try_from(i32::from_be_bytes(request_size)).unwrap();
            let mut request = vec![0u8; request_size];
            broker_stream.read_exact(&mut request).await.unwrap();

            assert_eq!(
                request,
                [
                    0, 18, // api key
                    0, 3, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    8, b'k', b'a', b'f', b'r', b'u', b's', b't', // software name
                    6, b'0', b'.', b'3', b'.', b'0', // software version
                    0,    // request body tagged fields
                ]
            );

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                2, // compact api key count: one entry
                0, 18, 0, 0, 0, 4, 0, // ApiVersions min/max + entry tags
                0, 0, 0, 0, // throttle time
                0, // response tagged fields
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));

        let response = client.api_versions_v3("kafrust", "0.3.0").await.unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.highest_supported_version(18, 4), Some(4));
        assert!(response.tagged_fields.is_empty());
        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.requests_started, 1);
        assert_eq!(metrics.requests_succeeded, 1);
        assert_eq!(metrics.response_bytes, 19);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_flexible_api_versions_v4_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 18, // api key
                    0, 4, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    8, b'k', b'a', b'f', b'r', b'u', b's', b't', // software name
                    6, b'0', b'.', b'3', b'.', b'0', // software version
                    0,    // request body tagged fields
                ]
            );

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                2, // compact api key count: one entry
                0, 18, 0, 0, 0, 4, 0, // ApiVersions min/max + entry tags
                0, 0, 0, 0, // throttle time
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client.api_versions_v4("kafrust", "0.3.0").await.unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.highest_supported_version(18, 4), Some(4));
        assert!(response.tagged_fields.is_empty());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_flexible_api_versions_v5_with_cluster_identity() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 18, // api key
                    0, 5, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    8, b'k', b'a', b'f', b'r', b'u', b's', b't', // software name
                    6, b'0', b'.', b'3', b'.', b'0', // software version
                    10, b'c', b'l', b'u', b's', b't', b'e', b'r', b'-', b'1', 0, 0, 0,
                    3, // node id
                    0, // request body tagged fields
                ]
            );

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                1, // empty compact api key array
                0, 0, 0, 0, // throttle time
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client
            .api_versions_v5("kafrust", "0.3.0", Some("cluster-1".to_owned()), 3)
            .await
            .unwrap();

        assert_eq!(response.error_code, 0);
        assert!(response.api_keys.is_empty());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn prefers_api_versions_v4_and_falls_back_to_v3() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let v4_request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&v4_request[2..4], &[0, 4]);
            let unsupported = [
                0, 0, 0, 1, // correlation id
                0, 35, // UNSUPPORTED_VERSION
                1,  // empty compact api key array
                0, 0, 0, 0, // throttle time
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &unsupported).await;

            let v3_request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&v3_request[2..4], &[0, 3]);
            let supported = [
                0, 0, 0, 2, // correlation id
                0, 0, // error code
                2, // compact api key count: one entry
                0, 18, 0, 0, 0, 4, 0, // ApiVersions min/max + entry tags
                0, 0, 0, 0, // throttle time
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &supported).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client
            .api_versions_cached("kafrust", "0.3.0")
            .await
            .unwrap();
        let cached = client.cached_api_versions_v3().unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.highest_supported_version(18, 4), Some(4));
        assert_eq!(cached, &response);
        assert_eq!(client.metrics().snapshot().requests_started, 2);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_list_config_resources_v1_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 74, // API key
                    0, 1, // API version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    3, 2, 32, // compact resource type array
                    0,  // request tagged fields
                ]
            );

            let response = [
                0, 0, 0, 1, // correlation id
                0, // response header tagged fields
                0, 0, 0, 0, // throttle time
                0, 0, // top-level error code
                3, // compact resource count
                7, b'o', b'r', b'd', b'e', b'r', b's', 2, 0, // orders topic
                9, b'p', b'a', b'y', b'm', b'e', b'n', b't', b's', 32, 0, // payments group
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client.list_config_resources_v1(vec![2, 32]).await.unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.resources.len(), 2);
        assert_eq!(response.resources[0].resource_name, "orders");
        assert_eq!(response.resources[0].resource_type, 2);
        assert_eq!(response.resources[1].resource_type, 32);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_list_client_metrics_resources_v0_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 74, // API key
                    0, 0, // API version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    0,    // empty v0 request body tagged fields
                ]
            );

            let response = [
                0, 0, 0, 1, // correlation id
                0, // response header tagged fields
                0, 0, 0, 4, // throttle time
                0, 0, // error code
                2, // compact resource count
                8, b'l', b'a', b't', b'e', b'n', b'c', b'y', // resource name
                0,    // resource tagged fields
                0,    // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client.list_config_resources_v0().await.unwrap();

        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.resources.len(), 1);
        assert_eq!(response.resources[0].name, "latency");
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_describe_cluster_v1_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 60, // API key
                    0, 1, // API version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    1,    // include cluster authorized operations
                    1,    // broker endpoint
                    0,    // request tagged fields
                ]
            );
            let response = [
                0, 0, 0, 1, // correlation id
                0, // response header tagged fields
                0, 0, 0, 0, // throttle time
                0, 0, // top-level error code
                1, // compact nullable error message: null
                1, // broker endpoint
                8, b'c', b'l', b'u', b's', b't', b'e', b'r', // cluster id
                0, 0, 0, 1, // controller id
                2, // compact broker count
                0, 0, 0, 1, // broker id
                8, b'b', b'r', b'o', b'k', b'e', b'r', b'1', // host
                0, 0, 35, 132, // port
                1,   // nullable rack: null
                0,   // broker tagged fields
                0, 0, 0, 7, // cluster authorized operations
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client.describe_cluster_v1(true, 1).await.unwrap();

        assert_eq!(response.cluster_id, "cluster");
        assert_eq!(response.controller_id, 1);
        assert_eq!(response.brokers.len(), 1);
        assert_eq!(response.brokers[0].port, 9092);
        assert_eq!(response.cluster_authorized_operations, 7);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_update_features_v0_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 57, // api key
                    0, 0, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    0, 0, 234, 96, // timeout ms
                    2,  // compact update count
                    17, // compact feature string length
                    b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', b'.', b'v', b'e', b'r', b's',
                    b'i', b'o', b'n', 0, 21, // max version level
                    0,  // allow downgrade
                    0,  // update tagged fields
                    0,  // request tagged fields
                ]
            );

            let response = [
                0, 0, 0, 1, // response correlation id
                0, // response header tagged fields
                0, 0, 0, 12, // throttle time
                0, 0, // top-level error code
                3, b'o', b'k', // compact nullable error message
                2,    // compact result count
                17,   // compact feature string length
                b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', b'.', b'v', b'e', b'r', b's', b'i',
                b'o', b'n', 0, 0, // per-feature error code
                3, b'o', b'k', // compact nullable error message
                0,    // result tagged fields
                0,    // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client
            .update_features_v0(
                60_000,
                vec![FeatureUpdateV0 {
                    feature: "metadata.version".to_owned(),
                    max_version_level: 21,
                    allow_downgrade: false,
                }],
            )
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message.as_deref(), Some("ok"));
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].feature, "metadata.version");
        assert_eq!(response.results[0].error_code, 0);
        assert_eq!(response.results[0].error_message.as_deref(), Some("ok"));
        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.requests_started, 1);
        assert_eq!(metrics.requests_succeeded, 1);
        assert_eq!(metrics.requests_failed, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_update_features_v1_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 57, // api key
                    0, 1, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    0, 0, 234, 96, // timeout ms
                    2,  // compact update count
                    17, // compact feature string length
                    b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', b'.', b'v', b'e', b'r', b's',
                    b'i', b'o', b'n', 0, 21, // max version level
                    2,  // safe downgrade
                    0,  // update tagged fields
                    1,  // validate only
                    0,  // request tagged fields
                ]
            );

            let response = [
                0, 0, 0, 1, // response correlation id
                0, // response header tagged fields
                0, 0, 0, 12, // throttle time
                0, 0, // top-level error code
                3, b'o', b'k', // compact nullable error message
                2,    // compact result count
                17,   // compact feature string length
                b'm', b'e', b't', b'a', b'd', b'a', b't', b'a', b'.', b'v', b'e', b'r', b's', b'i',
                b'o', b'n', 0, 0, // per-feature error code
                3, b'o', b'k', // compact nullable error message
                0,    // result tagged fields
                0,    // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client
            .update_features_v1(
                60_000,
                vec![FeatureUpdateV1 {
                    feature: "metadata.version".to_owned(),
                    max_version_level: 21,
                    upgrade_type: 2,
                }],
                true,
            )
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message.as_deref(), Some("ok"));
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].feature, "metadata.version");
        assert_eq!(response.results[0].error_code, 0);
        assert_eq!(response.results[0].error_message.as_deref(), Some("ok"));
        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.requests_started, 1);
        assert_eq!(metrics.requests_succeeded, 1);
        assert_eq!(metrics.requests_failed, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_add_raft_voter_v1_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 80, // API key
                    0, 1, // API version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    8, b'c', b'l', b'u', b's', b't', b'e', b'r', // cluster id
                    0, 0, 234, 96, // timeout ms
                    0, 0, 0, 4, // voter id
                    9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, // directory id
                    2, // compact listener count
                    11, b'C', b'O', b'N', b'T', b'R', b'O', b'L', b'L', b'E', b'R', 11, b'c', b'o',
                    b'n', b't', b'r', b'o', b'l', b'l', b'e', b'r', 35, 133, // port 9093
                    0,   // listener tagged fields
                    1,   // ack when committed
                    0,   // request tagged fields
                ]
            );
            let response = [
                0, 0, 0, 1, // correlation id
                0, // response header tagged fields
                0, 0, 0, 12, // throttle time
                0, 0, // error code
                0, // null compact error message
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client
            .add_raft_voter_v1(
                Some("cluster".to_owned()),
                60_000,
                4,
                [9; 16],
                vec![RaftVoterListener {
                    name: "CONTROLLER".to_owned(),
                    host: "controller".to_owned(),
                    port: 9093,
                }],
                true,
            )
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message, None);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_remove_raft_voter_v0_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 81, // API key
                    0, 0, // API version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    8, b'c', b'l', b'u', b's', b't', b'e', b'r', // cluster id
                    0, 0, 0, 2, // voter id
                    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // directory id
                    0, // request tagged fields
                ]
            );
            let response = [
                0, 0, 0, 1, // correlation id
                0, // response header tagged fields
                0, 0, 0, 0, // throttle time
                0, 0, // error code
                0, // null compact error message
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client
            .remove_raft_voter_v0(Some("cluster".to_owned()), 2, [3; 16])
            .await
            .unwrap();

        assert_eq!(response.error_code, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_unregister_broker_v0_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 64, // API key
                    0, 0, // API version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // nullable client id
                    0,    // request header tagged fields
                    0, 0, 0, 4, // broker id
                    0, // request tagged fields
                ]
            );
            let response = [
                0, 0, 0, 1, // correlation id
                0, // response header tagged fields
                0, 0, 0, 12, // throttle time
                0, 0, // error code
                0, // null compact error message
                0, // response tagged fields
            ];
            write_test_frame(&mut broker_stream, &response).await;
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client.unregister_broker_v0(4).await.unwrap();

        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message, None);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn records_sasl_v1_session_lifetime() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(
                request,
                [
                    0, 36, // api key
                    0, 1, // api version
                    0, 0, 0, 1, // correlation id
                    0xff, 0xff, // null client id
                    0, 0, 0, 2, // auth bytes length
                    1, 2,
                ]
            );

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                0xff, 0xff, // null error message
                0, 0, 0, 0, // auth bytes
                0, 0, 0, 0, 0, 0, 0, 123, // session lifetime ms
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        let response = client.sasl_authenticate_v1(vec![1, 2]).await.unwrap();

        assert_eq!(response.session_lifetime_ms, 123);
        assert_eq!(client.sasl_session_lifetime_ms(), Some(123));
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn reauthenticates_oauthbearer_provider_before_session_expiry() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 17, 0, 1]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);
            assert_eq!(&request[8..10], &[0xff, 0xff]);
            assert_eq!(
                &request[10..23],
                &[0, 11, b'O', b'A', b'U', b'T', b'H', b'B', b'E', b'A', b'R', b'E', b'R']
            );

            let handshake_response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                0, 0, 0, 1, // one enabled mechanism
                0, 11, b'O', b'A', b'U', b'T', b'H', b'B', b'E', b'A', b'R', b'E', b'R',
            ];
            broker_stream
                .write_all(&(handshake_response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&handshake_response).await.unwrap();
            broker_stream.flush().await.unwrap();

            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 36, 0, 1]);
            assert_eq!(&request[4..8], &[0, 0, 0, 2]);
            assert_eq!(&request[8..10], &[0xff, 0xff]);
            assert_eq!(&request[10..14], &[0, 0, 0, 29]);
            assert_eq!(&request[14..43], b"n,,\x01auth=Bearer fresh-token\x01\x01");

            let response = [
                0, 0, 0, 2, // correlation id
                0, 0, // error code
                0xff, 0xff, // null error message
                0, 0, 0, 0, // empty auth bytes
                0, 0, 0, 0, 0, 0, 3, 0xe8, // session lifetime ms: 1000
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        client.sasl_credentials = Some(SaslCredentials::oauthbearer_with_provider(|| async {
            Ok("fresh-token".to_owned())
        }));
        client.sasl_session_lifetime_ms = Some(1);
        client.sasl_authenticated_at = Some(std::time::Instant::now() - Duration::from_secs(1));

        client.maybe_reauthenticate().await.unwrap();

        assert_eq!(client.sasl_session_lifetime_ms(), Some(1000));
        assert!(!client.sasl_authentication_in_progress);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn poisons_connection_when_oauthbearer_provider_fails_after_handshake() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 17, 0, 1]);

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                0, 0, 0, 1, // one enabled mechanism
                0, 11, b'O', b'A', b'U', b'T', b'H', b'B', b'E', b'A', b'R', b'E', b'R',
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));
        client.sasl_credentials = Some(SaslCredentials::oauthbearer_with_provider(|| async {
            Err::<String, _>(Error::Unsupported("oauth provider unavailable"))
        }));
        client.sasl_session_lifetime_ms = Some(1);
        client.sasl_authenticated_at = Some(std::time::Instant::now() - Duration::from_secs(1));

        let error = client.maybe_reauthenticate().await.unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("oauth provider unavailable")
        ));
        assert!(!client.sasl_authentication_in_progress);
        assert!(client.connection_poisoned);
        assert!(matches!(
            client.ensure_connection_usable(),
            Err(Error::Io(_))
        ));
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn caches_flexible_api_versions_v3_per_connection() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..2], &[0, 18]);
            assert_eq!(&request[2..4], &[0, 3]);

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, // error code
                2, // compact api key count: one entry
                0, 18, 0, 0, 0, 4, 0, // ApiVersions min/max + entry tags
                0, 0, 0, 0, // throttle time
                0, // response tagged fields
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client =
            Client::from_stream(Box::new(client_stream), None, Some(Duration::from_secs(1)));

        let first = client
            .api_versions_v3_cached("kafrust", "0.3.0")
            .await
            .unwrap();
        let second = client
            .api_versions_v3_cached("kafrust", "0.3.0")
            .await
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(client.cached_api_versions_v3(), Some(&first));
        assert_eq!(client.metrics().snapshot().requests_started, 1);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn initializes_producer_id_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let mut request_size = [0u8; 4];
            broker_stream.read_exact(&mut request_size).await.unwrap();
            let request_size = usize::try_from(i32::from_be_bytes(request_size)).unwrap();
            let mut request = vec![0u8; request_size];
            broker_stream.read_exact(&mut request).await.unwrap();

            assert_eq!(&request[0..2], &[0, 22]);
            assert_eq!(&request[2..4], &[0, 0]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, 0, 0, // throttle time
                0, 0, // error code
                0, 0, 0, 0, 0, 0, 0, 42, // producer id
                0, 3, // producer epoch
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client.init_producer_id_v0(None, 60_000).await.unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.producer_id, 42);
        assert_eq!(response.producer_epoch, 3);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_kip_848_consumer_group_heartbeat_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 68, 0, 0]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);
            assert_eq!(request.last(), Some(&0));

            let mut response = Vec::new();
            response.extend_from_slice(&[0, 0, 0, 1]); // response correlation id
            response.push(0); // response header tagged fields
            response.extend_from_slice(&[0, 0, 0, 0]); // throttle time
            response.extend_from_slice(&[0, 0]); // error code
            response.push(0); // null error message
            response.push(9); // member id length + 1
            response.extend_from_slice(b"member-a");
            response.extend_from_slice(&[0, 0, 0, 2]); // member epoch
            response.extend_from_slice(&2500_i32.to_be_bytes());
            response.push(0xff); // null nullable assignment struct
            response.push(0); // response tagged fields
            write_test_frame(&mut broker_stream, &response).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .consumer_group_heartbeat_v0(
                "orders-group",
                "",
                0,
                None,
                None,
                30_000,
                Some(vec!["orders".to_owned()]),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.member_id.as_deref(), Some("member-a"));
        assert_eq!(response.member_epoch, 2);
        assert_eq!(response.heartbeat_interval_ms, 2500);
        assert!(response.assignment.is_none());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_kip_848_consumer_group_heartbeat_v1_with_topic_regex() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 68, 0, 1]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);
            assert!(request.windows(9).any(|bytes| bytes == b"orders-.*"));
            assert_eq!(request.last(), Some(&0));

            let mut response = Vec::new();
            response.extend_from_slice(&[0, 0, 0, 1]); // response correlation id
            response.push(0); // response header tagged fields
            response.extend_from_slice(&[0, 0, 0, 0]); // throttle time
            response.extend_from_slice(&[0, 0]); // error code
            response.push(0); // null error message
            response.push(9); // member id length + 1
            response.extend_from_slice(b"member-a");
            response.extend_from_slice(&[0, 0, 0, 3]); // member epoch
            response.extend_from_slice(&2500_i32.to_be_bytes());
            response.push(0xff); // null nullable assignment struct
            response.push(0); // response tagged fields
            write_test_frame(&mut broker_stream, &response).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .consumer_group_heartbeat_v1(
                "orders-group",
                "member-a",
                2,
                None,
                Some("rack-a".to_owned()),
                30_000,
                None,
                Some("orders-.*".to_owned()),
                Some("uniform".to_owned()),
                None,
            )
            .await
            .unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.member_id.as_deref(), Some("member-a"));
        assert_eq!(response.member_epoch, 3);
        assert_eq!(response.heartbeat_interval_ms, 2500);
        assert!(response.assignment.is_none());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_share_group_heartbeat_v1_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 76, 0, 1]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);
            assert_eq!(request.last(), Some(&0));

            let mut response = Vec::new();
            response.extend_from_slice(&[0, 0, 0, 1]); // response correlation id
            response.push(0); // response header tagged fields
            response.extend_from_slice(&[0, 0, 0, 0]); // throttle time
            response.extend_from_slice(&[0, 0]); // error code
            response.push(0); // null error message
            response.push(9); // member id length + 1
            response.extend_from_slice(b"member-1");
            response.extend_from_slice(&[0, 0, 0, 2]); // member epoch
            response.extend_from_slice(&2500_i32.to_be_bytes());
            response.push(0xff); // null nullable assignment struct
            response.push(0); // response tagged fields
            write_test_frame(&mut broker_stream, &response).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-share-heartbeat-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .share_group_heartbeat_v1("share-orders", "", 0, None, Some(vec!["orders".to_owned()]))
            .await
            .unwrap();

        assert_eq!(response.error_code, 0);
        assert_eq!(response.member_id.as_deref(), Some("member-1"));
        assert_eq!(response.member_epoch, 2);
        assert_eq!(response.heartbeat_interval_ms, 2500);
        assert!(response.assignment.is_none());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_consumer_group_describe_v1_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&request);
            assert_eq!(decoder.read_i16().unwrap(), 69);
            assert_eq!(decoder.read_i16().unwrap(), 1);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("kafrust-admin".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(
                decoder
                    .read_compact_array("group IDs", |decoder| decoder.read_compact_string())
                    .unwrap(),
                Some(vec!["orders".to_owned()])
            );
            assert!(decoder.read_bool().unwrap());
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());

            let mut response = Encoder::new();
            response.write_i32(1);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response
                .write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-admin".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .consumer_group_describe_v1(vec!["orders".to_owned()], true)
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert!(response.groups.is_empty());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_share_group_describe_v1_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&request);
            assert_eq!(decoder.read_i16().unwrap(), 77);
            assert_eq!(decoder.read_i16().unwrap(), 1);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("kafrust-admin".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(
                decoder
                    .read_compact_array("group IDs", |decoder| decoder.read_compact_string())
                    .unwrap(),
                Some(vec!["share-orders".to_owned()])
            );
            assert!(decoder.read_bool().unwrap());
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());

            let mut response = Encoder::new();
            response.write_i32(1);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response
                .write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-admin".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .share_group_describe_v1(vec!["share-orders".to_owned()], true)
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert!(response.groups.is_empty());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_streams_group_describe_v0_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&request);
            assert_eq!(decoder.read_i16().unwrap(), 89);
            assert_eq!(decoder.read_i16().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("kafrust-admin".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(
                decoder
                    .read_compact_array("group IDs", |decoder| decoder.read_compact_string())
                    .unwrap(),
                Some(vec!["streams-orders".to_owned()])
            );
            assert!(decoder.read_bool().unwrap());
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());

            let mut response = Encoder::new();
            response.write_i32(1);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response
                .write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-admin".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .streams_group_describe_v0(vec!["streams-orders".to_owned()], true)
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 0);
        assert!(response.groups.is_empty());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_streams_group_heartbeat_v0_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(2048);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&request);
            assert_eq!(decoder.read_i16().unwrap(), 88);
            assert_eq!(decoder.read_i16().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("kafrust-admin".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(decoder.read_compact_string().unwrap(), "streams-orders");
            assert_eq!(decoder.read_compact_string().unwrap(), "member-a");
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert!(decoder.read_compact_nullable_string().unwrap().is_none());
            assert!(decoder.read_compact_nullable_string().unwrap().is_none());
            assert_eq!(decoder.read_i32().unwrap(), 30_000);
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 1);
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 1);
            assert!(decoder.read_compact_nullable_string().unwrap().is_none());
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 0);
            assert!(!decoder.read_bool().unwrap());
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert!(decoder.is_empty());

            let mut response = Encoder::new();
            response.write_i32(1);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response.write_i16(0);
            response.write_compact_nullable_string(None).unwrap();
            response.write_compact_string("member-a").unwrap();
            response.write_i32(1);
            response.write_i32(2500);
            response.write_i32(10);
            response.write_i32(1000);
            response.write_unsigned_varint(0);
            response
                .write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))
                .unwrap();
            response
                .write_compact_array::<i8>(None, |_, _| Ok(()))
                .unwrap();
            response
                .write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))
                .unwrap();
            response.write_i32(2);
            response
                .write_compact_array::<i8>(None, |_, _| Ok(()))
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-admin".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .streams_group_heartbeat_v0(
                "streams-orders",
                "member-a",
                0,
                0,
                None,
                None,
                30_000,
                None,
                Some(Vec::new()),
                None,
                Some(Vec::new()),
                None,
                None,
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        assert_eq!(response.member_id, "member-a");
        assert_eq!(response.heartbeat_interval_ms, 2500);
        assert!(response.status.is_none());
        assert!(response.active_tasks.unwrap().is_empty());
        assert!(response.standby_tasks.is_none());
        assert!(response.warmup_tasks.unwrap().is_empty());
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_share_group_offset_mutations_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(2048);
        let broker = tokio::spawn(async move {
            let alter_request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&alter_request);
            assert_eq!(decoder.read_i16().unwrap(), 91);
            assert_eq!(decoder.read_i16().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("kafrust-admin".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(decoder.read_compact_string().unwrap(), "share-orders");
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 1);
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            let mut response = Encoder::new();
            response.write_i32(1);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response.write_i16(0);
            response.write_compact_nullable_string(None).unwrap();
            response
                .write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;

            let delete_request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&delete_request);
            assert_eq!(decoder.read_i16().unwrap(), 92);
            assert_eq!(decoder.read_i16().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 2);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("kafrust-admin".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(decoder.read_compact_string().unwrap(), "share-orders");
            assert_eq!(decoder.read_unsigned_varint().unwrap(), 1);
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            let mut response = Encoder::new();
            response.write_i32(2);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response.write_i16(0);
            response.write_compact_nullable_string(None).unwrap();
            response
                .write_compact_array::<i8>(Some(&[]), |_, _| Ok(()))
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-admin".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let alter = client
            .alter_share_group_offsets_v0("share-orders", Vec::new())
            .await
            .unwrap();
        assert_eq!(alter.error_code, 0);
        let delete = client
            .delete_share_group_offsets_v0("share-orders", Vec::new())
            .await
            .unwrap();
        assert_eq!(delete.error_code, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn sends_kip_714_telemetry_requests_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(4096);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&request);
            assert_eq!(decoder.read_i16().unwrap(), 71);
            assert_eq!(decoder.read_i16().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("telemetry-test".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(decoder.read_uuid().unwrap(), [0; 16]);
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());

            let mut response = Encoder::new();
            response.write_i32(1);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response.write_i16(0);
            response.write_uuid(&[9; 16]);
            response.write_i32(3);
            response
                .write_compact_array(Some(&[0_i8, 2_i8]), |encoder, value| {
                    encoder.write_i8(*value);
                    Ok(())
                })
                .unwrap();
            response.write_i32(30_000);
            response.write_i32(1024);
            response.write_bool(false);
            response
                .write_compact_array(Some(&["org.apache.kafka.".to_owned()]), |encoder, value| {
                    encoder.write_compact_string(value)
                })
                .unwrap();
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;

            let request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&request);
            assert_eq!(decoder.read_i16().unwrap(), 72);
            assert_eq!(decoder.read_i16().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 2);
            assert_eq!(
                decoder.read_nullable_string().unwrap(),
                Some("telemetry-test".to_owned())
            );
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());
            assert_eq!(decoder.read_uuid().unwrap(), [9; 16]);
            assert_eq!(decoder.read_i32().unwrap(), 3);
            assert!(!decoder.read_bool().unwrap());
            assert_eq!(decoder.read_i8().unwrap(), 0);
            assert_eq!(decoder.read_compact_bytes().unwrap(), vec![1, 2, 3]);
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());

            let mut response = Encoder::new();
            response.write_i32(2);
            response.write_empty_tagged_fields();
            response.write_i32(0);
            response.write_i16(0);
            response.write_empty_tagged_fields();
            write_test_frame(&mut broker_stream, &response.into_bytes()).await;
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("telemetry-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let subscription = client
            .get_telemetry_subscriptions_v0([0; 16])
            .await
            .unwrap();
        assert_eq!(subscription.client_instance_id, [9; 16]);
        assert_eq!(subscription.subscription_id, 3);
        assert_eq!(subscription.accepted_compression_types, vec![0, 2]);
        assert_eq!(
            subscription.requested_metrics,
            vec!["org.apache.kafka.".to_owned()]
        );

        let response = client
            .push_telemetry_v0([9; 16], 3, false, 0, vec![1, 2, 3])
            .await
            .unwrap();
        assert_eq!(response.error_code, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn ends_transaction_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let mut request_size = [0u8; 4];
            broker_stream.read_exact(&mut request_size).await.unwrap();
            let request_size = usize::try_from(i32::from_be_bytes(request_size)).unwrap();
            let mut request = vec![0u8; request_size];
            broker_stream.read_exact(&mut request).await.unwrap();

            assert_eq!(&request[0..2], &[0, 26]);
            assert_eq!(&request[2..4], &[0, 0]);
            assert_eq!(&request[4..8], &[0, 0, 0, 1]);
            assert_eq!(request.last(), Some(&1));

            let response = [
                0, 0, 0, 1, // correlation id
                0, 0, 0, 7, // throttle time
                0, 0, // error code
            ];
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client.end_txn_v0("orders-tx", 42, 3, true).await.unwrap();

        assert_eq!(response.throttle_time_ms, 7);
        assert_eq!(response.error_code, 0);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn finds_transaction_coordinator_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 10, 0, 1]);
            assert_eq!(request.last(), Some(&1));
            write_test_frame(
                &mut broker_stream,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, 0, 0, // throttle time
                    0, 0, // error code
                    0xff, 0xff, // null error message
                    0, 0, 0, 2, // node id
                    0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', 0, 0, 35,
                    132, // port 9092
                ],
            )
            .await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .find_transaction_coordinator("orders-tx")
            .await
            .unwrap();

        assert_eq!(response.node_id, 2);
        assert_eq!(response.host, "localhost");
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn adds_partitions_to_transaction_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 24, 0, 0]);
            write_test_frame(
                &mut broker_stream,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, 0, 0, // throttle time
                    0, 0, 0, 1, // topic count
                    0, 6, b'o', b'r', b'd', b'e', b'r', b's', 0, 0, 0, 1, // partition count
                    0, 0, 0, 2, // partition index
                    0, 47, // invalid producer epoch
                ],
            )
            .await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .add_partitions_to_txn_v0(
                "orders-tx",
                42,
                3,
                vec![AddPartitionsToTxnTopic {
                    name: "orders".to_owned(),
                    partitions: vec![2],
                }],
            )
            .await
            .unwrap();

        assert_eq!(response.errors[0].partitions[0].partition_index, 2);
        assert_eq!(response.errors[0].partitions[0].error_code, 47);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn adds_offsets_to_transaction_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 25, 0, 0]);
            write_test_frame(
                &mut broker_stream,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, 0, 4, // throttle time
                    0, 16, // not coordinator
                ],
            )
            .await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .add_offsets_to_txn_v0("orders-tx", 42, 3, "orders-group")
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 4);
        assert_eq!(response.error_code, 16);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn commits_transaction_offsets_over_injected_broker_stream() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(1024);
        let broker = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(&request[0..4], &[0, 28, 0, 0]);
            write_test_frame(
                &mut broker_stream,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, 0, 3, // throttle time
                    0, 0, 0, 1, // topic count
                    0, 6, b'o', b'r', b'd', b'e', b'r', b's', 0, 0, 0, 1, // partition count
                    0, 0, 0, 2, // partition index
                    0, 27, // rebalance in progress
                ],
            )
            .await;
        });
        let mut client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-stream-test".to_owned()),
            Some(Duration::from_secs(1)),
        );

        let response = client
            .txn_offset_commit_v0(
                "orders-tx",
                "orders-group",
                42,
                3,
                vec![TxnOffsetCommitTopic {
                    name: "orders".to_owned(),
                    partitions: vec![TxnOffsetCommitPartition {
                        partition_index: 2,
                        committed_offset: 81,
                        committed_metadata: None,
                    }],
                }],
            )
            .await
            .unwrap();

        assert_eq!(response.throttle_time_ms, 3);
        assert_eq!(response.topics[0].partitions[0].partition_index, 2);
        assert_eq!(response.topics[0].partitions[0].error_code, 27);
        broker.await.unwrap();
    }

    async fn read_test_frame(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
        let mut request_size = [0u8; 4];
        stream.read_exact(&mut request_size).await.unwrap();
        let request_size = usize::try_from(i32::from_be_bytes(request_size)).unwrap();
        let mut request = vec![0u8; request_size];
        stream.read_exact(&mut request).await.unwrap();
        request
    }

    async fn write_test_frame(stream: &mut tokio::io::DuplexStream, response: &[u8]) {
        stream
            .write_all(&(response.len() as i32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(response).await.unwrap();
        stream.flush().await.unwrap();
    }

    #[test]
    fn reads_request_trace_from_encoded_header() {
        let trace = RequestTrace::from_request(&[
            0, 18, // api key
            0, 3, // api version
            0, 0, 0, 7, // correlation id
            0, 0, // remaining bytes
        ])
        .unwrap();

        assert_eq!(trace.api_key, 18);
        assert_eq!(trace.api_version, 3);
        assert_eq!(trace.correlation_id, 7);
        assert_eq!(trace.request_bytes, 10);
    }
}
