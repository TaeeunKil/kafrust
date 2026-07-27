use kafrust_protocol::api::add_offsets_to_txn::{
    AddOffsetsToTxnRequestV0, AddOffsetsToTxnResponseV0,
};
use kafrust_protocol::api::add_partitions_to_txn::{
    AddPartitionsToTxnRequestV0, AddPartitionsToTxnResponseV0, AddPartitionsToTxnTopic,
};
use kafrust_protocol::api::api_versions::{ApiVersionsRequestV0, ApiVersionsResponseV0};
use kafrust_protocol::api::create_topics::{
    CreateTopicsRequestV2, CreateTopicsResponseV2, CreateTopicsTopicV2,
};
use kafrust_protocol::api::delete_groups::{DeleteGroupsRequestV1, DeleteGroupsResponseV1};
use kafrust_protocol::api::delete_topics::{DeleteTopicsRequestV3, DeleteTopicsResponseV3};
use kafrust_protocol::api::describe_configs::{
    DescribeConfigsRequestV1, DescribeConfigsResourceV1, DescribeConfigsResponseV1,
};
use kafrust_protocol::api::describe_groups::{DescribeGroupsRequestV1, DescribeGroupsResponseV1};
use kafrust_protocol::api::end_txn::{EndTxnRequestV0, EndTxnResponseV0};
use kafrust_protocol::api::fetch::{
    FetchPartitionV2, FetchRequestV4, FetchResponseV4, FetchTopicV2,
};
use kafrust_protocol::api::find_coordinator::{
    CoordinatorType, FindCoordinatorRequestV1, FindCoordinatorResponseV1,
};
use kafrust_protocol::api::heartbeat::{
    HeartbeatRequestV2, HeartbeatRequestV3, HeartbeatResponseV2,
};
use kafrust_protocol::api::incremental_alter_configs::{
    IncrementalAlterConfigsRequestV0, IncrementalAlterConfigsResourceV0,
    IncrementalAlterConfigsResponseV0,
};
use kafrust_protocol::api::init_producer_id::{InitProducerIdRequestV0, InitProducerIdResponseV0};
use kafrust_protocol::api::join_group::{
    JoinGroupProtocol, JoinGroupRequestV2, JoinGroupRequestV5, JoinGroupResponseV2,
    JoinGroupResponseV5,
};
use kafrust_protocol::api::leave_group::{
    LeaveGroupMemberIdentity, LeaveGroupRequestV3, LeaveGroupResponseV3,
};
use kafrust_protocol::api::list_groups::{ListGroupsRequestV1, ListGroupsResponseV1};
use kafrust_protocol::api::list_offsets::{
    ListOffsetsRequestV1, ListOffsetsResponseV1, ListOffsetsTopicV1,
};
use kafrust_protocol::api::metadata::{MetadataRequestV1, MetadataResponseV1};
use kafrust_protocol::api::offset_commit::{
    OffsetCommitRequestV2, OffsetCommitRequestV7, OffsetCommitResponseV2, OffsetCommitResponseV7,
    OffsetCommitTopic, OffsetCommitTopicV7,
};
use kafrust_protocol::api::offset_delete::{
    OffsetDeleteRequestTopicV0, OffsetDeleteRequestV0, OffsetDeleteResponseV0,
};
use kafrust_protocol::api::offset_fetch::{
    OffsetFetchRequestV2, OffsetFetchResponseV2, OffsetFetchTopic,
};
use kafrust_protocol::api::produce::{
    MessageSetMessage, ProducePartitionV2, ProducePartitionV3, ProduceRequestV2, ProduceRequestV3,
    ProduceRequestV7, ProduceResponseV2, ProduceResponseV7, ProduceTopicV2, ProduceTopicV3,
    RecordBatchIdentity, RecordBatchMessage,
};
use kafrust_protocol::api::sasl::{
    SaslAuthenticateRequestV0, SaslAuthenticateResponseV0, SaslHandshakeRequestV1,
    SaslHandshakeResponseV1,
};
use kafrust_protocol::api::sync_group::{
    SyncGroupAssignment, SyncGroupRequestV2, SyncGroupRequestV3, SyncGroupResponseV2,
};
use kafrust_protocol::api::txn_offset_commit::{
    TxnOffsetCommitRequestV0, TxnOffsetCommitRequestV3, TxnOffsetCommitResponseV0,
    TxnOffsetCommitResponseV3, TxnOffsetCommitTopic, TxnOffsetCommitTopicV3,
};
use kafrust_protocol::codec::{DecodeLimits, Decoder};
use kafrust_protocol::frame::encode_frame;
use kafrust_protocol::header::ResponseHeader;
use std::fmt;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, debug_span, Instrument, Span};

use crate::error::{Error, Result};
use crate::metrics::ClientMetrics;

pub(crate) const DEFAULT_MAX_RESPONSE_BYTES: usize = 100 * 1024 * 1024;

/// Low-level Kafka request client over a single broker connection.
pub struct Client {
    stream: Box<dyn BrokerStream>,
    client_id: Option<String>,
    next_correlation_id: i32,
    request_timeout: Option<Duration>,
    max_response_bytes: usize,
    decode_limits: DecodeLimits,
    metrics: ClientMetrics,
}

pub(crate) trait BrokerStream: AsyncRead + AsyncWrite + Unpin + Send + Sync {}

impl<T> BrokerStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}

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
        }
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
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(SaslHandshakeResponseV1::decode_body(&mut decoder)?)
    }

    pub(crate) async fn sasl_authenticate_v0(
        &mut self,
        auth_bytes: Vec<u8>,
    ) -> Result<SaslAuthenticateResponseV0> {
        let request = SaslAuthenticateRequestV0 {
            correlation_id: self.next_correlation_id(),
            client_id: self.client_id.clone(),
            auth_bytes,
        };
        let response = self.send_request(&request.encode()?).await?;
        let mut decoder = Decoder::with_limits(&response, self.decode_limits);
        let _header = ResponseHeader::decode_v0(&mut decoder)?;
        Ok(SaslAuthenticateResponseV0::decode_body(&mut decoder)?)
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

    /// Sends FindCoordinator v1 for a consumer group ID.
    pub async fn find_group_coordinator(
        &mut self,
        group_id: impl Into<String>,
    ) -> Result<FindCoordinatorResponseV1> {
        self.find_coordinator_v1(group_id.into(), CoordinatorType::Group)
            .await
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

    async fn send_request(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        let trace = RequestTrace::from_request(request);
        let span = RequestTrace::span(trace);
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

            RequestTrace::log_finish(trace, &result);
            match &result {
                Ok(response) => metrics.succeed(response.len()),
                Err(error) => metrics.fail(matches!(error, Error::RequestTimedOut { .. })),
            }
            result
        }
        .instrument(span)
        .await
    }

    async fn send_request_unbounded(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        let frame = encode_frame(request)?;
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;

        let mut size = [0u8; 4];
        self.stream.read_exact(&mut size).await?;
        let size = i32::from_be_bytes(size);
        if size < 0 {
            return Err(Error::Protocol(kafrust_protocol::Error::NegativeLength {
                kind: "response frame",
                length: size,
            }));
        }

        let size = usize::try_from(size).map_err(|_| {
            Error::Protocol(kafrust_protocol::Error::LengthOverflow("response frame"))
        })?;
        if size > self.max_response_bytes {
            return Err(Error::ResponseTooLarge {
                size,
                max: self.max_response_bytes,
            });
        }

        let mut response = vec![0; size];
        self.stream.read_exact(&mut response).await?;
        Ok(response)
    }

    fn next_correlation_id(&mut self) -> i32 {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1).max(1);
        correlation_id
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("client_id", &self.client_id)
            .field("next_correlation_id", &self.next_correlation_id)
            .field("request_timeout", &self.request_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("decode_limits", &self.decode_limits)
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AddPartitionsToTxnTopic, Client, RequestTrace, TxnOffsetCommitTopic,
        DEFAULT_MAX_RESPONSE_BYTES,
    };
    use crate::{ClientMetrics, Error};
    use kafrust_protocol::api::txn_offset_commit::TxnOffsetCommitPartition;
    use kafrust_protocol::codec::DecodeLimits;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

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
