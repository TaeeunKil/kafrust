//! Kafka Streams group membership and heartbeat lifecycle.

use crate::client::Client;
use crate::config::{ClientConfig, SecurityProtocol};
use crate::error::{BrokerErrorKind, Error, Result};
use crate::metrics::ClientMetrics;
use kafrust_protocol::api::api_versions::ApiVersionsResponseV3;
use kafrust_protocol::api::find_coordinator::FindCoordinatorResponseV1;
pub use kafrust_protocol::api::streams_group_heartbeat::{
    StreamsGroupHeartbeatEndpoint, StreamsGroupHeartbeatEndpointPartitions,
    StreamsGroupHeartbeatKeyValue, StreamsGroupHeartbeatRequestV0, StreamsGroupHeartbeatResponseV0,
    StreamsGroupHeartbeatStatus, StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTask,
    StreamsGroupHeartbeatTaskOffset, StreamsGroupHeartbeatTopic, StreamsGroupHeartbeatTopicConfig,
    StreamsGroupHeartbeatTopology,
};
use std::time::Duration;
use tokio::time::sleep;
use tracing::debug;

const STREAMS_GROUP_HEARTBEAT_API_KEY: i16 = 88;
const DEFAULT_STREAMS_GROUP_MAX_RETRIES: u32 = 5;
const STREAMS_GROUP_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const STREAMS_GROUP_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Configuration for a Kafka Streams group membership session.
///
/// This API manages the broker-side Streams group protocol. It does not
/// execute Kafka Streams processors or provide a DSL; applications remain
/// responsible for processing records and reporting task state.
pub struct StreamsGroupConfig {
    client: ClientConfig,
    group_id: String,
    topology: StreamsGroupHeartbeatTopology,
    instance_id: Option<String>,
    rack_id: Option<String>,
    rebalance_timeout_ms: i32,
    process_id: Option<String>,
    user_endpoint: Option<StreamsGroupHeartbeatEndpoint>,
    client_tags: Option<Vec<StreamsGroupHeartbeatKeyValue>>,
    max_retries: u32,
}

impl StreamsGroupConfig {
    /// Creates a session configuration with a required initial topology.
    pub fn new(
        bootstrap_servers: impl IntoIterator<Item = impl Into<String>>,
        group_id: impl Into<String>,
        topology: StreamsGroupHeartbeatTopology,
    ) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            group_id: group_id.into(),
            topology,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 30_000,
            process_id: None,
            user_endpoint: None,
            client_tags: None,
            max_retries: DEFAULT_STREAMS_GROUP_MAX_RETRIES,
        }
    }

    /// Replaces the underlying client configuration.
    ///
    /// Use this to configure TLS, SASL, bootstrap rotation, decode limits, or
    /// shared metrics without duplicating those client settings here.
    pub fn client_config(mut self, client: ClientConfig) -> Self {
        self.client = client;
        self
    }

    /// Replaces the shared client configuration using the common builder
    /// naming used by the other high-level clients.
    pub fn with_client_config(self, client: ClientConfig) -> Self {
        self.client_config(client)
    }

    /// Sets the Kafka client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    /// Sets the static membership instance ID.
    pub fn instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.instance_id = Some(instance_id.into());
        self
    }

    /// Sets the rack ID used by the Streams assignment algorithm.
    pub fn rack_id(mut self, rack_id: impl Into<String>) -> Self {
        self.rack_id = Some(rack_id.into());
        self
    }

    /// Sets the maximum time Kafka may wait for this member during rebalance.
    pub fn rebalance_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.rebalance_timeout_ms = timeout_ms;
        self
    }

    /// Sets the Streams process identity used by task assignment.
    pub fn process_id(mut self, process_id: impl Into<String>) -> Self {
        self.process_id = Some(process_id.into());
        self
    }

    /// Sets the Interactive Queries endpoint advertised by this member.
    pub fn user_endpoint(mut self, endpoint: StreamsGroupHeartbeatEndpoint) -> Self {
        self.user_endpoint = Some(endpoint);
        self
    }

    /// Sets rack-aware client tags.
    pub fn client_tags(mut self, tags: Vec<StreamsGroupHeartbeatKeyValue>) -> Self {
        self.client_tags = Some(tags);
        self
    }

    /// Sets the bounded reconnect and rejoin retry count.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the request timeout.
    pub fn request_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.client = self.client.request_timeout_ms(timeout_ms);
        self
    }

    /// Sets the security protocol.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.client = self.client.security_protocol(security_protocol);
        self
    }

    /// Sets the TLS server name used for broker certificate validation.
    pub fn tls_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.client = self.client.tls_server_name(server_name);
        self
    }

    /// Adds a DER-encoded TLS root certificate for broker validation.
    pub fn tls_root_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.client = self.client.tls_root_certificate_der(certificate);
        self
    }

    /// Adds a DER-encoded client certificate for TLS mutual authentication.
    pub fn tls_client_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.client = self.client.tls_client_certificate_der(certificate);
        self
    }

    /// Sets the DER-encoded private key for TLS mutual authentication.
    pub fn tls_client_private_key_der(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.client = self.client.tls_client_private_key_der(key);
        self
    }

    /// Sets the shared metrics handle.
    pub fn metrics(mut self, metrics: ClientMetrics) -> Self {
        self.client = self.client.metrics(metrics);
        self
    }

    /// Returns the configured group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the configured topology.
    pub fn topology(&self) -> &StreamsGroupHeartbeatTopology {
        &self.topology
    }

    /// Validates the session configuration without connecting to Kafka.
    pub fn validate(&self) -> Result<()> {
        self.client.validate()?;
        if self.group_id.trim().is_empty() {
            return Err(Error::InvalidConfiguration {
                field: "group_id",
                reason: "must not be empty",
            });
        }
        if self.topology.subtopologies.is_empty() {
            return Err(Error::InvalidConfiguration {
                field: "topology.subtopologies",
                reason: "must contain at least one subtopology",
            });
        }
        if self.rebalance_timeout_ms <= 0 {
            return Err(Error::InvalidConfiguration {
                field: "rebalance_timeout_ms",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }

    /// Validates and returns this Streams group configuration without opening
    /// a broker connection.
    pub fn build_config(self) -> Result<Self> {
        self.validate()?;
        Ok(self)
    }
}

/// An active Kafka Streams group membership session.
///
/// A session starts by sending API 88 with a client-generated member ID, member
/// epoch zero, and the configured topology. Subsequent calls send the current
/// member epoch and only transmit changed membership data according to Kafka's
/// nullable protocol fields. Call [`Self::close`] to leave gracefully.
pub struct StreamsGroupSession {
    config: StreamsGroupConfig,
    coordinator: Option<Client>,
    member_id: String,
    member_epoch: i32,
    endpoint_information_epoch: i32,
    heartbeat_interval: Duration,
    pending_active_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pending_standby_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pending_warmup_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    pending_task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    pending_task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    assignment: StreamsGroupSessionAssignment,
    closed: bool,
}

struct StreamsHeartbeatPayload {
    member_epoch: i32,
    topology: Option<StreamsGroupHeartbeatTopology>,
    instance_id: Option<String>,
    rack_id: Option<String>,
    process_id: Option<String>,
    user_endpoint: Option<StreamsGroupHeartbeatEndpoint>,
    client_tags: Option<Vec<StreamsGroupHeartbeatKeyValue>>,
    active_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    standby_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    warmup_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    shutdown_application: bool,
}

/// The latest assignment and task-state information returned by Kafka.
///
/// The Streams group protocol returns assignment state on every successful
/// heartbeat. Keeping the last successful snapshot on the session gives an
/// application a stable reconciliation point after a rebalance or reconnect;
/// it also preserves nullable broker responses instead of collapsing an
/// omitted update into an empty assignment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamsGroupSessionAssignment {
    /// Broker status entries for the current member, when present.
    pub status: Option<Vec<StreamsGroupHeartbeatStatus>>,
    /// Active tasks assigned to this member, if the broker sent the field.
    pub active_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    /// Standby tasks assigned to this member, if the broker sent the field.
    pub standby_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    /// Warmup tasks assigned to this member, if the broker sent the field.
    pub warmup_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    /// Broker-provided recovery lag threshold for the current assignment.
    pub acceptable_recovery_lag: i32,
    /// Broker-provided interval for reporting task offsets.
    pub task_offset_interval_ms: i32,
    /// Interactive Queries endpoint partition assignments, if present.
    pub partitions_by_user_endpoint: Option<Vec<StreamsGroupHeartbeatEndpointPartitions>>,
}

impl StreamsGroupSessionAssignment {
    fn from_response(response: &StreamsGroupHeartbeatResponseV0) -> Self {
        Self {
            status: response.status.clone(),
            active_tasks: response.active_tasks.clone(),
            standby_tasks: response.standby_tasks.clone(),
            warmup_tasks: response.warmup_tasks.clone(),
            acceptable_recovery_lag: response.acceptable_recovery_lag,
            task_offset_interval_ms: response.task_offset_interval_ms,
            partitions_by_user_endpoint: response.partitions_by_user_endpoint.clone(),
        }
    }
}

impl StreamsGroupSession {
    /// Joins the configured Kafka Streams group.
    pub async fn join(config: StreamsGroupConfig) -> Result<Self> {
        config.validate()?;
        let mut session = Self {
            config,
            coordinator: None,
            member_id: new_streams_member_id(),
            member_epoch: 0,
            endpoint_information_epoch: 0,
            heartbeat_interval: Duration::from_secs(1),
            pending_active_tasks: None,
            pending_standby_tasks: None,
            pending_warmup_tasks: None,
            pending_task_offsets: None,
            pending_task_end_offsets: None,
            assignment: StreamsGroupSessionAssignment::default(),
            closed: false,
        };
        session.join_with_retry().await?;
        Ok(session)
    }

    /// Returns the Kafka Streams group ID.
    pub fn group_id(&self) -> &str {
        self.config.group_id()
    }

    /// Returns the client-generated member ID used for this Streams session.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the current member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the broker-requested heartbeat interval.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Returns the latest successful assignment snapshot.
    ///
    /// A `None` collection means that Kafka omitted the nullable field in its
    /// response; it is distinct from `Some(Vec::new())`, which is an explicit
    /// empty assignment.
    pub fn assignment(&self) -> &StreamsGroupSessionAssignment {
        &self.assignment
    }

    /// Replaces the task state and changelog offsets reported on the next
    /// heartbeat.
    ///
    /// Empty vectors are meaningful and are sent as empty compact arrays. The
    /// values remain pending until a heartbeat succeeds, so a reconnect does
    /// not silently lose a task-state update.
    pub fn set_task_state(
        &mut self,
        active_tasks: Vec<StreamsGroupHeartbeatTask>,
        standby_tasks: Vec<StreamsGroupHeartbeatTask>,
        warmup_tasks: Vec<StreamsGroupHeartbeatTask>,
        task_offsets: Vec<StreamsGroupHeartbeatTaskOffset>,
        task_end_offsets: Vec<StreamsGroupHeartbeatTaskOffset>,
    ) {
        self.pending_active_tasks = Some(active_tasks);
        self.pending_standby_tasks = Some(standby_tasks);
        self.pending_warmup_tasks = Some(warmup_tasks);
        self.pending_task_offsets = Some(task_offsets);
        self.pending_task_end_offsets = Some(task_end_offsets);
    }

    /// Returns whether the session has left the group.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Sends one Streams heartbeat and returns the broker's assignment state.
    ///
    /// Coordinator transport failures and member/coordinator epoch errors are
    /// recovered within the configured retry budget. A rejoin resends the
    /// initial topology and updates the session's member identity.
    pub async fn heartbeat(&mut self) -> Result<StreamsGroupHeartbeatResponseV0> {
        self.ensure_open()?;
        let mut retry = 0;
        loop {
            match self.send_heartbeat(false).await {
                Ok(response) => return Ok(response),
                Err(error)
                    if retry < self.config.max_retries && is_retryable_streams_error(&error) =>
                {
                    retry += 1;
                    self.config.client.record_retry();
                    let rejoin = should_rejoin_streams_group(&error);
                    self.recover_streams_session(rejoin, &mut retry).await?;
                    sleep(streams_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Leaves the Streams group using member epoch `-1`.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let mut retry = 0;
        loop {
            self.ensure_open()?;
            match self.send_heartbeat(true).await {
                Ok(_) => {
                    self.closed = true;
                    self.coordinator = None;
                    return Ok(());
                }
                Err(error)
                    if retry < self.config.max_retries && is_retryable_streams_error(&error) =>
                {
                    retry += 1;
                    self.config.client.record_retry();
                    self.recover_streams_session(false, &mut retry).await?;
                    sleep(streams_retry_backoff(retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn join_with_retry(&mut self) -> Result<()> {
        let mut retry = 0;
        self.recover_streams_session(true, &mut retry).await
    }

    async fn recover_streams_session(&mut self, rejoin: bool, retry: &mut u32) -> Result<()> {
        loop {
            let result = async {
                self.reconnect().await?;
                if rejoin {
                    if self.member_epoch != 0 {
                        self.member_id = new_streams_member_id();
                    }
                    self.member_epoch = 0;
                    self.send_initial_heartbeat().await?;
                }
                Ok::<(), Error>(())
            }
            .await;

            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if *retry < self.config.max_retries && is_retryable_streams_error(&error) =>
                {
                    *retry += 1;
                    self.config.client.record_retry();
                    sleep(streams_retry_backoff(*retry)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn reconnect(&mut self) -> Result<()> {
        self.coordinator = None;
        let mut bootstrap = self.config.client.clone().connect().await?;
        let coordinator = bootstrap
            .find_group_coordinator(self.config.group_id.clone())
            .await?;
        if coordinator.error_code != 0 {
            return Err(self.config.client.broker_error(
                coordinator.error_code,
                format!("find Streams group coordinator {}", self.config.group_id),
            ));
        }
        let address = coordinator_addr(&coordinator);
        let mut client = self.config.client.connect_broker(address).await?;
        let versions = client
            .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
            .await?;
        ensure_streams_heartbeat_supported(&versions)?;
        self.coordinator = Some(client);
        Ok(())
    }

    async fn send_initial_heartbeat(&mut self) -> Result<StreamsGroupHeartbeatResponseV0> {
        let response = self
            .send_heartbeat_request(StreamsHeartbeatPayload {
                member_epoch: 0,
                topology: Some(self.config.topology.clone()),
                instance_id: self.config.instance_id.clone(),
                rack_id: self.config.rack_id.clone(),
                process_id: self.config.process_id.clone(),
                user_endpoint: self.config.user_endpoint.clone(),
                client_tags: self.config.client_tags.clone(),
                active_tasks: Some(Vec::new()),
                standby_tasks: Some(Vec::new()),
                warmup_tasks: Some(Vec::new()),
                task_offsets: Some(Vec::new()),
                task_end_offsets: Some(Vec::new()),
                shutdown_application: false,
            })
            .await?;
        self.apply_response(&response)?;
        debug!(
            group_id = self.config.group_id.as_str(),
            member_id = self.member_id.as_str(),
            member_epoch = self.member_epoch,
            "joined Kafka Streams group"
        );
        Ok(response)
    }

    async fn send_heartbeat(&mut self, leave: bool) -> Result<StreamsGroupHeartbeatResponseV0> {
        let member_epoch = if leave { -1 } else { self.member_epoch };
        let active_tasks = if leave {
            None
        } else {
            self.pending_active_tasks.clone()
        };
        let standby_tasks = if leave {
            None
        } else {
            self.pending_standby_tasks.clone()
        };
        let warmup_tasks = if leave {
            None
        } else {
            self.pending_warmup_tasks.clone()
        };
        let task_offsets = if leave {
            None
        } else {
            self.pending_task_offsets.clone()
        };
        let task_end_offsets = if leave {
            None
        } else {
            self.pending_task_end_offsets.clone()
        };
        let response = self
            .send_heartbeat_request(StreamsHeartbeatPayload {
                member_epoch,
                topology: None,
                instance_id: None,
                rack_id: None,
                process_id: None,
                user_endpoint: None,
                client_tags: None,
                active_tasks,
                standby_tasks,
                warmup_tasks,
                task_offsets,
                task_end_offsets,
                shutdown_application: leave,
            })
            .await?;
        self.apply_response(&response)?;
        if !leave {
            self.pending_active_tasks = None;
            self.pending_standby_tasks = None;
            self.pending_warmup_tasks = None;
            self.pending_task_offsets = None;
            self.pending_task_end_offsets = None;
        }
        Ok(response)
    }

    async fn send_heartbeat_request(
        &mut self,
        payload: StreamsHeartbeatPayload,
    ) -> Result<StreamsGroupHeartbeatResponseV0> {
        let coordinator = self.coordinator.as_mut().ok_or(Error::Unsupported(
            "Streams group coordinator is not connected",
        ))?;
        coordinator
            .streams_group_heartbeat_v0(
                self.config.group_id.clone(),
                self.member_id.clone(),
                payload.member_epoch,
                self.endpoint_information_epoch,
                payload.instance_id,
                payload.rack_id,
                if payload.member_epoch == -1 {
                    -1
                } else {
                    self.config.rebalance_timeout_ms
                },
                payload.topology,
                payload.active_tasks,
                payload.standby_tasks,
                payload.warmup_tasks,
                payload.process_id,
                payload.user_endpoint,
                payload.client_tags,
                payload.task_offsets,
                payload.task_end_offsets,
                payload.shutdown_application,
            )
            .await
    }

    fn apply_response(&mut self, response: &StreamsGroupHeartbeatResponseV0) -> Result<()> {
        if response.error_code != 0 {
            return Err(self.config.client.broker_error(
                response.error_code,
                format!(
                    "Streams group heartbeat {}: {}",
                    self.config.group_id,
                    response
                        .error_message
                        .as_deref()
                        .unwrap_or("broker returned a Streams group error")
                ),
            ));
        }
        self.member_id = response.member_id.clone();
        self.member_epoch = response.member_epoch;
        self.endpoint_information_epoch = response.endpoint_information_epoch;
        self.assignment = StreamsGroupSessionAssignment::from_response(response);
        if response.heartbeat_interval_ms > 0 {
            self.heartbeat_interval = Duration::from_millis(
                u64::try_from(response.heartbeat_interval_ms).unwrap_or(u64::MAX),
            );
        }
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            return Err(Error::Unsupported("Streams group session is closed"));
        }
        Ok(())
    }
}

fn ensure_streams_heartbeat_supported(versions: &ApiVersionsResponseV3) -> Result<()> {
    if versions
        .highest_supported_version(STREAMS_GROUP_HEARTBEAT_API_KEY, 0)
        .is_none()
    {
        return Err(Error::Unsupported(
            "broker does not advertise StreamsGroupHeartbeat v0",
        ));
    }
    Ok(())
}

fn coordinator_addr(coordinator: &FindCoordinatorResponseV1) -> String {
    format!("{}:{}", coordinator.host, coordinator.port)
}

fn new_streams_member_id() -> String {
    use rand::RngCore;

    let mut bytes = [0_u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = bytes
        .iter()
        .fold(String::with_capacity(32), |mut hex, byte| {
            use std::fmt::Write;

            let _ = write!(hex, "{byte:02x}");
            hex
        });
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn is_retryable_streams_error(error: &Error) -> bool {
    if matches!(error, Error::Io(_)) {
        return true;
    }
    matches!(
        error.broker_error_kind(),
        Some(
            BrokerErrorKind::CoordinatorLoadInProgress
                | BrokerErrorKind::CoordinatorNotAvailable
                | BrokerErrorKind::NotCoordinator
                | BrokerErrorKind::RebalanceInProgress
                | BrokerErrorKind::UnknownMemberId
                | BrokerErrorKind::FencedMemberEpoch
                | BrokerErrorKind::StaleMemberEpoch
        )
    )
}

fn should_rejoin_streams_group(error: &Error) -> bool {
    matches!(
        error.broker_error_kind(),
        Some(
            BrokerErrorKind::UnknownMemberId
                | BrokerErrorKind::FencedMemberEpoch
                | BrokerErrorKind::StaleMemberEpoch
        )
    )
}

fn streams_retry_backoff(attempt: u32) -> Duration {
    let factor = u32::pow(2, attempt.saturating_sub(1).min(5));
    STREAMS_GROUP_RETRY_BACKOFF
        .checked_mul(factor)
        .unwrap_or(STREAMS_GROUP_MAX_RETRY_BACKOFF)
        .min(STREAMS_GROUP_MAX_RETRY_BACKOFF)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        streams_retry_backoff, StreamsGroupConfig, StreamsGroupHeartbeatEndpoint,
        StreamsGroupHeartbeatKeyValue, StreamsGroupHeartbeatTask, StreamsGroupHeartbeatTaskOffset,
        StreamsGroupHeartbeatTopology, StreamsGroupSession, StreamsGroupSessionAssignment,
        STREAMS_GROUP_MAX_RETRY_BACKOFF,
    };
    use crate::client::Client;
    use kafrust_protocol::api::streams_group_heartbeat::StreamsGroupHeartbeatSubtopology;
    use kafrust_protocol::codec::{Decoder, Encoder};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn topology() -> StreamsGroupHeartbeatTopology {
        StreamsGroupHeartbeatTopology {
            epoch: 1,
            subtopologies: vec![StreamsGroupHeartbeatSubtopology {
                subtopology_id: "subtopology-0".to_owned(),
                source_topics: vec!["orders".to_owned()],
                source_topic_regex: Vec::new(),
                state_changelog_topics: Vec::new(),
                repartition_sink_topics: Vec::new(),
                repartition_source_topics: Vec::new(),
                copartition_groups: Vec::new(),
            }],
        }
    }

    trait DecoderLengthExt {
        fn read_compact_array_length(&mut self) -> Option<usize>;
    }

    impl<'a> DecoderLengthExt for Decoder<'a> {
        fn read_compact_array_length(&mut self) -> Option<usize> {
            match self.read_unsigned_varint().unwrap() {
                0 => None,
                length => Some(usize::try_from(length - 1).unwrap()),
            }
        }
    }

    #[test]
    fn validates_required_streams_topology() {
        let config = StreamsGroupConfig::new(["localhost:9092"], "orders", topology());
        assert_eq!(config.group_id(), "orders");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_empty_streams_topology() {
        let config = StreamsGroupConfig::new(
            ["localhost:9092"],
            "orders",
            StreamsGroupHeartbeatTopology {
                epoch: 1,
                subtopologies: Vec::new(),
            },
        );
        assert!(config.validate().is_err());
    }

    #[test]
    fn caps_streams_retry_backoff() {
        assert_eq!(streams_retry_backoff(100), STREAMS_GROUP_MAX_RETRY_BACKOFF);
        assert_eq!(streams_retry_backoff(1), Duration::from_millis(50));
    }

    #[test]
    fn generates_uuid_shaped_streams_member_ids() {
        let member_id = super::new_streams_member_id();
        assert_eq!(member_id.len(), 36);
        assert_eq!(&member_id[8..9], "-");
        assert_eq!(&member_id[13..14], "-");
        assert_eq!(&member_id[18..19], "-");
        assert_eq!(&member_id[23..24], "-");
        assert_eq!(&member_id[14..15], "4");
        assert!(matches!(&member_id[19..20], "8" | "9" | "a" | "b"));
    }

    #[tokio::test]
    async fn preserves_streams_group_lifecycle_on_the_wire() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(16 * 1024);
        let broker = tokio::spawn(async move {
            let request = read_frame(&mut broker_stream).await;
            let (correlation_id, mut decoder) = request_header(&request);
            assert_eq!(correlation_id, 1);
            assert_eq!(decoder.read_compact_string().unwrap(), "orders");
            assert_eq!(decoder.read_compact_string().unwrap(), "client-member");
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(
                decoder.read_compact_nullable_string().unwrap(),
                Some("instance-a".to_owned())
            );
            assert_eq!(
                decoder.read_compact_nullable_string().unwrap(),
                Some("rack-a".to_owned())
            );
            assert_eq!(decoder.read_i32().unwrap(), 30_000);
            assert_eq!(decoder.read_i8().unwrap(), 1);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            assert_eq!(decoder.read_compact_array_length(), Some(1));
            assert_eq!(decoder.read_compact_string().unwrap(), "subtopology-0");
            assert_eq!(decoder.read_compact_array_length(), Some(1));
            assert_eq!(decoder.read_compact_string().unwrap(), "orders");
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            decoder.read_tagged_fields().unwrap();
            decoder.read_tagged_fields().unwrap();
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(
                decoder.read_compact_nullable_string().unwrap(),
                Some("process-a".to_owned())
            );
            assert_eq!(decoder.read_i8().unwrap(), 1);
            assert_eq!(decoder.read_compact_string().unwrap(), "query-host");
            assert_eq!(decoder.read_i16().unwrap(), 7_777);
            decoder.read_tagged_fields().unwrap();
            assert_eq!(decoder.read_compact_array_length(), Some(1));
            assert_eq!(decoder.read_compact_string().unwrap(), "zone");
            assert_eq!(decoder.read_compact_string().unwrap(), "a");
            decoder.read_tagged_fields().unwrap();
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert!(!decoder.read_bool().unwrap());
            decoder.read_tagged_fields().unwrap();
            write_streams_response(&mut broker_stream, correlation_id, "member-1", 2, 4).await;

            let request = read_frame(&mut broker_stream).await;
            let (correlation_id, mut decoder) = request_header(&request);
            assert_eq!(correlation_id, 2);
            assert_eq!(decoder.read_compact_string().unwrap(), "orders");
            assert_eq!(decoder.read_compact_string().unwrap(), "member-1");
            assert_eq!(decoder.read_i32().unwrap(), 2);
            assert_eq!(decoder.read_i32().unwrap(), 4);
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_i32().unwrap(), 30_000);
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_compact_array_length(), Some(1));
            assert_eq!(decoder.read_compact_string().unwrap(), "subtopology-0");
            assert_eq!(decoder.read_compact_array_length(), Some(2));
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            decoder.read_tagged_fields().unwrap();
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_array_length(), Some(1));
            assert_eq!(decoder.read_compact_string().unwrap(), "subtopology-0");
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i64().unwrap(), 10);
            decoder.read_tagged_fields().unwrap();
            assert_eq!(decoder.read_compact_array_length(), Some(1));
            assert_eq!(decoder.read_compact_string().unwrap(), "subtopology-0");
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i64().unwrap(), 20);
            decoder.read_tagged_fields().unwrap();
            assert!(!decoder.read_bool().unwrap());
            decoder.read_tagged_fields().unwrap();
            write_streams_response(&mut broker_stream, correlation_id, "member-1", 3, 5).await;

            let request = read_frame(&mut broker_stream).await;
            let (correlation_id, mut decoder) = request_header(&request);
            assert_eq!(correlation_id, 3);
            assert_eq!(decoder.read_compact_string().unwrap(), "orders");
            assert_eq!(decoder.read_compact_string().unwrap(), "member-1");
            assert_eq!(decoder.read_i32().unwrap(), -1);
            assert_eq!(decoder.read_i32().unwrap(), 5);
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_i32().unwrap(), -1);
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert!(decoder.read_bool().unwrap());
            decoder.read_tagged_fields().unwrap();
            write_streams_response(&mut broker_stream, correlation_id, "member-1", -1, 5).await;
        });

        let config = StreamsGroupConfig::new(["localhost:9092"], "orders", topology())
            .client_id("streams-test")
            .instance_id("instance-a")
            .rack_id("rack-a")
            .process_id("process-a")
            .user_endpoint(StreamsGroupHeartbeatEndpoint {
                host: "query-host".to_owned(),
                port: 7_777,
            })
            .client_tags(vec![StreamsGroupHeartbeatKeyValue {
                key: "zone".to_owned(),
                value: "a".to_owned(),
            }])
            .max_retries(0);
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("streams-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let mut session = StreamsGroupSession {
            config,
            coordinator: Some(client),
            member_id: "client-member".to_owned(),
            member_epoch: 0,
            endpoint_information_epoch: 0,
            heartbeat_interval: Duration::from_secs(1),
            pending_active_tasks: None,
            pending_standby_tasks: None,
            pending_warmup_tasks: None,
            pending_task_offsets: None,
            pending_task_end_offsets: None,
            assignment: StreamsGroupSessionAssignment::default(),
            closed: false,
        };

        session.send_initial_heartbeat().await.unwrap();
        assert_eq!(session.member_id(), "member-1");
        assert_eq!(session.member_epoch(), 2);
        assert_eq!(session.assignment().task_offset_interval_ms, 100);
        assert_eq!(session.assignment().acceptable_recovery_lag, 0);
        assert!(session.assignment().active_tasks.is_none());
        session.set_task_state(
            vec![StreamsGroupHeartbeatTask {
                subtopology_id: "subtopology-0".to_owned(),
                partitions: vec![0, 1],
            }],
            Vec::new(),
            Vec::new(),
            vec![StreamsGroupHeartbeatTaskOffset {
                subtopology_id: "subtopology-0".to_owned(),
                partition: 0,
                offset: 10,
            }],
            vec![StreamsGroupHeartbeatTaskOffset {
                subtopology_id: "subtopology-0".to_owned(),
                partition: 0,
                offset: 20,
            }],
        );
        session.heartbeat().await.unwrap();
        assert_eq!(session.member_epoch(), 3);
        session.close().await.unwrap();
        assert!(session.is_closed());
        broker.await.unwrap();
    }

    fn request_header(request: &[u8]) -> (i32, Decoder<'_>) {
        let mut decoder = Decoder::new(request);
        assert_eq!(decoder.read_i16().unwrap(), 88);
        assert_eq!(decoder.read_i16().unwrap(), 0);
        let correlation_id = decoder.read_i32().unwrap();
        assert_eq!(
            decoder.read_nullable_string().unwrap(),
            Some("streams-test".to_owned())
        );
        decoder.read_tagged_fields().unwrap();
        (correlation_id, decoder)
    }

    async fn write_streams_response(
        stream: &mut tokio::io::DuplexStream,
        correlation_id: i32,
        member_id: &str,
        member_epoch: i32,
        endpoint_information_epoch: i32,
    ) {
        let mut response = Encoder::new();
        response.write_i32(correlation_id);
        response.write_empty_tagged_fields();
        response.write_i32(0);
        response.write_i16(0);
        response.write_compact_nullable_string(None).unwrap();
        response.write_compact_string(member_id).unwrap();
        response.write_i32(member_epoch);
        response.write_i32(1_000);
        response.write_i32(0);
        response.write_i32(100);
        response.write_unsigned_varint(0);
        response.write_unsigned_varint(0);
        response.write_unsigned_varint(0);
        response.write_unsigned_varint(0);
        response.write_i32(endpoint_information_epoch);
        response.write_unsigned_varint(0);
        response.write_empty_tagged_fields();
        write_frame(stream, &response.into_bytes()).await;
    }

    async fn read_frame(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
        let mut size = [0; 4];
        stream.read_exact(&mut size).await.unwrap();
        let size = usize::try_from(i32::from_be_bytes(size)).unwrap();
        let mut frame = vec![0; size];
        stream.read_exact(&mut frame).await.unwrap();
        frame
    }

    async fn write_frame(stream: &mut tokio::io::DuplexStream, frame: &[u8]) {
        stream
            .write_all(&(i32::try_from(frame.len()).unwrap()).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(frame).await.unwrap();
        stream.flush().await.unwrap();
    }
}
