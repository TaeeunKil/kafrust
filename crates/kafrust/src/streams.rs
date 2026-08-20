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
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::debug;

const STREAMS_GROUP_HEARTBEAT_API_KEY: i16 = 88;
const DEFAULT_STREAMS_GROUP_MAX_RETRIES: u32 = 5;
const STREAMS_GROUP_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const STREAMS_GROUP_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(1);
const STREAMS_GROUP_COMMAND_CAPACITY: usize = 16;

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

enum StreamsGroupCommand {
    SetTaskState {
        active_tasks: Vec<StreamsGroupHeartbeatTask>,
        standby_tasks: Vec<StreamsGroupHeartbeatTask>,
        warmup_tasks: Vec<StreamsGroupHeartbeatTask>,
        task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
        task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
        acknowledged: oneshot::Sender<()>,
    },
    Heartbeat {
        response: oneshot::Sender<Result<StreamsGroupHeartbeatResponseV0>>,
    },
    Close,
}

/// A handle for a Streams session whose heartbeat lifecycle is owned by one
/// background Tokio task.
///
/// The task owns [`StreamsGroupSession`] and is the only code that mutates its
/// member epoch, coordinator connection, pending task state, and assignment.
/// Commands use a bounded channel, so task-state updates apply backpressure
/// instead of growing an unbounded queue. Call [`Self::close`] and await it for
/// a graceful member-epoch `-1` leave; dropping the handle aborts the task and
/// cannot provide that broker-side leave guarantee.
#[must_use = "a StreamsGroupSessionHandle must be closed or kept alive"]
pub struct StreamsGroupSessionHandle {
    commands: mpsc::Sender<StreamsGroupCommand>,
    assignment: watch::Receiver<StreamsGroupSessionAssignment>,
    task: Option<JoinHandle<Result<()>>>,
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

/// The role a Kafka Streams task has on this member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StreamsTaskRole {
    /// The task actively processes its input partitions.
    Active,
    /// The task maintains standby state without processing records actively.
    Standby,
    /// The task is warming up before it can become active.
    Warmup,
}

impl StreamsTaskRole {
    /// Returns the stable role name used in diagnostics and logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Standby => "standby",
            Self::Warmup => "warmup",
        }
    }
}

/// The canonical identity of one Kafka Streams task assignment.
///
/// Kafka identifies the task by its subtopology and the input partitions it
/// processes. Partition order is not semantic, so this type stores partitions
/// in sorted order and rejects duplicates or negative partition indexes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamsTaskId {
    subtopology_id: String,
    partitions: Vec<i32>,
}

impl StreamsTaskId {
    /// Creates a canonical task identity from a subtopology and partitions.
    pub fn new(subtopology_id: impl Into<String>, mut partitions: Vec<i32>) -> Result<Self> {
        let subtopology_id = subtopology_id.into();
        if subtopology_id.trim().is_empty() {
            return Err(Error::StreamsTaskAssignmentInvalid {
                subtopology_id,
                reason: "subtopology ID must not be empty",
            });
        }
        if partitions.is_empty() {
            return Err(Error::StreamsTaskAssignmentInvalid {
                subtopology_id,
                reason: "task must contain at least one partition",
            });
        }
        if partitions.iter().any(|partition| *partition < 0) {
            return Err(Error::StreamsTaskAssignmentInvalid {
                subtopology_id,
                reason: "task partitions must not be negative",
            });
        }
        partitions.sort_unstable();
        if partitions.windows(2).any(|window| window[0] == window[1]) {
            return Err(Error::StreamsTaskAssignmentInvalid {
                subtopology_id,
                reason: "task partitions must not contain duplicates",
            });
        }
        Ok(Self {
            subtopology_id,
            partitions,
        })
    }

    fn from_heartbeat_task(task: &StreamsGroupHeartbeatTask) -> Result<Self> {
        Self::new(task.subtopology_id.clone(), task.partitions.clone())
    }

    /// Returns the subtopology identifier.
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Returns the canonical, sorted input partitions.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// A task and its currently reconciled role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsTaskAssignment {
    /// Canonical task identity.
    pub task: StreamsTaskId,
    /// Role currently held by the task.
    pub role: StreamsTaskRole,
}

/// A deterministic lifecycle change produced by [`StreamsTaskRuntime`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsTaskTransition {
    /// A task became assigned to this member.
    Added {
        /// Canonical task identity.
        task: StreamsTaskId,
        /// New task role.
        role: StreamsTaskRole,
    },
    /// A task was removed from this member.
    Removed {
        /// Canonical task identity.
        task: StreamsTaskId,
        /// Role held before removal.
        role: StreamsTaskRole,
    },
    /// A task stayed on this member but changed role.
    RoleChanged {
        /// Canonical task identity.
        task: StreamsTaskId,
        /// Role held before the assignment update.
        previous_role: StreamsTaskRole,
        /// Role after the assignment update.
        role: StreamsTaskRole,
    },
}

/// Bounded, deterministic reconciliation state for Streams task assignments.
///
/// This type intentionally stops at assignment lifecycle. It does not spawn
/// processors, own consumer assignments, or manage state stores. Applications
/// can apply the returned transitions to those components while keeping Kafka's
/// nullable response semantics and task identity rules in one place.
#[derive(Debug, Default)]
pub struct StreamsTaskRuntime {
    tasks: BTreeMap<StreamsTaskId, StreamsTaskRole>,
}

impl StreamsTaskRuntime {
    /// Creates an empty task runtime.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current task assignment in canonical order.
    pub fn assignment(&self) -> Vec<StreamsTaskAssignment> {
        self.tasks
            .iter()
            .map(|(task, role)| StreamsTaskAssignment {
                task: task.clone(),
                role: *role,
            })
            .collect()
    }

    /// Returns the current role for a task, if it is assigned.
    pub fn role(&self, task: &StreamsTaskId) -> Option<StreamsTaskRole> {
        self.tasks.get(task).copied()
    }

    /// Applies the changed task-role fields from a broker assignment.
    ///
    /// A `None` role field means that Kafka did not change that role since the
    /// previous heartbeat and is therefore retained. `Some(Vec::new())` is an
    /// explicit revocation of that role. The returned transitions are sorted by
    /// canonical task identity and the runtime is left unchanged if validation
    /// fails.
    pub fn reconcile_assignment(
        &mut self,
        assignment: &StreamsGroupSessionAssignment,
    ) -> Result<Vec<StreamsTaskTransition>> {
        let mut desired = self.tasks.clone();
        let updates = [
            (StreamsTaskRole::Active, assignment.active_tasks.as_ref()),
            (StreamsTaskRole::Standby, assignment.standby_tasks.as_ref()),
            (StreamsTaskRole::Warmup, assignment.warmup_tasks.as_ref()),
        ];

        for (role, tasks) in updates {
            if tasks.is_some() {
                desired.retain(|_, current_role| *current_role != role);
            }
        }

        let mut occupied = BTreeSet::new();
        for task in desired.keys() {
            for partition in task.partitions() {
                if !occupied.insert((task.subtopology_id().to_owned(), *partition)) {
                    return Err(Error::StreamsTaskAssignmentConflict {
                        subtopology_id: task.subtopology_id().to_owned(),
                        partition: *partition,
                    });
                }
            }
        }

        for (role, tasks) in updates {
            let Some(tasks) = tasks else {
                continue;
            };
            for task in tasks {
                let task = StreamsTaskId::from_heartbeat_task(task)?;
                for partition in task.partitions() {
                    if !occupied.insert((task.subtopology_id().to_owned(), *partition)) {
                        return Err(Error::StreamsTaskAssignmentConflict {
                            subtopology_id: task.subtopology_id().to_owned(),
                            partition: *partition,
                        });
                    }
                }
                if desired.insert(task.clone(), role).is_some() {
                    return Err(Error::StreamsTaskAssignmentConflict {
                        subtopology_id: task.subtopology_id().to_owned(),
                        partition: task.partitions()[0],
                    });
                }
            }
        }

        let mut transitions = Vec::new();
        for (task, previous_role) in &self.tasks {
            match desired.get(task) {
                None => transitions.push(StreamsTaskTransition::Removed {
                    task: task.clone(),
                    role: *previous_role,
                }),
                Some(role) if role != previous_role => {
                    transitions.push(StreamsTaskTransition::RoleChanged {
                        task: task.clone(),
                        previous_role: *previous_role,
                        role: *role,
                    });
                }
                Some(_) => {}
            }
        }
        for (task, role) in &desired {
            if !self.tasks.contains_key(task) {
                transitions.push(StreamsTaskTransition::Added {
                    task: task.clone(),
                    role: *role,
                });
            }
        }
        self.tasks = desired;
        Ok(transitions)
    }
}

impl StreamsGroupSessionHandle {
    /// Reconciles the latest broker assignment into an application-owned task runtime.
    pub fn reconcile_task_runtime(
        &self,
        runtime: &mut StreamsTaskRuntime,
    ) -> Result<Vec<StreamsTaskTransition>> {
        runtime.reconcile_assignment(&self.assignment())
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
        self.set_task_state_with_optional_offsets(
            active_tasks,
            standby_tasks,
            warmup_tasks,
            Some(task_offsets),
            Some(task_end_offsets),
        );
    }

    /// Replaces task state while optionally omitting changelog offsets.
    ///
    /// Kafka 4.3 currently rejects non-null task-offset fields for some Streams
    /// group configurations. Passing `None` preserves the protocol's nullable
    /// field semantics and lets the broker request offsets when supported.
    pub fn set_task_state_with_optional_offsets(
        &mut self,
        active_tasks: Vec<StreamsGroupHeartbeatTask>,
        standby_tasks: Vec<StreamsGroupHeartbeatTask>,
        warmup_tasks: Vec<StreamsGroupHeartbeatTask>,
        task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
        task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    ) {
        self.pending_active_tasks = Some(active_tasks);
        self.pending_standby_tasks = Some(standby_tasks);
        self.pending_warmup_tasks = Some(warmup_tasks);
        self.pending_task_offsets = task_offsets;
        self.pending_task_end_offsets = task_end_offsets;
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

    /// Moves this session into a background heartbeat task.
    ///
    /// The session must already be joined. The task sends heartbeats using the
    /// interval most recently returned by Kafka, publishes every successful
    /// assignment through [`StreamsGroupSessionHandle::subscribe_assignment`],
    /// and preserves the existing bounded retry and rejoin behavior.
    pub fn spawn_heartbeat_task(self) -> StreamsGroupSessionHandle {
        let (commands, command_receiver) = mpsc::channel(STREAMS_GROUP_COMMAND_CAPACITY);
        let (assignment_sender, assignment_receiver) = watch::channel(self.assignment.clone());
        let task = tokio::spawn(run_streams_heartbeat_task(
            self,
            command_receiver,
            assignment_sender,
        ));
        StreamsGroupSessionHandle {
            commands,
            assignment: assignment_receiver,
            task: Some(task),
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
                task_offsets: None,
                task_end_offsets: None,
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

impl StreamsGroupSessionHandle {
    /// Returns the latest successful broker assignment snapshot.
    pub fn assignment(&self) -> StreamsGroupSessionAssignment {
        self.assignment.borrow().clone()
    }

    /// Subscribes to successful assignment snapshots produced by the heartbeat
    /// task. The receiver always retains the newest snapshot when the caller is
    /// temporarily slower than the heartbeat interval.
    pub fn subscribe_assignment(&self) -> watch::Receiver<StreamsGroupSessionAssignment> {
        self.assignment.clone()
    }

    /// Replaces the task state reported by the next background heartbeat.
    pub async fn set_task_state(
        &self,
        active_tasks: Vec<StreamsGroupHeartbeatTask>,
        standby_tasks: Vec<StreamsGroupHeartbeatTask>,
        warmup_tasks: Vec<StreamsGroupHeartbeatTask>,
        task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
        task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    ) -> Result<()> {
        let (acknowledged, completion) = oneshot::channel();
        self.commands
            .send(StreamsGroupCommand::SetTaskState {
                active_tasks,
                standby_tasks,
                warmup_tasks,
                task_offsets,
                task_end_offsets,
                acknowledged,
            })
            .await
            .map_err(|_| Error::StreamsGroupBackgroundTaskClosed)?;
        completion
            .await
            .map_err(|_| Error::StreamsGroupBackgroundTaskClosed)
    }

    /// Sends a heartbeat immediately instead of waiting for the broker's
    /// advertised interval and returns its response.
    pub async fn heartbeat_now(&self) -> Result<StreamsGroupHeartbeatResponseV0> {
        let (response, completion) = oneshot::channel();
        self.commands
            .send(StreamsGroupCommand::Heartbeat { response })
            .await
            .map_err(|_| Error::StreamsGroupBackgroundTaskClosed)?;
        completion
            .await
            .map_err(|_| Error::StreamsGroupBackgroundTaskClosed)?
    }

    /// Gracefully leaves the Streams group and waits for the background task to
    /// finish. This consumes the handle so no later command can race with the
    /// leave operation.
    pub async fn close(mut self) -> Result<()> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        if self
            .commands
            .send(StreamsGroupCommand::Close)
            .await
            .is_err()
        {
            return task.await.map_err(Error::from)?;
        }
        task.await.map_err(Error::from)?
    }
}

impl Drop for StreamsGroupSessionHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }
}

async fn run_streams_heartbeat_task(
    mut session: StreamsGroupSession,
    mut commands: mpsc::Receiver<StreamsGroupCommand>,
    assignment_sender: watch::Sender<StreamsGroupSessionAssignment>,
) -> Result<()> {
    loop {
        let heartbeat = sleep(session.heartbeat_interval());
        tokio::pin!(heartbeat);
        tokio::select! {
            command = commands.recv() => match command {
                Some(StreamsGroupCommand::SetTaskState {
                    active_tasks,
                    standby_tasks,
                    warmup_tasks,
                    task_offsets,
                    task_end_offsets,
                    acknowledged,
                }) => {
                    session.set_task_state_with_optional_offsets(
                        active_tasks,
                        standby_tasks,
                        warmup_tasks,
                        task_offsets,
                        task_end_offsets,
                    );
                    let _ = acknowledged.send(());
                }
                Some(StreamsGroupCommand::Heartbeat { response }) => {
                    let result = session.heartbeat().await;
                    if result.is_ok() {
                        let _ = assignment_sender.send(session.assignment.clone());
                    }
                    let _ = response.send(result);
                }
                Some(StreamsGroupCommand::Close) => return session.close().await,
                None => return Ok(()),
            },
            _ = &mut heartbeat => {
                session.heartbeat().await?;
                let _ = assignment_sender.send(session.assignment.clone());
            }
        }
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
        StreamsTaskRole, StreamsTaskRuntime, StreamsTaskTransition,
        STREAMS_GROUP_MAX_RETRY_BACKOFF,
    };
    use crate::client::Client;
    use crate::error::Error;
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
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_array_length(), None);
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

    #[tokio::test]
    async fn background_streams_session_owns_heartbeat_and_graceful_close() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(16 * 1024);
        let broker = tokio::spawn(async move {
            let request = read_frame(&mut broker_stream).await;
            let (correlation_id, mut decoder) = request_header(&request);
            assert_eq!(decoder.read_compact_string().unwrap(), "orders");
            assert_eq!(decoder.read_compact_string().unwrap(), "member-1");
            assert_eq!(decoder.read_i32().unwrap(), 2);
            assert_eq!(decoder.read_i32().unwrap(), 4);
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_i32().unwrap(), 30_000);
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_nullable_string().unwrap(), None);
            assert_eq!(decoder.read_i8().unwrap(), -1);
            assert_eq!(decoder.read_compact_array_length(), None);
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert_eq!(decoder.read_compact_array_length(), Some(0));
            assert!(!decoder.read_bool().unwrap());
            decoder.read_tagged_fields().unwrap();
            write_streams_response(&mut broker_stream, correlation_id, "member-1", 3, 5).await;

            let request = read_frame(&mut broker_stream).await;
            let (correlation_id, mut decoder) = request_header(&request);
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
            .max_retries(0);
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("streams-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let session = StreamsGroupSession {
            config,
            coordinator: Some(client),
            member_id: "member-1".to_owned(),
            member_epoch: 2,
            endpoint_information_epoch: 4,
            heartbeat_interval: Duration::from_millis(100),
            pending_active_tasks: None,
            pending_standby_tasks: None,
            pending_warmup_tasks: None,
            pending_task_offsets: None,
            pending_task_end_offsets: None,
            assignment: StreamsGroupSessionAssignment::default(),
            closed: false,
        };

        let handle = session.spawn_heartbeat_task();
        handle
            .set_task_state(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Some(Vec::new()),
                Some(Vec::new()),
            )
            .await
            .unwrap();
        let mut assignments = handle.subscribe_assignment();
        tokio::time::timeout(Duration::from_secs(2), assignments.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handle.assignment().task_offset_interval_ms, 100);
        handle.close().await.unwrap();
        broker.await.unwrap();
    }

    fn assignment(
        active_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
        standby_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
        warmup_tasks: Option<Vec<StreamsGroupHeartbeatTask>>,
    ) -> StreamsGroupSessionAssignment {
        StreamsGroupSessionAssignment {
            active_tasks,
            standby_tasks,
            warmup_tasks,
            ..StreamsGroupSessionAssignment::default()
        }
    }

    fn task(subtopology_id: &str, partitions: &[i32]) -> StreamsGroupHeartbeatTask {
        StreamsGroupHeartbeatTask {
            subtopology_id: subtopology_id.to_owned(),
            partitions: partitions.to_vec(),
        }
    }

    #[test]
    fn task_runtime_reconciles_nullable_roles_and_canonical_task_ids() {
        let mut runtime = StreamsTaskRuntime::new();
        let transitions = runtime
            .reconcile_assignment(&assignment(
                Some(vec![task("subtopology-0", &[2, 1])]),
                Some(vec![task("subtopology-0", &[3])]),
                Some(Vec::new()),
            ))
            .unwrap();
        assert_eq!(transitions.len(), 2);
        assert!(matches!(
            &transitions[0],
            StreamsTaskTransition::Added {
                role: StreamsTaskRole::Active,
                ..
            }
        ));
        assert!(matches!(
            &transitions[1],
            StreamsTaskTransition::Added {
                role: StreamsTaskRole::Standby,
                ..
            }
        ));
        assert_eq!(runtime.assignment()[0].task.partitions(), &[1, 2]);

        let unchanged = runtime
            .reconcile_assignment(&assignment(None, None, None))
            .unwrap();
        assert!(unchanged.is_empty());

        let transitions = runtime
            .reconcile_assignment(&assignment(
                Some(Vec::new()),
                Some(vec![task("subtopology-0", &[2, 1])]),
                None,
            ))
            .unwrap();
        assert_eq!(
            transitions,
            vec![
                StreamsTaskTransition::RoleChanged {
                    task: super::StreamsTaskId::new("subtopology-0", vec![1, 2]).unwrap(),
                    previous_role: StreamsTaskRole::Active,
                    role: StreamsTaskRole::Standby,
                },
                StreamsTaskTransition::Removed {
                    task: super::StreamsTaskId::new("subtopology-0", vec![3]).unwrap(),
                    role: StreamsTaskRole::Standby,
                },
            ]
        );
    }

    #[test]
    fn task_runtime_rejects_conflicting_assignment_without_mutating_state() {
        let mut runtime = StreamsTaskRuntime::new();
        runtime
            .reconcile_assignment(&assignment(
                Some(vec![task("subtopology-0", &[0])]),
                Some(Vec::new()),
                Some(Vec::new()),
            ))
            .unwrap();
        let before = runtime.assignment();

        let error = runtime
            .reconcile_assignment(&assignment(
                Some(vec![
                    task("subtopology-0", &[1]),
                    task("subtopology-0", &[1]),
                ]),
                Some(Vec::new()),
                Some(Vec::new()),
            ))
            .unwrap_err();
        assert!(matches!(
            error,
            Error::StreamsTaskAssignmentConflict {
                subtopology_id,
                partition: 1,
            } if subtopology_id == "subtopology-0"
        ));
        assert_eq!(runtime.assignment(), before);
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
