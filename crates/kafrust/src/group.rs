use std::collections::{BTreeMap, BTreeSet};

use kafrust_protocol::api::find_coordinator::FindCoordinatorResponseV1;
use kafrust_protocol::api::join_group::JoinGroupMember;
use kafrust_protocol::api::metadata::MetadataResponseV1;
use kafrust_protocol::api::offset_commit::{
    OffsetCommitPartition, OffsetCommitTopic, OffsetCommitTopicResponse,
};
use kafrust_protocol::api::offset_fetch::{
    OffsetFetchPartitionResponse, OffsetFetchTopic, OffsetFetchTopicResponse,
};
use kafrust_protocol::api::sync_group::SyncGroupAssignment;
use kafrust_protocol::consumer_group::{
    ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0, ConsumerProtocolTopicAssignment,
};

use crate::client::Client;
use crate::config::ClientConfig;
use crate::consumer::{Consumer, ConsumerAssignment, ConsumerConfig, ConsumerRecord};
use crate::error::{Error, Result};

const PROTOCOL_TYPE: &str = "consumer";
const RANGE_PROTOCOL: &str = "range";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupConfig {
    client: ClientConfig,
    group_id: String,
    topics: Vec<String>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    retention_time_ms: i64,
    start_offset: i64,
    max_wait_ms: i32,
    min_bytes: i32,
    max_partition_bytes: i32,
    max_retries: u32,
}

impl ConsumerGroupConfig {
    pub fn new(
        bootstrap_servers: impl IntoIterator<Item = impl Into<String>>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            group_id: group_id.into(),
            topics: Vec::new(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            retention_time_ms: 86_400_000,
            start_offset: 0,
            max_wait_ms: 500,
            min_bytes: 1,
            max_partition_bytes: 1_048_576,
            max_retries: 1,
        }
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.client = self.client.request_timeout_ms(request_timeout_ms);
        self
    }

    pub fn subscribe(mut self, topic: impl Into<String>) -> Self {
        self.topics.push(topic.into());
        self
    }

    pub fn session_timeout_ms(mut self, session_timeout_ms: i32) -> Self {
        self.session_timeout_ms = session_timeout_ms;
        self
    }

    pub fn rebalance_timeout_ms(mut self, rebalance_timeout_ms: i32) -> Self {
        self.rebalance_timeout_ms = rebalance_timeout_ms;
        self
    }

    pub fn retention_time_ms(mut self, retention_time_ms: i64) -> Self {
        self.retention_time_ms = retention_time_ms;
        self
    }

    pub fn start_offset(mut self, start_offset: i64) -> Self {
        self.start_offset = start_offset;
        self
    }

    pub fn max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.max_wait_ms = max_wait_ms;
        self
    }

    pub fn min_bytes(mut self, min_bytes: i32) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    pub fn max_partition_bytes(mut self, max_partition_bytes: i32) -> Self {
        self.max_partition_bytes = max_partition_bytes;
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    pub async fn join(self) -> Result<ConsumerGroup> {
        if self.topics.is_empty() {
            return Err(Error::Unsupported("consumer group without subscriptions"));
        }

        let mut bootstrap = self.client.clone().connect().await?;
        let coordinator = bootstrap
            .find_group_coordinator(self.group_id.clone())
            .await?;
        if coordinator.error_code != 0 {
            return Err(Error::Broker {
                code: coordinator.error_code,
                context: format!("find group coordinator {}", self.group_id),
            });
        }

        let coordinator_addr = coordinator_addr(&coordinator);
        let mut coordinator_client = Client::connect_with_request_timeout(
            coordinator_addr,
            self.client.client_id_ref().map(str::to_owned),
            self.client.request_timeout(),
        )
        .await?;
        let subscription = ConsumerProtocolSubscriptionV0 {
            topics: self.topics.clone(),
            user_data: None,
        }
        .encode()?;
        let joined = coordinator_client
            .join_group_v2(
                self.group_id.clone(),
                self.session_timeout_ms,
                self.rebalance_timeout_ms,
                "",
                PROTOCOL_TYPE,
                vec![kafrust_protocol::api::join_group::JoinGroupProtocol {
                    name: RANGE_PROTOCOL.to_owned(),
                    metadata: subscription,
                }],
            )
            .await?;
        if joined.error_code != 0 {
            return Err(Error::Broker {
                code: joined.error_code,
                context: format!("join group {}", self.group_id),
            });
        }

        let assignments = if joined.member_id == joined.leader {
            let metadata = bootstrap.metadata(Some(self.topics.clone())).await?;
            range_assignments(&joined.members, &metadata)?
        } else {
            Vec::new()
        };
        let synced = coordinator_client
            .sync_group_v2(
                self.group_id.clone(),
                joined.generation_id,
                joined.member_id.clone(),
                assignments,
            )
            .await?;
        if synced.error_code != 0 {
            return Err(Error::Broker {
                code: synced.error_code,
                context: format!("sync group {}", self.group_id),
            });
        }

        let assignment = ConsumerProtocolAssignmentV0::decode(&synced.assignment)?;
        let consumer_assignments = assignments_from_protocol(
            &mut coordinator_client,
            &self.group_id,
            self.start_offset,
            &assignment,
        )
        .await?;
        let mut consumer_config =
            ConsumerConfig::new(self.client.bootstrap_servers().iter().cloned())
                .max_wait_ms(self.max_wait_ms)
                .min_bytes(self.min_bytes)
                .max_partition_bytes(self.max_partition_bytes)
                .max_retries(self.max_retries);
        if let Some(client_id) = self.client.client_id_ref() {
            consumer_config = consumer_config.client_id(client_id);
        }
        consumer_config =
            consumer_config.request_timeout_ms(duration_millis(self.client.request_timeout()));
        let consumer_client = self.client.clone().connect().await?;

        Ok(ConsumerGroup {
            group_id: self.group_id,
            generation_id: joined.generation_id,
            member_id: joined.member_id,
            retention_time_ms: self.retention_time_ms,
            coordinator: coordinator_client,
            consumer: Consumer::from_assignments(
                consumer_client,
                consumer_config,
                consumer_assignments,
            ),
        })
    }
}

#[derive(Debug)]
pub struct ConsumerGroup {
    group_id: String,
    generation_id: i32,
    member_id: String,
    retention_time_ms: i64,
    coordinator: Client,
    consumer: Consumer,
}

impl ConsumerGroup {
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    pub fn assignments(&self) -> &[ConsumerAssignment] {
        self.consumer.assignments()
    }

    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        self.heartbeat().await?;
        self.consumer.poll().await
    }

    pub async fn heartbeat(&mut self) -> Result<()> {
        let response = self
            .coordinator
            .heartbeat_v2(
                self.group_id.clone(),
                self.generation_id,
                self.member_id.clone(),
            )
            .await?;
        if response.error_code != 0 {
            return Err(Error::Broker {
                code: response.error_code,
                context: format!("heartbeat group {}", self.group_id),
            });
        }
        Ok(())
    }

    pub async fn commit_offsets(&mut self) -> Result<()> {
        let topics = offset_commit_topics(self.consumer.assignments());
        let response = self
            .coordinator
            .offset_commit_v2(
                self.group_id.clone(),
                self.generation_id,
                self.member_id.clone(),
                self.retention_time_ms,
                topics,
            )
            .await?;
        check_offset_commit_response(&self.group_id, &response.topics)
    }
}

pub(crate) fn range_assignments(
    members: &[JoinGroupMember],
    metadata: &MetadataResponseV1,
) -> Result<Vec<SyncGroupAssignment>> {
    let mut subscriptions_by_topic = BTreeMap::<String, Vec<String>>::new();
    for member in members {
        let subscription = ConsumerProtocolSubscriptionV0::decode(&member.metadata)?;
        for topic in &subscription.topics {
            subscriptions_by_topic
                .entry(topic.clone())
                .or_default()
                .push(member.member_id.clone());
        }
    }

    let mut assigned_by_member = BTreeMap::<String, BTreeMap<String, Vec<i32>>>::new();
    for member in members {
        assigned_by_member.insert(member.member_id.clone(), BTreeMap::new());
    }

    for (topic, mut topic_members) in subscriptions_by_topic {
        topic_members.sort();
        topic_members.dedup();

        let partitions = partitions_for(metadata, &topic)?;
        for (member_id, partitions) in range_for_topic(&topic_members, &partitions) {
            assigned_by_member
                .entry(member_id)
                .or_default()
                .insert(topic.clone(), partitions);
        }
    }

    assigned_by_member
        .into_iter()
        .map(|(member_id, topics)| {
            let assignments = topics
                .into_iter()
                .map(|(topic, partitions)| ConsumerProtocolTopicAssignment { topic, partitions })
                .collect();
            Ok(SyncGroupAssignment {
                member_id,
                assignment: ConsumerProtocolAssignmentV0 {
                    assignments,
                    user_data: None,
                }
                .encode()?,
            })
        })
        .collect()
}

async fn assignments_from_protocol(
    coordinator: &mut Client,
    group_id: &str,
    start_offset: i64,
    assignment: &ConsumerProtocolAssignmentV0,
) -> Result<Vec<ConsumerAssignment>> {
    let mut assignments = Vec::new();
    let offsets = coordinator
        .offset_fetch_v2(group_id.to_owned(), Some(offset_fetch_topics(assignment)))
        .await?;
    if offsets.error_code != 0 {
        return Err(Error::Broker {
            code: offsets.error_code,
            context: format!("offset fetch group {group_id}"),
        });
    }

    for topic in &assignment.assignments {
        for partition in &topic.partitions {
            let next_offset =
                committed_offset(group_id, &offsets.topics, &topic.topic, *partition)?
                    .filter(|offset| *offset >= 0)
                    .unwrap_or(start_offset);
            assignments.push(ConsumerAssignment::new(
                topic.topic.clone(),
                *partition,
                next_offset,
            ));
        }
    }
    Ok(assignments)
}

fn offset_fetch_topics(assignment: &ConsumerProtocolAssignmentV0) -> Vec<OffsetFetchTopic> {
    assignment
        .assignments
        .iter()
        .map(|topic| OffsetFetchTopic {
            name: topic.topic.clone(),
            partition_indexes: topic.partitions.clone(),
        })
        .collect()
}

fn committed_offset(
    group_id: &str,
    topics: &[OffsetFetchTopicResponse],
    topic_name: &str,
    partition_index: i32,
) -> Result<Option<i64>> {
    let Some(partition) = partition_response(topics, topic_name, partition_index) else {
        return Ok(None);
    };
    if partition.error_code != 0 {
        return Err(Error::Broker {
            code: partition.error_code,
            context: format!("offset fetch group {group_id} {topic_name}-{partition_index}"),
        });
    }
    Ok(Some(partition.committed_offset))
}

fn partition_response<'a>(
    topics: &'a [OffsetFetchTopicResponse],
    topic_name: &str,
    partition_index: i32,
) -> Option<&'a OffsetFetchPartitionResponse> {
    topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition_index == partition_index)
        })
}

fn offset_commit_topics(assignments: &[ConsumerAssignment]) -> Vec<OffsetCommitTopic> {
    let mut topics = BTreeMap::<String, Vec<OffsetCommitPartition>>::new();
    for assignment in assignments {
        topics
            .entry(assignment.topic().to_owned())
            .or_default()
            .push(OffsetCommitPartition {
                partition_index: assignment.partition(),
                committed_offset: assignment.next_offset(),
                committed_metadata: None,
            });
    }

    topics
        .into_iter()
        .map(|(name, mut partitions)| {
            partitions.sort_by_key(|partition| partition.partition_index);
            OffsetCommitTopic { name, partitions }
        })
        .collect()
}

fn check_offset_commit_response(
    group_id: &str,
    topics: &[OffsetCommitTopicResponse],
) -> Result<()> {
    for topic in topics {
        for partition in &topic.partitions {
            if partition.error_code != 0 {
                return Err(Error::Broker {
                    code: partition.error_code,
                    context: format!(
                        "offset commit group {group_id} {}-{}",
                        topic.name, partition.partition_index
                    ),
                });
            }
        }
    }
    Ok(())
}

fn coordinator_addr(coordinator: &FindCoordinatorResponseV1) -> String {
    format!("{}:{}", coordinator.host, coordinator.port)
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn partitions_for(metadata: &MetadataResponseV1, topic_name: &str) -> Result<Vec<i32>> {
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic_name.to_owned(),
            partition: -1,
        })?;

    let mut partitions = topic
        .partitions
        .iter()
        .map(|partition| partition.partition_index)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    partitions.sort();
    Ok(partitions)
}

fn range_for_topic(members: &[String], partitions: &[i32]) -> Vec<(String, Vec<i32>)> {
    if members.is_empty() {
        return Vec::new();
    }

    let partition_count = partitions.len();
    let member_count = members.len();
    let partitions_per_member = partition_count / member_count;
    let extra_partitions = partition_count % member_count;

    members
        .iter()
        .enumerate()
        .map(|(index, member)| {
            let start = partitions_per_member * index + extra_partitions.min(index);
            let length = partitions_per_member + usize::from(index < extra_partitions);
            let end = start + length;
            (member.clone(), partitions[start..end].to_vec())
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        check_offset_commit_response, committed_offset, offset_commit_topics, offset_fetch_topics,
        range_assignments, ConsumerGroupConfig,
    };
    use crate::consumer::ConsumerAssignment;
    use crate::Error;
    use kafrust_protocol::api::join_group::JoinGroupMember;
    use kafrust_protocol::api::metadata::{MetadataResponseV1, PartitionMetadata, TopicMetadata};
    use kafrust_protocol::api::offset_commit::{
        OffsetCommitPartitionResponse, OffsetCommitTopicResponse,
    };
    use kafrust_protocol::api::offset_fetch::{
        OffsetFetchPartitionResponse, OffsetFetchTopicResponse,
    };
    use kafrust_protocol::consumer_group::{
        ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0,
        ConsumerProtocolTopicAssignment,
    };

    #[test]
    fn builds_consumer_group_config() {
        let config = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
            .client_id("orders-reader")
            .request_timeout_ms(5_000)
            .subscribe("orders")
            .session_timeout_ms(8_000)
            .rebalance_timeout_ms(20_000)
            .retention_time_ms(60_000)
            .start_offset(5)
            .max_wait_ms(250)
            .min_bytes(10)
            .max_partition_bytes(1024)
            .max_retries(3);

        assert_eq!(config.group_id(), "orders-group");
        assert_eq!(config.topics(), &["orders".to_owned()]);
    }

    #[test]
    fn assigns_partitions_with_range_strategy() {
        let members = vec![
            member("member-b", &["orders"]),
            member("member-a", &["orders"]),
        ];
        let metadata = metadata_fixture("orders", &[0, 1, 2]);

        let assignments = range_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        let member_b = decode_assignment(&assignments[1].assignment);

        assert_eq!(assignments[0].member_id, "member-a");
        assert_eq!(member_a.assignments[0].topic, "orders");
        assert_eq!(member_a.assignments[0].partitions, vec![0, 1]);
        assert_eq!(assignments[1].member_id, "member-b");
        assert_eq!(member_b.assignments[0].topic, "orders");
        assert_eq!(member_b.assignments[0].partitions, vec![2]);
    }

    #[test]
    fn assigns_only_subscribed_members_per_topic() {
        let members = vec![
            member("member-a", &["orders", "payments"]),
            member("member-b", &["orders"]),
        ];
        let mut metadata = metadata_fixture("orders", &[0, 1]);
        metadata.topics.push(topic_metadata("payments", &[0, 1, 2]));

        let assignments = range_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        assert_eq!(member_a.assignments[0].topic, "orders");
        assert_eq!(member_a.assignments[0].partitions, vec![0]);
        assert_eq!(member_a.assignments[1].topic, "payments");
        assert_eq!(member_a.assignments[1].partitions, vec![0, 1, 2]);
    }

    #[test]
    fn builds_offset_fetch_topics_from_assignment() {
        let assignment = ConsumerProtocolAssignmentV0 {
            assignments: vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![0, 2],
            }],
            user_data: None,
        };

        let topics = offset_fetch_topics(&assignment);

        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "orders");
        assert_eq!(topics[0].partition_indexes, vec![0, 2]);
    }

    #[test]
    fn reads_committed_offset_and_surfaces_partition_error() {
        let topics = vec![OffsetFetchTopicResponse {
            name: "orders".to_owned(),
            partitions: vec![
                OffsetFetchPartitionResponse {
                    partition_index: 0,
                    committed_offset: 42,
                    metadata: None,
                    error_code: 0,
                },
                OffsetFetchPartitionResponse {
                    partition_index: 1,
                    committed_offset: -1,
                    metadata: None,
                    error_code: 25,
                },
            ],
        }];

        assert_eq!(
            committed_offset("orders-group", &topics, "orders", 0).unwrap(),
            Some(42)
        );
        assert_eq!(
            committed_offset("orders-group", &topics, "orders", 2).unwrap(),
            None
        );
        assert!(matches!(
            committed_offset("orders-group", &topics, "orders", 1).unwrap_err(),
            Error::Broker { code: 25, .. }
        ));
    }

    #[test]
    fn builds_offset_commit_topics_from_current_assignment_offsets() {
        let assignments = vec![
            ConsumerAssignment::new("orders".to_owned(), 1, 43),
            ConsumerAssignment::new("orders".to_owned(), 0, 11),
            ConsumerAssignment::new("payments".to_owned(), 0, 7),
        ];

        let topics = offset_commit_topics(&assignments);

        assert_eq!(topics[0].name, "orders");
        assert_eq!(topics[0].partitions[0].partition_index, 0);
        assert_eq!(topics[0].partitions[0].committed_offset, 11);
        assert_eq!(topics[0].partitions[1].partition_index, 1);
        assert_eq!(topics[1].name, "payments");
    }

    #[test]
    fn surfaces_offset_commit_partition_error() {
        let response = vec![OffsetCommitTopicResponse {
            name: "orders".to_owned(),
            partitions: vec![OffsetCommitPartitionResponse {
                partition_index: 0,
                error_code: 25,
            }],
        }];

        assert!(matches!(
            check_offset_commit_response("orders-group", &response).unwrap_err(),
            Error::Broker { code: 25, .. }
        ));
    }

    fn member(member_id: &str, topics: &[&str]) -> JoinGroupMember {
        JoinGroupMember {
            member_id: member_id.to_owned(),
            metadata: ConsumerProtocolSubscriptionV0 {
                topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
                user_data: None,
            }
            .encode()
            .unwrap(),
        }
    }

    fn decode_assignment(bytes: &[u8]) -> ConsumerProtocolAssignmentV0 {
        ConsumerProtocolAssignmentV0::decode(bytes).unwrap()
    }

    fn metadata_fixture(topic: &str, partitions: &[i32]) -> MetadataResponseV1 {
        MetadataResponseV1 {
            brokers: Vec::new(),
            controller_id: 1,
            topics: vec![topic_metadata(topic, partitions)],
        }
    }

    fn topic_metadata(topic: &str, partitions: &[i32]) -> TopicMetadata {
        TopicMetadata {
            error_code: 0,
            name: topic.to_owned(),
            is_internal: false,
            partitions: partitions
                .iter()
                .map(|partition| PartitionMetadata {
                    error_code: 0,
                    partition_index: *partition,
                    leader_id: 1,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                })
                .collect(),
        }
    }
}
