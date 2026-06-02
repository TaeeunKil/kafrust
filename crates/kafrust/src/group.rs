use std::collections::{BTreeMap, BTreeSet};

use kafrust_protocol::api::join_group::JoinGroupMember;
use kafrust_protocol::api::metadata::MetadataResponseV1;
use kafrust_protocol::api::sync_group::SyncGroupAssignment;
use kafrust_protocol::consumer_group::{
    ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0, ConsumerProtocolTopicAssignment,
};

use crate::error::{Error, Result};

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
    use super::range_assignments;
    use kafrust_protocol::api::join_group::JoinGroupMember;
    use kafrust_protocol::api::metadata::{MetadataResponseV1, PartitionMetadata, TopicMetadata};
    use kafrust_protocol::consumer_group::{
        ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0,
    };

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
