mod common;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use kafrust::{
    AddRaftVoterOptions, AdminClient, ClientConfig, DescribeQuorumPartitionResult,
    DescribeQuorumTopic, Error, RaftVoterListener, RemoveRaftVoterOptions,
};
use std::time::Duration;

const METADATA_TOPIC: &str = "__cluster_metadata";

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let mut config =
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-dynamic-quorum-example");
    if let Some(servers) = common::controller_bootstrap_servers_from_env() {
        config = config.controller_bootstrap_servers(servers);
    }
    let config = common::apply_security(config)?;
    let admin = AdminClient::new(config);

    let cluster_id =
        std::env::var("KAFRUST_CLUSTER_ID").map_err(|_| Error::InvalidConfiguration {
            field: "KAFRUST_CLUSTER_ID",
            reason: "the dynamic quorum example requires the Kafka cluster ID",
        })?;
    let voter_id = parse_i32_env("KAFRUST_NEW_VOTER_ID", 2)?;
    let directory_id = parse_directory_id(
        &std::env::var("KAFRUST_NEW_VOTER_DIRECTORY_ID").map_err(|_| {
            Error::InvalidConfiguration {
                field: "KAFRUST_NEW_VOTER_DIRECTORY_ID",
                reason: "the new controller directory UUID is required",
            }
        })?,
    )?;
    let listener = parse_listener(&std::env::var("KAFRUST_NEW_VOTER_LISTENER").map_err(|_| {
        Error::InvalidConfiguration {
            field: "KAFRUST_NEW_VOTER_LISTENER",
            reason: "the new controller listener is required",
        }
    })?)?;

    let before = describe_metadata_quorum(&admin).await?;
    println!(
        "before voters={} observers={} leader={}",
        before.current_voters().len(),
        before.observers().len(),
        before.leader_id(),
    );
    if before
        .current_voters()
        .iter()
        .any(|voter| voter.replica_id() == voter_id)
    {
        return Err(Error::InvalidConfiguration {
            field: "KAFRUST_NEW_VOTER_ID",
            reason: "the requested voter is already in the quorum",
        });
    }

    let add = admin
        .add_raft_voter(
            AddRaftVoterOptions::new(voter_id, directory_id)
                .cluster_id(cluster_id.clone())
                .listener(listener)
                .ack_when_committed(true),
        )
        .await?;
    ensure_mutation_success(&add, "AddRaftVoter")?;
    let after_add = wait_for_voter(&admin, voter_id, true).await?;
    println!(
        "after_add voters={} observers={} leader={}",
        after_add.current_voters().len(),
        after_add.observers().len(),
        after_add.leader_id(),
    );

    let remove = admin
        .remove_raft_voter(
            RemoveRaftVoterOptions::new(voter_id, directory_id).cluster_id(cluster_id),
        )
        .await?;
    ensure_mutation_success(&remove, "RemoveRaftVoter")?;
    let after_remove = wait_for_voter(&admin, voter_id, false).await?;
    println!(
        "after_remove voters={} observers={} leader={}",
        after_remove.current_voters().len(),
        after_remove.observers().len(),
        after_remove.leader_id(),
    );
    Ok(())
}

async fn describe_metadata_quorum(
    admin: &AdminClient,
) -> kafrust::Result<DescribeQuorumPartitionResult> {
    let result = admin
        .describe_quorum(&[DescribeQuorumTopic::new(METADATA_TOPIC).partition(0)])
        .await?;
    if !result.is_success() {
        return Err(Error::Broker {
            code: result.error_code(),
            context: "describe dynamic metadata quorum".to_owned(),
        });
    }
    result
        .topics()
        .first()
        .and_then(|topic| topic.partitions().first())
        .cloned()
        .ok_or(Error::Unsupported(
            "DescribeQuorum returned no metadata partition",
        ))
}

async fn wait_for_voter(
    admin: &AdminClient,
    voter_id: i32,
    expected_present: bool,
) -> kafrust::Result<DescribeQuorumPartitionResult> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let result = describe_metadata_quorum(admin).await?;
        let present = result
            .current_voters()
            .iter()
            .any(|voter| voter.replica_id() == voter_id);
        if present == expected_present {
            return Ok(result);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "dynamic quorum voter state did not converge",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn ensure_mutation_success(
    result: &kafrust::RaftVoterResult,
    operation: &'static str,
) -> kafrust::Result<()> {
    if result.is_success() {
        return Ok(());
    }
    Err(Error::Broker {
        code: result.error_code(),
        context: operation.to_owned(),
    })
}

fn parse_i32_env(name: &'static str, default: i32) -> kafrust::Result<i32> {
    let value = std::env::var(name).unwrap_or_else(|_| default.to_string());
    value.parse().map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "value must be a signed 32-bit integer",
    })
}

fn parse_directory_id(value: &str) -> kafrust::Result<[u8; 16]> {
    let decoded =
        URL_SAFE_NO_PAD
            .decode(value.trim())
            .map_err(|_| Error::InvalidConfiguration {
                field: "KAFRUST_NEW_VOTER_DIRECTORY_ID",
                reason: "value must be an unpadded URL-safe base64 UUID",
            })?;
    decoded.try_into().map_err(|_| Error::InvalidConfiguration {
        field: "KAFRUST_NEW_VOTER_DIRECTORY_ID",
        reason: "value must decode to exactly 16 bytes",
    })
}

fn parse_listener(value: &str) -> kafrust::Result<RaftVoterListener> {
    let (name, address) = value.split_once("://").ok_or(Error::InvalidConfiguration {
        field: "KAFRUST_NEW_VOTER_LISTENER",
        reason: "value must use NAME://HOST:PORT format",
    })?;
    let (host, port) = address
        .rsplit_once(':')
        .ok_or(Error::InvalidConfiguration {
            field: "KAFRUST_NEW_VOTER_LISTENER",
            reason: "value must use NAME://HOST:PORT format",
        })?;
    let port = port.parse().map_err(|_| Error::InvalidConfiguration {
        field: "KAFRUST_NEW_VOTER_LISTENER",
        reason: "listener port must be a valid u16",
    })?;
    if name.is_empty() || host.is_empty() {
        return Err(Error::InvalidConfiguration {
            field: "KAFRUST_NEW_VOTER_LISTENER",
            reason: "listener name and host must not be empty",
        });
    }
    Ok(RaftVoterListener::new(name, host, port))
}
