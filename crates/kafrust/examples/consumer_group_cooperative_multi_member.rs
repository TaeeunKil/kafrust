mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafrust::{
    ConsumerGroup, ConsumerGroupAssignmentStrategy, ConsumerGroupConfig, Error, RebalancePhase,
};

const POLL_ATTEMPTS: usize = 80;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    tokio::time::timeout(Duration::from_secs(45), run_scenario())
        .await
        .map_err(|_| Error::Unsupported("cooperative multi-member scenario timed out"))?
}

async fn run_scenario() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-cooperative-multi-member".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let before_rebalances = Arc::new(AtomicUsize::new(0));
    let after_rebalances = Arc::new(AtomicUsize::new(0));
    let before_callback = before_rebalances.clone();
    let after_callback = after_rebalances.clone();
    let config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id)
            .client_id("kafrust-cooperative-multi-member")
            .session_timeout_ms(6_000)
            .rebalance_timeout_ms(10_000)
            .max_wait_ms(100)
            .assignment_strategy(ConsumerGroupAssignmentStrategy::CooperativeSticky)
            .rebalance_listener(move |event| {
                match event.phase() {
                    RebalancePhase::Before => {
                        before_callback.fetch_add(1, Ordering::SeqCst);
                    }
                    RebalancePhase::After => {
                        after_callback.fetch_add(1, Ordering::SeqCst);
                    }
                }
                println!(
                    "rebalance {:?} member={} generation={} assignments={}",
                    event.phase(),
                    event.member_id(),
                    event.generation_id(),
                    event.assignments().len()
                );
            })
            .subscribe(topic),
    )?;

    println!("joining first cooperative member");
    let mut first = config
        .clone()
        .client_id("kafrust-cooperative-first")
        .join()
        .await?;
    let expected_partitions = first.assignments().len();
    if expected_partitions == 0 {
        return Err(Error::Unsupported(
            "cooperative scenario first member received no partitions",
        ));
    }
    println!(
        "first member {} joined with {} partitions",
        first.member_id(),
        expected_partitions
    );

    println!("joining second cooperative member");
    let second_join = tokio::spawn(
        config
            .clone()
            .client_id("kafrust-cooperative-second")
            .join(),
    );
    while !second_join.is_finished() {
        first.poll().await?;
    }
    let mut second = second_join.await??;
    println!("second member {} joined", second.member_id());

    println!("waiting for two-member coverage");
    wait_for_two_member_coverage(&mut first, &mut second, expected_partitions).await?;
    println!(
        "cooperative transfer established: {} partitions split between {} and {}",
        expected_partitions,
        first.member_id(),
        second.member_id()
    );

    println!("joining transient cooperative member");
    let third_join = tokio::spawn(
        config
            .clone()
            .client_id("kafrust-cooperative-transient")
            .join(),
    );
    while !third_join.is_finished() {
        poll_pair(&mut first, &mut second).await?;
    }
    let third = third_join.await??;
    let transient_member = third.member_id().to_owned();
    println!("transient member {} joined; dropping it", transient_member);
    drop(third);

    println!("waiting for rollback coverage");
    wait_for_two_member_coverage(&mut first, &mut second, expected_partitions).await?;
    println!(
        "cooperative rollback recovered after transient member {} left",
        transient_member
    );

    drop(second);
    println!("second member dropped; waiting for member-loss recovery");
    wait_for_single_member_coverage(&mut first, expected_partitions).await?;
    println!(
        "cooperative member-loss recovery restored all {} partitions to {}",
        expected_partitions,
        first.member_id()
    );

    if before_rebalances.load(Ordering::SeqCst) == 0 || after_rebalances.load(Ordering::SeqCst) < 2
    {
        return Err(Error::Unsupported(
            "cooperative scenario did not observe rebalance listener lifecycle callbacks",
        ));
    }

    first.leave().await?;
    Ok(())
}

async fn poll_pair(first: &mut ConsumerGroup, second: &mut ConsumerGroup) -> kafrust::Result<()> {
    let (first_result, second_result) = tokio::join!(first.poll(), second.poll(),);
    first_result?;
    second_result?;
    Ok(())
}

async fn wait_for_two_member_coverage(
    first: &mut ConsumerGroup,
    second: &mut ConsumerGroup,
    expected_partitions: usize,
) -> kafrust::Result<()> {
    for _ in 0..POLL_ATTEMPTS {
        poll_pair(first, second).await?;
        let first_partitions = assignment_keys(first);
        let second_partitions = assignment_keys(second);
        if !first_partitions.is_empty()
            && !second_partitions.is_empty()
            && first_partitions.is_disjoint(&second_partitions)
            && first_partitions.union(&second_partitions).count() == expected_partitions
        {
            return Ok(());
        }
    }
    Err(Error::Unsupported(
        "cooperative members did not converge on disjoint partition coverage",
    ))
}

async fn wait_for_single_member_coverage(
    first: &mut ConsumerGroup,
    expected_partitions: usize,
) -> kafrust::Result<()> {
    for _ in 0..POLL_ATTEMPTS {
        first.poll().await?;
        if assignment_keys(first).len() == expected_partitions {
            return Ok(());
        }
    }
    Err(Error::Unsupported(
        "cooperative member-loss recovery did not restore full partition coverage",
    ))
}

fn assignment_keys(group: &ConsumerGroup) -> BTreeSet<(String, i32)> {
    group
        .assignments()
        .iter()
        .map(|assignment| (assignment.topic().to_owned(), assignment.partition()))
        .collect()
}
