mod common;

use kafrust::{
    AclBinding, AclFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AdminClient, AlterConfigsOptions, ClientConfig, ClientQuotaAlteration, ClientQuotaEntity,
    ClientQuotaFilter, ClientQuotaFilterComponent, ClientQuotaMatchType, ConsumerGroupOffset,
    ConsumerGroupOffsetQuery, CreateDelegationTokenOptions, CreatePartitionsOptions,
    CreateTopicsOptions, DelegationTokenPrincipal, DeleteTopicsOptions, DescribeConfigsOptions,
    Error, NewPartitions, NewTopic, ScramCredentialMechanism, ScramCredentialUpsertion,
    TopicConfigAlteration, TopicConfigResource, TopicConfigUpdate,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_ADMIN_TOPIC")
        .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_TOPIC is required"))?;
    let mutation =
        std::env::var("KAFRUST_ADMIN_MUTATION").unwrap_or_else(|_| "create_topics".into());
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-ambiguity-example"),
    )?;
    let admin = AdminClient::new(config);

    match mutation.as_str() {
        "create_topics" => qualify_create_topics(&admin, &topic).await?,
        "create_partitions" => qualify_create_partitions(&admin, &topic).await?,
        "incremental_alter_configs" => qualify_incremental_alter_configs(&admin, &topic).await?,
        "alter_configs" => qualify_alter_configs(&admin, &topic).await?,
        "create_acls" => qualify_create_acls(&admin, &topic).await?,
        "delete_acls" => qualify_delete_acls(&admin, &topic).await?,
        "alter_client_quotas" => qualify_alter_client_quotas(&admin).await?,
        "alter_user_scram_credentials" => qualify_alter_user_scram_credentials(&admin).await?,
        "create_delegation_token" => qualify_create_delegation_token(&admin).await?,
        "alter_consumer_group_offsets" => {
            qualify_alter_consumer_group_offsets(&admin, &topic).await?
        }
        "delete_topics" => qualify_delete_topics(&admin, &topic).await?,
        _ => {
            return Err(Error::Unsupported(
                "KAFRUST_ADMIN_MUTATION must be create_topics, create_partitions, incremental_alter_configs, alter_configs, create_acls, delete_acls, alter_client_quotas, alter_user_scram_credentials, create_delegation_token, alter_consumer_group_offsets, or delete_topics",
            ))
        }
    }
    Ok(())
}

async fn qualify_create_topics(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    let error = match admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await
    {
        Ok(_) => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make CreateTopics ambiguous",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "CreateTopics"
        }
    ) {
        return Err(error);
    }
    println!("CreateTopics response was lost; outcome is explicitly unknown");
    wait_for_topic_state(admin, topic, Some(1)).await
}

async fn qualify_create_partitions(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let partitions = [NewPartitions::new(topic, 2)];
    let error = match admin
        .create_partitions(&partitions, CreatePartitionsOptions::new())
        .await
    {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make CreatePartitions ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "CreatePartitions returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "CreatePartitions"
        }
    ) {
        return Err(error);
    }
    println!("CreatePartitions response was lost; outcome is explicitly unknown");
    wait_for_topic_state(admin, topic, Some(2)).await
}

async fn qualify_delete_topics(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let topic_names = [topic.to_owned()];
    let error = match admin
        .delete_topics(&topic_names, DeleteTopicsOptions::new())
        .await
    {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make DeleteTopics ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "DeleteTopics returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "DeleteTopics"
        }
    ) {
        return Err(error);
    }
    println!("DeleteTopics response was lost; outcome is explicitly unknown");
    wait_for_topic_state(admin, topic, None).await
}

async fn qualify_incremental_alter_configs(
    admin: &AdminClient,
    topic: &str,
) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let alterations = [TopicConfigAlteration::new(topic).set("retention.ms", "120000")];
    let error =
        match admin
            .incremental_alter_topic_configs(&alterations, AlterConfigsOptions::new())
            .await
        {
            Ok(result) if !result.has_errors() => {
                return Err(Error::Unsupported(
                    "the response-drop proxy did not make IncrementalAlterConfigs ambiguous",
                ))
            }
            Ok(_) => return Err(Error::Unsupported(
                "IncrementalAlterConfigs returned a broker error before the response was dropped",
            )),
            Err(error) => error,
        };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "IncrementalAlterConfigs"
        }
    ) {
        return Err(error);
    }
    println!("IncrementalAlterConfigs response was lost; outcome is explicitly unknown");
    wait_for_topic_config_value(admin, topic, "retention.ms", "120000").await
}

async fn qualify_alter_configs(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let updates = [TopicConfigUpdate::new(topic).set("retention.ms", "180000")];
    let error = match admin
        .alter_topic_configs(&updates, AlterConfigsOptions::new())
        .await
    {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make AlterConfigs ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "AlterConfigs returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "AlterConfigs"
        }
    ) {
        return Err(error);
    }
    println!("AlterConfigs response was lost; outcome is explicitly unknown");
    wait_for_topic_config_value(admin, topic, "retention.ms", "180000").await
}

async fn qualify_create_acls(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    let binding = ambiguity_acl_binding(topic);
    let error = match admin.create_acls(std::slice::from_ref(&binding)).await {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make CreateAcls ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "CreateAcls returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "CreateAcls"
        }
    ) {
        return Err(error);
    }
    println!("CreateAcls response was lost; outcome is explicitly unknown");
    let filter = ambiguity_acl_filter(topic);
    wait_for_acl_state(admin, &filter, &binding, true).await
}

async fn qualify_delete_acls(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    let binding = ambiguity_acl_binding(topic);
    admin.create_acls(std::slice::from_ref(&binding)).await?;
    let filter = ambiguity_acl_filter(topic);
    let error = match admin.delete_acls(std::slice::from_ref(&filter)).await {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make DeleteAcls ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "DeleteAcls returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "DeleteAcls"
        }
    ) {
        return Err(error);
    }
    println!("DeleteAcls response was lost; outcome is explicitly unknown");
    wait_for_acl_state(admin, &filter, &binding, false).await
}

async fn qualify_alter_client_quotas(admin: &AdminClient) -> kafrust::Result<()> {
    let user = "kafrust-ambiguity";
    let quota_key = "producer_byte_rate";
    let quota_value = 4096.0;
    let entity = ClientQuotaEntity::user(user);
    let alteration = ClientQuotaAlteration::new(entity).set(quota_key, quota_value);
    let error = match admin.alter_client_quotas(&[alteration], false).await {
        Ok(result) if !result.has_errors() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make AlterClientQuotas ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "AlterClientQuotas returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "AlterClientQuotas"
        }
    ) {
        return Err(error);
    }
    println!("AlterClientQuotas response was lost; outcome is explicitly unknown");
    let filter = ClientQuotaFilter::any().component(ClientQuotaFilterComponent::new(
        "user",
        ClientQuotaMatchType::Exact,
        Some(user),
    ));
    wait_for_quota_value(admin, &filter, quota_key, quota_value).await
}

async fn qualify_alter_user_scram_credentials(admin: &AdminClient) -> kafrust::Result<()> {
    let username = "kafrust-ambiguity-scram";
    let mechanism = ScramCredentialMechanism::Sha256;
    let upsertion = ScramCredentialUpsertion::with_salt(
        username,
        mechanism,
        4096,
        b"kafrust-ambiguity-secret",
        [1_u8; 32],
    )?;
    let error =
        match admin.alter_user_scram_credentials(&[], &[upsertion]).await {
            Ok(result) if !result.has_errors() => {
                return Err(Error::Unsupported(
                    "the response-drop proxy did not make AlterUserScramCredentials ambiguous",
                ))
            }
            Ok(_) => return Err(Error::Unsupported(
                "AlterUserScramCredentials returned a broker error before the response was dropped",
            )),
            Err(error) => error,
        };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "AlterUserScramCredentials"
        }
    ) {
        return Err(error);
    }
    println!("AlterUserScramCredentials response was lost; outcome is explicitly unknown");
    wait_for_scram_credential(admin, username, mechanism, 4096).await
}

async fn qualify_create_delegation_token(admin: &AdminClient) -> kafrust::Result<()> {
    let before = admin.describe_delegation_tokens(None).await?;
    if !before.is_success() {
        return Err(Error::Broker {
            code: before.error_code(),
            context: "describe delegation tokens before ambiguity gate".to_owned(),
        });
    }
    let before_ids = before
        .tokens()
        .iter()
        .map(|token| token.token_id().to_owned())
        .collect::<Vec<_>>();
    let error = match admin
        .create_delegation_token(CreateDelegationTokenOptions::new())
        .await
    {
        Ok(result) if result.is_success() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make CreateDelegationToken ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "CreateDelegationToken returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "CreateDelegationToken"
        }
    ) {
        return Err(error);
    }
    println!("CreateDelegationToken response was lost; outcome is explicitly unknown");
    wait_for_new_delegation_token(
        admin,
        &before_ids,
        DelegationTokenPrincipal::new("User", "admin"),
    )
    .await
}

async fn qualify_alter_consumer_group_offsets(
    admin: &AdminClient,
    topic: &str,
) -> kafrust::Result<()> {
    admin
        .create_topics(&[NewTopic::new(topic, 1, 1)], CreateTopicsOptions::new())
        .await?;
    let group_id = format!("kafrust-admin-ambiguity-{topic}");
    let expected_offset = 42;
    wait_for_group_offset_query(admin, &group_id, topic).await?;
    let offsets = [ConsumerGroupOffset::new(topic, 0, expected_offset)];
    let error = match admin
        .alter_consumer_group_offsets(&group_id, &offsets)
        .await
    {
        Ok(result) if result.is_success() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make OffsetCommit ambiguous",
            ))
        }
        Ok(_) => {
            return Err(Error::Unsupported(
                "OffsetCommit returned a broker error before the response was dropped",
            ))
        }
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "OffsetCommit"
        }
    ) {
        return Err(error);
    }
    println!("OffsetCommit response was lost; outcome is explicitly unknown");
    wait_for_group_offset(admin, &group_id, topic, expected_offset).await
}

fn ambiguity_acl_binding(topic: &str) -> AclBinding {
    AclBinding::new(
        AclResourceType::Topic,
        topic,
        AclPatternType::Literal,
        "User:kafrust-ambiguity",
        "*",
        AclOperation::Read,
        AclPermissionType::Allow,
    )
}

fn ambiguity_acl_filter(topic: &str) -> AclFilter {
    AclFilter::any()
        .resource_type(AclResourceType::Topic)
        .resource_name(topic)
        .pattern_type(AclPatternType::Literal)
        .principal("User:kafrust-ambiguity")
        .host("*")
        .operation(AclOperation::Read)
        .permission_type(AclPermissionType::Allow)
}

async fn wait_for_acl_state(
    admin: &AdminClient,
    filter: &AclFilter,
    binding: &AclBinding,
    expected_present: bool,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let described = admin.describe_acls(filter).await?;
        if !described.is_success() {
            return Err(Error::Broker {
                code: described.error_code(),
                context: "describe ACL during ambiguity reconciliation".to_owned(),
            });
        }
        let present = described
            .bindings()
            .iter()
            .any(|candidate| candidate == binding);
        if present == expected_present {
            println!(
                "reconciled ACL state for topic {topic}",
                topic = binding.resource_name()
            );
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous ACL mutation reconciliation did not observe the expected state",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_quota_value(
    admin: &AdminClient,
    filter: &ClientQuotaFilter,
    key: &str,
    expected: f64,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let described = admin.describe_client_quotas(filter).await?;
        if !described.is_success() {
            return Err(Error::Broker {
                code: described.error_code(),
                context: "describe client quota during ambiguity reconciliation".to_owned(),
            });
        }
        let value = described
            .entries()
            .iter()
            .flat_map(|entry| entry.values())
            .find(|value| value.key() == key)
            .map(|value| value.value());
        if value == Some(expected) {
            println!("reconciled applied {key}={expected} client quota");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous AlterClientQuotas reconciliation did not observe the expected value",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_scram_credential(
    admin: &AdminClient,
    username: &str,
    mechanism: ScramCredentialMechanism,
    iterations: i32,
) -> kafrust::Result<()> {
    let users = [username.to_owned()];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let described = admin.describe_user_scram_credentials(Some(&users)).await?;
        let visible = described.users().iter().any(|user| {
            user.username() == username
                && user.is_success()
                && user.credentials().iter().any(|credential| {
                    credential.mechanism() == mechanism && credential.iterations() == iterations
                })
        });
        if visible {
            println!("reconciled applied SCRAM credential for user {username}");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous AlterUserScramCredentials reconciliation did not observe the expected credential",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_new_delegation_token(
    admin: &AdminClient,
    before_ids: &[String],
    expected_owner: DelegationTokenPrincipal,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let described = admin.describe_delegation_tokens(None).await?;
        if !described.is_success() {
            return Err(Error::Broker {
                code: described.error_code(),
                context: "describe delegation token during ambiguity reconciliation".to_owned(),
            });
        }
        if described.tokens().iter().any(|token| {
            !before_ids.iter().any(|id| id == token.token_id())
                && token.owner() == &expected_owner
                && !token.hmac().is_empty()
        }) {
            println!("reconciled applied delegation token without logging its HMAC");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous CreateDelegationToken reconciliation did not observe a new token",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_group_offset(
    admin: &AdminClient,
    group_id: &str,
    topic: &str,
    expected_offset: i64,
) -> kafrust::Result<()> {
    let query = [ConsumerGroupOffsetQuery::new(topic, [0])];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let described = admin
            .list_consumer_group_offsets(group_id, Some(&query))
            .await?;
        let visible = described.is_success()
            && described.topics().iter().any(|candidate| {
                candidate.topic() == topic
                    && candidate.partitions().iter().any(|partition| {
                        partition.partition_index() == 0
                            && partition.is_success()
                            && partition.committed_offset() == expected_offset
                    })
            });
        if visible {
            println!(
                "reconciled committed offset for group {group_id}: {topic}-0={expected_offset}"
            );
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous OffsetCommit reconciliation did not observe the expected offset",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_group_offset_query(
    admin: &AdminClient,
    group_id: &str,
    topic: &str,
) -> kafrust::Result<()> {
    let query = [ConsumerGroupOffsetQuery::new(topic, [0])];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let described = admin
            .list_consumer_group_offsets(group_id, Some(&query))
            .await?;
        let ready = described.is_success()
            && described.topics().iter().any(|candidate| {
                candidate.topic() == topic
                    && candidate
                        .partitions()
                        .iter()
                        .any(|partition| partition.partition_index() == 0 && partition.is_success())
            });
        if ready {
            println!("consumer-group offset coordinator is ready for {group_id}");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "consumer-group offset query did not become ready before OffsetCommit",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_topic_config_value(
    admin: &AdminClient,
    topic: &str,
    key: &str,
    expected: &str,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let response = admin
            .describe_topic_configs(
                &[TopicConfigResource::with_keys(topic, [key])],
                DescribeConfigsOptions::new(),
            )
            .await?;
        let value = response
            .resources()
            .first()
            .and_then(|resource| resource.entries().iter().find(|entry| entry.name() == key))
            .and_then(|entry| entry.value());
        if value == Some(expected) {
            println!("reconciled applied {key}={expected} for topic {topic}");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "ambiguous IncrementalAlterConfigs reconciliation did not observe the expected value",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_topic_state(
    admin: &AdminClient,
    topic: &str,
    expected_partitions: Option<usize>,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let listed = admin.list_topics().await?;
        let topic_listing = listed.iter().find(|listing| listing.name() == topic);
        if let Some(expected_partitions) = expected_partitions {
            if let Some(topic_listing) = topic_listing {
                if topic_listing.partition_count() != expected_partitions {
                    return Err(Error::Unsupported(
                        "ambiguous Admin mutation reconciliation returned an unexpected partition count",
                    ));
                }
                println!(
                    "reconciled applied topic {} with {} partition",
                    topic,
                    topic_listing.partition_count()
                );
                return Ok(());
            }
        } else if topic_listing.is_none() {
            println!("reconciled applied deletion of topic {topic}");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            if expected_partitions.is_some() {
                return Err(Error::Unsupported(
                    "ambiguous Admin mutation reconciliation did not observe the expected topic state",
                ));
            }
            return Err(Error::Unsupported(
                "ambiguous Admin mutation reconciliation still observed the deleted topic",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
