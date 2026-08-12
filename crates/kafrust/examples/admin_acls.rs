mod common;

use kafrust::{
    AclBinding, AclFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AdminClient, ClientConfig, Error,
};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic =
        std::env::var("KAFRUST_ACL_TOPIC").unwrap_or_else(|_| "kafrust-acl-smoke".to_owned());
    let principal =
        std::env::var("KAFRUST_ACL_PRINCIPAL").unwrap_or_else(|_| "User:ANONYMOUS".to_owned());
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-acl-example"),
    )?;
    let admin = AdminClient::new(config);
    let binding = AclBinding::new(
        AclResourceType::Topic,
        &topic,
        AclPatternType::Literal,
        &principal,
        "*",
        AclOperation::Read,
        AclPermissionType::Allow,
    );

    let created = admin.create_acls(std::slice::from_ref(&binding)).await?;
    if !created.is_success() {
        return Err(Error::Broker {
            code: created
                .results()
                .first()
                .map(|result| result.error_code())
                .unwrap_or(-1),
            context: format!("create ACL for topic {topic}"),
        });
    }
    println!("created ACL for {principal} on topic {topic}");

    let filter = AclFilter::any()
        .resource_type(AclResourceType::Topic)
        .resource_name(&topic)
        .pattern_type(AclPatternType::Literal)
        .principal(&principal)
        .host("*")
        .operation(AclOperation::Read)
        .permission_type(AclPermissionType::Allow);
    wait_for_acl_binding(&admin, &filter, &binding, &topic).await?;
    println!("described ACL for {principal} on topic {topic}");

    let deleted = admin.delete_acls(&[filter]).await?;
    if !deleted.is_success() {
        return Err(Error::Broker {
            code: deleted
                .filter_results()
                .first()
                .map(|result| result.error_code())
                .unwrap_or(-1),
            context: format!("delete ACL for topic {topic}"),
        });
    }
    println!("deleted ACL for {principal} on topic {topic}");

    Ok(())
}

async fn wait_for_acl_binding(
    admin: &AdminClient,
    filter: &AclFilter,
    binding: &AclBinding,
    topic: &str,
) -> kafrust::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let described = admin.describe_acls(filter).await?;
        if !described.is_success() {
            return Err(Error::Broker {
                code: described.error_code(),
                context: format!("describe ACL for topic {topic}"),
            });
        }
        if described
            .bindings()
            .iter()
            .any(|candidate| candidate == binding)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::Broker {
                code: 0,
                context: format!("ACL for topic {topic} was not visible before timeout"),
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
