use kafrust::{
    AdminClient, ClientConfig, ConfigResourceType, DescribeConfigsOptions, Error,
    ListConfigResourcesOptions, TopicConfigResource,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = std::env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS must be set"))?;
    let expected_version = parse_i16(
        "KAFRUST_EXPECT_LIST_CONFIG_RESOURCES_VERSION",
        "expected ListConfigResources version",
    )?;
    let resource_type = parse_resource_type()?;

    let admin = AdminClient::new(ClientConfig::new([bootstrap_servers]));
    let listed = admin
        .list_config_resources(ListConfigResourcesOptions::new().resource_type(resource_type))
        .await?;
    if !listed.is_success() {
        return Err(Error::Broker {
            code: listed.error_code(),
            context: "published API 74 configuration discovery".to_owned(),
        });
    }
    if listed.api_version() != expected_version {
        return Err(Error::Unsupported(
            "published API 74 negotiated an unexpected version",
        ));
    }
    println!(
        "api74 list_version={} resource_type={} resources={}",
        listed.api_version(),
        resource_type.code(),
        listed.resources().len()
    );

    if std::env::var("KAFRUST_EXPECT_DESCRIBE_CONFIGS")
        .ok()
        .as_deref()
        != Some("skip")
    {
        let topic = std::env::var("KAFRUST_CONFIG_TOPIC")
            .map_err(|_| Error::Unsupported("KAFRUST_CONFIG_TOPIC must be set"))?;
        let described = admin
            .describe_topic_configs(
                &[TopicConfigResource::new(topic)],
                DescribeConfigsOptions::new().include_documentation(true),
            )
            .await?;
        let resource = described
            .resources()
            .first()
            .ok_or(Error::Unsupported("DescribeConfigs returned no resource"))?;
        if !resource.is_success() {
            return Err(Error::Broker {
                code: resource.error_code(),
                context: "published DescribeConfigs v4 resource".to_owned(),
            });
        }
        let has_type = resource
            .entries()
            .iter()
            .any(|entry| entry.config_type().is_some());
        let has_documentation = resource
            .entries()
            .iter()
            .any(|entry| entry.documentation().is_some());
        if !has_type || !has_documentation {
            return Err(Error::Unsupported(
                "published DescribeConfigs did not preserve v4 metadata",
            ));
        }
        println!(
            "describe_documentation=true entries={}",
            resource.entries().len()
        );
    }

    Ok(())
}

fn parse_i16(name: &str, context: &'static str) -> kafrust::Result<i16> {
    std::env::var(name)
        .map_err(|_| Error::Unsupported(context))?
        .parse()
        .map_err(|_| Error::Unsupported(context))
}

fn parse_resource_type() -> kafrust::Result<ConfigResourceType> {
    let code = parse_i16(
        "KAFRUST_LIST_CONFIG_RESOURCES_RESOURCE_TYPE",
        "expected configuration resource type",
    )?;
    let code = i8::try_from(code)
        .map_err(|_| Error::Unsupported("configuration resource type is out of range"))?;
    Ok(ConfigResourceType::from_code(code))
}
