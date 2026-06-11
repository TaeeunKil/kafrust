use kafrust::{ClientConfig, SecurityProtocol};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());

    let mut client = ClientConfig::new([bootstrap])
        .client_id("kafrust-roundtrip")
        .security_protocol(security_protocol_from_env()?)
        .connect()
        .await?;

    let api_versions = client.api_versions().await?;
    println!("api_versions: {} APIs", api_versions.api_keys.len());

    let metadata = client.metadata(None).await?;
    println!(
        "metadata: {} brokers, {} topics, controller {}",
        metadata.brokers.len(),
        metadata.topics.len(),
        metadata.controller_id
    );

    Ok(())
}

fn security_protocol_from_env() -> kafrust::Result<SecurityProtocol> {
    let Ok(value) = std::env::var("KAFRUST_SECURITY_PROTOCOL") else {
        return Ok(SecurityProtocol::Plaintext);
    };

    parse_security_protocol(&value)
}

fn parse_security_protocol(value: &str) -> kafrust::Result<SecurityProtocol> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "" | "plaintext" => Ok(SecurityProtocol::Plaintext),
        "ssl" | "tls" => Ok(SecurityProtocol::Tls),
        "sasl_plaintext" => Ok(SecurityProtocol::SaslPlaintext),
        "sasl_ssl" | "sasl_tls" => Ok(SecurityProtocol::SaslTls),
        _ => Err(kafrust::Error::Unsupported(
            "unsupported KAFRUST_SECURITY_PROTOCOL; expected plaintext, tls, ssl, sasl_plaintext, sasl_ssl, or sasl_tls",
        )),
    }
}
