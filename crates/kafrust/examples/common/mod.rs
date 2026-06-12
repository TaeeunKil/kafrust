use kafrust::{
    ClientConfig, ConsumerConfig, ConsumerGroupConfig, ProducerConfig, SecurityProtocol,
};

pub(crate) fn apply_security<T>(config: T) -> kafrust::Result<T>
where
    T: ExampleSecurityConfig,
{
    let mut config = config.security_protocol(security_protocol_from_env()?);
    if let Some((username, password)) = sasl_credentials_from_env() {
        config = config.sasl_plain(username, password);
    }
    Ok(config)
}

pub(crate) trait ExampleSecurityConfig: Sized {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self;
    fn sasl_plain(self, username: String, password: String) -> Self;
}

impl ExampleSecurityConfig for ClientConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ClientConfig::security_protocol(self, security_protocol)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ClientConfig::sasl_plain(self, username, password)
    }
}

impl ExampleSecurityConfig for ProducerConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ProducerConfig::security_protocol(self, security_protocol)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ProducerConfig::sasl_plain(self, username, password)
    }
}

impl ExampleSecurityConfig for ConsumerConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ConsumerConfig::security_protocol(self, security_protocol)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ConsumerConfig::sasl_plain(self, username, password)
    }
}

impl ExampleSecurityConfig for ConsumerGroupConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ConsumerGroupConfig::security_protocol(self, security_protocol)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ConsumerGroupConfig::sasl_plain(self, username, password)
    }
}

fn security_protocol_from_env() -> kafrust::Result<SecurityProtocol> {
    let Ok(value) = std::env::var("KAFRUST_SECURITY_PROTOCOL") else {
        return Ok(SecurityProtocol::Plaintext);
    };

    parse_security_protocol(&value)
}

fn sasl_credentials_from_env() -> Option<(String, String)> {
    let username = std::env::var("KAFRUST_SASL_USERNAME").ok()?;
    let password = std::env::var("KAFRUST_SASL_PASSWORD").ok()?;
    Some((username, password))
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
