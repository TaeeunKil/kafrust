use kafrust::{
    ClientConfig, ConsumerConfig, ConsumerGroupConfig, ProducerConfig, SecurityProtocol,
};

pub(crate) fn apply_security<T>(config: T) -> kafrust::Result<T>
where
    T: ExampleSecurityConfig,
{
    let mut config = config.security_protocol(security_protocol_from_env()?);
    if let Some(credentials) = sasl_credentials_from_env()? {
        config = match credentials.mechanism {
            ExampleSaslMechanism::Plain => {
                config.sasl_plain(credentials.username, credentials.password)
            }
            ExampleSaslMechanism::ScramSha256 => {
                config.sasl_scram_sha_256(credentials.username, credentials.password)
            }
            ExampleSaslMechanism::ScramSha512 => {
                config.sasl_scram_sha_512(credentials.username, credentials.password)
            }
        };
    }
    if let Some(server_name) = tls_server_name_from_env() {
        config = config.tls_server_name(server_name);
    }
    if let Some(certificate) = tls_root_certificate_der_from_env()? {
        config = config.tls_root_certificate_der(certificate);
    }
    Ok(config)
}

pub(crate) trait ExampleSecurityConfig: Sized {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self;
    fn tls_server_name(self, server_name: String) -> Self;
    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self;
    fn sasl_plain(self, username: String, password: String) -> Self;
    fn sasl_scram_sha_256(self, username: String, password: String) -> Self;
    fn sasl_scram_sha_512(self, username: String, password: String) -> Self;
}

impl ExampleSecurityConfig for ClientConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ClientConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ClientConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ClientConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ClientConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ClientConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ClientConfig::sasl_scram_sha_512(self, username, password)
    }
}

impl ExampleSecurityConfig for ProducerConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ProducerConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ProducerConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ProducerConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ProducerConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ProducerConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ProducerConfig::sasl_scram_sha_512(self, username, password)
    }
}

impl ExampleSecurityConfig for ConsumerConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ConsumerConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ConsumerConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ConsumerConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ConsumerConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ConsumerConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ConsumerConfig::sasl_scram_sha_512(self, username, password)
    }
}

impl ExampleSecurityConfig for ConsumerGroupConfig {
    fn security_protocol(self, security_protocol: SecurityProtocol) -> Self {
        ConsumerGroupConfig::security_protocol(self, security_protocol)
    }

    fn tls_server_name(self, server_name: String) -> Self {
        ConsumerGroupConfig::tls_server_name(self, server_name)
    }

    fn tls_root_certificate_der(self, certificate: Vec<u8>) -> Self {
        ConsumerGroupConfig::tls_root_certificate_der(self, certificate)
    }

    fn sasl_plain(self, username: String, password: String) -> Self {
        ConsumerGroupConfig::sasl_plain(self, username, password)
    }

    fn sasl_scram_sha_256(self, username: String, password: String) -> Self {
        ConsumerGroupConfig::sasl_scram_sha_256(self, username, password)
    }

    fn sasl_scram_sha_512(self, username: String, password: String) -> Self {
        ConsumerGroupConfig::sasl_scram_sha_512(self, username, password)
    }
}

fn security_protocol_from_env() -> kafrust::Result<SecurityProtocol> {
    let Ok(value) = std::env::var("KAFRUST_SECURITY_PROTOCOL") else {
        return Ok(SecurityProtocol::Plaintext);
    };

    parse_security_protocol(&value)
}

struct ExampleSaslCredentials {
    mechanism: ExampleSaslMechanism,
    username: String,
    password: String,
}

enum ExampleSaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

fn sasl_credentials_from_env() -> kafrust::Result<Option<ExampleSaslCredentials>> {
    let Some(username) = std::env::var("KAFRUST_SASL_USERNAME").ok() else {
        return Ok(None);
    };
    let password = std::env::var("KAFRUST_SASL_PASSWORD").map_err(|_| {
        kafrust::Error::Unsupported(
            "KAFRUST_SASL_PASSWORD is required when KAFRUST_SASL_USERNAME is set",
        )
    })?;
    Ok(Some(ExampleSaslCredentials {
        mechanism: sasl_mechanism_from_env()?,
        username,
        password,
    }))
}

fn sasl_mechanism_from_env() -> kafrust::Result<ExampleSaslMechanism> {
    let Ok(value) = std::env::var("KAFRUST_SASL_MECHANISM") else {
        return Ok(ExampleSaslMechanism::Plain);
    };

    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "" | "plain" => Ok(ExampleSaslMechanism::Plain),
        "scram-sha-256" => Ok(ExampleSaslMechanism::ScramSha256),
        "scram-sha-512" => Ok(ExampleSaslMechanism::ScramSha512),
        _ => Err(kafrust::Error::Unsupported(
            "unsupported KAFRUST_SASL_MECHANISM; expected plain, scram-sha-256, or scram-sha-512",
        )),
    }
}

fn tls_server_name_from_env() -> Option<String> {
    std::env::var("KAFRUST_TLS_SERVER_NAME").ok()
}

fn tls_root_certificate_der_from_env() -> kafrust::Result<Option<Vec<u8>>> {
    let Ok(path) = std::env::var("KAFRUST_TLS_ROOT_CERT_DER_PATH") else {
        return Ok(None);
    };

    Ok(Some(std::fs::read(path)?))
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
