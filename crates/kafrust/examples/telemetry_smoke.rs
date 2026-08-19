#[cfg(feature = "otlp")]
#[tokio::main]
async fn main() -> kafrust::Result<()> {
    use kafrust::{
        ClientConfig, ClientMetrics, ClientMetricsTelemetryProvider, TelemetryClient,
        TelemetryConfig,
    };
    use std::time::Duration;
    use tokio::sync::watch;

    let bootstrap_servers =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let bootstrap_servers = bootstrap_servers
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if bootstrap_servers.is_empty() {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_BOOTSTRAP_SERVERS",
            reason: "must contain at least one broker",
        });
    }

    let metrics = ClientMetrics::new();
    let provider = ClientMetricsTelemetryProvider::new(metrics.clone());
    let telemetry = TelemetryClient::connect(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-telemetry-smoke"),
        provider,
        TelemetryConfig::new().jitter(false),
    )
    .await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(telemetry.run_until_shutdown(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(2500)).await;
    shutdown_tx
        .send(true)
        .map_err(|_| kafrust::Error::Unsupported("telemetry smoke shutdown"))?;
    task.await
        .map_err(|_| kafrust::Error::Unsupported("telemetry smoke task"))??;

    let snapshot = metrics.snapshot();
    println!(
        "telemetry-smoke-ok requests_started={} requests_succeeded={}",
        snapshot.requests_started, snapshot.requests_succeeded
    );
    Ok(())
}

#[cfg(not(feature = "otlp"))]
fn main() {
    eprintln!("telemetry_smoke requires the kafrust `otlp` feature");
}
