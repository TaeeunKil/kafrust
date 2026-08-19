#[cfg(feature = "otlp")]
#[tokio::main]
async fn main() -> kafrust::Result<()> {
    use kafrust::{
        ClientConfig, ClientMetrics, ClientMetricsTelemetryProvider, TelemetryClient,
        TelemetryConfig,
    };
    use std::io::Write;
    use std::time::{Duration, Instant};

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
    let mut telemetry = TelemetryClient::connect(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-telemetry-smoke"),
        provider,
        TelemetryConfig::new().jitter(false),
    )
    .await?;

    if std::env::var("KAFRUST_EXPECT_TELEMETRY_PAYLOAD_LIMIT").as_deref() == Ok("true") {
        match telemetry.push_once().await {
            Err(kafrust::Error::TelemetryPayloadTooLarge { size, max }) => {
                println!("telemetry-smoke-payload-limit size={} max={}", size, max);
                return Ok(());
            }
            Ok(Some(_)) => {
                return Err(kafrust::Error::Unsupported(
                    "telemetry payload-limit smoke unexpectedly pushed",
                ));
            }
            Ok(None) => {
                return Err(kafrust::Error::Unsupported(
                    "telemetry payload-limit smoke has no active subscription",
                ));
            }
            Err(error) => return Err(error),
        }
    }

    let duration_seconds = std::env::var("KAFRUST_TELEMETRY_SMOKE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds >= 2)
        .unwrap_or(7);
    let deadline = Instant::now() + Duration::from_secs(duration_seconds);
    let mut pushes = 0_u32;
    while Instant::now() < deadline {
        if let Some(summary) = telemetry.push_once().await? {
            pushes += 1;
            println!(
                "telemetry-smoke-push subscription_id={} payload_bytes={}",
                summary.subscription_id, summary.payload_bytes
            );
        } else {
            println!("telemetry-smoke-push subscription_empty=true");
        }
        std::io::stdout()
            .flush()
            .map_err(|_| kafrust::Error::Unsupported("telemetry smoke output"))?;
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    let terminating = telemetry.terminate().await?;
    println!(
        "telemetry-smoke-terminating pushed={}",
        terminating.is_some()
    );
    std::io::stdout()
        .flush()
        .map_err(|_| kafrust::Error::Unsupported("telemetry smoke output"))?;

    let snapshot = metrics.snapshot();
    println!(
        "telemetry-smoke-ok pushes={} requests_started={} requests_succeeded={}",
        pushes, snapshot.requests_started, snapshot.requests_succeeded
    );
    Ok(())
}

#[cfg(not(feature = "otlp"))]
fn main() {
    eprintln!("telemetry_smoke requires the kafrust `otlp` feature");
}
