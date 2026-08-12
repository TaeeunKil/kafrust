mod common;

use std::{
    fmt,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use kafrust::{
    AdminClient, ClientConfig, ClientMetrics, DeleteRecordsOptions, DeleteRecordsTopic, Error,
};
use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
};
use tracing_subscriber::{
    layer::SubscriberExt,
    layer::{Context, Layer},
    util::SubscriberInitExt,
    EnvFilter,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    init_tracing()?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = parse_i32("KAFRUST_PARTITION", 0)?;
    let offset = parse_i64("KAFRUST_ADMIN_OFFSET", 1)?;
    let request_timeout_ms = parse_u64("KAFRUST_REQUEST_TIMEOUT_MS", 30_000)?;
    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-delete-records-failover")
            .request_timeout_ms(request_timeout_ms)
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);

    println!("admin delete records failover target {topic}-{partition} offset={offset}");
    let result = admin
        .delete_records(
            &[DeleteRecordsTopic::new(topic.clone()).partition(partition, offset)],
            DeleteRecordsOptions::new(),
        )
        .await?;
    let topic_result = result
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition,
        })?;
    let partition_result = topic_result
        .partitions()
        .iter()
        .find(|candidate| candidate.partition_index() == partition)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition,
        })?;
    if !partition_result.is_success() {
        return Err(Error::Broker {
            code: partition_result.error_code(),
            context: format!("DeleteRecords for {topic}-{partition}"),
        });
    }
    let retries = metrics.snapshot().retries;
    println!(
        "admin delete records failover completed {topic}-{partition} low_watermark={} retries={retries}",
        partition_result.low_watermark(),
    );
    Ok(())
}

fn parse_i32(name: &'static str, default: i32) -> kafrust::Result<i32> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().map_err(|_| Error::Unsupported(name)))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_i64(name: &'static str, default: i64) -> kafrust::Result<i64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().map_err(|_| Error::Unsupported(name)))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_u64(name: &'static str, default: u64) -> kafrust::Result<u64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().map_err(|_| Error::Unsupported(name)))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn init_tracing() -> kafrust::Result<()> {
    let filter = EnvFilter::from_default_env();
    match (
        std::env::var_os("KAFRUST_REQUEST_SENT_FILE"),
        std::env::var_os("KAFRUST_REQUEST_RELEASE_FILE"),
    ) {
        (Some(sent_file), Some(release_file)) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .with(RequestGateLayer::new(sent_file.into(), release_file.into()))
            .try_init()
            .map_err(|_| Error::Unsupported("tracing subscriber was already initialized")),
        _ => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|_| Error::Unsupported("tracing subscriber was already initialized")),
    }
}

struct RequestGateLayer {
    sent_file: PathBuf,
    release_file: PathBuf,
    entered: AtomicBool,
}

impl RequestGateLayer {
    fn new(sent_file: PathBuf, release_file: PathBuf) -> Self {
        Self {
            sent_file,
            release_file,
            entered: AtomicBool::new(false),
        }
    }
}

impl<S> Layer<S> for RequestGateLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut visitor = RequestVisitor::default();
        event.record(&mut visitor);
        if visitor.api_key != Some(21)
            || !visitor
                .message
                .as_deref()
                .is_some_and(|message| message.contains("kafka request sent"))
            || self.entered.swap(true, Ordering::AcqRel)
        {
            return;
        }

        if let Err(error) = std::fs::write(&self.sent_file, b"delete-records-request-sent\n") {
            eprintln!("failed to write request gate file: {error}");
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.release_file.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[derive(Default)]
struct RequestVisitor {
    api_key: Option<i64>,
    message: Option<String>,
}

impl Visit for RequestVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "api_key" {
            self.api_key = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "api_key" {
            self.api_key = format!("{value:?}").parse().ok();
        } else if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }
}
