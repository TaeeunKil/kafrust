use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Shared, lock-free metrics collected for Kafka request roundtrips.
///
/// Clones refer to the same counters, so a handle obtained from a configuration
/// continues to observe connections created from that configuration.
#[derive(Clone, Default)]
pub struct ClientMetrics {
    inner: Arc<ClientMetricsInner>,
}

#[derive(Default)]
struct ClientMetricsInner {
    requests_started: AtomicU64,
    requests_succeeded: AtomicU64,
    requests_failed: AtomicU64,
    requests_timed_out: AtomicU64,
    requests_cancelled: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    in_flight_requests: AtomicU64,
    total_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
}

/// Point-in-time values from [`ClientMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientMetricsSnapshot {
    /// Request roundtrips that have started.
    pub requests_started: u64,
    /// Request roundtrips that completed successfully.
    pub requests_succeeded: u64,
    /// Request roundtrips that returned an error, including timeouts.
    pub requests_failed: u64,
    /// Failed request roundtrips caused by the configured request timeout.
    pub requests_timed_out: u64,
    /// Request futures dropped before producing a result.
    pub requests_cancelled: u64,
    /// Kafka request payload bytes, excluding the four-byte frame length.
    pub request_bytes: u64,
    /// Kafka response payload bytes, excluding the four-byte frame length.
    pub response_bytes: u64,
    /// Request roundtrips currently awaiting completion.
    pub in_flight_requests: u64,
    /// Sum of completed and cancelled request latency.
    pub total_latency: Duration,
    /// Highest observed completed or cancelled request latency.
    pub max_latency: Duration,
}

impl ClientMetrics {
    /// Creates an independent metrics handle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a point-in-time snapshot of all request counters.
    pub fn snapshot(&self) -> ClientMetricsSnapshot {
        ClientMetricsSnapshot {
            requests_started: self.inner.requests_started.load(Ordering::Relaxed),
            requests_succeeded: self.inner.requests_succeeded.load(Ordering::Relaxed),
            requests_failed: self.inner.requests_failed.load(Ordering::Relaxed),
            requests_timed_out: self.inner.requests_timed_out.load(Ordering::Relaxed),
            requests_cancelled: self.inner.requests_cancelled.load(Ordering::Relaxed),
            request_bytes: self.inner.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.inner.response_bytes.load(Ordering::Relaxed),
            in_flight_requests: self.inner.in_flight_requests.load(Ordering::Relaxed),
            total_latency: Duration::from_nanos(
                self.inner.total_latency_ns.load(Ordering::Relaxed),
            ),
            max_latency: Duration::from_nanos(self.inner.max_latency_ns.load(Ordering::Relaxed)),
        }
    }

    pub(crate) fn start_request(&self, request_bytes: usize) -> RequestMetricsGuard {
        self.inner.requests_started.fetch_add(1, Ordering::Relaxed);
        self.inner
            .request_bytes
            .fetch_add(usize_to_u64(request_bytes), Ordering::Relaxed);
        self.inner
            .in_flight_requests
            .fetch_add(1, Ordering::Relaxed);
        RequestMetricsGuard {
            metrics: self.clone(),
            started_at: Instant::now(),
            completed: false,
        }
    }
}

impl fmt::Debug for ClientMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ClientMetrics")
            .field(&self.snapshot())
            .finish()
    }
}

impl PartialEq for ClientMetrics {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for ClientMetrics {}

pub(crate) struct RequestMetricsGuard {
    metrics: ClientMetrics,
    started_at: Instant,
    completed: bool,
}

impl RequestMetricsGuard {
    pub(crate) fn succeed(mut self, response_bytes: usize) {
        self.metrics
            .inner
            .requests_succeeded
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .inner
            .response_bytes
            .fetch_add(usize_to_u64(response_bytes), Ordering::Relaxed);
        self.finish();
    }

    pub(crate) fn fail(mut self, timed_out: bool) {
        self.metrics
            .inner
            .requests_failed
            .fetch_add(1, Ordering::Relaxed);
        if timed_out {
            self.metrics
                .inner
                .requests_timed_out
                .fetch_add(1, Ordering::Relaxed);
        }
        self.finish();
    }

    fn finish(&mut self) {
        self.record_latency();
        self.metrics
            .inner
            .in_flight_requests
            .fetch_sub(1, Ordering::Relaxed);
        self.completed = true;
    }

    fn record_latency(&self) {
        let latency_ns = duration_nanos(self.started_at.elapsed());
        self.metrics
            .inner
            .total_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.metrics
            .inner
            .max_latency_ns
            .fetch_max(latency_ns, Ordering::Relaxed);
    }
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.metrics
            .inner
            .requests_cancelled
            .fetch_add(1, Ordering::Relaxed);
        self.record_latency();
        self.metrics
            .inner
            .in_flight_requests
            .fetch_sub(1, Ordering::Relaxed);
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::ClientMetrics;

    #[test]
    fn shared_handle_records_success_and_failure() {
        let metrics = ClientMetrics::new();
        let shared = metrics.clone();

        metrics.start_request(12).succeed(24);
        shared.start_request(8).fail(true);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests_started, 2);
        assert_eq!(snapshot.requests_succeeded, 1);
        assert_eq!(snapshot.requests_failed, 1);
        assert_eq!(snapshot.requests_timed_out, 1);
        assert_eq!(snapshot.request_bytes, 20);
        assert_eq!(snapshot.response_bytes, 24);
        assert_eq!(snapshot.in_flight_requests, 0);
    }

    #[test]
    fn dropped_guard_records_cancellation() {
        let metrics = ClientMetrics::new();
        let guard = metrics.start_request(4);
        assert_eq!(metrics.snapshot().in_flight_requests, 1);

        drop(guard);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests_cancelled, 1);
        assert_eq!(snapshot.in_flight_requests, 0);
    }
}
