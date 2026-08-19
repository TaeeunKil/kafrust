package com.kafrust;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Minimal KIP-714 receiver used only by the kafrust live qualification.
 *
 * The request thread only emits a bounded log marker. It deliberately does not
 * parse or persist the payload; the workflow checks receipt, compression-agnostic
 * payload size, and the terminating flag from the broker log.
 */
public final class KafrustTelemetryReporter
        implements ClientTelemetry, MetricsReporter, ClientTelemetryReceiver {
    private static final Logger LOG = LoggerFactory.getLogger(KafrustTelemetryReporter.class);

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("KAFRUST_TELEMETRY_PLUGIN_READY");
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        LOG.info("KAFRUST_TELEMETRY_PLUGIN_INIT metric_count={}", metrics.size());
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        // The live gate only qualifies client payload delivery.
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        // The live gate only qualifies client payload delivery.
    }

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return this;
    }

    @Override
    public void exportMetrics(
            AuthorizableRequestContext context,
            ClientTelemetryPayload payload) {
        ByteBuffer data = payload.data();
        LOG.info(
                "KAFRUST_TELEMETRY client_id={} instance_id={} terminating={} content_type={} bytes={}",
                context.clientId(),
                payload.clientInstanceId(),
                payload.isTerminating(),
                payload.contentType(),
                data.remaining());
    }

    @Override
    public void close() {
        LOG.info("KAFRUST_TELEMETRY_PLUGIN_CLOSED");
    }
}
