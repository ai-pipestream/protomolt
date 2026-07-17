package ai.pipestream.proto.kafka.serde.micrometer;

import ai.pipestream.proto.kafka.serde.SerdeMetricsListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

import java.util.List;

/**
 * The serde's metrics events as Micrometer counters. Put this module on the classpath and every
 * ProtoMolt serde reports — the serde discovers listeners with {@link java.util.ServiceLoader},
 * so there is nothing to configure, and the no-arg constructor binds to
 * {@link Metrics#globalRegistry}, which is where Micrometer-instrumented applications already
 * point their Prometheus (or other) registry.
 *
 * <p>What this buys: the serializer is the choke point every record passes through, and it is
 * already validating each one against the schema's declared rules. These counters turn that into
 * a data-quality feed — violations by rule, per topic and type — from inside the producer and
 * consumer, with no sidecar and no extra pass over the data.</p>
 *
 * <h2>Meters</h2>
 * <ul>
 *   <li>{@code protomolt.serde.records} (direction, topic, type) — records written/read</li>
 *   <li>{@code protomolt.serde.rejections} (direction, topic, type) — records refused for
 *       violating their schema's rules</li>
 *   <li>{@code protomolt.serde.violations} (topic, type, rule) — individual rule violations;
 *       one record can carry several</li>
 *   <li>{@code protomolt.serde.refusals} (topic, reason) — records refused on type identity</li>
 *   <li>{@code protomolt.serde.registry.fallbacks} — registry lookups the packaged descriptor
 *       set had to answer instead (per failed lookup, not per record)</li>
 * </ul>
 *
 * <p>Tag cardinality is bounded by design: topics and types are the deployment's own, reasons
 * are three constants, and rule ids come from the schema's declared rules.</p>
 */
public final class MicrometerSerdeMetrics implements SerdeMetricsListener {

    private final MeterRegistry registry;

    /** Binds to {@link Metrics#globalRegistry}; this is the constructor ServiceLoader uses. */
    public MicrometerSerdeMetrics() {
        this(Metrics.globalRegistry);
    }

    public MicrometerSerdeMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onSerialized(String topic, String type) {
        records("write", topic, type).increment();
    }

    @Override
    public void onDeserialized(String topic, String type) {
        records("read", topic, type).increment();
    }

    @Override
    public void onValidationRejected(String topic, String type, boolean write,
                                     List<String> ruleIds) {
        Counter.builder("protomolt.serde.rejections")
                .description("Records refused for violating their schema's declared rules")
                .tag("direction", write ? "write" : "read")
                .tag("topic", topic)
                .tag("type", type)
                .register(registry)
                .increment();
        for (String ruleId : ruleIds) {
            Counter.builder("protomolt.serde.violations")
                    .description("Individual rule violations; one record can carry several")
                    .tag("topic", topic)
                    .tag("type", type)
                    .tag("rule", ruleId)
                    .register(registry)
                    .increment();
        }
    }

    @Override
    public void onTypeRefused(String topic, String reason) {
        Counter.builder("protomolt.serde.refusals")
                .description("Records refused on type identity rather than data")
                .tag("topic", topic)
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    @Override
    public void onRegistryFallback() {
        Counter.builder("protomolt.serde.registry.fallbacks")
                .description("Registry lookups answered by the packaged descriptor set instead")
                .register(registry)
                .increment();
    }

    private Counter records(String direction, String topic, String type) {
        return Counter.builder("protomolt.serde.records")
                .description("Records serialized and deserialized")
                .tag("direction", direction)
                .tag("topic", topic)
                .tag("type", type)
                .register(registry);
    }
}
