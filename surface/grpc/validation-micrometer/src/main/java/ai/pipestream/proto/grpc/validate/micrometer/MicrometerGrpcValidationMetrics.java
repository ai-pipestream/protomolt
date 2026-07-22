package ai.pipestream.proto.grpc.validate.micrometer;

import ai.pipestream.proto.grpc.validate.GrpcValidationMetricsListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

import java.util.List;
import java.util.Map;

/**
 * The validating interceptors' events as Micrometer meters. Put this module on the classpath
 * and every interceptor reports — discovery is {@link java.util.ServiceLoader}, the no-arg
 * constructor binds to {@link Metrics#globalRegistry}, and nothing is configured. The gRPC
 * sibling of the serde's {@code protomolt-serde-micrometer}.
 *
 * <h2>Meters</h2>
 * <ul>
 *   <li>{@code protomolt.grpc.validation.requests} (side, method, type) — requests validated
 *       clean</li>
 *   <li>{@code protomolt.grpc.validation.rejections} (side, method, type) — requests refused
 *       for violating their schema's rules</li>
 *   <li>{@code protomolt.grpc.validation.violations} (side, method, type, rule) — individual
 *       rule violations; one request can carry several</li>
 *   <li>{@code protomolt.grpc.quality.score} (method, type) — distribution of composite quality
 *       scores</li>
 *   <li>{@code protomolt.grpc.quality.dimension} (method, type, dimension) — the same, per
 *       dimension</li>
 *   <li>{@code protomolt.grpc.quality.rejections} (method, type) — requests refused below the
 *       quality floor</li>
 * </ul>
 *
 * <p>Tag cardinality is bounded: methods and types are the service's own, sides are two
 * constants, rule and dimension ids come from the schema.</p>
 */
public final class MicrometerGrpcValidationMetrics implements GrpcValidationMetricsListener {

    private final MeterRegistry registry;

    /** Binds to {@link Metrics#globalRegistry}; this is the constructor ServiceLoader uses. */
    public MicrometerGrpcValidationMetrics() {
        this(Metrics.globalRegistry);
    }

    public MicrometerGrpcValidationMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onValidated(String side, String method, String type) {
        Counter.builder("protomolt.grpc.validation.requests")
                .description("Requests validated clean against their schema's declared rules")
                .tag("side", side)
                .tag("method", method)
                .tag("type", type)
                .register(registry)
                .increment();
    }

    @Override
    public void onRejected(String side, String method, String type, List<String> ruleIds) {
        Counter.builder("protomolt.grpc.validation.rejections")
                .description("Requests refused for violating their schema's declared rules")
                .tag("side", side)
                .tag("method", method)
                .tag("type", type)
                .register(registry)
                .increment();
        for (String ruleId : ruleIds) {
            Counter.builder("protomolt.grpc.validation.violations")
                    .description("Individual rule violations; one request can carry several")
                    .tag("side", side)
                    .tag("method", method)
                    .tag("type", type)
                    .tag("rule", ruleId)
                    .register(registry)
                    .increment();
        }
    }

    @Override
    public void onQualityScored(String method, String type, double composite,
                                Map<String, Double> dimensions) {
        DistributionSummary.builder("protomolt.grpc.quality.score")
                .description("Composite quality score of requests, per the schema's declared "
                        + "dimensions")
                .tag("method", method)
                .tag("type", type)
                .register(registry)
                .record(composite);
        for (Map.Entry<String, Double> dimension : dimensions.entrySet()) {
            DistributionSummary.builder("protomolt.grpc.quality.dimension")
                    .description("Per-dimension quality scores")
                    .tag("method", method)
                    .tag("type", type)
                    .tag("dimension", dimension.getKey())
                    .register(registry)
                    .record(dimension.getValue());
        }
    }

    @Override
    public void onQualityRejected(String method, String type, double composite) {
        Counter.builder("protomolt.grpc.quality.rejections")
                .description("Requests refused for scoring below the configured quality floor")
                .tag("method", method)
                .tag("type", type)
                .register(registry)
                .increment();
    }
}
