package ai.pipestream.proto.grpc.validate;

import java.util.List;
import java.util.Map;

/**
 * What the validating interceptors can tell a metrics system, without depending on one — the
 * same seam the ProtoMolt Kafka serde exposes, shaped for calls instead of topics.
 *
 * <p>Implementations are discovered with {@link java.util.ServiceLoader}: put one on the
 * classpath and every interceptor reports to it, nothing to configure. The
 * {@code protomolt-grpc-validation-micrometer} module ships one that turns these calls into
 * Micrometer meters. Every method has an empty default, and listener failures are contained —
 * an exception thrown here is logged once and never fails a call.</p>
 */
public interface GrpcValidationMetricsListener {

    /** The event happened in a server interceptor, judging inbound requests. */
    String SIDE_SERVER = "server";
    /** The event happened in a client interceptor, judging outbound requests. */
    String SIDE_CLIENT = "client";

    /** A request message validated clean and passed along. */
    default void onValidated(String side, String method, String type) {
    }

    /**
     * A request message refused for violating its schema's declared rules.
     *
     * @param ruleIds the id of every violated rule; bounded by the schema's declared rules
     */
    default void onRejected(String side, String method, String type, List<String> ruleIds) {
    }

    /** A request measured against the quality dimensions its schema declares. Server side only. */
    default void onQualityScored(String method, String type, double composite,
                                 Map<String, Double> dimensions) {
    }

    /** A request refused for scoring below the configured quality floor. Server side only. */
    default void onQualityRejected(String method, String type, double composite) {
    }
}
