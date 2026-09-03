package ai.protomolt.proto.metric.spi;

import java.util.List;

/**
 * A metric declaration that cannot build: every violation, each naming the
 * field path that carries it. Thrown at mapping-build time (mount), never at
 * the first query — a caller fixing violations one at a time is a slow loop,
 * so all of them report at once.
 */
public final class MetricSchemaException extends RuntimeException {

    private final List<String> violations;

    /**
     * @param messageType the message type whose declarations are broken
     * @param violations every violation, each naming its field path
     */
    public MetricSchemaException(String messageType, List<String> violations) {
        super("metric declarations on " + messageType + " cannot build: "
                + String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    /** Every violation, each naming its field path. */
    public List<String> violations() {
        return violations;
    }
}
