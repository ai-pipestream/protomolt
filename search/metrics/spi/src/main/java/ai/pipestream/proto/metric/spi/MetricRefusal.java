package ai.pipestream.proto.metric.spi;

import java.util.List;

/**
 * One refused metric request: a stable kebab-case code, a human sentence
 * naming what was wrong, and the legal set the caller may pick from. The
 * SPI owns every membership and capability refusal so executors never make
 * policy choices; hosts map the code onto their surface's error shape
 * (gRPC status, action error, HTTP problem).
 */
public final class MetricRefusal extends RuntimeException {

    /** Stable refusal codes; the wire contract of the action surface. */
    public static final String UNKNOWN_SUBJECT = "unknown-subject";
    public static final String UNKNOWN_MEMBER = "unknown-member";
    public static final String UNKNOWN_BACKEND = "unknown-backend";
    public static final String AMBIGUOUS_BACKEND = "ambiguous-backend";
    public static final String UNSUPPORTED_AGGREGATE = "unsupported-aggregate";
    public static final String UNSUPPORTED_FILTER = "unsupported-filter";
    public static final String INVALID_GRAIN = "invalid-grain";
    public static final String INVALID_LIMIT = "invalid-limit";
    public static final String EMPTY_MEASURES = "empty-measures";
    public static final String ROLE_MISMATCH = "role-mismatch";

    private final String code;
    private final List<String> legal;

    /**
     * @param code one of the stable codes above
     * @param message the human sentence naming what was refused
     * @param legal the legal set the caller may pick from; may be empty
     */
    public MetricRefusal(String code, String message, List<String> legal) {
        super(message);
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("code must not be blank");
        }
        this.code = code;
        this.legal = legal == null ? List.of() : List.copyOf(legal);
    }

    /** The stable kebab-case refusal code. */
    public String code() {
        return code;
    }

    /** The legal set the caller may pick from; empty when not enumerable. */
    public List<String> legal() {
        return legal;
    }
}
