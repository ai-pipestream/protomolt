package ai.pipestream.proto.index.spi;

/**
 * Thrown when descriptor hints cannot be turned into a valid {@link IndexingPlan}
 * (e.g. a range hint on a field without a resolvable bound pair, or an unparsable
 * {@code null_value}). Carries the protobuf field path for context.
 */
public class IndexingPlanException extends RuntimeException {

    private final String path;

    public IndexingPlanException(String message, String path) {
        super(message + (path != null ? " (Field: '" + path + "')" : ""));
        this.path = path;
    }

    /** Dot-separated protobuf field path of the offending hint. */
    public String path() {
        return path;
    }
}
