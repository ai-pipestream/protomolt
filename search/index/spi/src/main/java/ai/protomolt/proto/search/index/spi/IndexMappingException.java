package ai.protomolt.proto.search.index.spi;

/**
 * Thrown when descriptor hints cannot be turned into a valid {@link IndexMapping}
 * (e.g. a range hint on a field without a resolvable bound pair, or an unparsable
 * {@code null_value}). Carries the protobuf field path for context.
 */
public class IndexMappingException extends RuntimeException {

    private final String path;

    public IndexMappingException(String message, String path) {
        super(message + (path != null ? " (Field: '" + path + "')" : ""));
        this.path = path;
    }

    /** Dot-separated protobuf field path of the offending hint. */
    public String path() {
        return path;
    }
}
