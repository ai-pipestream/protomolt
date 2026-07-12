package ai.pipestream.proto.helpers;

/**
 * Raised when protobuf schema validation rejects content (malformed identifiers
 * or conflicting FQN definitions).
 */
public class ProtoSchemaValidationException extends Exception {

    private final String fqn;
    private final String firstSource;
    private final String secondSource;

    public ProtoSchemaValidationException(String message) {
        this(message, null, null, null, null);
    }

    public ProtoSchemaValidationException(String message, Throwable cause) {
        this(message, null, null, null, cause);
    }

    public ProtoSchemaValidationException(
            String message, String fqn, String firstSource, String secondSource) {
        this(message, fqn, firstSource, secondSource, null);
    }

    public ProtoSchemaValidationException(
            String message, String fqn, String firstSource, String secondSource, Throwable cause) {
        super(message, cause);
        this.fqn = fqn;
        this.firstSource = firstSource;
        this.secondSource = secondSource;
    }

    public String getFqn() {
        return fqn;
    }

    public String getFirstSource() {
        return firstSource;
    }

    public String getSecondSource() {
        return secondSource;
    }
}
