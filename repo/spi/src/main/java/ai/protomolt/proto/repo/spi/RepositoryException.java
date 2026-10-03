package ai.protomolt.proto.repo.spi;

/** Transport-independent repository failure. A transport translates the code at its boundary. */
public final class RepositoryException extends RuntimeException {
    public enum Code { INVALID_ARGUMENT, NOT_FOUND, FAILED_PRECONDITION, PERMISSION_DENIED, CONFLICT, UNSUPPORTED, UNAVAILABLE, INTERNAL, CANCELLED, UNKNOWN, DEADLINE_EXCEEDED, ALREADY_EXISTS, RESOURCE_EXHAUSTED, OUT_OF_RANGE, DATA_LOSS, UNAUTHENTICATED }
    private final Code code;
    public RepositoryException(Code code, String message) {
        super(message);
        this.code = java.util.Objects.requireNonNull(code);
    }
    public RepositoryException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = java.util.Objects.requireNonNull(code);
    }
    public Code code() { return code; }
}
