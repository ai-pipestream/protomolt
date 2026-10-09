package ai.protomolt.proto.repo.blob.spi;

/** Provider-neutral failure for a remote byte store, retaining its original cause. */
public final class BlobStoreException extends RuntimeException {
    public enum Code {
        CANCELLED, UNKNOWN, INVALID_ARGUMENT, DEADLINE_EXCEEDED, NOT_FOUND, ALREADY_EXISTS,
        PERMISSION_DENIED, RESOURCE_EXHAUSTED, FAILED_PRECONDITION, ABORTED, OUT_OF_RANGE,
        UNIMPLEMENTED, INTERNAL, UNAVAILABLE, DATA_LOSS, UNAUTHENTICATED
    }
    private final Code code;
    public BlobStoreException(Code code, String message, Throwable cause) {
        super(message, cause);
        this.code = java.util.Objects.requireNonNull(code);
    }
    public Code code() { return code; }
}
