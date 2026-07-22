package ai.pipestream.proto.rest;

/**
 * Thrown when a request body exceeds the host's configured limit.
 * Maps to {@code 413 Content Too Large}.
 */
public class RequestTooLargeException extends ProtoRestException {

    private final long maxRequestBytes;

    public RequestTooLargeException(long maxRequestBytes) {
        super("Request body exceeds the limit of " + maxRequestBytes + " bytes");
        this.maxRequestBytes = maxRequestBytes;
    }

    public long maxRequestBytes() {
        return maxRequestBytes;
    }
}
