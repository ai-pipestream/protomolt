package ai.protomolt.proto.http.connect;

/**
 * Size bounds for one gateway. Both are required; the gateway has no built-in limit.
 *
 * @param maxRequestBytes the largest request message accepted, in bytes; a larger request is
 *        refused with {@code resource_exhausted} before it reaches a service
 * @param maxResponseMessageBytes the largest single response message a service may send, in
 *        bytes; a larger one fails the call with {@code resource_exhausted}
 */
public record ConnectLimits(int maxRequestBytes, int maxResponseMessageBytes) {

    /** Refuses a limit that is not positive. */
    public ConnectLimits {
        if (maxRequestBytes < 1) {
            throw new IllegalArgumentException("maxRequestBytes must be positive");
        }
        if (maxResponseMessageBytes < 1) {
            throw new IllegalArgumentException("maxResponseMessageBytes must be positive");
        }
    }
}
