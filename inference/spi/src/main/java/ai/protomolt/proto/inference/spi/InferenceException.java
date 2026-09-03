package ai.protomolt.proto.inference.spi;

/**
 * A loud inference failure: an unknown model or provider id, a misconfigured
 * catalog entry, an unreachable backend, or a malformed backend response.
 *
 * <p>The SPI never substitutes a default, a fallback model, or a guessed
 * endpoint — every failure surfaces as this exception with the offending id
 * or endpoint in the message.</p>
 */
public class InferenceException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception with a message naming the failure.
     *
     * @param message what failed and with which id or endpoint
     */
    public InferenceException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and the underlying cause.
     *
     * @param message what failed and with which id or endpoint
     * @param cause the transport or parsing failure underneath
     */
    public InferenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
