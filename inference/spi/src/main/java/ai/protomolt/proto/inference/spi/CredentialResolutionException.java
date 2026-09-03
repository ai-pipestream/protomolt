package ai.protomolt.proto.inference.spi;

/**
 * A loud credential-resolution failure: a malformed reference, an unsupported
 * scheme, or a reference that does not resolve (e.g. an unset or empty
 * environment variable).
 *
 * <p>Messages describe the failure class only. Neither the offending
 * reference nor any resolved credential material ever appears in a message —
 * a reference is a pointer to a secret, and the secret itself must never
 * reach a log line through an exception.</p>
 */
public class CredentialResolutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception with a message naming the failure class.
     *
     * @param message what failed, without the reference or any credential material
     */
    public CredentialResolutionException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a message and the underlying cause.
     *
     * @param message what failed, without the reference or any credential material
     * @param cause the failure underneath
     */
    public CredentialResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
