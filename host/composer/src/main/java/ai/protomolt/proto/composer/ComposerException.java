package ai.protomolt.proto.composer;

/**
 * A composition failure: unknown role, dependency cycle, unreachable
 * role, or a lifecycle violation. Always names the offending role and,
 * where one exists, the configuration that would fix it.
 */
public final class ComposerException extends RuntimeException {

    public ComposerException(String message) {
        super(message);
    }

    public ComposerException(String message, Throwable cause) {
        super(message, cause);
    }
}
