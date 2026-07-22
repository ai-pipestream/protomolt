package ai.pipestream.proto.compat;

/**
 * A compatibility check could not be carried out — typically because a
 * {@link ai.pipestream.proto.sources.ProtoSourceSet} failed to compile. Distinct from
 * {@link IncompatibleSchemaException}, which reports a check that ran and found violations.
 */
public class CompatibilityException extends Exception {

    public CompatibilityException(String message) {
        super(message);
    }

    public CompatibilityException(String message, Throwable cause) {
        super(message, cause);
    }
}
