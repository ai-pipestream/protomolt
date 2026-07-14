package ai.pipestream.proto.registry;

/**
 * A candidate schema failed compilation at registration time — unparseable text, or imports
 * that do not link against the schema's resolved references. Carries the compiler's message.
 */
public class InvalidSchemaException extends RegistryStoreException {

    public InvalidSchemaException(String subject, String compileMessage, Throwable cause) {
        super("Invalid schema for subject " + subject + ": " + compileMessage, cause);
    }
}
