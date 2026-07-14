package ai.pipestream.proto.registry;

/**
 * A registry store operation failed. Unchecked so that read-style interface methods stay
 * clean; {@link SchemaRegistryStore#register} declares it anyway because callers are expected
 * to handle the typed subclasses ({@link IncompatibleRegistrationException},
 * {@link InvalidSchemaException}, {@link ReferenceNotFoundException}).
 */
public class RegistryStoreException extends RuntimeException {

    public RegistryStoreException(String message) {
        super(message);
    }

    public RegistryStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
