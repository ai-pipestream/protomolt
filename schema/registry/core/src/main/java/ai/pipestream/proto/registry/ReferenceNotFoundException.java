package ai.pipestream.proto.registry;

/**
 * A registration named a {@link SchemaReference} whose subject/version does not exist in the
 * store. Registration verifies every reference before anything is written.
 */
public class ReferenceNotFoundException extends RegistryStoreException {

    private final SchemaReference reference;

    public ReferenceNotFoundException(SchemaReference reference) {
        super("Reference " + reference.name() + " -> " + reference.subject()
                + " version " + reference.version() + " does not exist in the store");
        this.reference = reference;
    }

    /** The dangling reference. */
    public SchemaReference reference() {
        return reference;
    }
}
