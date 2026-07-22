package ai.pipestream.proto.registry;

/**
 * Two branches of a schema's reference graph map the same import path to different schema
 * content. Compiling would silently use whichever branch was traversed first, so resolution
 * refuses instead: the referencing schemas must agree on what an import path means.
 */
public class ReferenceConflictException extends RegistryStoreException {

    private final String importPath;

    public ReferenceConflictException(String importPath, SchemaReference conflicting) {
        super("Import path '" + importPath + "' resolves to conflicting schema content: "
                + conflicting.subject() + " version " + conflicting.version()
                + " does not match the content already resolved under that name. "
                + "References that share an import path must reference identical schemas.");
        this.importPath = importPath;
    }

    /** The contested import path. */
    public String importPath() {
        return importPath;
    }
}
