package ai.protomolt.proto.repo.admission;

/**
 * Where a save finds the {@code Document} schema it decodes typed content with
 * (requirements §15.7, decisions 34 and 35): the latest version registered under
 * the {@code ai.protomolt.proto.repo.v1.Document} subject. The save names no
 * writer version. A field the latest version does not know, including one only
 * an older version had, is refused with SCHEMA_MISSING and never dropped.
 */
@FunctionalInterface
public interface DocumentSchemas {
    /** The registry subject of the repository {@code Document}. */
    String SUBJECT = "ai.protomolt.proto.repo.v1.Document";

    /**
     * The latest registered {@code Document} version, as a complete definition.
     *
     * @return the definition every written and sourced fragment is decoded with
     * @throws DocumentCandidateAdmission.SchemaUnavailable when the registry cannot be reached
     * @throws DocumentCandidateAdmission.HostFailure when the subject is absent or does not compile
     */
    DocumentSchemaAdmission.Definition latest();

    /**
     * The compiled contract as the only registered version, for a host configured
     * without a schema registry. Selected explicitly; never a fallback for an
     * unreachable registry.
     */
    static DocumentSchemas compiled() { return BuiltinDocumentSchema::definition; }
}
