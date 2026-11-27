package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import java.util.Objects;

/**
 * Built-in typed {@code google.protobuf.Any} schema definitions for external-system
 * bindings and external publication ledgers stored through {@code DocumentService}.
 */
public final class ExternalPublicationSchemas {

    /** Canonical protobuf type URL for {@link ExternalSystemBinding}. */
    public static final String EXTERNAL_SYSTEM_BINDING_TYPE_URL =
            "type.googleapis.com/" + ExternalSystemBinding.getDescriptor().getFullName();

    /** Canonical protobuf type URL for {@link ExternalPublicationLedger}. */
    public static final String EXTERNAL_PUBLICATION_LEDGER_TYPE_URL =
            "type.googleapis.com/" + ExternalPublicationLedger.getDescriptor().getFullName();

    private static final DocumentSchemaAdmission.Definition BINDING_DEFINITION =
            BuiltinDocumentSchema.definition(ExternalSystemBinding.getDescriptor());

    private static final DocumentSchemaAdmission.Definition LEDGER_DEFINITION =
            BuiltinDocumentSchema.definition(ExternalPublicationLedger.getDescriptor());

    private ExternalPublicationSchemas() {
    }

    /**
     * Returns a {@link DocumentSchemaAdmission.Resolver} admitting built-in
     * {@link ExternalSystemBinding} and {@link ExternalPublicationLedger} payloads
     * and returning {@code null} for all other type URLs.
     *
     * @return the built-in external publication schema resolver
     */
    public static DocumentSchemaAdmission.Resolver resolver() {
        return selection -> {
            String typeUrl = selection.typeUrl();
            if (EXTERNAL_SYSTEM_BINDING_TYPE_URL.equals(typeUrl)) {
                return BINDING_DEFINITION;
            }
            if (EXTERNAL_PUBLICATION_LEDGER_TYPE_URL.equals(typeUrl)) {
                return LEDGER_DEFINITION;
            }
            return null;
        };
    }

    /**
     * Composes the built-in external publication schema resolver ahead of {@code fallback}.
     *
     * @param fallback the downstream resolver consulted for non-external payload types
     * @return a composed {@link DocumentSchemaAdmission.Resolver}
     */
    public static DocumentSchemaAdmission.Resolver compose(
            DocumentSchemaAdmission.Resolver fallback) {
        Objects.requireNonNull(fallback, "fallback");
        DocumentSchemaAdmission.Resolver builtin = resolver();
        return selection -> {
            DocumentSchemaAdmission.Definition resolved = builtin.select(selection);
            return resolved != null ? resolved : fallback.select(selection);
        };
    }
}
