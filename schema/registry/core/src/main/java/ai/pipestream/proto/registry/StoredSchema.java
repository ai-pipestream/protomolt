package ai.pipestream.proto.registry;

import java.util.List;
import java.util.Objects;

/**
 * One registered schema version.
 *
 * @param subject     subject the schema is registered under
 * @param version     1-based version within the subject
 * @param globalId    id unique and monotonic across the whole store (all subjects)
 * @param schemaText  the {@code .proto} source text
 * @param references  schema references resolving the text's imports (never {@code null})
 * @param contentHash SHA-256 hex over the canonical schema text plus references — the content
 *                    identity used for idempotent registration
 */
public record StoredSchema(String subject,
                           int version,
                           int globalId,
                           String schemaText,
                           List<SchemaReference> references,
                           String contentHash) {

    public StoredSchema {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(schemaText, "schemaText");
        Objects.requireNonNull(contentHash, "contentHash");
        references = List.copyOf(Objects.requireNonNull(references, "references"));
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1: " + version);
        }
        if (globalId < 1) {
            throw new IllegalArgumentException("globalId must be >= 1: " + globalId);
        }
    }
}
