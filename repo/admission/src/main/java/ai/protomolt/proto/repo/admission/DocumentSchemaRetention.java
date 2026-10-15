package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.Objects;

/**
 * The retained form of one definition: its exact asset reference and the bytes
 * (descriptor closure, canonical metadata, optional source) that reference names.
 * Produced by the same encoding admission uses, so a reference retained here
 * resolves through {@link DocumentRetainedSchemaAssets} like any admitted schema.
 */
public final class DocumentSchemaRetention {
    private static final DocumentSchemaAdmission.Limits LIMITS =
            new DocumentSchemaAdmission.Limits(1, 1, 1, 1, 1, 64L * 1024 * 1024, 1);

    private DocumentSchemaRetention() {}

    /** A reference and its artifacts, keyed by SHA-256. */
    public record Retained(RepositorySchemaAssetReference reference, Map<String, ByteString> artifacts) {
        public Retained { Objects.requireNonNull(reference); artifacts = Map.copyOf(artifacts); }
    }

    public static Retained retain(DocumentSchemaAdmission.Definition definition) {
        Objects.requireNonNull(definition);
        try (var resources = new DocumentAdmissionResources(bytes -> () -> {})) {
            var definitions = new DocumentSchemaDefinitions(LIMITS, resources, () -> {});
            definitions.add(definition);
            var reference = definitions.references.values().iterator().next().toProto();
            return new Retained(reference, definitions.artifacts);
        }
    }
}
