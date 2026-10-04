package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.RepositoryAnyResolution;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceKeyType;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceMapKey;
import ai.protomolt.proto.repo.v1.RepositoryResolvedSchema;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Projects completed strict checks; not a canonical wire codec or a publication capability. */
final class DocumentSchemaOccurrenceProjection {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));

    private DocumentSchemaOccurrenceProjection() {}

    static List<RepositorySchemaOccurrencePath> project(DocumentPayloadCheck checked, Runnable control) {
        Objects.requireNonNull(checked, "checked");
        Objects.requireNonNull(control, "control");
        active(control);
        if (checked.occurrences().isEmpty())
            throw new IllegalArgumentException("strict archival occurrence evidence required");
        var result = new ArrayList<RepositorySchemaOccurrencePath>();
        for (var occurrence : checked.occurrences()) {
            active(control);
            var path = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1);
            for (var item : occurrence.path()) {
                active(control);
                var step = RepositorySchemaOccurrenceStep.newBuilder();
                switch (item) {
                    case DocumentSchemaOccurrences.Field field -> step.setFieldNumber(field.number());
                    case DocumentSchemaOccurrences.Index index -> step.setRepeatedIndex(index.index());
                    case DocumentSchemaOccurrences.MapKey key -> step.setMapKey(mapKey(key));
                    case DocumentSchemaOccurrences.Boundary boundary -> {
                        var schema = checked.resolvedSchemas().get(new DocumentPayloadCheck.SchemaKey(
                                boundary.typeUrl(), boundary.artifactSha256()));
                        if (schema == null || !schema.artifactSha256().equals(boundary.artifactSha256()))
                            throw new IllegalArgumentException("occurrence schema differs from completed check");
                        step.setAnyBoundary(RepositoryAnyResolution.newBuilder()
                                .setTypeUrl(boundary.typeUrl()).setValueSha256(boundary.valueSha256())
                                .setValueSizeBytes(boundary.valueSizeBytes())
                                .setResolved(RepositoryResolvedSchema.newBuilder()
                                        .setSchema(schema.condition()).setArtifactSha256(schema.artifactSha256())));
                    }
                }
                path.addSteps(step);
            }
            var built = path.build();
            VALIDATOR.validate(built).throwIfInvalid();
            active(control);
            result.add(built);
        }
        return List.copyOf(result);
    }

    static RepositoryOccurrenceMapKey mapKey(DocumentSchemaOccurrences.MapKey key) {
        var result = RepositoryOccurrenceMapKey.newBuilder();
        switch (key.type()) {
            case STRING -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue((String) key.value());
            case BOOL -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_BOOL).setBoolValue((Boolean) key.value());
            case INT32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT32).setSignedValue((Integer) key.value());
            case SINT32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT32).setSignedValue((Integer) key.value());
            case SFIXED32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED32).setSignedValue((Integer) key.value());
            case UINT32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT32).setUnsignedValue(Integer.toUnsignedLong((Integer) key.value()));
            case FIXED32 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED32).setUnsignedValue(Integer.toUnsignedLong((Integer) key.value()));
            case INT64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_INT64).setSignedValue((Long) key.value());
            case SINT64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SINT64).setSignedValue((Long) key.value());
            case SFIXED64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED64).setSignedValue((Long) key.value());
            case UINT64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_UINT64).setUnsignedValue((Long) key.value());
            case FIXED64 -> result.setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED64).setUnsignedValue((Long) key.value());
            default -> throw new IllegalArgumentException("unsupported map key type");
        }
        return result.build();
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("occurrence projection interrupted");
        control.run();
    }
}
