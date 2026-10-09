package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.RepositoryAnyResolution;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/** Internal strict replay of one root. The host authenticates revision, metadata and policy. */
final class DocumentSchemaReplay {
    record Limits(int maxPaths, long maxEncodedBytes, long maxSteps) {
        Limits {
            if (maxPaths < 1 || maxEncodedBytes < 1 || maxSteps < 1)
                throw new IllegalArgumentException("positive schema replay limits required");
        }
    }

    private DocumentSchemaReplay() {}

    /**
     * Inputs are already parsed under their transport/storage allocation budgets.
     * Persisted path bytes must pass the occurrence codec's decode/digest checks
     * before this call; parsed objects cannot prove their original wire encoding.
     * Only exact recorded occurrences select retained assets; no registry lookup exists.
     * This reruns the supplied validator, not the historical runtime or semantic judge.
     */
    static DocumentPayloadCheck.AssetResult check(Any candidate, List<RepositorySchemaOccurrencePath> recorded,
            Map<DocumentPayloadCheck.SchemaKey, RepositorySchemaAsset> metadata,
            DocumentRetainedSchemaAssets retained, ProtoValidator validator,
            DocumentPayloadCheck.Limits payloadLimits, DocumentSchemaOccurrences.Limits evidenceLimits,
            Limits limits, Runnable control) throws InvalidProtocolBufferException {
        return check(candidate, recorded, metadata, retained, validator, payloadLimits, evidenceLimits,
                limits, java.time.Instant.now(), control);
    }

    static DocumentPayloadCheck.AssetResult check(Any candidate, List<RepositorySchemaOccurrencePath> recorded,
            Map<DocumentPayloadCheck.SchemaKey, RepositorySchemaAsset> metadata,
            DocumentRetainedSchemaAssets retained, ProtoValidator validator,
            DocumentPayloadCheck.Limits payloadLimits, DocumentSchemaOccurrences.Limits evidenceLimits,
            Limits limits, java.time.Instant evaluatedAt, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(evaluatedAt, "evaluatedAt");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(recorded, "recorded");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(retained, "retained");
        Objects.requireNonNull(limits, "limits");
        active(control);
        if (recorded.isEmpty() || recorded.size() > limits.maxPaths())
            throw new IllegalArgumentException("schema replay path count exceeds limit or is empty");
        var index = new HashMap<DocumentPayloadCheck.ResolutionRequest, RepositoryAnyResolution>();
        var expected = new HashSet<RepositorySchemaOccurrencePath>();
        long bytes = 0;
        long steps = 0;
        for (var path : recorded) {
            active(control);
            // Bound aggregate input before canonical encoding allocates its output buffer.
            if (path.getSerializedSize() > limits.maxEncodedBytes() - bytes
                    || path.getStepsCount() > limits.maxSteps() - steps)
                throw new IllegalArgumentException("aggregate schema replay evidence exceeds limit");
            // Replay consumes the parsed path, not its encoded bytes or digest.
            // The codec's validated measurement enforces the same canonical wire
            // bounds without allocating an output buffer solely to count it.
            int measured = DocumentSchemaEvidenceCodec.measureAndValidate(path, () -> active(control));
            if (measured > limits.maxEncodedBytes() - bytes)
                throw new IllegalArgumentException("aggregate schema replay evidence exceeds limit");
            bytes += measured;
            steps += path.getStepsCount();
            if (!expected.add(path)) throw new IllegalArgumentException("duplicate schema replay path");
            var prefix = new ArrayList<DocumentSchemaOccurrences.Step>();
            for (int i = 0; i < path.getStepsCount() - 1; i++) {
                active(control);
                prefix.add(step(path.getSteps(i)));
            }
            var boundary = path.getSteps(path.getStepsCount() - 1).getAnyBoundary();
            var request = new DocumentPayloadCheck.ResolutionRequest(boundary.getTypeUrl(), prefix,
                    boundary.getValueSha256(), boundary.getValueSizeBytes());
            if (index.putIfAbsent(request, boundary) != null)
                throw new IllegalArgumentException("conflicting schema replay selection");
            requireMetadata(boundary, metadata);
        }
        if (candidate.getValue().size() > payloadLimits.maxBytes())
            throw new IllegalArgumentException("aggregate payload bytes exceed limit");
        var rootRequest = new DocumentPayloadCheck.ResolutionRequest(candidate.getTypeUrl(), List.of(),
                DocumentSchemaOccurrences.sha256(candidate.getValue(), () -> active(control)), candidate.getValue().size());
        var rootBoundary = requireSelection(index, rootRequest);
        var root = retained.resolve(requireMetadata(rootBoundary, metadata), () -> active(control));
        var checked = DocumentPayloadCheck.checkContextualAssets(root, candidate, rootBoundary.getTypeUrl(),
                validator, payloadLimits, () -> active(control), request -> {
                    var selection = requireSelection(index, request);
                    return retained.resolve(requireMetadata(selection, metadata), () -> active(control));
                }, evidenceLimits, evaluatedAt);
        var actual = DocumentSchemaOccurrenceProjection.project(checked.payload(), () -> active(control));
        if (actual.size() != expected.size() || !expected.equals(new HashSet<>(actual)))
            throw new IllegalArgumentException("rechecked occurrences differ from retained evidence");
        active(control);
        return checked;
    }

    private static RepositoryAnyResolution requireSelection(
            Map<DocumentPayloadCheck.ResolutionRequest, RepositoryAnyResolution> index,
            DocumentPayloadCheck.ResolutionRequest request) {
        var selected = index.get(request);
        if (selected == null) throw new IllegalArgumentException("candidate occurrence has no exact retained schema selection");
        return selected;
    }

    private static RepositorySchemaAsset requireMetadata(RepositoryAnyResolution boundary,
            Map<DocumentPayloadCheck.SchemaKey, RepositorySchemaAsset> metadata) {
        var key = new DocumentPayloadCheck.SchemaKey(boundary.getTypeUrl(), boundary.getResolved().getArtifactSha256());
        var asset = metadata.get(key);
        if (asset == null || !asset.getTypeUrl().equals(key.typeUrl())
                || !asset.getArtifactSha256().equals(key.artifactSha256())
                || !asset.getSchema().equals(boundary.getResolved().getSchema()))
            throw new DocumentRetainedSchemaAssets.DataLoss("retained occurrence metadata differs from schema selection");
        return asset;
    }

    private static DocumentSchemaOccurrences.Step step(RepositorySchemaOccurrenceStep step) {
        return switch (step.getStepCase()) {
            case FIELD_NUMBER -> new DocumentSchemaOccurrences.Field(step.getFieldNumber());
            case REPEATED_INDEX -> new DocumentSchemaOccurrences.Index(step.getRepeatedIndex());
            case ANY_BOUNDARY -> new DocumentSchemaOccurrences.Boundary(step.getAnyBoundary().getTypeUrl(),
                    step.getAnyBoundary().getValueSha256(), step.getAnyBoundary().getResolved().getArtifactSha256(),
                    step.getAnyBoundary().getValueSizeBytes());
            case MAP_KEY -> {
                var key = step.getMapKey();
                yield switch (key.getType()) {
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_STRING -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.STRING, key.getStringValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_BOOL -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.BOOL, key.getBoolValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_INT32 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.INT32, (int) key.getSignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_SINT32 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.SINT32, (int) key.getSignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED32 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.SFIXED32, (int) key.getSignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_UINT32 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.UINT32, (int) key.getUnsignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED32 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.FIXED32, (int) key.getUnsignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_INT64 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.INT64, key.getSignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_SINT64 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.SINT64, key.getSignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_SFIXED64 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.SFIXED64, key.getSignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_UINT64 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.UINT64, key.getUnsignedValue());
                    case REPOSITORY_OCCURRENCE_KEY_TYPE_FIXED64 -> new DocumentSchemaOccurrences.MapKey(FieldDescriptor.Type.FIXED64, key.getUnsignedValue());
                    default -> throw new IllegalArgumentException("unsupported retained map key type");
                };
            }
            default -> throw new IllegalArgumentException("unsupported retained occurrence step");
        };
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("schema replay interrupted");
        control.run();
    }
}
