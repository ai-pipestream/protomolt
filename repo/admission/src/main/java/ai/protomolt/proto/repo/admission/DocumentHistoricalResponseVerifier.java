package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Objects;
import java.util.Optional;

/**
 * Bounded verification of selected-read wire evidence. No authorization, retained-source
 * access, compiler execution or fresh payload validation verdict. Inputs are borrowed and
 * must remain stable; the host owns their bytes and separately bounds concurrent parsed heap.
 */
public final class DocumentHistoricalResponseVerifier {
    public static final int MAX_BYTES = 8 * 1024 * 1024;
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private DocumentHistoricalResponseVerifier() {}

    public static void verify(ReadHistoricalOccurrenceRequest request, ReadHistoricalOccurrenceResponse response,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        verifyBinding(request, response, reservations, control);
    }

    static DocumentSchemaAssetBinding verifyBinding(ReadHistoricalOccurrenceRequest request,
            ReadHistoricalOccurrenceResponse response, DocumentAdmissionReservations reservations,
            Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(request); Objects.requireNonNull(response);
        Objects.requireNonNull(reservations); Objects.requireNonNull(control).run();
        if (response.getSerializedSize() > MAX_BYTES) throw new IllegalArgumentException("Selected response exceeds wire bound");
        if (!response.getUnknownFields().asMap().isEmpty() || !VALIDATOR.validate(response).valid())
            throw new IllegalArgumentException("Invalid selected response envelope");
        if (!response.getAddress().equals(request.getAddress()) || !response.getRevisionId().equals(request.getRevisionId())
                || !response.getSelection().equals(request.getSelection()))
            throw new IllegalArgumentException("Selected response differs from request");
        try (var root = DocumentSchemaEvidenceCodec.encodeOwned(response.getRoot(), reservations, control);
             var path = DocumentSchemaEvidenceCodec.encodeOwned(response.getPath(), reservations, control)) {
            if (!root.value().sha256().equals(request.getSelection().getRootSha256())
                    || !path.value().sha256().equals(request.getSelection().getPathSha256()))
                throw new IllegalArgumentException("Selected response evidence digest mismatch");
        }
        var definition = response.getDefinition();
        var reference = DocumentSchemaAdmission.Reference.fromProto(definition.getReference(), control);
        var metadata = DocumentSchemaAssetCodec.decode(reference.metadataCodec(), reference.metadataVersion(),
                definition.getMetadataArtifact(), reference.metadataSha256(), reservations, control);
        var binding = DocumentSchemaAssetBinding.bind(metadata, definition.getDescriptorArtifact(),
                new ClosedDescriptorSet.Limits(MAX_BYTES, 256, 4096, 64), reservations, control);
        var source = metadata.getCompilation().hasSourceArtifactSha256()
                ? Optional.of(metadata.getCompilation().getSourceArtifactSha256()) : Optional.<String>empty();
        if (!reference.typeUrl().equals(metadata.getTypeUrl())
                || !reference.descriptorSha256().equals(metadata.getArtifactSha256())
                || !reference.sourceSha256().equals(source))
            throw new IllegalArgumentException("Selected reference differs from metadata");
        var steps = response.getPath().getStepsList();
        var boundary = steps.getLast().getAnyBoundary();
        var original = response.getOriginal();
        if (!original.getTypeUrl().equals(reference.typeUrl()) || !boundary.getTypeUrl().equals(original.getTypeUrl())
                || original.getValue().size() != boundary.getValueSizeBytes()
                || !DocumentSchemaOccurrences.sha256(original.getValue(), control).equals(boundary.getValueSha256())
                || !boundary.getResolved().getSchema().equals(binding.schema().condition())
                || !boundary.getResolved().getArtifactSha256().equals(binding.schema().artifactSha256()))
            throw new IllegalArgumentException("Selected Any or descriptor differs from retained boundary");
        control.run();
        return binding;
    }
}
