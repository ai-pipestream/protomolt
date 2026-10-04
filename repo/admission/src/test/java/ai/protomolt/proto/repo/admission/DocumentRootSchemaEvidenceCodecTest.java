package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Structural codec fixtures; their placeholder hashes do not assert retained artifact existence. */
class DocumentRootSchemaEvidenceCodecTest {
    @Test void canonicalOrderingIsIndependentOfInputOrder() throws Exception {
        var evidence = fixture();
        var reversed = evidence.toBuilder().clearOccurrences().addOccurrences(evidence.getOccurrences(1))
                .addOccurrences(evidence.getOccurrences(0)).build();
        var encoded = DocumentRootSchemaEvidenceCodec.encode(evidence, () -> {});
        assertThat(DocumentRootSchemaEvidenceCodec.encode(reversed, () -> {})).isEqualTo(encoded);
        var decoded = read(encoded.bytes());
        assertThat(decoded.getRoot()).isEqualTo(evidence.getRoot());
        assertThat(decoded.getOccurrencesList()).containsExactlyInAnyOrderElementsOf(evidence.getOccurrencesList());
        var unsorted = decoded.toBuilder().clearOccurrences().addOccurrences(decoded.getOccurrences(1))
                .addOccurrences(decoded.getOccurrences(0)).build().toByteString();
        assertThatThrownBy(() -> read(unsorted)).hasMessageContaining("ordering");
    }

    @Test void rejectsDuplicatePathsAndDifferentRootBoundaries() {
        var evidence = fixture();
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(evidence.toBuilder()
                .addOccurrences(evidence.getOccurrences(1)).build(), () -> {})).hasMessageContaining("duplicate");
        var nested = evidence.getOccurrences(1);
        var changed = nested.toBuilder().setSteps(0, nested.getSteps(0).toBuilder()
                .setAnyBoundary(nested.getSteps(0).getAnyBoundary().toBuilder().setValueSha256("f".repeat(64))));
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(evidence.toBuilder()
                .setOccurrences(1, changed).build(), () -> {})).hasMessageContaining("disagree on root");
        var conflicting = nested.toBuilder().setSteps(nested.getStepsCount() - 1, nested.getSteps(nested.getStepsCount() - 1)
                .toBuilder().setAnyBoundary(nested.getSteps(nested.getStepsCount() - 1).getAnyBoundary()
                        .toBuilder().setValueSha256("f".repeat(64))));
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(evidence.toBuilder()
                .addOccurrences(conflicting).build(), () -> {})).hasMessageContaining("conflicting evidence occurrence selector");
    }

    @Test void rejectsUnknownFieldsWrongFormatAndNoncanonicalWire() throws Exception {
        var encoded = DocumentRootSchemaEvidenceCodec.encode(fixture(), () -> {});
        var unknown = encoded.bytes().concat(ByteString.copyFrom(new byte[]{(byte) 0xa0, 0x06, 1}));
        assertThatThrownBy(() -> read(unknown)).hasMessageContaining("unknown schema evidence");
        var duplicateVersion = encoded.bytes().concat(ByteString.copyFrom(new byte[]{8, 1}));
        assertThatThrownBy(() -> read(duplicateVersion)).hasMessageContaining("noncanonical");
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.decode(DocumentSchemaRootCodec.CODEC, 1,
                encoded.bytes(), encoded.sha256(), () -> {})).hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.decode(DocumentRootSchemaEvidenceCodec.CODEC, 2,
                encoded.bytes(), encoded.sha256(), () -> {})).hasMessageContaining("unsupported");
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.decode(DocumentRootSchemaEvidenceCodec.CODEC, 1,
                encoded.bytes(), "0".repeat(64), () -> {})).hasMessageContaining("digest mismatch");
    }

    @Test void boundsAggregateTextAndBytesAndPreservesCancellation() {
        var evidence = fixture();
        var path = evidence.getOccurrences(1);
        var longKey = RepositorySchemaOccurrenceStep.newBuilder().setMapKey(RepositoryOccurrenceMapKey.newBuilder()
                .setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue("x".repeat(600_000))).build();
        var first = path.toBuilder().addSteps(1, longKey).build();
        var second = path.toBuilder().addSteps(1, longKey).addSteps(2,
                RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(3)).build();
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(evidence.toBuilder().clearOccurrences()
                .addOccurrences(evidence.getOccurrences(0)).addOccurrences(first).addOccurrences(second).build(), () -> {}))
                .hasMessageContaining("text exceeds limit");
        assertThatThrownBy(() -> read(ByteString.copyFrom(new byte[DocumentSchemaEvidenceCodec.MAX_BYTES + 1])))
                .hasMessageContaining("byte bound");
        var cancelled = new CancellationException("cancel evidence");
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(evidence, () -> { throw cancelled; })).isSameAs(cancelled);
    }

    private static DocumentRootSchemaEvidence read(ByteString bytes) throws Exception {
        return DocumentRootSchemaEvidenceCodec.decode(DocumentRootSchemaEvidenceCodec.CODEC, 1, bytes,
                DocumentSchemaOccurrences.sha256(bytes, () -> {}), () -> {});
    }

    private static DocumentRootSchemaEvidence fixture() {
        var schema = RepositoryResolvedSchema.newBuilder().setArtifactSha256("b".repeat(64))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("fixture.Root").setDescriptorFingerprint("c".repeat(64))).build();
        var boundary = RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                .setTypeUrl("fixture/fixture.Root").setValueSha256("d".repeat(64)).setValueSizeBytes(1).setResolved(schema)).build();
        var rootPath = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary).build();
        var nested = rootPath.toBuilder().addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(1)).addSteps(boundary).build();
        var root = DocumentSchemaRootLocator.newBuilder().setEncodingVersion(1)
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                .setLayoutPolicy("protomolt-document-parts/v1").setFragmentSha256("a".repeat(64)).setFragmentSizeBytes(1)
                .setContainerSchema(schema.toBuilder().setSchema(schema.getSchema().toBuilder().setTypeName(Document.getDescriptor().getFullName())))
                .addAccess(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4));
        return DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(root)
                .addAllOccurrences(List.of(rootPath, nested)).build();
    }
}
