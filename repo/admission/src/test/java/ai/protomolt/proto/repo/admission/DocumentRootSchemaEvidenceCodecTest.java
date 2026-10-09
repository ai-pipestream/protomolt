package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Structural codec fixtures; their placeholder hashes do not assert retained artifact existence. */
class DocumentRootSchemaEvidenceCodecTest {
    @Test void releasesSortingBuffersBeforeReservingFinalEvidence() throws Exception {
        var evidence = fixture();
        var expected = DocumentRootSchemaEvidenceCodec.encode(evidence, () -> {});
        long paths = evidence.getOccurrencesList().stream()
                .mapToLong(path -> DocumentSchemaOccurrenceCodec.encode(path, () -> {}).bytes().size()).sum();
        var budget = new Reservations(Math.max(paths, expected.bytes().size()));
        try (var owned = DocumentRootSchemaEvidenceCodec.encodeOwned(evidence, expected.bytes().size(), budget, () -> {})) {
            assertThat(owned.value().bytes()).isEqualTo(expected.bytes());
            assertThat(owned.value().sha256()).isEqualTo(expected.sha256());
            assertThat(budget.current).isEqualTo(expected.bytes().size());
            assertThat(budget.peak).isEqualTo(Math.max(paths, expected.bytes().size()));
            assertThat(budget.calls).isEqualTo(evidence.getOccurrencesCount() + 1);
        }
        assertThat(budget.current).isZero();
        var decoded = DocumentRootSchemaEvidenceCodec.decode(DocumentRootSchemaEvidenceCodec.CODEC, 1,
                expected.bytes(), expected.sha256(), budget, () -> {});
        assertThat(decoded).isEqualTo(read(expected.bytes()));
        assertThat(budget.current).isZero();
        assertThat(budget.peak).isEqualTo(Math.max(paths, expected.bytes().size()));
    }

    @Test void releasesEarlierPathReservationsOnCapacityAndCancellationFailures() {
        var evidence = fixture();
        long first = DocumentSchemaOccurrenceCodec.encode(evidence.getOccurrences(0), () -> {}).bytes().size();
        var tiny = new Reservations(first);
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encodeOwned(evidence, Long.MAX_VALUE, tiny, () -> {}))
                .isSameAs(tiny.exhausted);
        assertThat(tiny.peak).isEqualTo(first);
        assertThat(tiny.current).isZero();
        var budget = new Reservations(1_000_000);
        var cancelled = new CancellationException("cancel second path allocation");
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encodeOwned(evidence, Long.MAX_VALUE, budget, () -> {
            if (budget.calls == 2) throw cancelled;
        })).isSameAs(cancelled);
        assertThat(budget.calls).isEqualTo(2);
        assertThat(budget.current).isZero();
    }

    @Test void rejectsNoncanonicalOrderAndDuplicatePathsWithoutLeakingScratch() throws Exception {
        var canonical = DocumentRootSchemaEvidenceCodec.encode(fixture(), () -> {});
        var parsed = read(canonical.bytes());
        var reversed = parsed.toBuilder().clearOccurrences().addOccurrences(parsed.getOccurrences(1))
                .addOccurrences(parsed.getOccurrences(0)).build();
        var wire = DocumentSchemaEvidenceCodec.encode(reversed, () -> {});
        var budget = new Reservations(1_000_000);
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.decode(DocumentRootSchemaEvidenceCodec.CODEC, 1,
                wire.bytes(), wire.sha256(), budget, () -> {})).hasMessageContaining("ordering");
        assertThat(budget.current).isZero();
        var duplicate = fixture().toBuilder().addOccurrences(fixture().getOccurrences(1)).build();
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encodeOwned(duplicate, Long.MAX_VALUE, budget, () -> {}))
                .hasMessageContaining("duplicate");
        assertThat(budget.current).isZero();
    }

    private static final class Reservations implements DocumentAdmissionReservations {
        final long capacity;
        final IllegalStateException exhausted = new IllegalStateException("test byte capacity exhausted");
        long current, peak;
        int calls;
        Reservations(long capacity) { this.capacity = capacity; }
        @Override public Lease reserve(long bytes) {
            calls++;
            if (bytes > capacity - current) throw exhausted;
            current += bytes; peak = Math.max(peak, current);
            var closed = new java.util.concurrent.atomic.AtomicBoolean();
            return () -> { if (closed.compareAndSet(false, true)) current -= bytes; };
        }
    }

    @Test void enforcesRemainingMemberBytesBeforeCanonicalPathProcessing() {
        var evidence = fixture();
        var encoded = DocumentRootSchemaEvidenceCodec.encode(evidence, () -> {});
        assertThat(DocumentRootSchemaEvidenceCodec.encode(evidence, encoded.bytes().size(), () -> {})).isEqualTo(encoded);
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(evidence, encoded.bytes().size() - 1, () -> {}))
                .hasMessageContaining("member evidence byte limit");
        // Duplicate selectors are detected during path processing. The member allowance
        // must fail before reaching that phase and its canonical byte allocations.
        var duplicate = evidence.toBuilder().addOccurrences(evidence.getOccurrences(1)).build();
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(duplicate, 0, () -> {}))
                .hasMessageContaining("member evidence byte limit");
        assertThatThrownBy(() -> DocumentRootSchemaEvidenceCodec.encode(duplicate, () -> {}))
                .hasMessageContaining("duplicate");
    }

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
