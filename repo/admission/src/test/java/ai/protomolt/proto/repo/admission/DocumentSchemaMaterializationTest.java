package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

/** Actual serialized nested Any and file-backed definitions; evidence is a fixture, not an admission proof. */
class DocumentSchemaMaterializationTest {
    private static final DocumentSchemaMaterialization.Limits LIMITS =
            new DocumentSchemaMaterialization.Limits(1_000_000, 1_000_000, 16_000_000, 8, 1_000_000, 8);
    @TempDir Path store;

    @Test void ownsNestedResultUntilCloseAndDoesNotResolveUnselectedAssociations() throws Exception {
        var f = fixture(); var budget = new Budget();
        var reads = new ArrayList<String>(); var borrowed = new ArrayList<byte[]>();
        var bytes = f.input.fragment().toByteArray();
        var input = input(f, UnsafeByteOperations.unsafeWrap(bytes), f.input.root());
        var result = DocumentSchemaMaterialization.read(input, f.selection, hash -> {
            reads.add(hash);
            try {
                var buffer = Files.readAllBytes(store.resolve(hash)); borrowed.add(buffer);
                return Optional.of(UnsafeByteOperations.unsafeWrap(buffer));
            } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
        }, LIMITS, budget, () -> {});
        assertThat(budget.bytes).isPositive();
        Arrays.fill(bytes, (byte) 0); borrowed.forEach(value -> Arrays.fill(value, (byte) 0));
        assertThat(result.original().unpack(StringValue.class)).isEqualTo(StringValue.of("retained"));
        assertThat(result.value().getField(result.value().getDescriptorForType().findFieldByNumber(1))).isEqualTo("retained");
        assertThat(result.schema().getArtifactSha256()).isEqualTo(f.child.reference.descriptorSha256());
        assertThat(result.reference()).isEqualTo(f.child.reference.toProto());
        assertThat(DocumentSchemaOccurrenceCodec.encode(result.path(), () -> {}).sha256()).isEqualTo(f.selection.pathSha256());
        assertThat(result.descriptorArtifact()).isEqualTo(ByteString.copyFrom(Files.readAllBytes(store.resolve(f.child.reference.descriptorSha256()))));
        assertThat(result.metadataArtifact()).isEqualTo(ByteString.copyFrom(Files.readAllBytes(store.resolve(f.child.reference.metadataSha256()))));
        assertThat(result.metadata()).isEqualTo(f.child.binding.metadata());
        // A remote consumer can reconstruct the selected type using only delivered bytes.
        // Delete the retained fixture store to ensure the result owns its complete inputs.
        try (var paths = Files.list(store)) { for (var path : paths.toList()) Files.delete(path); }
        var files = ClosedDescriptorSet.load(result.descriptorArtifact(), new ClosedDescriptorSet.Limits(16_000_000, 256, 4096, 64));
        var type = files.stream().flatMap(file -> file.getMessageTypes().stream())
                .filter(value -> value.getFullName().equals(result.schema().getSchema().getTypeName())).findFirst().orElseThrow();
        var offline = DynamicMessage.parseFrom(type, result.original().getValue());
        assertThat(offline.getField(type.findFieldByNumber(1))).isEqualTo("retained");
        assertThat(sha(result.descriptorArtifact())).isEqualTo(result.reference().getDescriptorSha256());
        assertThat(sha(result.metadataArtifact())).isEqualTo(result.reference().getMetadataSha256());
        assertThat(reads).doesNotHaveDuplicates();
        assertThat(reads).doesNotContain(f.unused.reference.metadataSha256(), f.unused.reference.descriptorSha256());
        result.close(); result.close();
        assertThatThrownBy(result::descriptorArtifact).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(result::metadataArtifact).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(result::reference).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(result::path).isInstanceOf(IllegalStateException.class);
        assertThat(budget.bytes).isZero();
        assertThatThrownBy(result::value).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(result::original).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(result::schema).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(result::occurrence).isInstanceOf(IllegalStateException.class);
    }

    @Test void rootPathAndFragmentMustBelongToTheRecordedSelectionBeforeAnyAssetRead() throws Exception {
        var f = fixture();
        var wrong = new DocumentSchemaMaterialization.Selection(f.selection.rootSha256(), "a".repeat(64));
        var wrongRoot = new DocumentSchemaMaterialization.Selection("a".repeat(64), f.selection.pathSha256());
        for (var selected : List.of(wrong, wrongRoot)) {
            var budget = new Budget();
            assertThatThrownBy(() -> DocumentSchemaMaterialization.read(f.input, selected,
                    hash -> { throw new AssertionError("must fail before asset read"); }, LIMITS, budget, () -> {}))
                    .isInstanceOf(selected.equals(wrong) ? DocumentSchemaMaterialization.OccurrenceNotFound.class : DocumentSchemaMaterialization.DataLoss.class);
            assertThat(budget.bytes).isZero();
        }
        var budget = new Budget();
        assertThatThrownBy(() -> DocumentSchemaMaterialization.read(input(f, ByteString.copyFromUtf8("wrong"), f.input.root()),
                f.selection, hash -> { throw new AssertionError("must fail before asset read"); }, LIMITS, budget, () -> {}))
                .isInstanceOf(DocumentSchemaMaterialization.DataLoss.class).hasMessageContaining("fragment");
        assertThat(budget.bytes).isZero();
    }

    @Test void changedRetainedRowAndEvidenceBytesCannotBeSubstituted() throws Exception {
        var f = fixture(); var row = f.input.root();
        var e = row.evidence();
        var badEvidence = new DocumentSchemaAdmission.EncodedEvidence(e.codec(), e.version(),
                e.bytes().concat(ByteString.copyFrom(new byte[] {0})), e.sha256());
        for (var bad : List.of(new DocumentSchemaMaterialization.Root(row.locatorSha256(), "a".repeat(64), row.fragmentSize(), e),
                new DocumentSchemaMaterialization.Root(row.locatorSha256(), row.fragmentSha256(), row.fragmentSize() + 1, e),
                new DocumentSchemaMaterialization.Root(row.locatorSha256(), row.fragmentSha256(), row.fragmentSize(), badEvidence))) {
            var budget = new Budget();
            assertThatThrownBy(() -> DocumentSchemaMaterialization.read(input(f, f.input.fragment(), bad), f.selection,
                    hash -> { throw new AssertionError("must fail before asset read"); }, LIMITS, budget, () -> {}))
                    .isInstanceOf(DocumentSchemaMaterialization.DataLoss.class);
            assertThat(budget.bytes).isZero();
        }
    }

    @Test void missingRequiredDefinitionIsDataLossAndResourceRefusalIsSeparate() throws Exception {
        var f = fixture();
        Files.delete(store.resolve(f.child.reference.descriptorSha256()));
        var budget = new Budget();
        assertThatThrownBy(() -> read(f, LIMITS, budget, () -> {}))
                .isInstanceOf(DocumentSchemaMaterialization.DataLoss.class).hasMessageContaining("missing");
        assertThat(budget.bytes).isZero();
        var tiny = new DocumentSchemaMaterialization.Limits(1_000_000, 1_000_000, 16_000_000, 8, 1, 8);
        assertThatThrownBy(() -> read(f, tiny, budget, () -> {})).isInstanceOf(DocumentSchemaMaterialization.LimitExceeded.class);
        assertThat(budget.bytes).isZero();
    }

    @Test void everyReservationRefusalReleasesEarlierLeasesAndPreservesTheHostException() throws Exception {
        var f = fixture(); var count = new Budget();
        try (var result = read(f, LIMITS, count, () -> {})) { assertThat(result.value()).isNotNull(); }
        for (int at = 1; at <= count.calls; at++) {
            var budget = new Budget(); budget.failAt = at;
            assertThatThrownBy(() -> read(f, LIMITS, budget, () -> {})).isSameAs(budget.refusal);
            assertThat(budget.bytes).as("reservation %s", at).isZero();
        }
    }

    @Test void cancellationAndHostLimitErrorsAreNeverCorruptionAndReleaseOwnership() throws Exception {
        var f = fixture(); var calls = new AtomicInteger();
        try (var result = read(f, LIMITS, new Budget(), calls::incrementAndGet)) { assertThat(result.value()).isNotNull(); }
        // Sample each phase plus the last delivery callback; reservation failures are exhaustive above.
        for (int at : new int[] {1, calls.get() / 4, calls.get() / 2, calls.get() - 1, calls.get()}) {
            for (var failure : List.of(new CancellationException("cancel"),
                    new IllegalArgumentException("host limit"), new DocumentSchemaMaterialization.DataLoss("host error"))) {
                var budget = new Budget(); var current = new AtomicInteger();
                assertThatThrownBy(() -> read(f, LIMITS, budget, () -> {
                    if (current.incrementAndGet() == at) throw failure;
                })).isSameAs(failure);
                assertThat(budget.bytes).isZero();
            }
        }
    }

    @Test void exactDecodedAllowanceSucceedsAndSmallerBoundsRefuseWithoutLeaking() throws Exception {
        var f = fixture(); var e = f.input.root().evidence();
        var evidence = DocumentRootSchemaEvidenceCodec.decode(e.codec(), e.version(), e.bytes(), e.sha256(), () -> {});
        var path = evidence.getOccurrencesList().stream().filter(value -> value.getStepsCount() == 2).findFirst().orElseThrow();
        long bytes = f.input.fragment().size() + (long) e.bytes().size()
                + path.getStepsList().stream().mapToLong(value -> value.getAnyBoundary().getValueSizeBytes()).sum();
        var exact = new DocumentSchemaMaterialization.Limits(f.input.fragment().size(), e.bytes().size(), 16_000_000, 4, bytes, 2);
        var budget = new Budget();
        try (var result = read(f, exact, budget, () -> {})) { assertThat(result.original().unpack(StringValue.class).getValue()).isEqualTo("retained"); }
        assertThat(budget.bytes).isZero();
        for (var limit : List.of(new DocumentSchemaMaterialization.Limits(1_000_000, 1_000_000, 16_000_000, 4, bytes - 1, 2),
                new DocumentSchemaMaterialization.Limits(1_000_000, 1_000_000, 16_000_000, 3, bytes, 2),
                new DocumentSchemaMaterialization.Limits(1_000_000, 1_000_000, 16_000_000, 4, bytes, 1),
                new DocumentSchemaMaterialization.Limits(1_000_000, 1_000_000, 1, 4, bytes, 2))) {
            assertThatThrownBy(() -> read(f, limit, budget, () -> {})).isInstanceOf(DocumentSchemaMaterialization.LimitExceeded.class);
            assertThat(budget.bytes).isZero();
        }
    }

    @Test void oversizedDescriptorClosureIsAResourceFailureNotCorruption() throws Exception {
        var f = fixture();
        var descriptors = DescriptorProtos.FileDescriptorSet.parseFrom(Files.readAllBytes(store.resolve(f.child.reference.descriptorSha256()))).toBuilder();
        for (int i = 0; i < 257; i++) descriptors.addFile(DescriptorProtos.FileDescriptorProto.newBuilder().setName("extra-" + i + ".proto").setSyntax("proto3"));
        var descriptorBytes = descriptors.build().toByteString();
        var metadata = f.child.binding.metadata().toBuilder().setArtifactSha256(sha(descriptorBytes)).build();
        var encodedMetadata = DocumentSchemaAssetCodec.encode(metadata, () -> {});
        Files.write(store.resolve(sha(descriptorBytes)), descriptorBytes.toByteArray());
        Files.write(store.resolve(encodedMetadata.sha256()), encodedMetadata.bytes().toByteArray());
        var reference = new DocumentSchemaAdmission.Reference(metadata.getTypeUrl(), sha(descriptorBytes),
                DocumentSchemaAssetCodec.CODEC, 1, encodedMetadata.sha256(), Optional.empty());
        var original = f.input.root().evidence();
        var evidence = DocumentRootSchemaEvidenceCodec.decode(original.codec(), original.version(), original.bytes(), original.sha256(), () -> {}).toBuilder();
        RepositorySchemaOccurrencePath path = null;
        for (int i = 0; i < evidence.getOccurrencesCount(); i++) if (evidence.getOccurrences(i).getStepsCount() == 2) {
            var builder = evidence.getOccurrences(i).toBuilder();
            var boundary = builder.getSteps(1).getAnyBoundary().toBuilder();
            boundary.setResolved(boundary.getResolved().toBuilder().setArtifactSha256(sha(descriptorBytes)));
            path = builder.setSteps(1, RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(boundary)).build();
            evidence.setOccurrences(i, path);
        }
        var encoded = DocumentRootSchemaEvidenceCodec.encode(evidence.build(), () -> {});
        var row = f.input.root();
        var root = new DocumentSchemaMaterialization.Root(row.locatorSha256(), row.fragmentSha256(), row.fragmentSize(),
                new DocumentSchemaAdmission.EncodedEvidence(original.codec(), original.version(), encoded.bytes(), encoded.sha256()));
        var refs = new ArrayList<>(f.input.references()); refs.set(refs.indexOf(f.child.reference), reference);
        var input = new DocumentSchemaMaterialization.Input(f.input.member(), 0, f.input.fragment(), root, f.input.container(), refs);
        var selected = new DocumentSchemaMaterialization.Selection(row.locatorSha256(), DocumentSchemaOccurrenceCodec.encode(path, () -> {}).sha256());
        var budget = new Budget();
        assertThatThrownBy(() -> read(new Fixture(input, selected, f.child, f.unused), LIMITS, budget, () -> {}))
                .isInstanceOf(DocumentSchemaMaterialization.LimitExceeded.class).hasMessageContaining("file count");
        assertThat(budget.bytes).isZero();
    }

    @Test void readerFailuresKeepTheirIdentityEvenWhenTheirTypeResemblesAnInternalLimit() throws Exception {
        var f = fixture();
        for (var failure : List.of(new IllegalArgumentException("reader error"),
                new ClosedDescriptorSet.LimitExceededException("reader capacity"),
                new java.io.UncheckedIOException(new java.io.IOException("storage unavailable")))) {
            var budget = new Budget();
            assertThatThrownBy(() -> DocumentSchemaMaterialization.read(f.input, f.selection,
                    hash -> { throw failure; }, LIMITS, budget, () -> {})).isSameAs(failure);
            assertThat(budget.bytes).isZero();
        }
    }

    @Test void malformedStoredAssociationFailsAsDataLossBeforeAssetReads() throws Exception {
        var f = fixture(); var reference = f.child.reference;
        var malformed = new DocumentSchemaAdmission.Reference(reference.typeUrl(), "not-a-hash", reference.metadataCodec(),
                reference.metadataVersion(), reference.metadataSha256(), reference.sourceSha256());
        var refs = new ArrayList<>(f.input.references()); refs.set(refs.indexOf(reference), malformed);
        var input = new DocumentSchemaMaterialization.Input(f.input.member(), 0, f.input.fragment(), f.input.root(), f.input.container(), refs);
        var budget = new Budget();
        assertThatThrownBy(() -> DocumentSchemaMaterialization.read(input, f.selection,
                hash -> { throw new AssertionError("must fail before asset read"); }, LIMITS, budget, () -> {}))
                .isInstanceOf(DocumentSchemaMaterialization.DataLoss.class).hasMessageContaining("association");
        assertThat(budget.bytes).isZero();
    }

    private DocumentSchemaMaterialization.Result read(Fixture f, DocumentSchemaMaterialization.Limits limits,
            Budget budget, Runnable control) {
        return DocumentSchemaMaterialization.read(f.input, f.selection, hash -> {
            try { return Files.exists(store.resolve(hash)) ? Optional.of(ByteString.copyFrom(Files.readAllBytes(store.resolve(hash)))) : Optional.empty(); }
            catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
        }, limits, budget, control);
    }
    private static DocumentSchemaMaterialization.Input input(Fixture f, ByteString bytes, DocumentSchemaMaterialization.Root row) {
        return new DocumentSchemaMaterialization.Input(f.input.member(), f.input.ordinal(), bytes, row, f.input.container(), f.input.references());
    }
    private Fixture fixture() throws Exception {
        var child = asset(StringValue.getDescriptor()); var parent = asset(Any.getDescriptor());
        var container = asset(Document.getDescriptor()); var unused = asset(Timestamp.getDescriptor());
        // Its absence must not affect selected decoding.
        Files.delete(store.resolve(unused.reference.metadataSha256()));
        var childAny = Any.pack(StringValue.of("retained")); var rootAny = Any.pack(childAny);
        var document = Document.newBuilder().setDocId("doc").setStructuredData(rootAny).build();
        var part = DocumentPartCodec.split(document, PartLayouts.document()).stream()
                .filter(value -> value.part() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
        var bytes = ByteString.copyFrom(part.bytes());
        var slot = DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var member = DocumentPublicationMember.newBuilder().setDestination(DocumentRevisionCondition.newBuilder()
                .setAddress(NodeAddress.newBuilder().setDocId("doc")))
                .addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setUpload(PublicationUpload.newBuilder()
                        .setSizeBytes(bytes.size()).setSha256(sha(bytes)))).build();
        var inventory = DocumentAnyRootInventory.inspect(container.binding.schema(), slot, bytes, "doc",
                new DocumentAnyRootInventory.Limits(1_000_000, 1_000_000, 64, 1024, 10000), () -> {});
        var locator = DocumentSchemaRootProjection.project(inventory, inventory.roots().getFirst(), () -> {});
        var rootPath = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary(rootAny, parent)).build();
        var path = rootPath.toBuilder().addSteps(boundary(childAny, child)).build();
        var evidence = DocumentRootSchemaEvidenceCodec.encode(DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1)
                .setRoot(locator).addOccurrences(rootPath).addOccurrences(path).build(), () -> {});
        var rootSha = DocumentSchemaRootCodec.encode(locator, () -> {}).sha256();
        var root = new DocumentSchemaMaterialization.Root(rootSha, sha(bytes), bytes.size(),
                new DocumentSchemaAdmission.EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC, 1, evidence.bytes(), evidence.sha256()));
        return new Fixture(new DocumentSchemaMaterialization.Input(member, 0, bytes, root, container.reference,
                List.of(parent.reference, child.reference, unused.reference)),
                new DocumentSchemaMaterialization.Selection(rootSha, DocumentSchemaOccurrenceCodec.encode(path, () -> {}).sha256()), child, unused);
    }
    private Asset asset(com.google.protobuf.Descriptors.Descriptor type) throws Exception {
        var closure = DescriptorFingerprints.closure(type); var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.googleapis.com/" + type.getFullName())
                .setArtifactSha256(sha(bytes)).setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN).setUnknownCompilerReason("fixture compiler unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("fixture").setVersion("1"))).build();
        var encoded = DocumentSchemaAssetCodec.encode(metadata, () -> {});
        Files.write(store.resolve(sha(bytes)), bytes.toByteArray()); Files.write(store.resolve(encoded.sha256()), encoded.bytes().toByteArray());
        return new Asset(new DocumentSchemaAdmission.Reference(metadata.getTypeUrl(), sha(bytes), DocumentSchemaAssetCodec.CODEC,
                1, encoded.sha256(), Optional.empty()), DocumentSchemaAssetBinding.bind(metadata, bytes,
                new ClosedDescriptorSet.Limits(16_000_000, 256, 4096, 64), () -> {}));
    }
    private static RepositorySchemaOccurrenceStep boundary(Any value, Asset asset) {
        return RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                .setTypeUrl(value.getTypeUrl()).setValueSha256(sha(value.getValue())).setValueSizeBytes(value.getValue().size())
                .setResolved(RepositoryResolvedSchema.newBuilder().setSchema(asset.binding.metadata().getSchema())
                        .setArtifactSha256(asset.reference.descriptorSha256()))).build();
    }
    private static String sha(ByteString bytes) { return DocumentSchemaOccurrences.sha256(bytes, () -> {}); }
    private record Asset(DocumentSchemaAdmission.Reference reference, DocumentSchemaAssetBinding binding) {}
    private record Fixture(DocumentSchemaMaterialization.Input input, DocumentSchemaMaterialization.Selection selection, Asset child, Asset unused) {}
    private static final class Budget implements DocumentAdmissionReservations {
        long bytes; int calls; int failAt = -1;
        final IllegalArgumentException refusal = new IllegalArgumentException("host capacity refused");
        @Override public Lease reserve(long size) {
            if (++calls == failAt) throw refusal;
            bytes += size;
            return new Lease() { boolean closed; public void close() { if (!closed) { closed = true; bytes -= size; } } };
        }
    }
}
