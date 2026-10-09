package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.protomolt.proto.repo.admission.DocumentAnyMaterialization.*;
import static org.assertj.core.api.Assertions.*;

class DocumentRetainedRootMaterializationTest {
    private static final ClosedDescriptorSet.Limits DESCRIPTORS = new ClosedDescriptorSet.Limits(16_000_000, 256, 4096, 100);
    private static final Limits VALUES = new Limits(100_000, 1000, 20);
    @TempDir Path store;

    @Test void freshReadersDecodeFromExactFileRetainedArtifactsWithoutARegistry() throws Exception {
        var f = fixture(Any.pack(StringValue.of("archived")));
        for (int restart = 0; restart < 2; restart++) {
            var reads = new AtomicInteger();
            var decoded = (Decoded) read(f, f.root, f.boundary, f.reference, Mode.MATERIALIZE_IF_AVAILABLE,
                    retained(reads), VALUES, () -> {});
            assertThat(decoded.value().getField(decoded.value().getDescriptorForType().findFieldByName("value")))
                    .isEqualTo("archived");
            assertThat(decoded.schema()).isEqualTo(f.boundary.getResolved());
            assertThat(decoded.occurrence().root()).isEqualTo(f.root);
            assertThat(decoded.occurrence().ordinal()).isZero();
            assertThat(reads).hasValue(2); // Canonical metadata and complete descriptor closure.
        }
    }

    @Test void missingOrCorruptRequiredAssetsAreDataLossButPreserveDoesNotLoadThem() throws Exception {
        var f = fixture(Any.pack(StringValue.of("archived")));
        for (var artifact : f.artifacts.entrySet()) {
            Files.delete(store.resolve(artifact.getKey()));
            var reads = new AtomicInteger();
            var preserved = read(f, f.root, f.boundary, f.reference, Mode.PRESERVE, retained(reads), VALUES, () -> {});
            assertThat(preserved).isInstanceOf(Preserved.class);
            assertThat(preserved.original()).isEqualTo(f.envelope);
            assertThat(reads).hasValue(0);
            assertThatThrownBy(() -> read(f, f.root, f.boundary, f.reference, Mode.MATERIALIZE_IF_AVAILABLE,
                    retained(new AtomicInteger()), VALUES, () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class);
            Files.write(store.resolve(artifact.getKey()), new byte[] {1, 2, 3});
            assertThatThrownBy(() -> read(f, f.root, f.boundary, f.reference, Mode.MATERIALIZE_IF_AVAILABLE,
                    retained(new AtomicInteger()), VALUES, () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class);
            Files.write(store.resolve(artifact.getKey()), artifact.getValue().toByteArray());
        }
    }

    @Test void parsedMapRootIsSelectedByItsExactCoordinateRatherThanFirstMatch() throws Exception {
        var wanted = Any.pack(StringValue.of("wanted"));
        var f = fixture(wanted);
        var document = Document.newBuilder().setDocId("doc")
                .putParserResults("one", ParserResult.newBuilder().setDocument(ParserDocument.newBuilder()
                        .setShape(Any.pack(StringValue.of("other")))).build())
                .putParserResults("two", ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(wanted)).build()).build();
        var part = DocumentPartCodec.split(document, PartLayouts.document()).stream()
                .filter(value -> value.part() == DocumentPart.DOCUMENT_PART_PARSED).findFirst().orElseThrow();
        var inventory = DocumentAnyRootInventory.inspect(f.inventory.containerSchema(),
                DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).build(),
                ByteString.copyFrom(part.bytes()), "doc", new DocumentAnyRootInventory.Limits(1_000_000, 100_000, 30, 100, 100), () -> {});
        var selected = inventory.roots().stream().filter(root -> root.envelope().equals(wanted)).findFirst().orElseThrow();
        var locator = DocumentSchemaRootProjection.project(inventory, selected, () -> {});
        var result = (Decoded) DocumentRetainedRootMaterialization.read(1, inventory, locator, f.boundary,
                Mode.MATERIALIZE_IF_AVAILABLE, f.reference, retained(new AtomicInteger()), VALUES, () -> {});
        assertThat(result.value().getField(result.value().getDescriptorForType().findFieldByName("value"))).isEqualTo("wanted");
        assertThat(result.occurrence().ordinal()).isEqualTo(1);
        assertThat(result.occurrence().root().getAccess(1).getMapKey().getStringValue()).isEqualTo("two");
    }

    @Test void wrongFragmentAndValueIdentityFailBeforeAssetLookup() throws Exception {
        var f = fixture(Any.pack(StringValue.of("archived")));
        var reads = new AtomicInteger();
        var reader = retained(reads);
        assertThatThrownBy(() -> read(f, f.root.toBuilder().setFragmentSha256("a".repeat(64)).build(),
                f.boundary, f.reference, Mode.MATERIALIZE_IF_AVAILABLE, reader, VALUES, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("inventory");
        for (var mode : Mode.values()) {
            assertThatThrownBy(() -> read(f, f.root, f.boundary.toBuilder().setValueSha256("a".repeat(64)).build(),
                    f.reference, mode, reader, VALUES, () -> {}))
                    .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("value identity");
        }
        assertThat(reads).hasValue(0);
    }

    @Test void exactReferenceAndSchemaConditionMustMatchTheRecordedBoundary() throws Exception {
        var f = fixture(Any.pack(StringValue.of("archived")));
        var wrong = new DocumentSchemaAdmission.Reference(f.reference.typeUrl(), "b".repeat(64),
                f.reference.metadataCodec(), f.reference.metadataVersion(), f.reference.metadataSha256(), Optional.empty());
        var reads = new AtomicInteger();
        assertThatThrownBy(() -> read(f, f.root, f.boundary, wrong, Mode.MATERIALIZE_IF_AVAILABLE, retained(reads), VALUES, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("association");
        assertThat(reads).hasValue(0);
        var changed = f.boundary.toBuilder().setResolved(f.boundary.getResolved().toBuilder()
                .setSchema(f.boundary.getResolved().getSchema().toBuilder().setDescriptorFingerprint("b".repeat(64)))).build();
        assertThatThrownBy(() -> read(f, f.root, changed, f.reference, Mode.MATERIALIZE_IF_AVAILABLE, retained(reads), VALUES, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("condition");
    }

    @Test void retainedMalformedPayloadIsDataLossWhileConfiguredCapacityIsNot() throws Exception {
        var valid = fixture(Any.pack(StringValue.of("archived")));
        var refused = (Failed) read(valid, valid.root, valid.boundary, valid.reference, Mode.MATERIALIZE_IF_AVAILABLE,
                retained(new AtomicInteger()), new Limits(1, 100, 10), () -> {});
        assertThat(refused.failure().reason()).isEqualTo(Reason.RESOURCE_LIMIT);
        // Construct inconsistent retained evidence deliberately; this is a corruption case, not a publication.
        var corrupt = fixture(Any.newBuilder().setTypeUrl(valid.envelope.getTypeUrl())
                .setValue(ByteString.copyFrom(new byte[] {0x0a, 5, 1})).build());
        assertThatThrownBy(() -> read(corrupt, corrupt.root, corrupt.boundary, corrupt.reference, Mode.MATERIALIZE_IF_AVAILABLE,
                retained(new AtomicInteger()), VALUES, () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class)
                .hasMessageContaining("cannot be decoded");
    }

    @Test void cancellationAndHostIdentityExceptionsAreNeverReclassifiedAsDataLoss() throws Exception {
        var f = fixture(Any.pack(StringValue.of("archived")));
        var total = new AtomicInteger();
        read(f, f.root, f.boundary, f.reference, Mode.MATERIALIZE_IF_AVAILABLE, retained(new AtomicInteger()), VALUES, total::incrementAndGet);
        for (int stopAt = 1; stopAt <= total.get(); stopAt++) {
            int at = stopAt;
            for (var failure : List.of(new CancellationException("revoked"), new DocumentAnyMaterialization.IdentityMismatch("host check"))) {
                var checks = new AtomicInteger();
                assertThatThrownBy(() -> read(f, f.root, f.boundary, f.reference, Mode.MATERIALIZE_IF_AVAILABLE,
                        retained(new AtomicInteger()), VALUES, () -> { if (checks.incrementAndGet() == at) throw failure; }))
                        .isSameAs(failure);
            }
        }
    }

    @Test void storageExceptionsCannotBeMistakenForEnvelopeIdentityFailures() throws Exception {
        var f = fixture(Any.pack(StringValue.of("archived")));
        var failure = new DocumentAnyMaterialization.IdentityMismatch("storage callback failure");
        var reader = new DocumentRetainedSchemaAssets(ignored -> { throw failure; },
                new DocumentRetainedSchemaAssets.Limits(10, 32_000_000, DESCRIPTORS));
        assertThatThrownBy(() -> read(f, f.root, f.boundary, f.reference, Mode.MATERIALIZE_IF_AVAILABLE,
                reader, VALUES, () -> {})).isSameAs(failure);
    }

    private View read(Fixture f, DocumentSchemaRootLocator root, RepositoryAnyResolution boundary,
            DocumentSchemaAdmission.Reference reference, Mode mode, DocumentRetainedSchemaAssets retained, Limits limits, Runnable control) {
        return DocumentRetainedRootMaterialization.read(0, f.inventory, root, boundary, mode, reference, retained, limits, control);
    }
    private DocumentRetainedSchemaAssets retained(AtomicInteger reads) {
        return new DocumentRetainedSchemaAssets(hash -> {
            reads.incrementAndGet();
            try {
                var path = store.resolve(hash);
                if (!Files.exists(path)) return Optional.empty();
                if (Files.size(path) > 16_000_000) throw new IllegalStateException("fixture artifact exceeds load bound");
                return Optional.of(ByteString.copyFrom(Files.readAllBytes(path)));
            } catch (java.io.IOException failure) { throw new UncheckedIOException(failure); }
        }, new DocumentRetainedSchemaAssets.Limits(10, 32_000_000, DESCRIPTORS));
    }
    private Fixture fixture(Any envelope) throws Exception {
        var descriptor = DescriptorFingerprints.closure(StringValue.getDescriptor());
        var bytes = descriptor.toByteString();
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(StringValue.getDescriptor().getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(descriptor)).build();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl(envelope.getTypeUrl()).setSchema(condition)
                .setArtifactSha256(sha(bytes)).setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("fixture producer compiler not recorded")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("fixture").setVersion("1"))).build();
        var encoded = DocumentSchemaAssetCodec.encode(metadata, () -> {});
        var artifacts = Map.of(sha(bytes), bytes, encoded.sha256(), encoded.bytes());
        for (var artifact : artifacts.entrySet()) Files.write(store.resolve(artifact.getKey()), artifact.getValue().toByteArray());
        var reference = new DocumentSchemaAdmission.Reference(envelope.getTypeUrl(), sha(bytes),
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, encoded.sha256(), Optional.empty());
        var document = Document.newBuilder().setDocId("doc").setStructuredData(envelope).build();
        var part = DocumentPartCodec.split(document, PartLayouts.document()).stream()
                .filter(value -> value.part() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
        var containerSet = DescriptorFingerprints.closure(Document.getDescriptor());
        var container = DocumentSchemaBinding.bind(PublicationSchemaCondition.newBuilder()
                .setTypeName(Document.getDescriptor().getFullName()).setDescriptorFingerprint(DescriptorFingerprints.fingerprint(containerSet)).build(),
                containerSet.toByteString(), DESCRIPTORS, () -> {});
        var inventory = DocumentAnyRootInventory.inspect(container,
                DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build(),
                ByteString.copyFrom(part.bytes()), "doc", new DocumentAnyRootInventory.Limits(1_000_000, 100_000, 30, 100, 100), () -> {});
        var root = DocumentSchemaRootProjection.project(inventory, inventory.roots().getFirst(), () -> {});
        var boundary = RepositoryAnyResolution.newBuilder().setTypeUrl(envelope.getTypeUrl())
                .setValueSha256(sha(envelope.getValue())).setValueSizeBytes(envelope.getValue().size())
                .setResolved(RepositoryResolvedSchema.newBuilder().setSchema(condition).setArtifactSha256(sha(bytes))).build();
        return new Fixture(inventory, root, boundary, reference, artifacts, envelope);
    }
    private static String sha(ByteString bytes) { return DocumentPartCodec.sha256Hex(bytes.toByteArray()); }
    private record Fixture(DocumentAnyRootInventory.Result inventory, DocumentSchemaRootLocator root,
            RepositoryAnyResolution boundary, DocumentSchemaAdmission.Reference reference,
            Map<String, ByteString> artifacts, Any envelope) {}
}
