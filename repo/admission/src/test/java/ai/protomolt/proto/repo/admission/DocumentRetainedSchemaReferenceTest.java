package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DynamicMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class DocumentRetainedSchemaReferenceTest {
    private static final ClosedDescriptorSet.Limits DESCRIPTOR_LIMITS =
            new ClosedDescriptorSet.Limits(1_000_000, 10, 10, 10);
    @TempDir Path store;

    @Test void freshReaderResolvesCanonicalOfflineImportAndRetainedSource() throws Exception {
        var fixture = fixture(false);
        write(fixture);
        var reads = counts();
        var reader = new DocumentRetainedSchemaAssets(readCounting(reads), limits(fixture.totalBytes()));
        var bound = reader.resolve(fixture.reference(), () -> {});
        assertThat(bound.schema().type().getFullName()).isEqualTo("retained.Root");
        var rootType = bound.schema().type();
        var childField = rootType.findFieldByName("child");
        var childType = childField.getMessageType();
        var child = DynamicMessage.newBuilder(childType).setField(childType.findFieldByName("name"), "offline import").build();
        var root = DynamicMessage.newBuilder(rootType).setField(childField, child).build();
        assertThat(DynamicMessage.parseFrom(rootType, root.toByteString()).getField(childField)).isEqualTo(child);
        assertThat(bound.metadata().getCompilation().getUnknownCompilerReason())
                .isEqualTo("fixture producer did not identify its compiler");
        assertThat(reads).containsEntry(fixture.descriptorHash(), 1).containsEntry(fixture.metadataHash(), 1)
                .containsEntry(fixture.sourceHash(), 1);
    }

    @Test void exactAssociationAndSourcePresenceAreRequired() throws Exception {
        var fixture = fixture(false);
        write(fixture);
        var wrongUrl = ref(fixture, "type.test/retained.Other", fixture.descriptorHash(), Optional.of(fixture.sourceHash()));
        var wrongDescriptor = ref(fixture, fixture.metadata().getTypeUrl(), "e".repeat(64), Optional.of(fixture.sourceHash()));
        var wrongSource = ref(fixture, fixture.metadata().getTypeUrl(), fixture.descriptorHash(), Optional.of("f".repeat(64)));
        var absentSource = ref(fixture, fixture.metadata().getTypeUrl(), fixture.descriptorHash(), Optional.empty());
        for (var reference : new DocumentRetainedSchemaAssets.Reference[]{wrongUrl, wrongDescriptor, wrongSource, absentSource}) {
            assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read, limits(fixture.totalBytes()))
                    .resolve(reference, () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class)
                    .hasMessageContaining("association differs");
        }
        var noSource = fixture.metadata().toBuilder().setCompilation(fixture.metadata().getCompilation().toBuilder()
                .clearSourceArtifactSha256()).build();
        var noSourceBytes = DocumentSchemaAssetCodec.encode(noSource, () -> {}).bytes();
        String noSourceHash = sha(noSourceBytes);
        var mismatch = new DocumentRetainedSchemaAssets.Reference(noSource.getTypeUrl(), fixture.descriptorHash(),
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, noSourceHash, Optional.of(fixture.sourceHash()));
        Files.write(path(noSourceHash), noSourceBytes.toByteArray());
        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read, limits(fixture.totalBytes() + noSourceBytes.size()))
                .resolve(mismatch, () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class)
                .hasMessageContaining("association differs");
    }

    @Test void importedReferenceWithoutSourceLoadsExactlyMetadataAndDescriptor() throws Exception {
        var fixture = fixture(false);
        var metadata = fixture.metadata().toBuilder().setCompilation(fixture.metadata().getCompilation()
                .toBuilder().clearSourceArtifactSha256()).build();
        var encoded = DocumentSchemaAssetCodec.encode(metadata, () -> {});
        Files.write(path(fixture.descriptorHash()), fixture.descriptor().toByteArray());
        Files.write(path(encoded.sha256()), encoded.bytes().toByteArray());
        var reference = new DocumentRetainedSchemaAssets.Reference(metadata.getTypeUrl(), fixture.descriptorHash(),
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, encoded.sha256(), Optional.empty());
        var reads = counts();
        var reader = new DocumentRetainedSchemaAssets(readCounting(reads),
                limits((long) fixture.descriptor().size() + encoded.bytes().size()));
        assertThat(reader.resolve(reference, () -> {}).metadata()).isEqualTo(metadata);
        assertThat(reads).hasSize(2).containsEntry(encoded.sha256(), 1).containsEntry(fixture.descriptorHash(), 1);
        assertThat(Files.exists(path(fixture.sourceHash()))).isFalse();
    }

    @Test void corruptionAbsenceAndStorageDenialKeepTheirDistinctOutcomes() throws Exception {
        var fixture = fixture(false);
        write(fixture);
        Files.delete(path(fixture.metadataHash()));
        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read, limits(fixture.totalBytes()))
                .resolve(fixture.reference(), () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class)
                .hasMessageContaining("missing");
        write(fixture);
        Files.write(path(fixture.metadataHash()), new byte[]{1, 2, 3});
        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read, limits(fixture.totalBytes()))
                .resolve(fixture.reference(), () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class);

        write(fixture);
        Files.delete(path(fixture.sourceHash()));
        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read, limits(fixture.totalBytes()))
                .resolve(fixture.reference(), () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class)
                .hasMessageContaining("missing");

        var denied = new SecurityException("retained asset access denied");
        var failing = new DocumentRetainedSchemaAssets(hash -> { throw denied; }, limits(fixture.totalBytes()));
        assertThatThrownBy(() -> failing.resolve(fixture.reference(), () -> {})).isSameAs(denied);
        var ioFailure = new UncheckedIOException(new IOException("storage denied"));
        var ioReader = new DocumentRetainedSchemaAssets(hash -> { throw ioFailure; }, limits(fixture.totalBytes()));
        assertThatThrownBy(() -> ioReader.resolve(fixture.reference(), () -> {})).isSameAs(ioFailure);
    }

    @Test void failedSourceResolutionDoesNotCommitAndRetryRereadsEveryUncachedArtifact() throws Exception {
        var fixture = fixture(false);
        write(fixture);
        Files.delete(path(fixture.sourceHash()));
        var reads = counts();
        var reader = new DocumentRetainedSchemaAssets(readCounting(reads), limits(fixture.totalBytes()));
        assertThatThrownBy(() -> reader.resolve(fixture.reference(), () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class);
        assertThat(reads).containsEntry(fixture.metadataHash(), 1).containsEntry(fixture.descriptorHash(), 1)
                .containsEntry(fixture.sourceHash(), 1);
        Files.write(path(fixture.sourceHash()), fixture.source().toByteArray());
        assertThat(reader.resolve(fixture.reference(), () -> {}).schema().type().getFullName()).isEqualTo("retained.Root");
        assertThat(reads).containsEntry(fixture.metadataHash(), 2).containsEntry(fixture.descriptorHash(), 2)
                .containsEntry(fixture.sourceHash(), 2);
    }

    @Test void combinedMetadataDescriptorAndSourceBudgetIsExactAndCacheStillChecksAccess() throws Exception {
        var fixture = fixture(false);
        write(fixture);
        var reads = counts();
        var reader = new DocumentRetainedSchemaAssets(readCounting(reads), limits(fixture.totalBytes()));
        reader.resolve(fixture.reference(), () -> {});
        var denied = new SecurityException("cached reference access revoked");
        assertThatThrownBy(() -> reader.resolve(fixture.reference(), () -> { throw denied; })).isSameAs(denied);
        assertThat(reads).containsEntry(fixture.metadataHash(), 1).containsEntry(fixture.descriptorHash(), 1)
                .containsEntry(fixture.sourceHash(), 1);

        var alias = fixture.metadata().toBuilder().setTypeUrl("alias/retained.Root").build();
        var aliasBytes = DocumentSchemaAssetCodec.encode(alias, () -> {}).bytes();
        String aliasHash = sha(aliasBytes);
        var aliasReference = ref(fixture, alias.getTypeUrl(), fixture.descriptorHash(), Optional.of(fixture.sourceHash()), aliasHash);
        assertThatThrownBy(() -> reader.resolve(aliasReference, () -> {})).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("byte count exceeds limit");
        assertThat(reads).doesNotContainKey(aliasHash);

        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read,
                limits(fixture.totalBytes() - 1)).resolve(fixture.reference(), () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("byte count exceeds limit");
    }

    @Test void equalDescriptorAndSourceDigestIsChargedAndReadOnlyOnce() throws Exception {
        var fixture = fixture(true);
        write(fixture);
        var reads = counts();
        var budget = fixture.metadataBytes().size() + fixture.descriptor().size();
        var reader = new DocumentRetainedSchemaAssets(readCounting(reads), limits(budget));
        reader.resolve(fixture.reference(), () -> {});
        assertThat(reads).containsEntry(fixture.metadataHash(), 1).containsEntry(fixture.descriptorHash(), 1)
                .hasSize(2);
    }

    @Test void rejectsMetadataEncodingBeforeIoAndRejectsRawDuplicateFieldAfterCanonicalReencode() throws Exception {
        var fixture = fixture(false);
        write(fixture);
        var reads = counts();
        var reader = new DocumentRetainedSchemaAssets(readCounting(reads), limits(fixture.totalBytes()));
        var unsupported = new DocumentRetainedSchemaAssets.Reference(fixture.metadata().getTypeUrl(), fixture.descriptorHash(),
                "other-codec", 1, fixture.metadataHash(), Optional.of(fixture.sourceHash()));
        assertThatThrownBy(() -> reader.resolve(unsupported, () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class)
                .hasMessageContaining("unsupported");
        assertThat(reads).isEmpty();

        // Repeated type_url is accepted by protobuf parsing but canonical re-encoding collapses it.
        var raw = duplicateTypeUrl(fixture.metadata());
        assertThat(RepositorySchemaAsset.parseFrom(raw)).isEqualTo(fixture.metadata());
        String rawHash = sha(raw);
        Files.write(path(rawHash), raw.toByteArray());
        var malformedRef = ref(fixture, fixture.metadata().getTypeUrl(), fixture.descriptorHash(), Optional.of(fixture.sourceHash()), rawHash);
        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read, limits(fixture.totalBytes() + raw.size()))
                .resolve(malformedRef, () -> {})).isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class)
                .hasMessageContaining("invalid retained schema metadata encoding");
    }

    @Test void existingAuthenticatedMetadataOnlyResolutionRemainsAvailable() throws Exception {
        var fixture = fixture(false);
        Files.createDirectories(store);
        Files.write(path(fixture.descriptorHash()), fixture.descriptor().toByteArray());
        var reader = new DocumentRetainedSchemaAssets(this::read, limits(fixture.descriptor().size()));
        var bound = reader.resolve(fixture.metadata(), () -> {});
        assertThat(bound.schema().type().getFullName()).isEqualTo("retained.Root");
        assertThat(Files.exists(path(fixture.metadataHash()))).isFalse();
        assertThat(Files.exists(path(fixture.sourceHash()))).isFalse();
    }

    @Test void controlFailuresDuringResolutionAndBeforeDeliveryKeepIdentityAndDoNotPopulateCache() throws Exception {
        var fixture = fixture(false);
        write(fixture);
        var completedChecks = new AtomicInteger();
        new DocumentRetainedSchemaAssets(this::read, limits(fixture.totalBytes()))
                .resolve(fixture.reference(), completedChecks::incrementAndGet);
        for (int stopAt : new int[]{completedChecks.get() / 2, completedChecks.get()}) {
            var reads = counts();
            var reader = new DocumentRetainedSchemaAssets(readCounting(reads), limits(fixture.totalBytes()));
            var checks = new AtomicInteger();
            // This exception class would be mistaken for corrupt metadata without
            // the explicit control-failure boundary around canonical decoding.
            var failure = new IllegalArgumentException("authorization context invalidated");
            assertThatThrownBy(() -> reader.resolve(fixture.reference(), () -> {
                if (checks.incrementAndGet() == stopAt) throw failure;
            })).isSameAs(failure);
            int metadataReads = reads.getOrDefault(fixture.metadataHash(), 0);
            reader.resolve(fixture.reference(), () -> {});
            assertThat(reads.get(fixture.metadataHash())).isEqualTo(metadataReads + 1);
            assertThat(reads.get(fixture.sourceHash())).isGreaterThanOrEqualTo(1);
        }
    }

    private DocumentRetainedSchemaAssets.Reader readCounting(Map<String, Integer> counts) {
        return hash -> {
            counts.merge(hash, 1, Integer::sum);
            return read(hash);
        };
    }

    private Optional<ByteString> read(String hash) {
        try { return Optional.of(ByteString.copyFrom(Files.readAllBytes(path(hash)))); }
        catch (java.nio.file.NoSuchFileException missing) { return Optional.empty(); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    private Path path(String hash) { return store.resolve(hash + ".asset"); }
    private void write(Fixture fixture) throws IOException {
        Files.createDirectories(store);
        Files.write(path(fixture.descriptorHash()), fixture.descriptor().toByteArray());
        Files.write(path(fixture.metadataHash()), fixture.metadataBytes().toByteArray());
        Files.write(path(fixture.sourceHash()), fixture.source().toByteArray());
    }

    private static Map<String, Integer> counts() { return new ConcurrentHashMap<>(); }
    private static DocumentRetainedSchemaAssets.Limits limits(long bytes) {
        return new DocumentRetainedSchemaAssets.Limits(10, bytes, DESCRIPTOR_LIMITS);
    }

    private static DocumentRetainedSchemaAssets.Reference ref(Fixture fixture, String url, String descriptor,
            Optional<String> source) {
        return ref(fixture, url, descriptor, source, fixture.metadataHash());
    }
    private static DocumentRetainedSchemaAssets.Reference ref(Fixture fixture, String url, String descriptor,
            Optional<String> source, String metadataHash) {
        return new DocumentRetainedSchemaAssets.Reference(url, descriptor, DocumentSchemaAssetCodec.CODEC,
                DocumentSchemaAssetCodec.VERSION, metadataHash, source);
    }

    private static Fixture fixture(boolean sourceEqualsDescriptor) throws Exception {
        var dependency = FileDescriptorProto.newBuilder().setName("child.proto").setPackage("retained").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Child")
                        .addField(FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING))).build();
        var root = FileDescriptorProto.newBuilder().setName("root.proto").setPackage("retained").setSyntax("proto3")
                .addDependency("child.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Root")
                        .addField(FieldDescriptorProto.newBuilder().setName("child").setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".retained.Child"))).build();
        var set = FileDescriptorSet.newBuilder().addFile(dependency).addFile(root).build().toByteString();
        var descriptors = ClosedDescriptorSet.load(set, DESCRIPTOR_LIMITS);
        var type = descriptors.stream().filter(file -> file.getName().equals("root.proto")).findFirst().orElseThrow()
                .findMessageTypeByName("Root");
        var closure = DescriptorFingerprints.closure(type).toByteString();
        String descriptorHash = sha(closure);
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(DescriptorFingerprints.closure(type))).build();
        var source = sourceEqualsDescriptor ? closure : ByteString.copyFromUtf8("synthetic retained source archive bytes");
        String sourceHash = sha(source);
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(condition).setArtifactSha256(descriptorHash)
                .setTypeUrl("type.test/retained.Root")
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("fixture producer did not identify its compiler")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))
                        .setSourceArtifactSha256(sourceHash))
                .build();
        var metadataBytes = DocumentSchemaAssetCodec.encode(metadata, () -> {}).bytes();
        String metadataHash = sha(metadataBytes);
        var reference = new DocumentRetainedSchemaAssets.Reference(metadata.getTypeUrl(), descriptorHash,
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, metadataHash, Optional.of(sourceHash));
        return new Fixture(closure, source, metadata, metadataBytes, descriptorHash, metadataHash, sourceHash, reference);
    }

    private static ByteString duplicateTypeUrl(RepositorySchemaAsset metadata) throws IOException {
        var bytes = new ByteArrayOutputStream();
        var output = CodedOutputStream.newInstance(bytes);
        output.writeByteArray(1, metadata.getSchema().toByteArray());
        output.writeString(2, metadata.getArtifactSha256());
        output.writeString(3, metadata.getTypeUrl());
        output.writeString(3, metadata.getTypeUrl());
        output.writeByteArray(4, metadata.getCompilation().toByteArray());
        output.flush();
        return ByteString.copyFrom(bytes.toByteArray());
    }

    private static String sha(ByteString bytes) { return HexFormat.of().formatHex(sha(bytes.toByteArray())); }
    private static byte[] sha(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private record Fixture(ByteString descriptor, ByteString source, RepositorySchemaAsset metadata,
            ByteString metadataBytes, String descriptorHash, String metadataHash, String sourceHash,
            DocumentRetainedSchemaAssets.Reference reference) {
        long totalBytes() { return (long)descriptor.size() + metadataBytes.size() + source.size(); }
    }
}
