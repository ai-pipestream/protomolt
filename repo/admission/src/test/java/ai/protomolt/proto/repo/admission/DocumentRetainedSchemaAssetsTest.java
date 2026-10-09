package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentRetainedSchemaAssetsTest {
    private static final ClosedDescriptorSet.Limits DESCRIPTOR_LIMITS =
            new ClosedDescriptorSet.Limits(1_000_000, 10, 10, 10);

    @TempDir Path storage;

    @Test
    void freshReaderRebuildsCompleteImportClosureAndDecodesWithoutCompilerFallback() throws Exception {
        var fixture = fixture();
        store(fixture);
        var reader = new DocumentRetainedSchemaAssets(this::readCounting, limits());
        var bound = reader.resolve(fixture.metadata(), () -> {});

        assertThat(readingCalls).hasValue(1);
        assertThat(bound.schema().type().getFullName()).isEqualTo("retained.Root");
        assertThat(bound.schema().type().getFile().getDependencies()).hasSize(1);
        assertThat(bound.schema().type().findFieldByName("child").getMessageType().getFullName())
                .isEqualTo("retained.Child");
        assertThat(bound.metadata().getCompilation().getUnknownCompilerReason())
                .isEqualTo("fixture producer did not identify its compiler");
        var childField = bound.schema().type().findFieldByName("child");
        var child = DynamicMessage.newBuilder(childField.getMessageType())
                .setField(childField.getMessageType().findFieldByName("name"), "decoded").build();
        var decoded = DynamicMessage.parseFrom(bound.schema().type(),
                DynamicMessage.newBuilder(bound.schema().type()).setField(childField, child).build().toByteString());
        assertThat(decoded.getField(childField)).isEqualTo(child);
        assertThat(readingCalls).hasValue(1);
    }

    @Test
    void missingAndCorruptArtifactsAreDataLossAndFailedReadsCanRetryAfterRepair() throws Exception {
        var fixture = fixture();
        var path = artifactPath(fixture.metadata());
        var reader = new DocumentRetainedSchemaAssets(this::read, limits());
        assertThatThrownBy(() -> reader.resolve(fixture.metadata(), () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("missing");
        Files.createDirectories(path.getParent());
        Files.write(path, new byte[]{1, 2, 3});
        assertThatThrownBy(() -> reader.resolve(fixture.metadata(), () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class);
        Files.write(path, fixture.bytes().toByteArray());
        assertThat(reader.resolve(fixture.metadata(), () -> {}).schema().type().getFullName()).isEqualTo("retained.Root");
    }

    @Test
    void storageFailuresPropagateAndCachedReadsRecheckAccess() throws Exception {
        var fixture = fixture();
        var outage = new IllegalStateException("provider unavailable");
        var failing = new DocumentRetainedSchemaAssets(hash -> { throw outage; }, limits());
        assertThatThrownBy(() -> failing.resolve(fixture.metadata(), () -> {})).isSameAs(outage);
        var denied = new SecurityException("access denied");
        var deniedReader = new DocumentRetainedSchemaAssets(hash -> { throw denied; }, limits());
        assertThatThrownBy(() -> deniedReader.resolve(fixture.metadata(), () -> {})).isSameAs(denied);

        store(fixture);
        var cache = new DocumentRetainedSchemaAssets(this::read, limits());
        cache.resolve(fixture.metadata(), () -> {});
        assertThatThrownBy(() -> cache.resolve(fixture.metadata(), () -> { throw denied; })).isSameAs(denied);
        assertThat(cache.resolve(fixture.metadata(), () -> {}).schema().type().getFullName()).isEqualTo("retained.Root");
    }

    @Test
    void controlFailureDuringBindingPropagatesByIdentityAndDoesNotCachePartialResult() throws Exception {
        var fixture = fixture();
        store(fixture);
        var reader = new DocumentRetainedSchemaAssets(this::readCounting, limits());
        var denied = new SecurityException("access revoked during descriptor binding");
        var checks = new AtomicInteger();
        assertThatThrownBy(() -> reader.resolve(fixture.metadata(), () -> {
            if (checks.incrementAndGet() == 5) throw denied;
        })).isSameAs(denied);
        assertThat(reader.resolve(fixture.metadata(), () -> {}).schema().type().getFullName()).isEqualTo("retained.Root");
        assertThat(readingCalls).hasValue(2);
    }

    @Test
    void bindingByteAndDescriptorLimitsAreEnforced() throws Exception {
        var fixture = fixture();
        store(fixture);
        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read,
                new DocumentRetainedSchemaAssets.Limits(1, fixture.bytes().size() - 1L, DESCRIPTOR_LIMITS))
                .resolve(fixture.metadata(), () -> {})).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("byte count");
        assertThatThrownBy(() -> new DocumentRetainedSchemaAssets(this::read,
                new DocumentRetainedSchemaAssets.Limits(1, fixture.bytes().size(),
                        new ClosedDescriptorSet.Limits(1_000_000, 1, 10, 10)))
                .resolve(fixture.metadata(), () -> {}))
                .isInstanceOf(ClosedDescriptorSet.LimitExceededException.class).hasMessageContaining("file count");

        var alias = fixture.metadata().toBuilder().setTypeUrl("alternate/retained.Root").build();
        var countLimited = new DocumentRetainedSchemaAssets(this::read,
                new DocumentRetainedSchemaAssets.Limits(1, fixture.bytes().size() * 2L, DESCRIPTOR_LIMITS));
        countLimited.resolve(fixture.metadata(), () -> {});
        assertThatThrownBy(() -> countLimited.resolve(alias, () -> {})).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("binding count");
    }

    private final AtomicInteger readingCalls = new AtomicInteger();

    private Optional<ByteString> readCounting(String hash) {
        readingCalls.incrementAndGet();
        return read(hash);
    }

    private Optional<ByteString> read(String hash) {
        try {
            Path path = storage.resolve(hash + ".fds");
            return Optional.of(ByteString.copyFrom(Files.readAllBytes(path)));
        } catch (java.nio.file.NoSuchFileException missing) {
            return Optional.empty();
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private void store(Fixture fixture) throws Exception {
        Files.createDirectories(storage);
        Files.write(artifactPath(fixture.metadata()), fixture.bytes().toByteArray());
    }

    private Path artifactPath(RepositorySchemaAsset metadata) {
        return storage.resolve(metadata.getArtifactSha256() + ".fds");
    }

    private static DocumentRetainedSchemaAssets.Limits limits() {
        return new DocumentRetainedSchemaAssets.Limits(10, 1_000_000, DESCRIPTOR_LIMITS);
    }

    private static Fixture fixture() throws Exception {
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
        var bytes = FileDescriptorSet.newBuilder().addFile(dependency).addFile(root).build().toByteString();
        var descriptors = ai.protomolt.proto.descriptors.ClosedDescriptorSet.load(bytes, DESCRIPTOR_LIMITS);
        var type = descriptors.stream().filter(file -> file.getName().equals("root.proto")).findFirst().orElseThrow()
                .findMessageTypeByName("Root");
        var closure = DescriptorFingerprints.closure(type);
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build();
        var schema = DocumentSchemaBinding.bind(condition, closure.toByteString(), DESCRIPTOR_LIMITS, () -> {});
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(condition).setArtifactSha256(schema.artifactSha256())
                .setTypeUrl("fixture/retained.Root")
                .setCompilation(ai.protomolt.proto.repo.v1.SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(ai.protomolt.proto.repo.v1.SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("fixture producer did not identify its compiler")
                        .setAdmissionRuntime(ai.protomolt.proto.repo.v1.SchemaToolIdentity.newBuilder()
                                .setName("test-runtime").setVersion("1")))
                .build();
        return new Fixture(closure.toByteString(), metadata);
    }

    private record Fixture(ByteString bytes, RepositorySchemaAsset metadata) {}
}
