package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v1.SchemaCompilerDetails;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentSchemaBindingTest {
    private static final ClosedDescriptorSet.Limits LIMITS = new ClosedDescriptorSet.Limits(100_000, 10, 20, 10);

    @Test
    void bindsAssetMetadataToExactBytesAndPreservesReportedProvenance() {
        var bytes = schema().toByteString();
        var metadata = asset(bytes);
        var bound = DocumentSchemaAssetBinding.bind(metadata, bytes, LIMITS, () -> {});
        assertThat(bound.metadata()).isSameAs(metadata);
        assertThat(bound.schema().artifact()).isSameAs(bytes);
        assertThat(bound.schema().type().getFullName()).isEqualTo("archive.Record");
        assertThat(bound.metadata().getCompilation().getUnknownCompilerReason()).isEqualTo("Producer did not report its compiler");
    }

    @Test
    void refusesSameCanonicalSchemaWithDifferentExactBytes() {
        var set = schema();
        var metadata = asset(set.toByteString());
        var reordered = FileDescriptorSet.newBuilder().addFile(set.getFile(1)).addFile(set.getFile(0)).build();
        assertThatThrownBy(() -> DocumentSchemaAssetBinding.bind(metadata, reordered.toByteString(), LIMITS, () -> {}))
                .hasMessageContaining("exact artifact digest mismatch");
    }

    @Test
    void checksReportedCompilerWithOptionsAndNestedUnknownToolFields() {
        var bytes = schema().toByteString();
        var original = asset(bytes);
        var compiler = SchemaCompilerDetails.newBuilder()
                .setTool(SchemaToolIdentity.newBuilder().setName("test-compiler").setVersion("1"))
                .putOptions("syntax", "proto3");
        var metadata = original.toBuilder().setCompilation(original.getCompilation().toBuilder()
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_PRODUCER_REPORTED)
                .setKnownCompiler(compiler)).build();
        assertThat(DocumentSchemaAssetBinding.bind(metadata, bytes, LIMITS, () -> {}).metadata()).isEqualTo(metadata);
        var unknown = UnknownFieldSet.newBuilder().addField(999,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        compiler.setTool(compiler.getTool().toBuilder().setUnknownFields(unknown));
        var invalid = metadata.toBuilder().setCompilation(metadata.getCompilation().toBuilder().setKnownCompiler(compiler)).build();
        assertThatThrownBy(() -> DocumentSchemaAssetBinding.bind(invalid, bytes, LIMITS, () -> {}))
                .hasMessageContaining("unsupported schema asset metadata fields");
    }

    @Test
    void refusesUnknownEvidenceInvalidTypeAndOversizedMetadata() {
        var bytes = schema().toByteString();
        var metadata = asset(bytes);
        var unknown = UnknownFieldSet.newBuilder().addField(999,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        for (var invalid : java.util.List.of(metadata.toBuilder().setUnknownFields(unknown).build(),
                metadata.toBuilder().setCompilation(metadata.getCompilation().toBuilder().setUnknownFields(unknown)).build(),
                metadata.toBuilder().setCompilation(metadata.getCompilation().toBuilder().setAdmissionRuntime(
                        metadata.getCompilation().getAdmissionRuntime().toBuilder().setUnknownFields(unknown))).build())) {
            assertThatThrownBy(() -> DocumentSchemaAssetBinding.bind(invalid, bytes, LIMITS, () -> {}))
                    .hasMessageContaining("unsupported schema asset metadata fields");
        }
        assertThatThrownBy(() -> DocumentSchemaAssetBinding.bind(metadata.toBuilder().setTypeUrl("types.test/archive.Other").build(),
                bytes, LIMITS, () -> {})).hasMessageContaining("invalid schema asset metadata");
        assertThatThrownBy(() -> DocumentSchemaAssetBinding.bind(metadata.toBuilder().setTypeUrl("x".repeat(524289)).build(),
                bytes, LIMITS, () -> {})).hasMessageContaining("metadata exceeds byte limit");
        var stopped = new CancellationException("stop asset binding");
        assertThatThrownBy(() -> DocumentSchemaAssetBinding.bind(metadata, bytes, LIMITS, () -> { throw stopped; })).isSameAs(stopped);
    }

    private static RepositorySchemaAsset asset(ByteString bytes) {
        var schema = DocumentSchemaBinding.bind(condition(schema(), "archive.Record"), bytes, LIMITS, () -> {});
        return RepositorySchemaAsset.newBuilder().setSchema(schema.condition()).setArtifactSha256(schema.artifactSha256())
                .setTypeUrl("types.test/archive.Record").setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("Producer did not report its compiler")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))).build();
    }

    @Test
    void bindsExactNestedTypeAndRetainsOriginalBytes() throws Exception {
        var set = schema();
        ByteString artifact = set.toByteString();
        var condition = condition(set, "archive.Record.Details");
        var binding = DocumentSchemaBinding.bind(condition, artifact, LIMITS, () -> {});
        assertThat(binding.type().getFullName()).isEqualTo("archive.Record.Details");
        assertThat(binding.condition()).isEqualTo(condition);
        assertThat(binding.artifact()).isSameAs(artifact);
        assertThat(binding.artifactSha256()).isEqualTo(HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(artifact.toByteArray())));
    }

    @Test
    void canonicalFingerprintAndExactArtifactDigestHaveDifferentPurposes() {
        var set = schema();
        var reversed = FileDescriptorSet.newBuilder().addFile(set.getFile(1)).addFile(set.getFile(0)).build();
        var condition = condition(set, "archive.Record");
        var first = DocumentSchemaBinding.bind(condition, set.toByteString(), LIMITS, () -> {});
        var second = DocumentSchemaBinding.bind(condition, reversed.toByteString(), LIMITS, () -> {});
        assertThat(first.condition()).isEqualTo(second.condition());
        assertThat(first.artifactSha256()).isNotEqualTo(second.artifactSha256());
        var envelope = set.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(999,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build().toByteString();
        var third = DocumentSchemaBinding.bind(condition, envelope, LIMITS, () -> {});
        assertThat(third.artifact()).isEqualTo(envelope);
        assertThat(third.artifactSha256()).isNotEqualTo(first.artifactSha256());
    }

    @Test
    void rejectsChangedDescriptorEvenWhenTypeNameMatches() {
        var set = schema();
        var changed = set.toBuilder();
        changed.getFileBuilder(0).getMessageTypeBuilder(0).getFieldBuilder(0)
                .setName("changed_name");
        assertThatThrownBy(() -> DocumentSchemaBinding.bind(condition(set, "archive.Record"),
                changed.build().toByteString(), LIMITS, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("fingerprint mismatch");
    }

    @Test
    void rejectsMissingTypesImportsAndUnboundExtraFiles() {
        var set = schema();
        assertThatThrownBy(() -> DocumentSchemaBinding.bind(condition(set, "archive.Missing"),
                set.toByteString(), LIMITS, () -> {})).hasMessageContaining("type is missing");
        assertThatThrownBy(() -> DocumentSchemaBinding.bind(condition(set, "archive.Record"),
                FileDescriptorSet.newBuilder().addFile(set.getFile(0)).build().toByteString(), LIMITS, () -> {}))
                .hasMessageContaining("missing import");
        var extra = set.toBuilder().addFile(FileDescriptorProto.newBuilder().setName("extra.proto")).build();
        assertThatThrownBy(() -> DocumentSchemaBinding.bind(condition(set, "archive.Record"),
                extra.toByteString(), LIMITS, () -> {})).hasMessageContaining("outside");
    }

    @Test
    void rejectsInvalidConditionsAndCancelledWork() {
        var set = schema();
        var condition = condition(set, "archive.Record");
        var unknown = condition.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(999,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        for (var invalid : java.util.List.of(PublicationSchemaCondition.getDefaultInstance(), unknown,
                condition.toBuilder().setTypeName("type.googleapis.com/archive.Record").build(),
                condition.toBuilder().setDescriptorFingerprint("not-a-hash").build())) {
            assertThatThrownBy(() -> DocumentSchemaBinding.bind(invalid, set.toByteString(), LIMITS, () -> {}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("invalid publication");
        }
        var cancelled = new CancellationException("stop binding");
        assertThatThrownBy(() -> DocumentSchemaBinding.bind(condition, set.toByteString(), LIMITS,
                () -> { throw cancelled; })).isSameAs(cancelled);
    }

    private static PublicationSchemaCondition condition(FileDescriptorSet set, String name) {
        return PublicationSchemaCondition.newBuilder().setTypeName(name)
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(set)).build();
    }

    private static FileDescriptorSet schema() {
        var dependency = FileDescriptorProto.newBuilder().setName("common.proto").setPackage("archive.common")
                .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName("Common"));
        var root = FileDescriptorProto.newBuilder().setName("record.proto").setPackage("archive").setSyntax("proto3")
                .addDependency("common.proto").addMessageType(DescriptorProto.newBuilder().setName("Record")
                        .addNestedType(DescriptorProto.newBuilder().setName("Details"))
                        .addField(FieldDescriptorProto.newBuilder().setName("common").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".archive.common.Common")));
        return FileDescriptorSet.newBuilder().addFile(root).addFile(dependency).build();
    }
}
