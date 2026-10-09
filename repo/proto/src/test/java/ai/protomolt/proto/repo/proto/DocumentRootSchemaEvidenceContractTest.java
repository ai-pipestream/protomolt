package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DocumentRootSchemaEvidenceContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String SHA = "a".repeat(64);

    @Test
    void acceptsOneRootOnlyOccurrenceAlongsideNestedOccurrence() throws Exception {
        var evidence = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(root())
                .addOccurrences(path(boundary()))
                .addOccurrences(path(boundary(), field(1), boundary("type.test/child.Record")))
                .build();
        check(evidence, true);
        check(evidence.toBuilder().clearOccurrences().addOccurrences(path(boundary()))
                .addAllOccurrences(java.util.Collections.nCopies(4096,
                        path(boundary(), field(1), boundary("type.test/child.Record")))).build(), false);
    }

    @Test
    void requiresRootAndExactlyOneRootOnlyOccurrence() throws Exception {
        var rootPath = path(boundary());
        var nestedPath = path(boundary(), field(1), boundary("type.test/child.Record"));

        var absentRoot = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1)
                .addOccurrences(rootPath).build();
        var empty = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(root()).build();
        var duplicateRootOnly = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(root())
                .addOccurrences(rootPath).addOccurrences(rootPath).build();
        var noRootOnly = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(root())
                .addOccurrences(nestedPath).build();
        for (var invalid : List.of(absentRoot, empty, duplicateRootOnly, noRootOnly)) check(invalid, false);
    }

    @Test
    void rejectsUnsupportedVersionAndInvalidNestedLocatorOrOccurrencePath() throws Exception {
        var path = path(boundary());
        var version = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(2).setRoot(root())
                .addOccurrences(path).build();
        var invalidLocator = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1)
                .setRoot(root().toBuilder().setLayoutPolicy("other-layout")).addOccurrences(path).build();
        var invalidPath = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(root())
                .addOccurrences(RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1)
                        .addSteps(field(1))).build();
        for (var invalid : List.of(version, invalidLocator, invalidPath)) check(invalid, false);
    }

    @Test
    void jsonSchemaKeepsRepeatedBoundsAndMarksCelRulesAsRuntimeMetadata() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(
                generator.generateRooted(DocumentRootSchemaEvidence.getDescriptor()));
        assertThat(schema.at("/properties/occurrences/minItems").asInt()).isEqualTo(1);
        assertThat(schema.at("/properties/occurrences/maxItems").asInt()).isEqualTo(4096);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("root-evidence-single-root");
        // x-protomolt-cel carries runtime validation metadata; JSON Schema consumers do not execute CEL.
    }

    private static DocumentSchemaRootLocator root() {
        return DocumentSchemaRootLocator.newBuilder().setEncodingVersion(1)
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                .setLayoutPolicy("protomolt-document-parts/v1").setFragmentSha256(SHA).setFragmentSizeBytes(1)
                .setContainerSchema(RepositoryResolvedSchema.newBuilder().setArtifactSha256("b".repeat(64))
                        .setSchema(PublicationSchemaCondition.newBuilder()
                                .setTypeName(Document.getDescriptor().getFullName()).setDescriptorFingerprint("c".repeat(64))))
                .addAccess(field(4)).build();
    }

    private static RepositorySchemaOccurrencePath path(RepositorySchemaOccurrenceStep... steps) {
        var path = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1);
        for (var step : steps) path.addSteps(step);
        return path.build();
    }

    private static RepositorySchemaOccurrenceStep field(int number) {
        return RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(number).build();
    }

    private static RepositorySchemaOccurrenceStep boundary() { return boundary("type.test/root.Record"); }

    private static RepositorySchemaOccurrenceStep boundary(String typeUrl) {
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        return RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                .setTypeUrl(typeUrl).setValueSha256(SHA).setValueSizeBytes(1)
                .setResolved(RepositoryResolvedSchema.newBuilder().setArtifactSha256("b".repeat(64))
                        .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(typeName)
                                .setDescriptorFingerprint("c".repeat(64)))))
                .build();
    }

    private static void check(Message value, boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s", value).isEqualTo(valid);
        var dynamic = DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString());
        assertThat(VALIDATOR.validate(dynamic).valid()).as("dynamic %s", value).isEqualTo(valid);
    }
}
