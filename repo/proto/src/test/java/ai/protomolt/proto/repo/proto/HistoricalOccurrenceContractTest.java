package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Envelope fixtures are synthetic; byte hashes and retained traversal require handler checks. */
class HistoricalOccurrenceContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String SHA = "a".repeat(64);
    private static final String URL = "type.test/sample.Value";
    private static final String REVISION = "abcdefab-cdef-4abc-8def-abcdefabcdef";
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setAccountId("account")
            .setDocId("document").setGraphId("graph").setGraphAddressId("source").build();

    @Test void requestRequiresExplicitSelectionLimitsAndExactRevision() throws Exception {
        var request = ReadHistoricalOccurrenceRequest.newBuilder().setAddress(ADDRESS).setRevisionId(REVISION)
                .setSelection(selection()).setLimits(limits()).build();
        check(request, true);
        check(request.toBuilder().clearSelection().build(), false);
        check(request.toBuilder().clearLimits().build(), false);
        check(request.toBuilder().setRevisionId("latest").build(), false);
        check(request.toBuilder().setAddress(ADDRESS.toBuilder().setGraphId(" ")).build(), false);
        check(selection().setRevisionOrdinal(9999).build(), true);
        check(selection().setRevisionOrdinal(10000).build(), false);
        check(selection().setPathSha256("bad").build(), false);
    }

    @Test void workLimitsRejectUnsetAndOverflowingValues() throws Exception {
        check(limits().build(), true);
        check(limits().clearMaxRetainedBytes().build(), false);
        check(limits().setMaxFragmentBytes(-1L).build(), false);
        check(limits().setMaxEvidenceBytes(16777217).build(), false);
        check(limits().setMaxReferences(65).build(), false);
        check(limits().setMaxDecodedBytes(268435457).build(), false);
        check(limits().setMaxBoundaries(102).build(), false);
    }

    @Test void responseBindsFinalBoundaryAndRequiresCompleteEnvelope() throws Exception {
        var response = response();
        check(response, true);
        check(response.toBuilder().clearOriginal().build(), false);
        check(response.toBuilder().clearRoot().build(), false);
        check(response.toBuilder().clearPath().build(), false);
        check(response.toBuilder().clearSelection().build(), false);
        check(response.toBuilder().clearDefinition().build(), false);
        check(response.toBuilder().setDefinition(response.getDefinition().toBuilder()
                .setReference(response.getDefinition().getReference().toBuilder().setTypeUrl("type.test/other.Value"))).build(), false);
        check(response.toBuilder().setDefinition(response.getDefinition().toBuilder()
                .setReference(response.getDefinition().getReference().toBuilder().setDescriptorSha256("b".repeat(64)))).build(), false);
        check(response.toBuilder().setPath(response.getPath().toBuilder().addSteps(
                RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(1))).build(), false);
    }

    @Test void artifactBoundsDoNotPretendToVerifyTheirContents() throws Exception {
        var definition = response().getDefinition();
        check(definition, true); // Deliberately not a valid descriptor or metadata encoding.
        check(definition.toBuilder().clearDescriptorArtifact().build(), false);
        check(definition.toBuilder().setMetadataArtifact(ByteString.copyFrom(new byte[524289])).build(), false);
        check(definition.toBuilder().setDescriptorArtifact(ByteString.copyFrom(new byte[8388609])).build(), false);
        check(definition.toBuilder().setReference(definition.getReference().toBuilder().setMetadataVersion(2)).build(), false);
    }

    @Test void definitionBindsTheFinalNestedBoundaryRatherThanTheRoot() throws Exception {
        var response = response();
        var leaf = response.getPath().getSteps(0);
        var parent = leaf.toBuilder().setAnyBoundary(leaf.getAnyBoundary().toBuilder().setTypeUrl("type.test/parent.Value")
                .setResolved(leaf.getAnyBoundary().getResolved().toBuilder().setArtifactSha256("b".repeat(64))
                        .setSchema(leaf.getAnyBoundary().getResolved().getSchema().toBuilder().setTypeName("parent.Value"))));
        var nested = response.toBuilder().setPath(response.getPath().toBuilder().clearSteps()
                .addSteps(parent).addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(1)).addSteps(leaf)).build();
        check(nested, true);
        check(nested.toBuilder().setDefinition(nested.getDefinition().toBuilder().setReference(
                nested.getDefinition().getReference().toBuilder().setTypeUrl("type.test/parent.Value")
                        .setDescriptorSha256("b".repeat(64)))).build(), false);
    }

    @Test void schemaProjectionRecordsRuntimeOnlyRules() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(ReadHistoricalOccurrenceResponse.getDescriptor()));
        assertThat(schema.path("x-protomolt-cel").toString()).contains("occurrence-read-response-definition");
        assertThat(generator.fieldValidationSchema(HistoricalRetainedDefinition.getDescriptor().findFieldByName("descriptor_artifact")))
                .containsEntry("x-protomolt-runtime-rules", java.util.List.of("bytes"));
    }

    private static HistoricalOccurrenceSelection.Builder selection() {
        return HistoricalOccurrenceSelection.newBuilder().setRootSha256(SHA).setPathSha256(SHA);
    }
    private static HistoricalMaterializationLimits.Builder limits() {
        return HistoricalMaterializationLimits.newBuilder().setMaxFragmentBytes(1048576).setMaxEvidenceBytes(1048576)
                .setMaxRetainedBytes(16777216).setMaxReferences(64).setMaxDecodedBytes(8388608).setMaxBoundaries(64);
    }
    private static ReadHistoricalOccurrenceResponse response() {
        var resolved = RepositoryResolvedSchema.newBuilder().setArtifactSha256(SHA)
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("sample.Value").setDescriptorFingerprint(SHA));
        var path = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(
                RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                        .setTypeUrl(URL).setValueSha256(SHA).setValueSizeBytes(1).setResolved(resolved)));
        var root = DocumentSchemaRootLocator.newBuilder().setEncodingVersion(1)
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                .setLayoutPolicy("protomolt-document-parts/v1").setFragmentSha256(SHA).setFragmentSizeBytes(1)
                .setContainerSchema(resolved.clone().setSchema(resolved.getSchema().toBuilder().setTypeName(Document.getDescriptor().getFullName())))
                .addAccess(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4));
        var reference = RepositorySchemaAssetReference.newBuilder().setTypeUrl(URL).setDescriptorSha256(SHA)
                .setMetadataCodec("repository-schema-asset").setMetadataVersion(1).setMetadataSha256(SHA);
        return ReadHistoricalOccurrenceResponse.newBuilder().setAddress(ADDRESS).setRevisionId(REVISION).setSelection(selection())
                .setOriginal(Any.newBuilder().setTypeUrl(URL).setValue(ByteString.copyFrom(new byte[]{(byte) 0xff})))
                .setRoot(root).setPath(path).setDefinition(HistoricalRetainedDefinition.newBuilder().setReference(reference)
                        .setDescriptorArtifact(ByteString.copyFromUtf8("synthetic")).setMetadataArtifact(ByteString.copyFromUtf8("synthetic"))).build();
    }
    private static void check(Message value, boolean valid) throws Exception {
        var result = VALIDATOR.validate(value);
        assertThat(result.valid()).as("generated %s: %s", value.getDescriptorForType().getFullName(), result).isEqualTo(valid);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString())).valid())
                .as("dynamic %s", value.getDescriptorForType().getFullName()).isEqualTo(valid);
    }
}
