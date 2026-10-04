package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaRootContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    @Test void coreAndParsedRootsHaveDistinctExactAccessPatterns() throws Exception {
        var core = core();
        check(core, true);
        var parsed = core.toBuilder().setSlot(core.getSlot().toBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                .clearAccess().addAccess(field(5)).addAccess(RepositorySchemaOccurrenceStep.newBuilder().setMapKey(
                        RepositoryOccurrenceMapKey.newBuilder().setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue("")))
                .addAccess(field(7)).addAccess(field(1));
        check(parsed.build(), true);
        check(parsed.clone().setAccess(1, RepositorySchemaOccurrenceStep.newBuilder().setRepeatedIndex(0)).build(), false);
        check(parsed.clone().setAccess(2, field(8)).build(), false);
        check(core.toBuilder().clearAccess().build(), false);
        check(core.toBuilder().setAccess(0, field(5)).build(), false);
        check(core.toBuilder().addAccess(field(1)).build(), false);
        check(core.toBuilder().setAccess(0, RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.getDefaultInstance())).build(), false);
        check(core.toBuilder().setSlot(core.getSlot().toBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)).build(), false);
    }

    @Test void layoutContainerAndRawIdentityAreRequired() throws Exception {
        var core = core();
        check(core.toBuilder().setEncodingVersion(0).build(), false);
        check(core.toBuilder().setEncodingVersion(2).build(), false);
        check(core.toBuilder().setLayoutPolicy("protomolt-document-parts/v2").build(), false);
        check(core.toBuilder().clearSlot().build(), false);
        check(core.toBuilder().setSlot(core.getSlot().toBuilder().setSubKey("other")).build(), false);
        check(core.toBuilder().clearContainerSchema().build(), false);
        check(core.toBuilder().setContainerSchema(core.getContainerSchema().toBuilder().setSchema(
                core.getContainerSchema().getSchema().toBuilder().setTypeName("other.Document"))).build(), false);
        check(core.toBuilder().setFragmentSha256("bad").build(), false);
        check(core.toBuilder().setFragmentSizeBytes(0).build(), false);
    }

    @Test void jsonSchemaReportsRuntimeOnlyAccessRules() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(DocumentSchemaRootLocator.getDescriptor()));
        assertThat(schema.path("x-protomolt-cel").toString()).contains("schema-root-access", "schema-root-container");
        assertThat(schema.at("/properties/fragmentSha256/pattern").asText()).isEqualTo("^[0-9a-f]{64}$");
    }

    private static RepositorySchemaOccurrenceStep field(int number) { return RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(number).build(); }
    private static DocumentSchemaRootLocator core() {
        return DocumentSchemaRootLocator.newBuilder().setEncodingVersion(1)
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                .setLayoutPolicy("protomolt-document-parts/v1").setFragmentSha256("a".repeat(64)).setFragmentSizeBytes(1)
                .setContainerSchema(RepositoryResolvedSchema.newBuilder().setArtifactSha256("b".repeat(64))
                        .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(Document.getDescriptor().getFullName()).setDescriptorFingerprint("c".repeat(64))))
                .addAccess(field(4)).build();
    }
    private static void check(Message value, boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s", value).isEqualTo(valid);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString())).valid())
                .as("dynamic %s", value).isEqualTo(valid);
    }
}
