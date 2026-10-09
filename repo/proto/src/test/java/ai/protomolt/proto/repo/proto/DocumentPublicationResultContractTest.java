package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class DocumentPublicationResultContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    @Test void acceptsOneAndMaximumMemberResultsWithoutUploadAttempts() throws Exception {
        check(result().addMembers(member(0)).build(), true);
        var maximum = result();
        for (int i = 0; i < 64; i++) maximum.addMembers(member(i));
        check(maximum.build(), true);
        check(maximum.addMembers(member(64)).build(), false);
    }

    @Test void rejectsAmbiguousOrIncompleteShape() throws Exception {
        check(result().build(), false);
        var valid = result().addMembers(member(0)).build();
        check(valid.toBuilder().clearPrincipal().build(), false);
        check(valid.toBuilder().setOwnerGeneration(0).build(), false);
        check(valid.toBuilder().setCommandEncodingVersion(2).build(), false);
        check(valid.toBuilder().setCommandSha256("bad").build(), false);
        check(valid.toBuilder().setOperationId("ABCDEFAB-CDEF-4ABC-8DEF-ABCDEFABCDEF").build(), false);
        check(valid.toBuilder().setOperationId("1-1-1-1-1").build(), false);
        check(valid.toBuilder().setMembers(0, member(0).setRevisionId("1-1-1-1-1")).build(), false);
        check(valid.toBuilder().addMembers(member(1).setMemberId("member-0")).build(), false);
        check(valid.toBuilder().addMembers(member(1).setRevisionId(member(0).getRevisionId())).build(), false);
        check(valid.toBuilder().addMembers(member(1).setAddress(member(0).getAddress())).build(), false);
        check(valid.toBuilder().setMembers(0, member(0).setMutationRevision(0)).build(), false);
        check(valid.toBuilder().setMembers(0, member(0).clearAddress()).build(), false);
        check(valid.toBuilder().setMembers(0, member(0).setAddress(member(0).getAddress().toBuilder().setAccountId("other"))).build(), false);
        check(valid.toBuilder().setMembers(0, member(0).setAddress(member(0).getAddress().toBuilder().setDocId(" "))).build(), false);
    }

    @Test void jsonSchemaRecordsCrossFieldRulesAsRuntimeMetadata() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(DocumentPublicationResult.getDescriptor()));
        assertThat(schema.at("/properties/members/maxItems").asInt()).isEqualTo(64);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("publication-result-account", "publication-result-unique");
    }

    private static DocumentPublicationResult.Builder result() {
        return DocumentPublicationResult.newBuilder().setOperationId("abcdefab-cdef-4abc-8def-abcdefabcdef")
                .setAccountId("account").setPrincipal("principal").setOwnerGeneration(1)
                .setCommandEncodingVersion(1).setCommandSha256("a".repeat(64));
    }

    private static DocumentPublishedRevision.Builder member(int number) {
        return DocumentPublishedRevision.newBuilder().setMemberId("member-" + number)
                .setAddress(NodeAddress.newBuilder().setAccountId("account").setGraphId("graph")
                        .setGraphAddressId("source").setDocId("doc-" + number))
                .setRevisionId(new java.util.UUID(1, number).toString()).setMutationRevision(1);
    }

    private static void check(DocumentPublicationResult result, boolean expected) throws Exception {
        assertThat(VALIDATOR.validate(result).valid()).as("generated %s", result).isEqualTo(expected);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(result.getDescriptorForType(), result.toByteArray())).valid())
                .as("dynamic %s", result).isEqualTo(expected);
    }
}
