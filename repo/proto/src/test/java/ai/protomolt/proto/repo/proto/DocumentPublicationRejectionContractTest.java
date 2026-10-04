package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentPublicationRejectionContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    @Test void validatesBothDispositionsOnGeneratedAndDynamicMessages() throws Exception {
        var valid = rejection().build();
        check(valid, true);
        check(valid.toBuilder().setReasonValue(2).build(), true);
        check(valid.toBuilder().setDispositionValue(2).setReasonValue(3).build(), true);
        check(valid.toBuilder().setDispositionValue(2).build(), false);
        check(valid.toBuilder().setDispositionValue(2).setReasonValue(2).build(), false);
        check(valid.toBuilder().setReasonValue(3).build(), false);
        for (int value : new int[]{0, -1, 99}) {
            check(valid.toBuilder().setDispositionValue(value).build(), false);
            check(valid.toBuilder().setReasonValue(value).build(), false);
        }
    }

    @Test void rejectsMissingIdentityAndOutOfBoundsValues() throws Exception {
        var valid = rejection().build();
        for (var field : DocumentPublicationRejection.getDescriptor().getFields())
            check(valid.toBuilder().clearField(field).build(), false);
        check(valid.toBuilder().setAccountId(" ").build(), false);
        check(valid.toBuilder().setPrincipal("x".repeat(201)).build(), false);
        check(valid.toBuilder().setCommandSha256("A".repeat(64)).build(), false);
        check(valid.toBuilder().setCommandEncodingVersion(2).build(), false);
        check(valid.toBuilder().setCommandCodec("other").build(), false);
        check(valid.toBuilder().setOperationId("1-1-1-1-1").build(), false);
        check(valid.toBuilder().setOwnerGeneration(-1).build(), false);
        check(valid.toBuilder().setRecordedAtEpochMicros(-1).build(), false);
        check(valid.toBuilder().setRecordedAtEpochMicros(253402300800000000L).build(), false);
        check(valid.toBuilder().setRecordedAtEpochMicros(253402300799999999L).build(), true);
    }

    @Test void jsonSchemaExposesBoundsAndPreservesRuntimeRule() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(DocumentPublicationRejection.getDescriptor()));
        assertThat(schema.at("/properties/accountId/maxLength").asInt()).isEqualTo(200);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("publication-rejection-disposition");
    }

    private static DocumentPublicationRejection.Builder rejection() {
        return DocumentPublicationRejection.newBuilder().setOperationId("abcdefab-cdef-4abc-8def-abcdefabcdef")
                .setAccountId("account").setPrincipal("principal").setOwnerGeneration(1)
                .setCommandEncodingVersion(1).setCommandCodec("document-publication").setCommandSha256("a".repeat(64))
                .setRecordedAtEpochMicros(1).setDispositionValue(1).setReasonValue(1);
    }

    private static void check(DocumentPublicationRejection value, boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s", value).isEqualTo(valid);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString())).valid())
                .as("dynamic %s", value).isEqualTo(valid);
    }
}
