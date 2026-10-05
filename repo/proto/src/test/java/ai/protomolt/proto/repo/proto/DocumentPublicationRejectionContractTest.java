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
        check(valid.toBuilder().setReasonValue(2).build(), false);
        check(valid.toBuilder().setReasonValue(2).setAssessment(assessment()).build(), true);
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
            if (!field.getName().equals("assessment")) check(valid.toBuilder().clearField(field).build(), false);
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

    @Test void admissionRequiresAnExactBoundedBindingWithFutureRetention() throws Exception {
        var valid = rejection().setReasonValue(2).setAssessment(assessment()).build();
        check(valid, true);
        check(valid.toBuilder().clearAssessment().build(), false);
        check(valid.toBuilder().setReasonValue(1).build(), false);
        check(valid.toBuilder().setDispositionValue(2).setReasonValue(3).build(), false);
        for (var field : DocumentPublicationAssessmentBinding.getDescriptor().getFields())
            check(valid.toBuilder().setAssessment(assessment().clearField(field)).build(), false);
        for (var bad : new DocumentPublicationAssessmentBinding[]{
                assessment().setAssessmentId("1-1-1-1-1").build(),
                assessment().setAssessmentId("ABCDEFAB-CDEF-4ABC-8DEF-ABCDEFABCDEF").build(),
                assessment().setManifestCodec("other").build(),
                assessment().setManifestEncodingVersion(2).build(),
                assessment().setManifestSha256("A".repeat(64)).build(),
                assessment().setRetainUntilEpochMicros(-1).build(),
                assessment().setRetainUntilEpochMicros(253402300800000000L).build(),
                assessment().setRetainUntilEpochMicros(valid.getRecordedAtEpochMicros()).build()})
            check(valid.toBuilder().setAssessment(bad).build(), false);
        check(valid.toBuilder().setRecordedAtEpochMicros(3).build(), false);
        check(valid.toBuilder().setAssessment(assessment().setRetainUntilEpochMicros(253402300799999999L)).build(), true);
        // Shape cannot establish that this evidence exists or proves an invalid
        // candidate. The decision handler must verify those state-dependent facts.
        check(valid.toBuilder().setAssessment(assessment().setManifestSha256("0".repeat(64))).build(), true);
    }

    @Test void jsonSchemaExposesBoundsAndPreservesRuntimeRule() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(DocumentPublicationRejection.getDescriptor()));
        assertThat(schema.at("/properties/accountId/maxLength").asInt()).isEqualTo(200);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("publication-rejection-disposition");
        assertThat(schema.path("x-protomolt-cel").toString())
                .contains("publication-rejection-assessment", "publication-rejection-retention");
        com.fasterxml.jackson.databind.JsonNode binding = mapper.valueToTree(generator.generateRooted(DocumentPublicationAssessmentBinding.getDescriptor()));
        assertThat(binding.at("/properties/manifestSha256/pattern").asText()).isEqualTo("^[0-9a-f]{64}$");
    }

    private static DocumentPublicationAssessmentBinding.Builder assessment() {
        return DocumentPublicationAssessmentBinding.newBuilder().setAssessmentId("abcdefab-cdef-4abc-8def-abcdefabcdef")
                .setManifestCodec("document-publication-assessment").setManifestEncodingVersion(1)
                .setManifestSha256("b".repeat(64)).setRetainUntilEpochMicros(2);
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
