package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Shape tests use the real runtime; synthetic bytes do not claim repository validity. */
class DocumentHistoryContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String REVISION = "abcdefab-cdef-4abc-8def-abcdefabcdef";
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setAccountId("account")
            .setDocId("document").setGraphId("graph").setGraphAddressId("source").build();

    @Test void requiresExactAddressRevisionAndExplicitMode() throws Exception {
        var request = ReadRevisionRequest.newBuilder().setAddress(ADDRESS).setRevisionId(REVISION)
                .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW).build();
        check(request, true);
        check(request.toBuilder().setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED).build(), true);
        check(request.toBuilder().clearAddress().build(), false);
        check(request.toBuilder().setAddress(ADDRESS.toBuilder().setGraphId(" ")).build(), false);
        check(request.toBuilder().setAddress(ADDRESS.toBuilder().setDocId("d".repeat(4097))).build(), false);
        check(request.toBuilder().clearMode().build(), false);
        check(request.toBuilder().setModeValue(999).build(), false);
        check(request.toBuilder().setRevisionId("latest").build(), false);
        check(request.toBuilder().setRevisionId(REVISION.toUpperCase(java.util.Locale.ROOT)).build(), false);
    }

    @Test void requiresOneRepresentationAndConsistentCapturedIdentity() throws Exception {
        var raw = response().setRaw(RawHistoricalDocument.newBuilder().addFragments(fragment())).build();
        check(raw, true);
        check(raw.toBuilder().clearRepresentation().build(), false);
        check(raw.toBuilder().clearManifest().build(), false);
        check(raw.toBuilder().setManifest(raw.getManifest().toBuilder()
                .setAddress(ADDRESS.toBuilder().setAccountId("foreign"))).build(), false);
        check(raw.toBuilder().setMutationRevision(0).build(), false);
        check(raw.toBuilder().setRaw(RawHistoricalDocument.getDefaultInstance()).build(), false);
        check(fragment().setRevisionOrdinal(9999).build(), true);
        check(fragment().setRevisionOrdinal(10000).build(), false);
        check(fragment().clearContent().build(), true);
        check(fragment().setContent(ByteString.copyFrom(new byte[8 * 1024 * 1024 + 1])).build(), false);
    }

    @Test void requiresRetainedValidationIdentityWithoutClaimingSemanticCorrectness() throws Exception {
        // These bytes are deliberately not a valid encoding of a known payload.
        // The envelope validator cannot substitute for retained-schema replay.
        var document = Document.newBuilder().setDocId("document").setStructuredData(com.google.protobuf.Any.newBuilder()
                .setTypeUrl("type.test/unknown.Payload").setValue(ByteString.copyFrom(new byte[] {(byte) 0xff}))).build();
        var typed = ValidatedHistoricalDocument.newBuilder().setDocument(document)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setPolicySha256("a".repeat(64)).setCommandSha256(ByteString.copyFrom(new byte[32])).build();
        check(response().setValidated(typed).build(), true);
        check(typed.toBuilder().clearDocument().build(), false);
        check(typed.toBuilder().setValidationProfile(" ").build(), false);
        check(typed.toBuilder().setPolicySha256("bad").build(), false);
        check(typed.toBuilder().setCommandSha256(ByteString.copyFrom(new byte[31])).build(), false);
    }

    @Test void boundsTheRawFragmentCollection() throws Exception {
        var raw = RawHistoricalDocument.newBuilder();
        for (int ordinal = 0; ordinal < 10000; ordinal++) raw.addFragments(fragment().setRevisionOrdinal(ordinal));
        check(raw.build(), true);
        check(raw.addFragments(fragment()).build(), false);
    }

    @Test void jsonSchemaRecordsBoundsAndRuntimeOnlyIdentityRules() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode raw = mapper.valueToTree(generator.generateRooted(RawHistoricalDocument.getDescriptor()));
        assertThat(raw.at("/properties/fragments/minItems").asInt()).isEqualTo(1);
        assertThat(raw.at("/properties/fragments/maxItems").asInt()).isEqualTo(10000);
        com.fasterxml.jackson.databind.JsonNode response = mapper.valueToTree(generator.generateRooted(ReadRevisionResponse.getDescriptor()));
        assertThat(response.path("x-protomolt-cel").toString()).contains("history-response-mode", "history-response-address");
        assertThat(generator.fieldValidationSchema(HistoricalDocumentFragment.getDescriptor().findFieldByName("content")))
                .containsEntry("x-protomolt-runtime-rules", java.util.List.of("bytes"));
    }

    private static ReadRevisionResponse.Builder response() {
        return ReadRevisionResponse.newBuilder().setAddress(ADDRESS).setRevisionId(REVISION).setMutationRevision(1)
                .setManifest(DocumentManifest.newBuilder().setAddress(ADDRESS));
    }

    private static HistoricalDocumentFragment.Builder fragment() {
        return HistoricalDocumentFragment.newBuilder().setRevisionOrdinal(0).setContent(ByteString.copyFromUtf8("fixture"));
    }

    private static void check(Message message, boolean expected) throws Exception {
        assertThat(VALIDATOR.validate(message).valid()).as("generated %s", message.getDescriptorForType().getFullName()).isEqualTo(expected);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray())).valid())
                .as("dynamic %s", message.getDescriptorForType().getFullName()).isEqualTo(expected);
    }
}
