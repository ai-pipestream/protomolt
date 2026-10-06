package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Staged transport shape only; no mounted handler or durable outcome claim. */
class DocumentPublicationTransportContractTest {
    private static final ProtoValidator VALIDATOR=ProtoValidator.create();
    private static final String ID="10000000-0000-4000-8000-000000000001";

    @Test void requiresExactExplicitMemberModes() throws Exception {
        var request=request();
        check(request,true);
        check(request.toBuilder().clearIntent().build(),false);
        check(request.toBuilder().clearModes().build(),false);
        check(request.toBuilder().setModes(0,request.getModes(0).toBuilder().setMemberId("other")).build(),false);
        check(request.toBuilder().addModes(request.getModes(0)).build(),false);
        for (int mode:new int[]{0,99,-1})
            check(request.toBuilder().setModes(0,request.getModes(0).toBuilder().setModeValue(mode)).build(),false);
        check(request.toBuilder().setModes(0,request.getModes(0).toBuilder().setModeValue(2)).build(),true);
    }

    @Test void boundsPayloadCoordinatesAndContent() throws Exception {
        var payload=DocumentPublicationPayload.newBuilder().setMemberId("member").build();
        check(payload,true); // A present zero-byte upload is valid.
        check(payload.toBuilder().clearMemberId().build(),false);
        check(payload.toBuilder().setRevisionOrdinal(9999).build(),true);
        check(payload.toBuilder().setRevisionOrdinal(10000).build(),false);
        check(payload.toBuilder().setRevisionOrdinal(-1).build(),false);
        check(payload.toBuilder().setContent(ByteString.copyFrom(new byte[8388608])).build(),true);
        check(payload.toBuilder().setContent(ByteString.copyFrom(new byte[8388609])).build(),false);
    }

    @Test void receiptEnvelopeRequiresOneValidDurableOutcome() throws Exception {
        check(PublishDocumentResponse.getDefaultInstance(),false);
        check(PublishDocumentResponse.newBuilder().setCommitted(DocumentPublicationResult.getDefaultInstance()).build(),false);
        var receipt=DocumentPublicationResult.newBuilder().setAccountId("account").setOperationId(ID)
                .setPrincipal("principal").setCommandSha256("a".repeat(64)).setCommandEncodingVersion(1).setOwnerGeneration(1)
                .addMembers(DocumentPublishedRevision.newBuilder().setMemberId("member").setAddress(address())
                        .setRevisionId(ID).setMutationRevision(1)).build();
        check(PublishDocumentResponse.newBuilder().setCommitted(receipt).build(),true);
        var rejection=DocumentPublicationRejection.newBuilder().setAccountId("account").setOperationId(ID)
                .setPrincipal("principal").setCommandSha256("a".repeat(64)).setCommandEncodingVersion(1)
                .setCommandCodec("document-publication").setOwnerGeneration(1).setRecordedAtEpochMicros(1)
                .setDispositionValue(1).setReasonValue(1).build();
        check(PublishDocumentResponse.newBuilder().setRejected(rejection).build(),true);
    }

    @Test void shapeValidationDoesNotReplaceIndexedCoverageOrChecksumChecks() throws Exception {
        var request=request();
        // The future shared boundary must reject these despite valid annotation shape.
        check(request.toBuilder().clearPayloads().build(),true);
        check(request.toBuilder().addPayloads(request.getPayloads(0)).build(),true);
        check(request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder().setContent(ByteString.copyFromUtf8("wrong"))).build(),true);
    }

    @Test void jsonSchemaRecordsRuntimeCrossFieldRules() {
        var generator=ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper=new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema=mapper.valueToTree(generator.generateRooted(PublishDocumentRequest.getDescriptor()));
        assertThat(schema.at("/properties/modes/maxItems").asInt()).isEqualTo(64);
        assertThat(schema.at("/properties/payloads/maxItems").asInt()).isEqualTo(10000);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("publication-request-modes");
    }

    private static NodeAddress address() {
        return NodeAddress.newBuilder().setAccountId("account").setGraphId("graph")
                .setGraphAddressId("source").setDocId("doc").build();
    }
    private static PublishDocumentRequest request() {
        var member=DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(ID)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address()).setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)).setUpload(PublicationUpload.newBuilder()
                        .setSha256("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                        .setContentType("application/protobuf")));
        return PublishDocumentRequest.newBuilder().setIntent(DocumentPublicationIntent.newBuilder()
                .setEncodingVersion(1).setAccountId("account").setOperationId(ID).addMembers(member))
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId("member").setModeValue(1))
                .addPayloads(DocumentPublicationPayload.newBuilder().setMemberId("member")).build();
    }
    private static void check(Message value, boolean expected) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s",value.getDescriptorForType().getName()).isEqualTo(expected);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(value.getDescriptorForType(),value.toByteString())).valid())
                .as("dynamic %s",value.getDescriptorForType().getName()).isEqualTo(expected);
    }
}
