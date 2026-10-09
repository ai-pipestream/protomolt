package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentPublicationInputTest {
    private static final String ID="10000000-0000-4000-8000-000000000001";
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;
    private static final ByteString BODY=ByteString.copyFromUtf8("abc");
    private static final String SHA="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    @Test void hostObjectLimitPrecedesChecksumAndPreservesProtocolBounds() {
        var request=request(BODY,SHA);
        assertThat(DocumentPublicationInput.validate(request,NONE,3).payloads()
                .get(new DocumentPublicationInput.PayloadKey("member",0))).isSameAs(BODY);
        assertThatThrownBy(() -> DocumentPublicationInput.validate(request(BODY,"0".repeat(64)),NONE,2))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("configured object limit");
        assertThatThrownBy(() -> DocumentPublicationInput.validate(request,NONE,0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Upload object limit");
        assertThatThrownBy(() -> DocumentPublicationInput.validate(request,NONE,8*1024*1024+1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Upload object limit");
        var empty=request(ByteString.EMPTY,"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertThat(DocumentPublicationInput.validate(empty,NONE,1).uploadBytes()).isZero();
    }

    @Test void retainsImmutableBytesAndExactCommand() {
        var request=request(BODY,SHA);
        var input=DocumentPublicationInput.validate(request,NONE);
        assertThat(input.command().intent()).isEqualTo(request.getIntent());
        assertThat(input.uploadBytes()).isEqualTo(3);
        assertThat(input.payloads().get(new DocumentPublicationInput.PayloadKey("member",0))).isSameAs(BODY);
        assertThatThrownBy(() -> input.payloads().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> input.modes().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(input.toString()).doesNotContain("abc",SHA,ID);
    }

    @Test void checksCoverageEvenWhenAnnotationsAcceptIt() {
        var request=request(BODY,SHA);
        invalid(request.toBuilder().clearPayloads().build(),"missing");
        invalid(request.toBuilder().addPayloads(request.getPayloads(0)).build(),"Duplicate");
        invalid(request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder().setMemberId("other")).build(),"coordinate");
        invalid(request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder().setRevisionOrdinal(1)).build(),"coordinate");
        invalid(request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder().setRevisionOrdinal(-1)).build(),"coordinate");
        invalid(request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder().setContent(ByteString.copyFromUtf8("abd"))).build(),"checksum");
        invalid(request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder().clearContent()).build(),"length");
    }

    @Test void requiresExactMemberModesAndKnownFields() {
        var request=request(BODY,SHA);
        invalid(request.toBuilder().clearModes().build(),"incomplete");
        invalid(request.toBuilder().addModes(request.getModes(0)).build(),"exactly once");
        for (int number:new int[]{0,-1,99})
            invalid(request.toBuilder().setModes(0,request.getModes(0).toBuilder().setModeValue(number)).build(),"exactly once");
        var unknown=UnknownFieldSet.newBuilder().addField(99,UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        invalid(request.toBuilder().setUnknownFields(unknown).build(),"Unknown");
        invalid(request.toBuilder().setModes(0,request.getModes(0).toBuilder().setUnknownFields(unknown)).build(),"exactly once");
        invalid(request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder().setUnknownFields(unknown)).build(),"coordinate");
    }

    @Test void allowsZeroBytesButDoesNotConfuseEmptySlotsWithUploads() {
        var request=request(ByteString.EMPTY,"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assertThat(DocumentPublicationInput.validate(request,NONE).uploadBytes()).isZero();
        var extra=DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                .setPart(DocumentPart.DOCUMENT_PART_PARSED)).setEmpty(true);
        var changed=request.toBuilder(); changed.getIntentBuilder().getMembersBuilder(0).addParts(extra);
        assertThat(DocumentPublicationInput.validate(changed.build(),NONE).payloads()).hasSize(1);
        changed.addPayloads(DocumentPublicationPayload.newBuilder().setMemberId("member").setRevisionOrdinal(1));
        invalid(changed.build(),"does not name an upload");
    }

    @Test void checksAggregateUploadLimitBeforeHashingAndHonorsCancellation() throws Exception {
        var bytes=ByteString.copyFrom(new byte[(int)DocumentPublicationInput.MAX_UPLOAD_BYTES]);
        var request=request(bytes,sha(bytes));
        assertThat(DocumentPublicationInput.validate(request,NONE).uploadBytes()).isEqualTo(DocumentPublicationInput.MAX_UPLOAD_BYTES);
        var second=request.toBuilder();
        var next=second.getIntentBuilder().getMembersBuilder(0).getParts(0).toBuilder();
        next.setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED));
        next.getUploadBuilder().setSizeBytes(1).setSha256("0".repeat(64));
        second.getIntentBuilder().getMembersBuilder(0).addParts(next);
        second.addPayloads(DocumentPublicationPayload.newBuilder().setMemberId("member").setRevisionOrdinal(1)
                .setContent(ByteString.copyFromUtf8("x")));
        invalid(second.build(),"exceed 8 MiB");
        var checks=new AtomicInteger();
        var cancellation=new RepositoryReadControl() {
            public boolean isCancelled() { return checks.incrementAndGet()>20; }
            public long remainingNanos() { return Long.MAX_VALUE; }
        };
        assertThatThrownBy(() -> DocumentPublicationInput.validate(request,cancellation))
                .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
        assertThat(checks.get()).isEqualTo(21);
    }

    @Test void rejectsEnvelopeAndIntentLimitsBeforeBodyHashing() {
        invalid(request(ByteString.copyFrom(new byte[DocumentPublicationInput.MAX_ENVELOPE_BYTES]),"0".repeat(64)),
                "envelope exceeds");
        var oversized=request(BODY,SHA).toBuilder();
        oversized.getIntentBuilder().getMembersBuilder(0).putMetadata("large","x".repeat(DocumentPublicationCommand.MAX_COMMAND_BYTES));
        invalid(oversized.build(),"intent exceeds");
        invalid(request(BODY,SHA).toBuilder().clearIntent().build(),"intent");
    }

    @Test void indexesMixedMembersByFullOrdinalRatherThanUploadOrder() {
        var request=request(BODY,SHA);
        var first=request.getIntent().getMembers(0);
        var source=first.getDestination().toBuilder().setExpectedMutationRevision(1)
                .setAddress(first.getDestination().getAddress().toBuilder().setDocId("retained"));
        var reused=first.getParts(0).toBuilder().setReuse(PublicationReuse.newBuilder().setSource(source)
                .setSourceSlot(first.getParts(0).getSlot()).setObject(PublicationObjectIdentity.newBuilder()
                        .setObjectId(ID).setBackendGeneration("profile").setStorageRealm("realm")
                        .setNamespace("docs").setObjectKey("original").setSizeBytes(3).setSha256(SHA)
                        .setContentType("application/protobuf")));
        var second=first.toBuilder().setMemberId("second").clearParts()
                .setDestination(first.getDestination().toBuilder().setAddress(first.getDestination().getAddress().toBuilder().setDocId("other")))
                .addParts(reused)
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_PARSED)).setEmpty(true))
                .addParts(first.getParts(0).toBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CHUNKS).setSubKey("chunk")));
        var mixed=request.toBuilder().setIntent(request.getIntent().toBuilder().clearMembers().addMembers(second).addMembers(first))
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId("second").setModeValue(2))
                .addPayloads(DocumentPublicationPayload.newBuilder().setMemberId("second").setRevisionOrdinal(2).setContent(BODY)).build();
        var input=DocumentPublicationInput.validate(mixed,NONE);
        assertThat(input.uploadBytes()).isEqualTo(6);
        assertThat(input.payloads()).containsOnlyKeys(new DocumentPublicationInput.PayloadKey("member",0),
                new DocumentPublicationInput.PayloadKey("second",2));
        invalid(mixed.toBuilder().setPayloads(1,mixed.getPayloads(1).toBuilder().setRevisionOrdinal(0)).build(),"does not name an upload");
    }

    private static PublishDocumentRequest request(ByteString bytes,String sha) {
        var address=NodeAddress.newBuilder().setAccountId("account").setGraphId("graph").setGraphAddressId("source").setDocId("doc");
        var member=DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(ID)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address).setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)).setUpload(PublicationUpload.newBuilder()
                        .setSizeBytes(bytes.size()).setSha256(sha).setContentType("application/protobuf")));
        return PublishDocumentRequest.newBuilder().setIntent(DocumentPublicationIntent.newBuilder()
                .setEncodingVersion(1).setAccountId("account").setOperationId(ID).addMembers(member))
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId("member").setModeValue(1))
                .addPayloads(DocumentPublicationPayload.newBuilder().setMemberId("member").setContent(bytes)).build();
    }
    private static String sha(ByteString bytes) throws Exception {
        var digest=java.security.MessageDigest.getInstance("SHA-256");
        for (var buffer:bytes.asReadOnlyByteBufferList()) digest.update(buffer);
        return HexFormat.of().formatHex(digest.digest());
    }
    private static void invalid(PublishDocumentRequest request,String message) {
        assertThatThrownBy(() -> DocumentPublicationInput.validate(request,NONE))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message);
    }
}
