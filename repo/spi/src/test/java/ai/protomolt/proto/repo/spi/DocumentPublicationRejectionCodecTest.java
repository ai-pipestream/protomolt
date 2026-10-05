package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentPublicationRejectionCodecTest {
    private static final String ID = "10000000-0000-4000-8000-000000000001";

    @Test void bindsAllIdentityCoordinatesAndRejectsUnknownFields() {
        var command = command();
        var valid = rejection(command);
        assertThatCode(() -> command.requireRejection(valid, "principal", 2)).doesNotThrowAnyException();
        for (var invalid : new DocumentPublicationRejection[]{
                valid.toBuilder().setOperationId("20000000-0000-4000-8000-000000000001").build(),
                valid.toBuilder().setAccountId("other").build(),
                valid.toBuilder().setPrincipal("other").build(),
                valid.toBuilder().setOwnerGeneration(3).build(),
                valid.toBuilder().setCommandSha256("a".repeat(64)).build()})
            assertThatThrownBy(() -> command.requireRejection(invalid, "principal", 2)).hasMessageContaining("operation identity");
        assertThatThrownBy(() -> command.requireRejection(valid, "other", 2)).hasMessageContaining("operation identity");
        assertThatThrownBy(() -> command.requireRejection(valid, "principal", 3)).hasMessageContaining("operation identity");
        var unknown = UnknownFieldSet.newBuilder().addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        assertThatThrownBy(() -> command.requireRejection(valid.toBuilder().setUnknownFields(unknown).build(), "principal", 2))
                .hasMessageContaining("Unknown publication fields");
        assertThatThrownBy(() -> command.requireRejection(valid.toBuilder().setPrincipal("a\0b").build(), "principal", 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void replaysExactBoundedBytesAndRejectsCorruptOrNoncanonicalEncodings() throws Exception {
        var command = command();
        var valid = rejection(command);
        var encoded = DocumentPublicationRejectionCodec.encode(command, valid, "principal", 2);
        assertThat(DocumentPublicationRejectionCodec.encode(command, valid, "principal", 2).bytes()).isEqualTo(encoded.bytes());
        assertThat(decode(command, encoded.bytes(), encoded.sha256())).isEqualTo(valid);
        assertThat(encoded.toString()).doesNotContain("principal", command.sha256());
        assertThatThrownBy(() -> decode(command, encoded.bytes(), "0".repeat(64))).hasMessageContaining("digest mismatch");
        assertThatThrownBy(() -> decode(command, ByteString.EMPTY, encoded.sha256())).hasMessageContaining("byte bounds");
        assertThatThrownBy(() -> decode(command, ByteString.copyFrom(new byte[4097]), encoded.sha256())).hasMessageContaining("byte bounds");
        assertThatThrownBy(() -> DocumentPublicationRejectionCodec.decode(command, "principal", 2,
                "other", 1, encoded.bytes(), encoded.sha256())).hasMessageContaining("Unsupported");
        assertThatThrownBy(() -> DocumentPublicationRejectionCodec.decode(command, "principal", 2,
                DocumentPublicationRejectionCodec.CODEC, 2, encoded.bytes(), encoded.sha256())).hasMessageContaining("Unsupported");
        // Duplicate scalar tags parse successfully but cannot be a canonical stored receipt.
        var duplicate = encoded.bytes().concat(ByteString.copyFrom(new byte[]{40, 2}));
        assertThatThrownBy(() -> decode(command, duplicate, digest(duplicate))).hasMessageContaining("Noncanonical");
        var flooded = encoded.bytes();
        for (int i = 0; i < 40; i++) flooded = flooded.concat(ByteString.copyFrom(new byte[]{40, 2}));
        var excessive = flooded;
        assertThatThrownBy(() -> decode(command, excessive, digest(excessive))).hasMessageContaining("wire value count exceeds bound");
        var truncated = encoded.bytes().substring(0, encoded.bytes().size() - 1);
        assertThatThrownBy(() -> decode(command, truncated, digest(truncated)))
                .isInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
    }

    @Test void admissionBindingSurvivesCanonicalReplayAndRejectsNestedUnknownFields() throws Exception {
        var command = command();
        var binding = DocumentPublicationAssessmentBinding.newBuilder().setAssessmentId(ID)
                .setManifestCodec("document-publication-assessment").setManifestEncodingVersion(1)
                .setManifestSha256("b".repeat(64)).setRetainUntilEpochMicros(2).build();
        var valid = rejection(command).toBuilder().setReasonValue(2).setAssessment(binding).build();
        var encoded = DocumentPublicationRejectionCodec.encode(command, valid, "principal", 2);
        assertThat(decode(command, encoded.bytes(), encoded.sha256())).isEqualTo(valid);
        assertThat(encoded.bytes().size()).isLessThan(DocumentPublicationRejectionCodec.MAX_BYTES);
        assertThatThrownBy(() -> DocumentPublicationRejectionCodec.encode(command,
                valid.toBuilder().clearAssessment().build(), "principal", 2))
                .hasMessageContaining("Invalid publication rejection");
        var unknown = UnknownFieldSet.newBuilder().addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var invalid = valid.toBuilder().setAssessment(binding.toBuilder().setUnknownFields(unknown)).build();
        assertThatThrownBy(() -> DocumentPublicationRejectionCodec.encode(command, invalid, "principal", 2))
                .hasMessageContaining("Unknown publication fields");
        assertThatThrownBy(() -> decode(command, invalid.toByteString(), digest(invalid.toByteString())))
                .hasMessageContaining("Unknown publication fields");
        // Existing precondition and cancellation receipts need no new field.
        var cancelled = rejection(command).toBuilder().setReasonValue(3).setDispositionValue(2).build();
        var cancellation = DocumentPublicationRejectionCodec.encode(command, cancelled, "principal", 2);
        assertThat(decode(command, cancellation.bytes(), cancellation.sha256())).isEqualTo(cancelled);
    }

    private static DocumentPublicationRejection decode(DocumentPublicationCommand command, ByteString bytes, String sha) throws Exception {
        return DocumentPublicationRejectionCodec.decode(command, "principal", 2, DocumentPublicationRejectionCodec.CODEC, 1, bytes, sha);
    }

    private static String digest(ByteString bytes) throws Exception {
        return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }

    private static DocumentPublicationRejection rejection(DocumentPublicationCommand command) {
        return DocumentPublicationRejection.newBuilder().setOperationId(ID).setAccountId("account").setPrincipal("principal")
                .setOwnerGeneration(2).setCommandEncodingVersion(1).setCommandCodec(DocumentPublicationCommand.CODEC)
                .setCommandSha256(command.sha256()).setRecordedAtEpochMicros(1).setDispositionValue(1).setReasonValue(1).build();
    }

    private static DocumentPublicationCommand command() {
        var address = NodeAddress.newBuilder().setAccountId("account").setGraphId("graph").setGraphAddressId("source").setDocId("doc");
        var member = DocumentPublicationMember.newBuilder().setMemberId("m").setDriveId(ID)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address).setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)).setUpload(PublicationUpload.newBuilder()
                        .setSha256("a".repeat(64)).setContentType("application/protobuf")));
        return new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                .setAccountId("account").setOperationId(ID).addMembers(member).build());
    }
}
