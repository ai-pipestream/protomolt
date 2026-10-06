package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class DocumentPublicationCommandTest {
    private static final String ID = "10000000-0000-4000-8000-000000000001";
    private static final String OTHER = "10000000-0000-4000-8000-000000000002";
    private static final String SHA = "a".repeat(64);

    @Test void successfulResultMustMatchFullCanonicalCommandAndCommittingOwner() throws Exception {
        var command = command(intent().toBuilder().clearMembers().addMembers(member("z")).addMembers(member("a")).build());
        var result = DocumentPublicationResult.newBuilder().setOperationId(ID).setAccountId("a")
                .setCommandEncodingVersion(1).setCommandSha256(command.sha256()).setPrincipal("principal").setOwnerGeneration(2);
        for (var m : command.intent().getMembersList()) result.addMembers(DocumentPublishedRevision.newBuilder()
                .setMemberId(m.getMemberId()).setAddress(m.getDestination().getAddress())
                .setRevisionId(java.util.UUID.randomUUID().toString()).setMutationRevision(1));
        var valid = result.build();
        assertThatCode(() -> command.requireResult(valid, "principal", 2)).doesNotThrowAnyException();
        assertThatThrownBy(() -> command.requireResult(valid, "other-principal", 2)).hasMessageContaining("operation identity");
        assertThatThrownBy(() -> command.requireResult(valid, "principal", 3)).hasMessageContaining("operation identity");
        assertThatThrownBy(() -> command.requireResult(valid.toBuilder().setOperationId(OTHER).build(), "principal", 2))
                .hasMessageContaining("operation identity");
        assertThatThrownBy(() -> command.requireResult(valid.toBuilder().setCommandSha256("b".repeat(64)).build(), "principal", 2))
                .hasMessageContaining("operation identity");
        assertThatThrownBy(() -> command.requireResult(valid.toBuilder().removeMembers(1).build(), "principal", 2))
                .hasMessageContaining("incomplete member set");
        assertThatThrownBy(() -> command.requireResult(valid.toBuilder().setMembers(0, valid.getMembers(1))
                .setMembers(1, valid.getMembers(0)).build(), "principal", 2)).hasMessageContaining("canonical member order");
        assertThatThrownBy(() -> command.requireResult(valid.toBuilder().setMembers(0, valid.getMembers(0).toBuilder()
                .setAddress(address("wrong"))).build(), "principal", 2)).hasMessageContaining("address");
        var unknown = UnknownFieldSet.newBuilder().addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        assertThatThrownBy(() -> command.requireResult(valid.toBuilder().setUnknownFields(unknown).build(), "principal", 2))
                .hasMessageContaining("Unknown publication fields");
        var encoded = DocumentPublicationResultCodec.encode(command, valid, "principal", 2);
        var repeated = DocumentPublicationResultCodec.encode(command, valid, "principal", 2);
        assertThat(repeated.bytes()).isEqualTo(encoded.bytes());
        assertThat(repeated.sha256()).isEqualTo(encoded.sha256());
        assertThat(DocumentPublicationResultCodec.decode(command, "principal", 2,
                DocumentPublicationResultCodec.CODEC, 1, encoded.bytes(), encoded.sha256())).isEqualTo(valid);
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "principal", 2,
                DocumentPublicationResultCodec.CODEC, 2, encoded.bytes(), encoded.sha256())).hasMessageContaining("Unsupported");
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "principal", 2,
                "another-codec", 1, encoded.bytes(), encoded.sha256())).hasMessageContaining("Unsupported");
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "principal", 2,
                DocumentPublicationResultCodec.CODEC, 1, encoded.bytes(), "0".repeat(64))).hasMessageContaining("digest mismatch");
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "another", 2,
                DocumentPublicationResultCodec.CODEC, 1, encoded.bytes(), encoded.sha256())).hasMessageContaining("operation identity");
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "principal", 3,
                DocumentPublicationResultCodec.CODEC, 1, encoded.bytes(), encoded.sha256())).hasMessageContaining("operation identity");
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "principal", 2,
                DocumentPublicationResultCodec.CODEC, 1, com.google.protobuf.ByteString.EMPTY, encoded.sha256())).hasMessageContaining("byte bounds");
        var malformed = com.google.protobuf.ByteString.copyFrom(new byte[]{0});
        var malformedHash = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(malformed.toByteArray()));
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "principal", 2,
                DocumentPublicationResultCodec.CODEC, 1, malformed, malformedHash)).isInstanceOf(com.google.protobuf.InvalidProtocolBufferException.class);
        var repeatedEmptyMembers = new byte[2 * (DocumentPublicationResultCodec.MAX_WIRE_VALUES + 1)];
        for (int i = 0; i < repeatedEmptyMembers.length; i += 2) repeatedEmptyMembers[i] = 50; // field 6, empty message
        var excessiveWire = com.google.protobuf.ByteString.copyFrom(repeatedEmptyMembers);
        var excessiveHash = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(repeatedEmptyMembers));
        assertThatThrownBy(() -> DocumentPublicationResultCodec.decode(command, "principal", 2,
                DocumentPublicationResultCodec.CODEC, 1, excessiveWire, excessiveHash)).hasMessageContaining("wire value");
    }

    @Test void resultCodecAcceptsAll64MembersWithinWireBudget() throws Exception {
        var intent = intent().toBuilder().clearMembers();
        for (int i = 0; i < 64; i++) intent.addMembers(member("member-" + i));
        var command = command(intent.build());
        var result = DocumentPublicationResult.newBuilder().setOperationId(ID).setAccountId("a")
                .setCommandEncodingVersion(1).setCommandSha256(command.sha256())
                .setPrincipal("principal").setOwnerGeneration(Long.MAX_VALUE);
        int index = 1;
        for (var member : command.intent().getMembersList()) {
            result.addMembers(DocumentPublishedRevision.newBuilder().setMemberId(member.getMemberId())
                    .setAddress(member.getDestination().getAddress())
                    .setRevisionId(new java.util.UUID(0, index++).toString())
                    .setMutationRevision(Long.MAX_VALUE));
        }
        var expected = result.build();
        var encoded = DocumentPublicationResultCodec.encode(command, expected, "principal", Long.MAX_VALUE);
        assertThat(DocumentPublicationResultCodec.decode(command, "principal", Long.MAX_VALUE,
                DocumentPublicationResultCodec.CODEC, DocumentPublicationResultCodec.VERSION,
                encoded.bytes(), encoded.sha256())).isEqualTo(expected);
    }

    private static NodeAddress address(String doc) {
        return NodeAddress.newBuilder().setAccountId("a").setDocId(doc)
                .setGraphId("g").setGraphAddressId("n").build();
    }
    private static DocumentPublicationPart part(DocumentPart part, String subKey) {
        return DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part).setSubKey(subKey))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(7).setSha256(SHA).setContentType("application/protobuf")).build();
    }
    private static DocumentPublicationMember member(String id) {
        return DocumentPublicationMember.newBuilder().setMemberId(id).setDriveId(ID)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address(id)).setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("a").setDatasourceId("s")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .addParts(part(DocumentPart.DOCUMENT_PART_CORE, "")).build();
    }
    private static DocumentPublicationIntent intent() {
        return DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("a")
                .setOperationId(ID).addMembers(member("m")).build();
    }
    private static DocumentPublicationCommand command(DocumentPublicationIntent intent) {
        return new DocumentPublicationCommand(intent);
    }
    private static void refuses(DocumentPublicationIntent value, String reason) {
        assertThatThrownBy(() -> command(value)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(reason);
    }
    private static DocumentRevisionCondition source(long revision) {
        return DocumentRevisionCondition.newBuilder().setAddress(address("source")).setExpectedMutationRevision(revision).build();
    }

    @Test void canonicalVersionOneHasPinnedIdentityAndRetainsExecutableIntent() throws Exception {
        var command = command(intent());
        assertThat(command.operationId().toString()).isEqualTo(ID);
        assertThat(command.intent()).isEqualTo(intent());
        assertThat(DocumentPublicationIntent.parseFrom(command.canonical()))
                .isEqualTo(intent().toBuilder().clearOperationId().build());
        assertThat(command.sha256()).isEqualTo("ee77ccd68b657965717ca7fb893ce181b754f38ae0e7c572aae1f8930149c273");
        // Golden wire bytes were assembled independently from the field/tag specification.
        try (var fixture = getClass().getResourceAsStream("/document-publication-v1.hex")) {
            assertThat(fixture).isNotNull();
            assertThat(command.canonical().toByteArray()).isEqualTo(java.util.HexFormat.of().parseHex(
                    new String(fixture.readAllBytes(), java.nio.charset.StandardCharsets.US_ASCII).trim()));
        }
        assertThat(command.toString()).doesNotContain("application/protobuf", "datasource", "ownership");
    }

    @Test void onlyOperationIdAndMemberOrderAreNormalized() {
        var first = intent().toBuilder().addMembers(member("b")).build();
        var reordered = first.toBuilder().setOperationId(OTHER).clearMembers()
                .addMembers(member("b")).addMembers(member("m")).build();
        assertThat(command(first).canonical()).isEqualTo(command(reordered).canonical());
        assertThat(command(first).intent().getMembers(0).getMemberId()).isEqualTo("b");
        assertThat(command(first).operationId()).isNotEqualTo(command(reordered).operationId());
    }

    @Test void metadataInsertionOrderIsNotSemanticButChunkOrderIs() {
        var first = member("m").toBuilder().putMetadata("z", "last").putMetadata("a", "first");
        var second = member("m").toBuilder().putMetadata("a", "first").putMetadata("z", "last");
        assertThat(command(intent().toBuilder().setMembers(0, first).build()).canonical())
                .isEqualTo(command(intent().toBuilder().setMembers(0, second).build()).canonical());
        first.addParts(part(DocumentPart.DOCUMENT_PART_CHUNKS, "one")).addParts(part(DocumentPart.DOCUMENT_PART_CHUNKS, "two"));
        second.addParts(part(DocumentPart.DOCUMENT_PART_CHUNKS, "two")).addParts(part(DocumentPart.DOCUMENT_PART_CHUNKS, "one"));
        assertThat(command(intent().toBuilder().setMembers(0, first).build()).canonical())
                .isNotEqualTo(command(intent().toBuilder().setMembers(0, second).build()).canonical());
    }

    @Test void semanticChangesChangeIdentity() {
        var original = command(intent()).canonical();
        for (var changed : new DocumentPublicationMember[]{
                member("m").toBuilder().setDriveId(OTHER).build(),
                member("m").toBuilder().setCrawlId("crawl").build(),
                member("m").toBuilder().setClusterId("cluster").build(),
                member("m").toBuilder().putMetadata("k", "v").build(),
                member("m").toBuilder().setDestination(member("m").getDestination().toBuilder().setExpectedMutationRevision(1)).build(),
                member("m").toBuilder().addSources(source(1)).build(),
                member("m").toBuilder().setOwnership(member("m").getOwnership().toBuilder().setConnectorId("connector")).build(),
                member("m").toBuilder().setStructuredSchema(PublicationSchemaCondition.newBuilder().setTypeName("x.Type").setDescriptorFingerprint(SHA)).build(),
                member("m").toBuilder().setParts(0, member("m").getParts(0).toBuilder().setUpload(
                        member("m").getParts(0).getUpload().toBuilder().setSha256("b".repeat(64)))).build()}) {
            assertThat(command(intent().toBuilder().setMembers(0, changed).build()).canonical()).isNotEqualTo(original);
        }
    }

    @Test void rejectsDuplicateMembersDestinationsAndSlots() {
        refuses(intent().toBuilder().addMembers(member("m")).build(), "Duplicate publication member");
        refuses(intent().toBuilder().addMembers(member("other").toBuilder().setDestination(member("m").getDestination())).build(), "Duplicate publication member");
        var chunks = part(DocumentPart.DOCUMENT_PART_CHUNKS, "set");
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().addParts(chunks).addParts(chunks)).build(), "Duplicate publication slot");
    }

    @Test void rejectsConflictingSourcesIncludingDestinationPrestate() {
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().addSources(source(1)).addSources(source(1))).build(), "Duplicate explicit source");
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().addSources(source(1)))
                .addMembers(member("b").toBuilder().addSources(source(2))).build(), "Conflicting source revisions");
        var self = source(1).toBuilder().setAddress(address("m"));
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().addSources(self)).build(), "pre-change revision");
        var matching = member("m").toBuilder().addSources(self)
                .setDestination(member("m").getDestination().toBuilder().setExpectedMutationRevision(1));
        assertThatCode(() -> command(intent().toBuilder().setMembers(0, matching).build())).doesNotThrowAnyException();
    }

    @Test void failsClosedOnNestedUnknownFieldsAndEnumsAndNul() {
        var unknown = UnknownFieldSet.newBuilder().addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        refuses(intent().toBuilder().setUnknownFields(unknown).build(), "Unknown publication fields");
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().setOwnership(member("m").getOwnership()
                .toBuilder().setUnknownFields(unknown))).build(), "Unknown publication fields");
        var acl = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder().setIdentity("user")
                .setIdentityType("local").setAccessValue(99));
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().setOwnership(member("m").getOwnership()
                .toBuilder().setSecurity(acl))).build(), "Unknown publication enum");
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().putMetadata("k", "bad\0value")).build(), "NUL");
    }

    @Test void malformedLegacyOwnershipCannotBypassCommandBoundary() {
        var acl = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder().setAccess(Access.ACCESS_READ));
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().setOwnership(member("m").getOwnership()
                .toBuilder().setSecurity(acl))).build(), "Malformed ownership");
    }

    @Test void invalidUtf16CannotCollideAfterProtobufReplacement() {
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().putMetadata("k", "\ud800")).build(), "Malformed UTF-16");
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().putMetadata("k", "\udc00")).build(), "Malformed UTF-16");
        assertThatCode(() -> command(intent().toBuilder().setMembers(0, member("m").toBuilder()
                .putMetadata("k", "\ud83d\ude00")).build())).doesNotThrowAnyException();
    }

    @Test void reuseBindsExactObjectVersionSourceAndSlot() {
        var object = PublicationObjectIdentity.newBuilder().setObjectId(ID).setBackendGeneration("generation")
                .setStorageRealm("realm").setNamespace("ns").setObjectKey("original-key")
                .setSha256(SHA).setSizeBytes(7).setContentType("application/protobuf");
        var reused = member("m").getParts(0).toBuilder().setReuse(PublicationReuse.newBuilder()
                .setSource(source(1)).setSourceSlot(member("m").getParts(0).getSlot()).setObject(object)).build();
        var selected = intent().toBuilder().setMembers(0, member("m").toBuilder().setParts(0, reused)).build();
        var canonical = command(selected).canonical();
        for (var changed : new PublicationReuse[]{
                reused.getReuse().toBuilder().setSource(source(2)).build(),
                reused.getReuse().toBuilder().setObject(object.clone().setProviderVersion("v2")).build(),
                reused.getReuse().toBuilder().setObject(object.clone().setObjectKey("other-key")).build(),
                reused.getReuse().toBuilder().setObject(object.clone().setBackendGeneration("other-generation")).build()}) {
            assertThat(command(selected.toBuilder().setMembers(0, selected.getMembers(0).toBuilder()
                    .setParts(0, reused.toBuilder().setReuse(changed))).build()).canonical()).isNotEqualTo(canonical);
        }
        var wrongSlot = reused.toBuilder().setReuse(reused.getReuse().toBuilder().setSourceSlot(
                DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)));
        refuses(selected.toBuilder().setMembers(0, selected.getMembers(0).toBuilder().setParts(0, wrongSlot)).build(), "preserve the source slot");
    }

    @Test void aggregateSourceLimitCountsAcrossMembers() {
        var bounded = intent().toBuilder().clearMembers();
        for (int i = 0; i < 2; i++) {
            var m = member("m" + i).toBuilder();
            for (int j = 0; j < 5000; j++) m.addSources(source(1).toBuilder().setAddress(address("source" + j)));
            bounded.addMembers(m);
        }
        assertThatCode(() -> command(bounded.build())).doesNotThrowAnyException();
        bounded.setMembers(0, bounded.getMembers(0).toBuilder().addSources(source(1)));
        refuses(bounded.build(), "Aggregate publication part/source limit");
    }

    @Test void historicalContractCannotReachUnimplementedExecutionOrLegacyReuse() {
        var m = member("m");
        var selected = PublicationHistoricalReuse.newBuilder().setSource(m.getDestination().getAddress())
                .setRevisionId(OTHER).setRevisionOrdinal(0).setSourceSlot(m.getParts(0).getSlot())
                .setObject(PublicationObjectIdentity.newBuilder().setObjectId(ID).setBackendGeneration("generation")
                        .setStorageRealm("realm").setNamespace("ns").setObjectKey("original-key")
                        .setSha256(SHA).setSizeBytes(7).setContentType("application/protobuf"));
        var supplied = intent().toBuilder().setMembers(0, m.toBuilder()
                .setDestination(m.getDestination().toBuilder().setExpectedMutationRevision(3))
                .setParts(0, m.getParts(0).toBuilder().setHistoricalReuse(selected))).build();
        assertThat(ai.protomolt.proto.validate.ProtoValidator.create().validate(supplied).valid()).isTrue();
        assertThatThrownBy(() -> command(supplied)).isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Historical reuse execution is not implemented");
        // Unknown fields must still be refused before an unsupported arm can hide them.
        var unknown = selected.clone().setUnknownFields(UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build());
        assertThatThrownBy(() -> command(supplied.toBuilder().setMembers(0, supplied.getMembers(0).toBuilder()
                .setParts(0, m.getParts(0).toBuilder().setHistoricalReuse(unknown))).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown");
    }

    @Test void aggregateByteOverflowAndWireBudgetAreRejectedBeforeAdmission() {
        var huge = part(DocumentPart.DOCUMENT_PART_CHUNKS, "set").toBuilder()
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(Long.MAX_VALUE).setSha256(SHA).setContentType("x"));
        refuses(intent().toBuilder().setMembers(0, member("m").toBuilder().addParts(huge)).build(), "overflows");
        var oversized = intent().toBuilder();
        for (int i = 0; i < 3; i++) {
            var m = member("large" + i).toBuilder();
            for (int j = 0; j < 128; j++) m.putMetadata("k" + j, "x".repeat(4096));
            oversized.addMembers(m);
        }
        refuses(oversized.build(), "1 MiB");
    }

    @Test void globalSlotLimitIsIndependentOfPerMemberLimit() {
        var bounded = intent().toBuilder().clearMembers();
        for (int i = 0; i < 2; i++) {
            var m = member("m" + i).toBuilder();
            for (int j = 0; j < 4999; j++) m.addParts(DocumentPublicationPart.newBuilder()
                    .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CHUNKS).setSubKey("c" + j))
                    .setEmpty(true));
            bounded.addMembers(m);
        }
        assertThatCode(() -> command(bounded.build())).doesNotThrowAnyException();
        bounded.setMembers(0, bounded.getMembers(0).toBuilder().addParts(DocumentPublicationPart.newBuilder()
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)).setEmpty(true)));
        refuses(bounded.build(), "Aggregate publication part/source limit");
    }
}
