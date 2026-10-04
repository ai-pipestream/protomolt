package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentCommandContentTest {
    private static final DocumentRevisionAssembly.Limits LIMITS = new DocumentRevisionAssembly.Limits(100_000, 10, 100, 100, 100_000);
    private static OwnershipContext owner(String account) {
        return OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
    }
    private record Fixture(DocumentPublicationCommand command, Map<Integer, ByteString> bytes) {}
    private static Fixture fixture(String decodedOwner) {
        var doc = Document.newBuilder().setDocId("doc").setOwnership(owner(decodedOwner))
                .setSearchMetadata(SearchMetadata.newBuilder().addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("run"))).build();
        return fixture(doc);
    }
    private static Fixture fixture(Document doc) {
        var parts = DocumentPartCodec.split(doc, PartLayouts.document());
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(UUID.randomUUID().toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId("doc")))
                .setOwnership(owner("account")).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE);
        for (var part : parts) member.addParts(DocumentPublicationPart.newBuilder()
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(part.sha256()).setContentType("application/protobuf")));
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                .setOperationId(UUID.randomUUID().toString()).setAccountId("account").addMembers(member).build());
        var bytes = new java.util.HashMap<Integer, ByteString>();
        for (int i = 0; i < parts.size(); i++) bytes.put(i, ByteString.copyFrom(parts.get(i).bytes()));
        return new Fixture(command, Map.copyOf(bytes));
    }
    @Test void opaqueAnyPreservesUnknownTypeAndUnparsedValueWithoutGrantingTypedAdmission() throws Exception {
        // Invalid protobuf field tag proves this path does not parse the packed value.
        var opaque = com.google.protobuf.Any.newBuilder().setTypeUrl("archive.example/unavailable.FutureRecord")
                .setValue(ByteString.copyFrom(new byte[]{0, (byte) 255, 1})).build();
        var doc = Document.newBuilder().setDocId("doc").setOwnership(owner("account")).setStructuredData(opaque).build();
        var f = fixture(doc);
        var result = DocumentCommandContent.check(f.command, "member", f.bytes, false, LIMITS, () -> {});
        assertThat(result.assembly().document().getStructuredData()).isEqualTo(opaque);
        for (var fragment : result.assembly().fragments()) {
            assertThat(f.bytes.values()).contains(fragment.bytes());
        }
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", f.bytes, true, LIMITS, () -> {}))
                .isInstanceOf(UnsupportedOperationException.class);
        var explicit = f.command.intent().getMembers(0).toBuilder().setStructuredSchema(PublicationSchemaCondition.newBuilder()
                .setTypeName("unavailable.FutureRecord").setDescriptorFingerprint("a".repeat(64)));
        var typed = new DocumentPublicationCommand(f.command.intent().toBuilder().setMembers(0, explicit).build());
        assertThatThrownBy(() -> DocumentCommandContent.check(typed, "member", f.bytes, false, LIMITS, () -> {}))
                .isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void bindsCanonicalCommandAndDecodedOwnershipWithoutPublishing() throws Exception {
        var f = fixture("account");
        var result = DocumentCommandContent.check(f.command, "member", f.bytes, false, LIMITS, () -> {});
        assertThat(result.command()).isSameAs(f.command);
        assertThat(result.member()).isEqualTo(f.command.intent().getMembers(0));
        assertThat(result.assembly().document().getOwnership()).isEqualTo(owner("account"));
        assertThat(result.assembly().fragments().getFirst().bytes()).isSameAs(f.bytes.get(0));
    }
    @Test void mismatchedDecodedOwnershipCannotBeRepairedFromTheRequest() {
        var f = fixture("forged");
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", f.bytes, false, LIMITS, () -> {}))
                .hasMessageContaining("Decoded ownership");
    }
    @Test void wrongBytesAndIncompleteOrdinalsFail() {
        var f = fixture("account");
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", Map.of(0, f.bytes.get(0)), false, LIMITS, () -> {}))
                .hasMessageContaining("slots");
        var corrupt = new java.util.HashMap<>(f.bytes);
        var changed = f.bytes.get(0).toByteArray(); changed[changed.length - 1] ^= 1;
        corrupt.put(0, ByteString.copyFrom(changed));
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", corrupt, false, LIMITS, () -> {}))
                .hasMessageContaining("bytes differ");
    }
    @Test void missingSchemaRequestCannotBypassRequiredHostPolicy() {
        var f = fixture("account");
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", f.bytes, true, LIMITS, () -> {}))
                .isInstanceOf(UnsupportedOperationException.class);
        var member = f.command.intent().getMembers(0).toBuilder().setStructuredSchema(PublicationSchemaCondition.newBuilder()
                .setTypeName("example.Record").setDescriptorFingerprint("a".repeat(64))).build();
        var typed = new DocumentPublicationCommand(f.command.intent().toBuilder().setMembers(0, member).build());
        assertThatThrownBy(() -> DocumentCommandContent.check(typed, "member", f.bytes, false, LIMITS, () -> {}))
                .isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void aggregateBoundAndCancellationAreEnforcedBeforeContentWork() {
        var f = fixture("account");
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", f.bytes, false,
                new DocumentRevisionAssembly.Limits(1, 10, 10, 10, 100_000), () -> {})).hasMessageContaining("aggregate bound");
        var failure = new java.util.concurrent.CancellationException();
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", f.bytes, false, LIMITS, () -> { throw failure; })).isSameAs(failure);
    }
    @Test void retainedCoreAndEmptySlotUseCompleteRevisionOrdinals() throws Exception {
        var f = fixture("account");
        var original = f.command.intent().getMembers(0);
        var condition = original.getDestination().toBuilder().clearIfAbsent().setExpectedMutationRevision(1).build();
        var upload = original.getParts(0).getUpload();
        // Synthetic retained declaration: this content-only check grants no storage provenance.
        var core = original.getParts(0).toBuilder().clearUpload().setReuse(PublicationReuse.newBuilder()
                .setSource(condition).setSourceSlot(original.getParts(0).getSlot()).setObject(PublicationObjectIdentity.newBuilder()
                        .setObjectId(UUID.randomUUID().toString()).setBackendGeneration("original").setStorageRealm("realm")
                        .setNamespace("namespace").setObjectKey("original/core").setSizeBytes(upload.getSizeBytes())
                        .setSha256(upload.getSha256()).setContentType(upload.getContentType()))).build();
        var empty = DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)).setEmpty(true);
        var member = original.toBuilder().setDestination(condition).clearParts().addParts(core).addParts(empty).addParts(original.getParts(1)).build();
        var command = new DocumentPublicationCommand(f.command.intent().toBuilder().setMembers(0, member).build());
        var bytes = Map.of(0, f.bytes.get(0), 2, f.bytes.get(1));
        var result = DocumentCommandContent.check(command, "member", bytes, false, LIMITS, () -> {});
        assertThat(result.member().getParts(1).hasEmpty()).isTrue();
        assertThat(result.assembly().fragments()).hasSize(2);
        assertThatThrownBy(() -> DocumentCommandContent.check(command, "member", f.bytes, false, LIMITS, () -> {})).hasMessageContaining("slots");
        var changed = core.toBuilder().setReuse(core.getReuse().toBuilder().setObject(core.getReuse().getObject().toBuilder().setSha256("b".repeat(64)))).build();
        var wrong = new DocumentPublicationCommand(command.intent().toBuilder().setMembers(0, member.toBuilder().setParts(0, changed)).build());
        assertThatThrownBy(() -> DocumentCommandContent.check(wrong, "member", bytes, false, LIMITS, () -> {})).hasMessageContaining("bytes differ");
    }
    @Test void validatesTheSnapshotRatherThanAnEarlierMapView() {
        var f = fixture("account");
        // Deterministic mutation at snapshot traversal, equivalent to a concurrent
        // insertion after checking the earlier key view. No provider is simulated.
        var changing = new java.util.HashMap<Integer, ByteString>(f.bytes) {
            @Override public java.util.Set<Map.Entry<Integer, ByteString>> entrySet() {
                put(2, f.bytes.get(0));
                return super.entrySet();
            }
        };
        assertThatThrownBy(() -> DocumentCommandContent.check(f.command, "member", changing, false, LIMITS, () -> {}))
                .hasMessageContaining("slots");
    }
}
