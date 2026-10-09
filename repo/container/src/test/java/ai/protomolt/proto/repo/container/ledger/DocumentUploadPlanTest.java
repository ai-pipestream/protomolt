package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.*;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Pure intent mapping only; no fixture represents a verified upload or authorized source. */
class DocumentUploadPlanTest {
    private static final UUID DRIVE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID ATTEMPT = UUID.fromString("10000000-0000-4000-8000-000000000002");
    private static final String SHA = "a".repeat(64);

    @Test void uploadsOnlyChangedSlotsWhileRetainingCompleteRevisionAndSources() {
        var member = member().toBuilder().setParts(0, reuseCore())
                .addParts(DocumentPublicationPart.newBuilder().setSlot(slot(DocumentPart.DOCUMENT_PART_BLOBS, "")).setEmpty(true))
                .addParts(upload(DocumentPart.DOCUMENT_PART_CHUNKS, "changed"))
                .addParts(upload(DocumentPart.DOCUMENT_PART_CHUNKS, "also-changed"));
        var command = command(member.build());
        var plan = DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("prefix/")), Map.of("member", ATTEMPT));
        assertThat(plan.command()).isSameAs(command);
        var selected = plan.members().getFirst();
        assertThat(selected.intent()).isEqualTo(member.build());
        assertThat(selected.sources()).hasSize(1).containsValue(7L);
        var attempt = selected.attempt().orElseThrow();
        assertThat(attempt.id()).isEqualTo(ATTEMPT);
        assertThat(attempt.uploads()).extracting(DocumentUploadPlan.Upload::revisionOrdinal).containsExactly(2, 3);
        assertThat(attempt.uploads()).extracting(u -> u.object().subKey()).containsExactly("changed", "also-changed");
        assertThat(attempt.uploads()).allSatisfy(u -> {
            assertThat(u.object().part()).isEqualTo(DocumentPart.DOCUMENT_PART_CHUNKS);
            assertThat(u.object().size()).isEqualTo(9007199254740993L);
            assertThat(u.object().sha256()).isEqualTo(SHA);
            assertThat(u.object().contentType()).isEqualTo("application/protobuf");
            assertThat(u.object().objectKey()).isEqualTo("prefix/documents/account/" + selected.nodeId()
                    + "/attempts/" + ATTEMPT + "/part-" + u.revisionOrdinal());
        });
    }

    @Test void zeroUploadRevisionHasNoAttemptAndRejectsDummyAttempt() {
        var command = command(member().toBuilder().setParts(0, reuseCore()).build());
        var prepared = DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("")), Map.of());
        assertThat(prepared.members().getFirst().attempt()).isEmpty();
        assertThat(prepared.members().getFirst().intent().getParts(0).getReuse()).isEqualTo(reuseCore().getReuse());
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("")), Map.of("member", ATTEMPT)))
                .hasMessageContaining("non-uploading member");
    }

    @Test void generatedAttemptsAndPlacementDoNotChangeSemanticIdentity() {
        var command = command(member());
        var first = DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("first")), Map.of("member", ATTEMPT));
        var second = DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("second")), Map.of("member", UUID.randomUUID()));
        assertThat(first.command().canonical()).isEqualTo(second.command().canonical());
        assertThat(first.members().getFirst().attempt().orElseThrow().uploads().getFirst().object().objectKey())
                .isNotEqualTo(second.members().getFirst().attempt().orElseThrow().uploads().getFirst().object().objectKey());
    }

    @Test void zeroUploadStillRequiresValidPlacementCoordinates() {
        var command = command(member().toBuilder().setParts(0, reuseCore()).build());
        var invalidGeneration = DocumentUploadPlan.Placement.sample(drive(""), "", profile());
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(DRIVE, invalidGeneration), Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("backendGeneration");
        var drive = drive(""); drive.bucket = "";
        var invalidNamespace = DocumentUploadPlan.Placement.sample(drive, "generation", profile());
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(DRIVE, invalidNamespace), Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("namespace");
    }

    @Test void exactDriveAndAttemptSelectionsAreMandatory() {
        var command = command(member());
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(), Map.of("member", ATTEMPT)))
                .hasMessageContaining("Selected drive");
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("")), Map.of()))
                .hasMessageContaining("distinct attempt");
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement(""), UUID.randomUUID(), placement("")), Map.of("member", ATTEMPT)))
                .hasMessageContaining("Extraneous drive");
        var second = member().toBuilder().setMemberId("second").setDestination(member().getDestination().toBuilder()
                .setAddress(address("second"))).build();
        var two = new DocumentPublicationCommand(command.intent().toBuilder().addMembers(second).build());
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(two, Map.of(DRIVE, placement("")), Map.of("member", ATTEMPT, "second", ATTEMPT)))
                .hasMessageContaining("distinct attempt");
        var drive = drive(""); drive.accountId = "other";
        var wrong = DocumentUploadPlan.Placement.sample(drive, "generation", profile());
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(DRIVE, wrong), Map.of("member", ATTEMPT)))
                .hasMessageContaining("command scope");
    }

    @Test void rejectsInactiveWrongProviderAndReservedOrOversizedGeneratedKeys() {
        var drive = drive(""); drive.status = "DISABLED";
        assertThatThrownBy(() -> DocumentUploadPlan.Placement.sample(drive, "generation", profile()))
                .hasMessageContaining("inactive");
        drive.status = "ACTIVE"; drive.provider = "unselected";
        assertThatThrownBy(() -> DocumentUploadPlan.Placement.sample(drive, "generation", profile()))
                .hasMessageContaining("provider");
        for (String prefix : new String[]{"archive", ".protomolt-managed", "x".repeat(2048)}) {
            assertThatThrownBy(() -> DocumentUploadPlan.prepare(command(member()), Map.of(DRIVE, placement(prefix)), Map.of("member", ATTEMPT)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void refusesAmbiguousLegacyAddressEncodingRatherThanSilentlyRekeying() {
        var one = address("a|b").toBuilder().setGraphAddressId("c").build();
        var two = address("a").toBuilder().setGraphAddressId("b|c").build();
        assertThat(ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(one))
                .isEqualTo(ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(two));
        for (var ambiguous : new NodeAddress[]{one, two}) {
            var command = command(member().toBuilder().setDestination(member().getDestination().toBuilder().setAddress(ambiguous)).build());
            assertThatThrownBy(() -> DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("")), Map.of("member", ATTEMPT)))
                    .hasMessageContaining("explicit identity migration");
        }
        var source = reuseCore().toBuilder().setReuse(reuseCore().getReuse().toBuilder().setSource(
                reuseCore().getReuse().getSource().toBuilder().setAddress(one)));
        assertThatThrownBy(() -> DocumentUploadPlan.prepare(command(member().toBuilder().setParts(0, source).build()),
                Map.of(DRIVE, placement("")), Map.of())).hasMessageContaining("explicit identity migration");
    }

    @Test void returnedCollectionsAndDriveSnapshotDoNotFollowCallerMutation() {
        var drive = drive("original");
        var placement = DocumentUploadPlan.Placement.sample(drive, "generation", profile());
        drive.prefix = "changed";
        var plan = DocumentUploadPlan.prepare(command(member()), Map.of(DRIVE, placement), Map.of("member", ATTEMPT));
        assertThat(plan.members().getFirst().attempt().orElseThrow().uploads().getFirst().object().objectKey()).startsWith("original/");
        assertThatThrownBy(() -> plan.members().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> plan.members().getFirst().sources().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> plan.members().getFirst().attempt().orElseThrow().uploads().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void thousandsOfNewPartsKeepDistinctKeysAndExactRevisionPositions() {
        var member = member().toBuilder().setParts(0, reuseCore());
        for (int i = 1; i <= 4096; i++) {
            var part = upload(DocumentPart.DOCUMENT_PART_CHUNKS, "chunk-" + i);
            member.addParts(part.toBuilder().setUpload(part.getUpload().toBuilder().setSizeBytes(1)));
        }
        var prepared = DocumentUploadPlan.prepare(command(member.build()), Map.of(DRIVE, placement("")),
                Map.of("member", ATTEMPT));
        var uploads = prepared.members().getFirst().attempt().orElseThrow().uploads();
        assertThat(uploads).hasSize(4096);
        assertThat(uploads.stream().map(u -> u.object().objectKey()).distinct().count()).isEqualTo(4096);
        for (int i = 0; i < uploads.size(); i++) {
            assertThat(uploads.get(i).revisionOrdinal()).isEqualTo(i + 1);
            assertThat(uploads.get(i).object().subKey()).isEqualTo("chunk-" + (i + 1));
        }
        assertThat(prepared.members().getFirst().intent().getPartsCount()).isEqualTo(4097);
    }

    @Test void sharedDriveAndSourceDoNotMixMemberUploadScopes() {
        var first = member().toBuilder().setParts(0, reuseCore())
                .addParts(upload(DocumentPart.DOCUMENT_PART_CHUNKS, "changed")).build();
        var second = first.toBuilder().setMemberId("second").removeParts(1)
                .setDestination(first.getDestination().toBuilder().setAddress(address("second"))).build();
        var command = new DocumentPublicationCommand(command(first).intent().toBuilder().addMembers(second).build());
        var plan = DocumentUploadPlan.prepare(command, Map.of(DRIVE, placement("")), Map.of("member", ATTEMPT));
        assertThat(plan.members()).hasSize(2);
        assertThat(plan.members().get(0).nodeId()).isNotEqualTo(plan.members().get(1).nodeId());
        assertThat(plan.members().get(0).sources()).isEqualTo(plan.members().get(1).sources()).hasSize(1);
        assertThat(plan.members().get(0).attempt()).isPresent();
        assertThat(plan.members().get(1).attempt()).isEmpty();
    }

    private static DocumentPublicationCommand command(DocumentPublicationMember member) {
        return new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
    }
    private static NodeAddress address(String id) {
        return NodeAddress.newBuilder().setDocId(id).setGraphAddressId("node").setGraphId("graph").setAccountId("account").build();
    }
    private static DocumentPublicationSlot slot(DocumentPart part, String sub) {
        return DocumentPublicationSlot.newBuilder().setPart(part).setSubKey(sub).build();
    }
    private static DocumentPublicationPart upload(DocumentPart part, String sub) {
        return DocumentPublicationPart.newBuilder().setSlot(slot(part, sub)).setUpload(PublicationUpload.newBuilder()
                .setSizeBytes(9007199254740993L).setSha256(SHA).setContentType("application/protobuf")).build();
    }
    private static DocumentPublicationPart reuseCore() {
        return DocumentPublicationPart.newBuilder().setSlot(slot(DocumentPart.DOCUMENT_PART_CORE, ""))
                .setReuse(PublicationReuse.newBuilder().setSource(DocumentRevisionCondition.newBuilder().setAddress(address("source"))
                                .setExpectedMutationRevision(7)).setSourceSlot(slot(DocumentPart.DOCUMENT_PART_CORE, ""))
                        .setObject(PublicationObjectIdentity.newBuilder().setObjectId(DRIVE.toString()).setBackendGeneration("original")
                                .setStorageRealm("original-realm").setNamespace("original-ns").setObjectKey("original-key")
                                .setSizeBytes(7).setSha256(SHA).setContentType("application/protobuf"))).build();
    }
    private static DocumentPublicationMember member() {
        return DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(DRIVE.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address("destination")).setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .addParts(upload(DocumentPart.DOCUMENT_PART_CORE, "")).build();
    }
    private static DriveRecord drive(String prefix) {
        var drive = new DriveRecord(); drive.driveId = DRIVE; drive.accountId = "account"; drive.name = "logical-name";
        drive.status = "ACTIVE"; drive.provider = "test-provider"; drive.bucket = "selected-namespace"; drive.prefix = prefix;
        return drive;
    }
    private static ManagedBackendLedger.Profile profile() {
        return new ManagedBackendLedger.Profile(new BackendIdentity("test-provider", "test-provider/v1",
                Map.of("endpoint", "synthetic-plan-fixture")), "selected-realm");
    }
    private static DocumentUploadPlan.Placement placement(String prefix) {
        return DocumentUploadPlan.Placement.sample(drive(prefix), "generation", profile());
    }
}
