package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.*;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Payload admission only: synthetic bytes do not claim protobuf semantic validity. */
class DocumentUploadPayloadsTest {
    private static final UUID DRIVE = UUID.randomUUID();
    private static final byte[] BYTES = {1, 2, 3};
    private static final String SHA = DocumentPartCodec.sha256Hex(BYTES);
    private static final DocumentUploadPayloads.Key KEY = new DocumentUploadPayloads.Key("first", 2);

    @Test void recoverySnapshotRejectsCorruptionAndOwnsStableBytesWithinBudget() {
        var original=plan().command();
        var command=new DocumentPublicationCommand(original.intent().toBuilder().clearMembers()
                .addMembers(original.intent().getMembers(0)).build());
        var budget=new PayloadBudget(3);
        var input=payload();
        try (var snapshot=DocumentRecoveryPayloads.prepare(command,Map.of(KEY,input),budget,
                ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
            assertThat(budget.reservedBytes()).isEqualTo(3);
            input.bytes()[0]=99;
            assertThat(snapshot.bodies().get(KEY).bytes()).containsExactly(BYTES);
        }
        assertThat(budget.reservedBytes()).isZero();
        assertThatThrownBy(() -> DocumentRecoveryPayloads.prepare(command,Map.of(KEY,input),budget,
                ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)).hasMessageContaining("checksum");
        assertThat(budget.reservedBytes()).isZero();
        assertThatThrownBy(() -> DocumentRecoveryPayloads.prepare(command,Map.of(KEY,payload()),new PayloadBudget(2),
                ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)).isInstanceOf(PayloadBudget.CapacityExceededException.class);
    }

    @Test void sparseUploadKeepsRevisionOrdinalAttemptAndPrivateBytes() {
        var plan = plan();
        var input = payload();
        var budget = new PayloadBudget(12);
        try (var prepared = DocumentUploadPayloads.prepare(plan, Set.of("first"), Map.of(KEY, input), budget)) {
            assertThat(budget.reservedBytes()).isEqualTo(6);
            input.bytes()[0] = 99;
            var replacement = DocumentUploadPlan.prepare(plan.command(),
                    Map.of(DRIVE, plan.members().getFirst().placement()),
                    Map.of("first", UUID.randomUUID(), "second", UUID.randomUUID()));
            assertThatThrownBy(() -> prepared.claim(replacement, Set.of("first"))).hasMessageContaining("another prepared plan");
            assertThatThrownBy(() -> prepared.claim(plan, Set.of("second"))).hasMessageContaining("another member subset");
            try (var use = prepared.claim(plan, Set.of("first"))) {
                var entries = use.entries();
                assertThat(entries).hasSize(1);
                var entry = entries.getFirst();
                assertThat(entry.body()).containsExactly(BYTES);
                assertThat(entry.key().revisionOrdinal()).isEqualTo(2);
                assertThat(entry.upload()).isEqualTo(plan.members().getFirst().attempt().orElseThrow().uploads().getFirst());
                assertThat(entry.attempt()).isEqualTo(plan.members().getFirst().attempt().orElseThrow().id());
                assertThatThrownBy(() -> entries.clear()).isInstanceOf(UnsupportedOperationException.class);
                assertThatThrownBy(() -> prepared.claim(plan, Set.of("first"))).hasMessageContaining("already claimed");
                assertThatThrownBy(prepared::close).hasMessageContaining("Active transfer");
                assertThat(budget.reservedBytes()).isEqualTo(6);
            }
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void exactKeysRejectMissingExtraCompactOrdinalAndUnselectedMember() {
        var plan = plan();
        var budget = new PayloadBudget(12);
        for (var key : new DocumentUploadPayloads.Key[] {
                new DocumentUploadPayloads.Key("first", 0), // retained CORE, also wrong compact upload ordinal
                new DocumentUploadPayloads.Key("first", 1), // explicit empty slot
                new DocumentUploadPayloads.Key("second", 2),
                new DocumentUploadPayloads.Key("unknown", 2)}) {
            assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("first"), Map.of(key, payload()), budget))
                    .hasMessageContaining("Payload keys");
            assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("first"), Map.of(KEY, payload(), key, payload()), budget))
                    .hasMessageContaining("Payload keys");
        }
        assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("first"), Map.of(), budget)).hasMessageContaining("Payload keys");
        assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("unknown"), Map.of(), budget)).hasMessageContaining("Payload keys");
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void rejectsSlotSizeAndDeclaredOrActualChecksumMismatchWithoutLeakingBudget() {
        var plan = plan();
        var budget = new PayloadBudget(12);
        for (var invalid : new PartObject[] {
                new PartObject(DocumentPart.DOCUMENT_PART_CORE, "changed", BYTES, SHA),
                new PartObject(DocumentPart.DOCUMENT_PART_CHUNKS, "other", BYTES, SHA),
                new PartObject(DocumentPart.DOCUMENT_PART_CHUNKS, "changed", new byte[2], SHA),
                new PartObject(DocumentPart.DOCUMENT_PART_CHUNKS, "changed", BYTES, "f".repeat(64)),
                new PartObject(DocumentPart.DOCUMENT_PART_CHUNKS, "changed", new byte[3], SHA)}) {
            assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("first"), Map.of(KEY, invalid), budget))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void sharedReservationBoundsMultipleMembersAndOverlappingBundles() {
        var plan = plan();
        var budget = new PayloadBudget(6);
        var both = Map.of(KEY, payload(), new DocumentUploadPayloads.Key("second", 2), payload());
        assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("first", "second"), both, budget))
                .isInstanceOf(PayloadBudget.CapacityExceededException.class);
        assertThat(budget.reservedBytes()).isZero();
        var first = DocumentUploadPayloads.prepare(plan, Set.of("first"), Map.of(KEY, payload()), budget);
        try {
            assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("second"),
                    Map.of(new DocumentUploadPayloads.Key("second", 2), payload()), budget))
                    .isInstanceOf(PayloadBudget.CapacityExceededException.class);
            assertThat(budget.reservedBytes()).isEqualTo(6);
        } finally { first.close(); }
        first.close();
        assertThatThrownBy(() -> first.claim(plan, Set.of("first"))).hasMessageContaining("closed");
        assertThat(budget.reservedBytes()).isZero();
        try (var second = DocumentUploadPayloads.prepare(plan, Set.of("second"),
                Map.of(new DocumentUploadPayloads.Key("second", 2), payload()), budget)) {
            try (var use = second.claim(plan, Set.of("second"))) { assertThat(use.entries()).hasSize(1); }
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void reuseOnlyMemberNeedsNoBodyAndNoByteReservation() {
        var plan = plan();
        var budget = new PayloadBudget(1);
        try (var prepared = DocumentUploadPayloads.prepare(plan, Set.of("retained"), Map.of(), budget)) {
            try (var use = prepared.claim(plan, Set.of("retained"))) { assertThat(use.entries()).isEmpty(); }
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void admissionPreparationOwnsPayloadHandoffWithoutExposingItsPlan() {
        var plan = plan();
        var placements = Map.of(DRIVE, plan.members().getFirst().placement());
        var ids = Map.of("first", UUID.randomUUID(), "second", UUID.randomUUID());
        var admission = DocumentOperationUploadAdmission.prepare(plan.command(), placements, ids, java.time.Duration.ofMinutes(1));
        var other = DocumentOperationUploadAdmission.prepare(plan.command(), placements, ids, java.time.Duration.ofMinutes(1));
        var budget = new PayloadBudget(6);
        try (var payloads = admission.preparePayloads(Set.of("first"), Map.of(KEY, payload()), budget)) {
            assertThatThrownBy(() -> other.claimPayloads(payloads, Set.of("first"))).hasMessageContaining("another prepared plan");
            try (var use = admission.claimPayloads(payloads, Set.of("first"))) {
                assertThat(use.entries()).singleElement().satisfies(entry -> {
                    assertThat(entry.attempt()).isEqualTo(ids.get("first"));
                    assertThat(entry.body()).containsExactly(BYTES);
                });
            }
        }
        assertThat(budget.reservedBytes()).isZero();
    }

    @Test void laterChecksumFailureReleasesEntireReservationAndCapacityIsCheckedFirst() {
        var plan = plan();
        var budget = new PayloadBudget(12);
        var corrupt = new PartObject(DocumentPart.DOCUMENT_PART_CHUNKS, "changed", new byte[3], SHA);
        var supplied = Map.of(KEY, payload(), new DocumentUploadPayloads.Key("second", 2), corrupt);
        assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("first", "second"), supplied, budget))
                .hasMessageContaining("Private payload checksum");
        assertThat(budget.reservedBytes()).isZero();
        try (var held = budget.reserve(1)) {
            assertThatThrownBy(() -> DocumentUploadPayloads.prepare(plan, Set.of("first", "second"), supplied, budget))
                    .isInstanceOf(PayloadBudget.CapacityExceededException.class);
            assertThat(budget.reservedBytes()).isEqualTo(1);
        }
    }

    private static PartObject payload() {
        return new PartObject(DocumentPart.DOCUMENT_PART_CHUNKS, "changed", BYTES.clone(), SHA);
    }

    private static DocumentUploadPlan.Prepared plan() {
        var first = member("first");
        var second = member("second");
        var retained = member("retained").toBuilder().removeParts(2).build();
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                .setAccountId("account").setOperationId(UUID.randomUUID().toString())
                .addMembers(first).addMembers(second).addMembers(retained).build());
        var drive = new DriveRecord();
        drive.driveId = DRIVE; drive.accountId = "account"; drive.name = "payload-fixture";
        drive.status = "ACTIVE"; drive.provider = "test-provider"; drive.bucket = "namespace"; drive.prefix = "";
        var profile = new ManagedBackendLedger.Profile(new BackendIdentity("test-provider", "test-provider/v1",
                Map.of("endpoint", "synthetic-payload-fixture")), "realm");
        return DocumentUploadPlan.prepare(command,
                Map.of(DRIVE, DocumentUploadPlan.Placement.sample(drive, "generation", profile)),
                Map.of("first", UUID.randomUUID(), "second", UUID.randomUUID()));
    }

    private static DocumentPublicationMember member(String id) {
        var core = DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var reuse = DocumentPublicationPart.newBuilder().setSlot(core).setReuse(PublicationReuse.newBuilder()
                .setSource(DocumentRevisionCondition.newBuilder().setAddress(address("source")).setExpectedMutationRevision(1))
                .setSourceSlot(core).setObject(PublicationObjectIdentity.newBuilder().setObjectId(DRIVE.toString())
                        .setBackendGeneration("generation").setStorageRealm("realm").setNamespace("namespace")
                        .setObjectKey("retained-core").setSizeBytes(3).setSha256(SHA).setContentType("application/protobuf")));
        return DocumentPublicationMember.newBuilder().setMemberId(id).setDriveId(DRIVE.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address(id)).setIfAbsent(true))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE).addParts(reuse)
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_BLOBS)).setEmpty(true))
                .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CHUNKS).setSubKey("changed"))
                        .setUpload(PublicationUpload.newBuilder().setSizeBytes(3).setSha256(SHA).setContentType("application/protobuf")))
                .build();
    }

    private static NodeAddress address(String id) {
        return NodeAddress.newBuilder().setAccountId("account").setDocId(id).setGraphId("graph").setGraphAddressId("node").build();
    }
}
