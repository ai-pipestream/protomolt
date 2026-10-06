package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** SQL source projection uses synthetic provider observations, not a production assessment manifest. */
@Testcontainers
class DocumentAssessmentSlotsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void replaysVersionOneSnapshotAfterVersionTwoMigration() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES, "86")) {
            var f = DocumentNativePublicationFixture.prepare(c, 1);
            var staged = stage(c, f, f.command(), false, true, 1);
            String schema = c.tx().readOnly(em -> (String) em.createNativeQuery("SELECT current_schema()").getSingleResult());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("87").load().migrate();
            verifyAuthorized(c, f, staged, CALLER);
            assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT snapshot_version FROM document_assessment_slot_snapshots WHERE assessment_id=:id")
                    .setParameter("id", staged.identity().assessment()).getSingleResult()).intValue())).isEqualTo(1);
        }
    }

    @Test void retainsExactCurrentSourceRevisionsForReuseOnlyAndMixedCandidates() throws Exception {
        for (boolean mixed : new boolean[]{false, true}) {
            try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
                var f = DocumentNativePublicationFixture.prepare(c, 2, mixed, java.time.Duration.ofMinutes(5), true);
                var slots = stage(c, f, f.command(), false).slots();
                assertThat(slots).hasSize(4);
                assertThat(slots.stream().filter(s -> s.declaration().equals("NEW_CONTENT")).count()).isEqualTo(mixed ? 1 : 0);
                for (var slot : slots) {
                    assertThat(slot.selection()).isEqualTo(1);
                    var source = f.sources().get(Integer.parseInt(slot.member().substring("member-".length())));
                    if (slot.declaration().equals("NEW_CONTENT")) {
                        assertThat(slot.sourceRevision()).isNull(); assertThat(slot.sourceOrdinal()).isNull();
                    } else {
                        Object[] row = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                                SELECT p.revision_id,p.revision_ordinal,p.object_id FROM document_revision_current r
                                JOIN document_revision_parts p USING(revision_id)
                                WHERE r.node_id=:node AND p.part=:part AND p.sub_key=:sub
                                """).setParameter("node", source.row().nodeId)
                                .setParameter("part", source.slots().get(slot.ordinal()).getPartValue())
                                .setParameter("sub", source.slots().get(slot.ordinal()).getSubKey()).getSingleResult());
                        assertThat(slot.sourceRevision()).isEqualTo(row[0]);
                        assertThat(slot.sourceOrdinal()).isEqualTo(((Number) row[1]).intValue());
                        assertThat(slot.sourceOrdinal()).isNotEqualTo(slot.ordinal());
                        assertThat(slot.object()).isEqualTo(row[2]);
                    }
                }
            }
        }
    }

    @Test void unknownSourceObjectCannotBeSubstitutedByARealBoundPart() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 1);
            var changed = f.command().intent().toBuilder();
            changed.getMembersBuilder(0).getPartsBuilder(0).getReuseBuilder().getObjectBuilder().setObjectId(UUID.randomUUID().toString());
            var wrong = new DocumentPublicationCommand(changed.build());
            assertThatThrownBy(() -> stage(c, f, wrong, false))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class).hasMessageContaining("Assessment slots differ");
            assertNoOwner(c, f);
        }
    }

    @Test void incompletePhysicalBindingRollsBackTheOwner() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 1);
            assertThatThrownBy(() -> stage(c, f, f.command(), true))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class).hasMessageContaining("Assessment slots differ");
            assertNoOwner(c, f);
        }
    }

    @Test void retainedAssociationsSurviveSourcePointerAdvance() throws Exception {
        for (boolean mixed : new boolean[]{false, true}) try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 1, mixed, java.time.Duration.ofMinutes(5), true);
            var retained = stage(c, f, f.command(), false);
            UUID originalSource = retained.slots().stream().filter(s -> s.sourceRevision() != null).findFirst().orElseThrow().sourceRevision();
            var intent = f.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString());
            var member = intent.getMembersBuilder(0);
            // A separate operation publishes another revision of the same source.
            // Reuse its existing bytes, including the slot our candidate replaces.
            for (int i = 0; i < member.getPartsCount(); i++) if (member.getParts(i).hasUpload()) {
                var reuse = member.getParts(0).getReuse().toBuilder().setSourceSlot(f.sources().getFirst().slots().get(i))
                        .setObject(f.sources().getFirst().identities().get(i));
                member.setParts(i, member.getParts(i).toBuilder().setReuse(reuse));
            }
            var command = new DocumentPublicationCommand(intent.build());
            var owner = new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", command.operationId()),
                    command, UUID.randomUUID(), java.time.Duration.ofMinutes(5)).owner().orElseThrow();
            var placement = retained.plan().members().getFirst().placement();
            new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner,
                    DocumentOperationUploadAdmission.prepare(command, Map.of(placement.drive().id(), placement), Map.of(), java.time.Duration.ofMinutes(5)));
            var successor = new DocumentNativePublicationFixture.Prepared(command, owner, f.sources(), Map.of());
            var published = DocumentNativePublicationFixture.publish(c, successor, DocumentNativePublicationFixture.Fault.NONE, em -> {});
            assertThat(UUID.fromString(published.getMembers(0).getRevisionId())).isNotEqualTo(originalSource);
            var current = c.tx().readOnly(em -> (UUID) em.createNativeQuery("SELECT revision_id FROM document_revision_current WHERE node_id=:node")
                    .setParameter("node", f.sources().getFirst().row().nodeId).getSingleResult());
            assertThat(current.toString()).isEqualTo(published.getMembers(0).getRevisionId());
            c.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, f.owner());
                DocumentAdmissionAuthorization.authorizeRejection(em, CALLER, f.command());
                em.createNativeQuery("SELECT assessment_id FROM document_assessment_owners WHERE assessment_id=:id FOR UPDATE")
                        .setParameter("id", retained.identity().assessment()).getSingleResult();
                var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(20_000_000);
                DocumentAssessmentRetainedSlots.verify(em, retained.identity(), retained.plan().command(), DocumentAssessmentRetainedSlots.uploadSelections(retained.selected()), budget, () -> {});
                assertThat(budget.reservedBytes()).isZero();
            });
        }
    }

    private record Staged(List<DocumentAssessmentSlots.Slot> slots, DocumentAssessmentSlotSnapshot.Identity identity,
                          DocumentUploadPlan.Prepared plan, Map<String,DocumentSelectedAttemptLedger.Selected> selected) {}

    @Test void smallRetainedSnapshotUsesItsActualByteBudget() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 1, true);
            var retained = stage(c, f, f.command(), false);
            verifyAuthorized(c, f, retained, CALLER, 20_000);
        }
    }

    @Test void corruptedSnapshotLengthsAndBytesFailWithoutMaxSizeReservation() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 1, true);
            var retained = stage(c, f, f.command(), false);
            int originalSize = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT octet_length(snapshot_bytes) FROM document_assessment_slot_snapshots WHERE assessment_id=:id")
                    .setParameter("id", retained.identity().assessment()).getSingleResult()).intValue());
            for (int size : new int[] {1, 200_000, originalSize}) {
                // Deliberate privileged corruption; normal SQL updates are forbidden.
                c.tx().inTransaction(em -> {
                    em.createNativeQuery("ALTER TABLE document_assessment_slot_snapshots DISABLE TRIGGER document_assessment_slot_snapshot_guard").executeUpdate();
                    em.createNativeQuery("""
                            UPDATE document_assessment_slot_snapshots
                            SET snapshot_bytes=decode(repeat('ab',:size),'hex'),
                                snapshot_sha256=sha256(decode(repeat('ab',:size),'hex'))
                            WHERE assessment_id=:id
                            """).setParameter("size", size).setParameter("id", retained.identity().assessment()).executeUpdate();
                    em.createNativeQuery("ALTER TABLE document_assessment_slot_snapshots ENABLE TRIGGER document_assessment_slot_snapshot_guard").executeUpdate();
                });
                assertThatThrownBy(() -> verifyAuthorized(c, f, retained, CALLER, 20_000))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("original staging identity");
            }
        }
    }

    @Test void revokedReadAccessPreventsRetainedAssociationDisclosure() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 2);
            var retained = stage(c, f, f.command(), false);
            var caller = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb)")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .executeUpdate(); });
            verifyAuthorized(c, f, retained, caller);
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_DENY\"}]}")
                    .setParameter("node", f.sources().get(1).row().nodeId).executeUpdate(); });
            assertThatThrownBy(() -> verifyAuthorized(c, f, retained, caller))
                    .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.NOT_FOUND));
            long snapshots = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_slot_snapshots WHERE assessment_id=:id")
                    .setParameter("id", retained.identity().assessment()).getSingleResult()).longValue());
            assertThat(snapshots).isEqualTo(1);
        }
    }

    @Test void stageWithoutSnapshotCannotBeAdoptedAsOriginalProvenance() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 1);
            var retained = stage(c, f, f.command(), false, false);
            assertThatThrownBy(() -> verifyAuthorized(c, f, retained, CALLER))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("original staging identity");
            long objects = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_objects WHERE assessment_id=:id")
                    .setParameter("id", retained.identity().assessment()).getSingleResult()).longValue());
            assertThat(objects).isEqualTo(2);
        }
    }

    @Test void originalPlacementSurvivesDriveConfigurationChanges() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentNativePublicationFixture.prepare(c, 1, true);
            var retained = stage(c, f, f.command(), false);
            UUID drive = UUID.fromString(f.command().intent().getMembers(0).getDriveId());
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE drives SET bucket='different-namespace',prefix='new-prefix' WHERE drive_id=:id")
                    .setParameter("id", drive).executeUpdate(); });
            var changed = new DriveLedger(c.tx()).findById(drive).orElseThrow();
            assertThat(changed.bucket).isEqualTo("different-namespace");
            assertThat(changed.prefix).isEqualTo("new-prefix");
            verifyAuthorized(c, f, retained, CALLER);
            // These frozen records, not current drive configuration or lease tokens,
            // are all the retained verifier receives for upload selection.
            var selections = DocumentAssessmentRetainedSlots.uploadSelections(retained.selected());
            assertThat(selections).hasSize(1);
            assertThat(selections.get("member-0").attempt()).isEqualTo(f.uploads().get(0).attempt());
        }
    }

    private static void verifyAuthorized(DocumentNativePublicationFixture.Context c, DocumentNativePublicationFixture.Prepared f,
            Staged retained, RepositoryCaller caller) {
        verifyAuthorized(c, f, retained, caller, 20_000_000);
    }

    private static void verifyAuthorized(DocumentNativePublicationFixture.Context c, DocumentNativePublicationFixture.Prepared f,
            Staged retained, RepositoryCaller caller, long capacity) {
        c.tx().inTransaction(em -> {
            DocumentAdmissionAuthorization.requireCaller(caller, f.owner(), "account");
            RepositoryOperationLedger.fenceLiveOwner(em, f.owner());
            DocumentAdmissionAuthorization.authorizeRejection(em, caller, f.command());
            em.createNativeQuery("SELECT assessment_id FROM document_assessment_owners WHERE assessment_id=:id FOR UPDATE")
                    .setParameter("id", retained.identity().assessment()).getSingleResult();
            var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(capacity);
            try { DocumentAssessmentRetainedSlots.verify(em, retained.identity(), retained.plan().command(), DocumentAssessmentRetainedSlots.uploadSelections(retained.selected()), budget, () -> {}); }
            finally { assertThat(budget.reservedBytes()).isZero(); }
        });
    }

    private static Staged stage(DocumentNativePublicationFixture.Context c,
            DocumentNativePublicationFixture.Prepared f, DocumentPublicationCommand projected, boolean omitPhysical) {
        return stage(c, f, projected, omitPhysical, true);
    }

    private static Staged stage(DocumentNativePublicationFixture.Context c,
            DocumentNativePublicationFixture.Prepared f, DocumentPublicationCommand projected, boolean omitPhysical, boolean snapshotPresent) {
        return stage(c, f, projected, omitPhysical, snapshotPresent, DocumentAssessmentSlotSnapshot.VERSION);
    }

    private static Staged stage(DocumentNativePublicationFixture.Context c,
            DocumentNativePublicationFixture.Prepared f, DocumentPublicationCommand projected, boolean omitPhysical,
            boolean snapshotPresent, int snapshotVersion) {
        var driveId = UUID.fromString(f.command().intent().getMembers(0).getDriveId());
        var drive = new DriveLedger(c.tx()).findById(driveId).orElseThrow();
        var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
        var attempts = new HashMap<String,UUID>();
        var selected = new HashMap<String,DocumentSelectedAttemptLedger.Selected>();
        f.uploads().forEach((member, part) -> {
            var attempt = c.tx().readOnly(em -> DocumentPartAttemptLedger.read(em, part.attempt(), false).orElseThrow());
            attempts.put("member-" + member, attempt.id());
            selected.put("member-" + member, new DocumentSelectedAttemptLedger.Selected("member-" + member, 1, attempt.id(), attempt.token()));
        });
        var plan = DocumentUploadPlan.prepare(f.command(), Map.of(driveId,
                DocumentUploadPlan.Placement.sample(drive, "native-test", profile)), attempts);
        var authorization = DocumentAdmissionAuthorization.prepare(plan);
        var reuse = DocumentReuseAdmission.prepare(plan);
        var prepared = DocumentAssessmentSlots.prepare(projected, () -> {});
        return c.tx().inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, f.owner());
            RepositoryOperationLedger.requireCommand(em, f.owner().key(), f.command());
            DocumentSchemaPolicies.lockUnboundWriter(em, "account");
            DocumentAdmissionAuthorization.lockAndAuthorize(em, CALLER, plan, authorization);
            plan.members().getFirst().placement().drive().lock(em, new DriveLedger(c.tx()));
            UUID assessment = UUID.randomUUID();
            em.createNativeQuery("""
                    INSERT INTO document_assessment_owners(assessment_id,account_id,principal,operation_id,owner_generation,
                        command_codec,command_version,command_sha256,manifest_bytes,manifest_sha256,expected_slots,retain_until,creation_xid)
                    SELECT :id,account_id,principal,operation_id,:generation,command_codec,command_version,command_sha256,
                        decode('01','hex'),sha256(decode('01','hex')),:count,clock_timestamp()+interval '1 minute','0'::xid8
                    FROM repository_operations WHERE account_id='account' AND principal='principal' AND operation_id=:op
                    """).setParameter("id", assessment).setParameter("generation", f.owner().generation())
                    .setParameter("count", f.command().intent().getMembersList().stream().mapToInt(m -> m.getPartsCount()).sum())
                    .setParameter("op", f.command().operationId()).executeUpdate();
            var bound = DocumentCommitParts.bindAssessment(em, f.owner(), plan, selected, reuse, () -> {});
            if (omitPhysical) {
                var incomplete = new HashMap<>(bound.parts()); incomplete.remove(incomplete.keySet().iterator().next());
                bound = new DocumentCommitParts.Bound(incomplete, bound.selections());
            }
            var slots = DocumentAssessmentSlots.bind(em, prepared, bound, () -> {});
            for (var slot : slots) {
                em.createNativeQuery("""
                        INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,selection_revision,
                            object_id,declaration,source_revision,source_ordinal)
                        VALUES(:id,:member,:ordinal,:selection,:object,:declaration,CAST(:source AS uuid),CAST(:sourceOrdinal AS integer))
                        """).setParameter("id", assessment).setParameter("member", slot.member()).setParameter("ordinal", slot.ordinal())
                        .setParameter("selection", slot.selection()).setParameter("object", slot.object()).setParameter("declaration", slot.declaration())
                        .setParameter("source", slot.sourceRevision()).setParameter("sourceOrdinal", slot.sourceOrdinal()).executeUpdate();
            }
            em.createNativeQuery("UPDATE document_assessment_owners SET sealed=true WHERE assessment_id=:id")
                    .setParameter("id", assessment).executeUpdate();
            Object[] header = (Object[]) em.createNativeQuery("""
                    SELECT encode(manifest_sha256,'hex'),CAST(EXTRACT(EPOCH FROM retain_until)*1000000 AS bigint)
                    FROM document_assessment_owners WHERE assessment_id=:id
                    """).setParameter("id", assessment).getSingleResult();
            long micros = ((Number) header[1]).longValue();
            var identity = new DocumentAssessmentSlotSnapshot.Identity(assessment, f.owner().key(), f.owner().generation(),
                    f.command().sha256(), (String) header[0], java.time.Instant.ofEpochSecond(micros / 1000000, micros % 1000000 * 1000));
            var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(20_000_000);
            if (snapshotPresent) try (var snapshot = DocumentAssessmentSlotSnapshot.encode(snapshotVersion, identity, slots, budget, () -> {})) {
                em.createNativeQuery("""
                        INSERT INTO document_assessment_slot_snapshots VALUES(:id,:codec,:version,:bytes,decode(:sha,'hex'))
                        """).setParameter("id", assessment).setParameter("codec", DocumentAssessmentSlotSnapshot.CODEC)
                        .setParameter("version", snapshotVersion).setParameter("bytes", snapshot.bytes().toByteArray())
                        .setParameter("sha", snapshot.sha256()).executeUpdate();
            }
            // Synthetic manifest, genuine frozen selection/physical history: this
            // verifies retained association identity, not observed admission.
            if (snapshotPresent) DocumentAssessmentRetainedSlots.verify(em, identity, plan.command(), DocumentAssessmentRetainedSlots.uploadSelections(selected), budget, () -> {});
            assertThat(budget.reservedBytes()).isZero();
            return new Staged(slots, identity, plan, Map.copyOf(selected));
        });
    }
    private static void assertNoOwner(DocumentNativePublicationFixture.Context c, DocumentNativePublicationFixture.Prepared f) {
        long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_assessment_owners WHERE operation_id=:op")
                .setParameter("op", f.command().operationId()).getSingleResult()).longValue());
        assertThat(count).isZero();
    }
}
