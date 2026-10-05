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

    @Test void retainsExactCurrentSourceRevisionsForReuseOnlyAndMixedCandidates() throws Exception {
        for (boolean mixed : new boolean[]{false, true}) {
            try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
                var f = DocumentNativePublicationFixture.prepare(c, 2, mixed, java.time.Duration.ofMinutes(5), true);
                var slots = stage(c, f, f.command(), false);
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

    private static List<DocumentAssessmentSlots.Slot> stage(DocumentNativePublicationFixture.Context c,
            DocumentNativePublicationFixture.Prepared f, DocumentPublicationCommand projected, boolean omitPhysical) {
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
            try (var snapshot = DocumentAssessmentSlotSnapshot.encode(identity, slots, budget, () -> {})) {
                em.createNativeQuery("""
                        INSERT INTO document_assessment_slot_snapshots VALUES(:id,:codec,:version,:bytes,decode(:sha,'hex'))
                        """).setParameter("id", assessment).setParameter("codec", DocumentAssessmentSlotSnapshot.CODEC)
                        .setParameter("version", DocumentAssessmentSlotSnapshot.VERSION).setParameter("bytes", snapshot.bytes().toByteArray())
                        .setParameter("sha", snapshot.sha256()).executeUpdate();
            }
            // Synthetic manifest, genuine frozen selection/physical history: this
            // verifies retained association identity, not observed admission.
            DocumentAssessmentRetainedSlots.verify(em, identity, plan, selected, budget, () -> {});
            assertThat(budget.reservedBytes()).isZero();
            return slots;
        });
    }
    private static void assertNoOwner(DocumentNativePublicationFixture.Context c, DocumentNativePublicationFixture.Prepared f) {
        long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_assessment_owners WHERE operation_id=:op")
                .setParameter("op", f.command().operationId()).getSingleResult()).longValue());
        assertThat(count).isZero();
    }
}
