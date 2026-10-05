package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real selection/retention SQL with synthetic provider observations; not a terminal assessment writer. */
@Testcontainers
class DocumentAssessmentPartsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void bindsAndRetainsTheCompleteVerifiedCandidateWithoutPublishing() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            UUID assessment = UUID.randomUUID();
            var parts = stage(c, f, assessment, f.selected());
            int expected = f.command().intent().getMembers(0).getPartsCount();
            assertThat(parts.parts()).hasSize(expected);
            assertThat(parts.selections()).containsExactly(Map.entry("member", 1L));
            for (int ordinal = 0; ordinal < expected; ordinal++) {
                var physical = parts.parts().get(new DocumentCommitParts.Slot("member", ordinal));
                var declared = f.command().intent().getMembers(0).getParts(ordinal).getUpload();
                assertThat(physical.size()).isEqualTo(declared.getSizeBytes());
                assertThat(physical.sha256()).isEqualTo(declared.getSha256());
                assertThat(physical.version()).isEqualTo("fixture-version");
            }
            assertThat(count(c, "document_assessment_slots", "assessment_id", assessment)).isEqualTo(expected);
            long revisions = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                    .setParameter("op", f.command().operationId()).getSingleResult()).longValue());
            assertThat(revisions).isZero();
        }
    }

    @Test void wrongAttemptTokenRollsBackTheAssessmentOwner() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            UUID assessment = UUID.randomUUID();
            var wrong = new DocumentSelectedAttemptLedger.Selected("member", 1, f.selected().attempt(), UUID.randomUUID());
            assertThatThrownBy(() -> stage(c, f, assessment, wrong))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
            assertThat(count(c, "document_assessment_owners", "assessment_id", assessment)).isZero();
        }
    }

    @Test void displacedSelectionCannotRetainThePreviouslyVerifiedCandidate() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var placement = f.prepared().plan().members().getFirst().placement();
            var next = DocumentOperationUploadAdmission.prepare(f.command(), Map.of(placement.drive().id(), placement),
                    Map.of("member", UUID.randomUUID()), Duration.ofMinutes(5));
            new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).retry(CALLER, f.owner(), next,
                    Map.of("member", new DocumentOperationSelection.Expected(1, f.selected().attempt())));
            UUID assessment = UUID.randomUUID();
            assertThatThrownBy(() -> stage(c, f, assessment, f.selected()))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class)
                    .hasMessageContaining("selected or retained physical evidence");
            assertThat(count(c, "document_assessment_owners", "assessment_id", assessment)).isZero();
        }
    }

    @Test void extraSelectedAttemptIsRefusedBeforeAnyPhysicalLookupOrLock() throws Exception {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            UUID assessment = UUID.randomUUID();
            var extra = new DocumentSelectedAttemptLedger.Selected("extra", 1, UUID.randomUUID(), UUID.randomUUID());
            assertThatThrownBy(() -> stage(c, f, assessment, Map.of("member", f.selected(), "extra", extra)))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class)
                    .hasMessageContaining("selected or retained physical evidence");
            assertThat(count(c, "document_assessment_owners", "assessment_id", assessment)).isZero();
        }
    }

    private static DocumentCommitParts.Bound stage(DocumentNativePublicationFixture.Context c,
            DocumentSchemaRetentionFixture.Fixture f, UUID assessment, DocumentSelectedAttemptLedger.Selected selected) {
        return stage(c, f, assessment, Map.of("member", selected));
    }

    private static DocumentCommitParts.Bound stage(DocumentNativePublicationFixture.Context c,
            DocumentSchemaRetentionFixture.Fixture f, UUID assessment, Map<String,DocumentSelectedAttemptLedger.Selected> selected) {
        var plan = f.prepared().plan();
        var authorization = DocumentAdmissionAuthorization.prepare(plan);
        var reuse = DocumentReuseAdmission.prepare(plan);
        return c.tx().inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, f.owner());
            RepositoryOperationLedger.requireCommand(em, f.owner().key(), f.command());
            DocumentSchemaPolicies.lockUnboundWriter(em, "account");
            DocumentAdmissionAuthorization.lockAndAuthorize(em, CALLER, plan, authorization);
            plan.members().getFirst().placement().drive().lock(em, new DriveLedger(c.tx()));
            // Synthetic manifest tests physical binding only; this is not an
            // observed-runtime assessment nor the forthcoming admission writer.
            em.createNativeQuery("""
                    INSERT INTO document_assessment_owners(assessment_id,account_id,principal,operation_id,owner_generation,
                        command_codec,command_version,command_sha256,manifest_bytes,manifest_sha256,expected_slots,retain_until,creation_xid)
                    SELECT :id,account_id,principal,operation_id,:generation,command_codec,command_version,command_sha256,
                        decode('01','hex'),sha256(decode('01','hex')),:count,clock_timestamp()+interval '1 minute','0'::xid8
                    FROM repository_operations WHERE account_id='account' AND principal='principal' AND operation_id=:op
                    """).setParameter("id", assessment).setParameter("generation", f.owner().generation())
                    .setParameter("count", f.command().intent().getMembers(0).getPartsCount())
                    .setParameter("op", f.command().operationId()).executeUpdate();
            var parts = DocumentCommitParts.bindAssessment(em, f.owner(), plan, selected, reuse, () -> {});
            for (var entry : parts.parts().entrySet()) {
                em.createNativeQuery("""
                        INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,selection_revision,object_id,declaration)
                        VALUES(:id,:member,:ordinal,:selection,:object,'NEW_CONTENT')
                        """).setParameter("id", assessment).setParameter("member", entry.getKey().member())
                        .setParameter("ordinal", entry.getKey().ordinal()).setParameter("selection", parts.selections().get(entry.getKey().member()))
                        .setParameter("object", entry.getValue().id()).executeUpdate();
            }
            em.createNativeQuery("UPDATE document_assessment_owners SET sealed=true WHERE assessment_id=:id")
                    .setParameter("id", assessment).executeUpdate();
            return parts;
        });
    }
    private static long count(DocumentNativePublicationFixture.Context c, String table, String field, UUID id) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE " + field + "=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
}
