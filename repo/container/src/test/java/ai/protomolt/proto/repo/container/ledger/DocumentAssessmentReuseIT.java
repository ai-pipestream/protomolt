package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** SQL lifecycle coverage for retained REUSE slots; this does not verify provider bytes. */
@Testcontainers
class DocumentAssessmentReuseIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void historicalMigrationPreservesExistingUploadAndCurrentReuseAssociations() throws Exception {
        try (var c = context(POSTGRES, "85")) {
            var source = retainedSource(c);
            var fixture = new DocumentAssessmentRetentionFixture(c.tx());
            var uploads = fixture.candidate(120);
            fixture.stage(uploads, 120);
            var reuse = fixture.reuseCandidate();
            var object = sourceObject(c, source.revision());
            stageReuse(c, fixture, reuse, source.revision(), object, object.sourceOrdinal());
            String before = slotSnapshot(c, false);
            long references = count(c, "SELECT count(*) FROM repository_object_references WHERE owner_id=:id", reuse.assessment());
            String schema = c.tx().readOnly(em -> (String) em.createNativeQuery("SELECT current_schema()").getSingleResult());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("86").load().migrate();
            assertThat(slotSnapshot(c, true)).isEqualTo(before);
            assertThat(c.tx().<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_slots WHERE source_node IS NOT NULL").getSingleResult()).longValue())).isZero();
            assertThat(count(c, "SELECT count(*) FROM repository_object_references WHERE owner_id=:id", reuse.assessment()))
                    .isEqualTo(references).isEqualTo(1);
            // Existing insert shapes still work after migration, not only old rows.
            var after = fixture.reuseCandidate();
            stageReuse(c, fixture, after, source.revision(), object, object.sourceOrdinal());
        }
    }

    private static String slotSnapshot(Context c, boolean historicalColumn) {
        return c.tx().readOnly(em -> (String) em.createNativeQuery(
                "SELECT jsonb_agg(" + (historicalColumn ? "to_jsonb(s)-'source_node'" : "to_jsonb(s)")
                        + " ORDER BY assessment_id,member_id,revision_ordinal)::text FROM document_assessment_slots s")
                .getSingleResult());
    }

    @Test void retainsOneAssessmentReferenceForTwoReuseSlotsBoundToTheSamePhysicalObject() throws Exception {
        try (var c = context(POSTGRES)) {
            var source = retainedSource(c);
            var candidateFixture = new DocumentAssessmentRetentionFixture(c.tx());
            var candidate = candidateFixture.reuseCandidate();
            var object = sourceObject(c, source.revision());

            stageReuse(c, candidateFixture, candidate, source.revision(), object, object.sourceOrdinal());

            assertThat(count(c, "SELECT count(*) FROM document_assessment_slots WHERE assessment_id=:id", candidate.assessment()))
                    .isEqualTo(2);
            assertThat(count(c, "SELECT count(*) FROM document_assessment_objects WHERE assessment_id=:id", candidate.assessment()))
                    .isEqualTo(1);
            assertThat(count(c, "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id",
                    candidate.assessment())).isEqualTo(1);
            assertThat(count(c, "SELECT count(*) FROM document_assessment_slots WHERE assessment_id=:id AND object_id=:object",
                    candidate.assessment(), object.id())).isEqualTo(2);
        }
    }

    @Test void rejectsReuseSlotWhoseSourceOrdinalDoesNotContainItsPhysicalObject() throws Exception {
        try (var c = context(POSTGRES)) {
            var source = retainedSource(c);
            var candidateFixture = new DocumentAssessmentRetentionFixture(c.tx());
            var candidate = candidateFixture.reuseCandidate();
            var object = sourceObject(c, source.revision());
            int wrongOrdinal = object.sourceOrdinal() + 100;

            assertThatThrownBy(() -> stageReuse(c, candidateFixture, candidate, source.revision(), object, wrongOrdinal))
                    .hasStackTraceContaining("Assessment candidate differs from selected verified uploads or retained current sources");
            assertThat(count(c, "SELECT count(*) FROM document_assessment_owners WHERE assessment_id=:id", candidate.assessment()))
                    .isZero();
        }
    }

    private static RetainedSource retainedSource(Context c) throws Exception {
        var f = DocumentSchemaRetentionFixture.prepare(c);
        new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
        var revision = DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                (em, id, manifest) -> f.retention().write(em, f.owner(), id, () -> {}));
        return new RetainedSource(revision);
    }

    private record RetainedSource(UUID revision) {}
    private record SourceObject(UUID id, int sourceOrdinal) {}

    private static SourceObject sourceObject(Context c, UUID revision) {
        return c.tx().readOnly(em -> {
            Object[] row = (Object[]) em.createNativeQuery("""
                    SELECT object_id,revision_ordinal FROM document_revision_parts
                    WHERE revision_id=:revision ORDER BY revision_ordinal LIMIT 1
                    """).setParameter("revision", revision).getSingleResult();
            return new SourceObject((UUID) row[0], ((Number) row[1]).intValue());
        });
    }

    private static void stageReuse(Context c, DocumentAssessmentRetentionFixture fixture,
            DocumentAssessmentRetentionFixture.Candidate candidate, UUID sourceRevision, SourceObject object,
            int sourceOrdinal) {
        c.tx().inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, candidate.owner());
            fixture.insertOwner(em, candidate, 120, 2);
            for (int targetOrdinal : new int[] { 2, 5 }) {
                em.createNativeQuery("""
                        INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,
                            selection_revision,object_id,declaration,source_revision,source_ordinal)
                        VALUES (:assessment,'member',:target,1,:object,'REUSE',:source,:sourceOrdinal)
                        """).setParameter("assessment", candidate.assessment()).setParameter("target", targetOrdinal)
                        .setParameter("object", object.id()).setParameter("source", sourceRevision)
                        .setParameter("sourceOrdinal", sourceOrdinal).executeUpdate();
            }
            fixture.seal(em, candidate);
        });
    }

    private static long count(Context c, String sql, UUID assessment) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery(sql).setParameter("id", assessment).getSingleResult()).longValue());
    }

    private static long count(Context c, String sql, UUID assessment, UUID object) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery(sql).setParameter("id", assessment)
                .setParameter("object", object).getSingleResult()).longValue());
    }
}
