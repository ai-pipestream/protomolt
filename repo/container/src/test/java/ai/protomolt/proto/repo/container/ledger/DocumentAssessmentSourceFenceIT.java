package ai.protomolt.proto.repo.container.ledger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** SQL source-retention boundary; fixture provider observations and assessment declarations are synthetic. */
@Testcontainers
class DocumentAssessmentSourceFenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(strings = {"REUSE", "HISTORICAL_REUSE"})
    void exclusiveDocumentFenceRejectsNewAssessmentSource(String declaration) throws Exception {
        try (var c = context(POSTGRES); var competing = c.pool().getConnection()) {
            var f = prepare(c, 1);
            var published = publish(c, f, Fault.NONE, em -> {});
            var node = f.sources().getFirst().row().nodeId;
            var revision = UUID.fromString(published.getMembers(0).getRevisionId());
            var object = UUID.fromString(f.sources().getFirst().identities().getFirst().getObjectId());
            var fixture = new DocumentAssessmentRetentionFixture(c.tx());
            var candidate = fixture.reuseCandidate();
            competing.setAutoCommit(false);
            assertThat(exclusive(competing, node)).isTrue();
            var failure = catchThrowable(() -> stage(c, fixture, candidate, declaration, node, revision, object, () -> {}));
            assertThat(failure).hasStackTraceContaining("Historical source acquisition conflicts with document mutation");
            Throwable cause = failure;
            while (cause != null && !(cause instanceof SQLException)) cause = cause.getCause();
            assertThat(cause).isInstanceOf(SQLException.class);
            assertThat(((SQLException) cause).getSQLState()).isEqualTo("40001");
            assertCounts(c, candidate, 0);
            competing.rollback();
            stage(c, fixture, candidate, declaration, node, revision, object, () -> {});
            assertCounts(c, candidate, 1);
        }
    }

    @ParameterizedTest @ValueSource(strings = {"REUSE", "HISTORICAL_REUSE"})
    void assessmentSourceHoldsFenceThroughSealAndCommit(String declaration) throws Exception {
        try (var c = context(POSTGRES); var competing = c.pool().getConnection()) {
            var f = prepare(c, 1);
            var published = publish(c, f, Fault.NONE, em -> {});
            var node = f.sources().getFirst().row().nodeId;
            var revision = UUID.fromString(published.getMembers(0).getRevisionId());
            var object = UUID.fromString(f.sources().getFirst().identities().getFirst().getObjectId());
            var fixture = new DocumentAssessmentRetentionFixture(c.tx());
            var candidate = fixture.reuseCandidate();
            competing.setAutoCommit(false);
            stage(c, fixture, candidate, declaration, node, revision, object,
                    () -> assertThat(exclusive(competing, node)).isFalse());
            assertThat(exclusive(competing, node)).isTrue();
            competing.rollback();
            assertCounts(c, candidate, 1);
        }
    }

    private static void stage(Context c, DocumentAssessmentRetentionFixture fixture,
            DocumentAssessmentRetentionFixture.Candidate candidate, String declaration,
            UUID node, UUID revision, UUID object, Runnable beforeSeal) {
        c.tx().inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, candidate.owner());
            fixture.insertOwner(em, candidate, 120, 1);
            em.createNativeQuery("""
                    INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,
                        selection_revision,object_id,declaration,source_node,source_revision,source_ordinal)
                    VALUES(:assessment,'member',0,1,:object,:declaration,:node,:revision,0)
                    """).setParameter("assessment", candidate.assessment()).setParameter("object", object)
                    .setParameter("declaration", declaration).setParameter("node", declaration.equals("REUSE") ? null : node)
                    .setParameter("revision", revision).executeUpdate();
            beforeSeal.run();
            fixture.seal(em, candidate);
        });
    }

    private static void assertCounts(Context c, DocumentAssessmentRetentionFixture.Candidate candidate, long expected) {
        for (String table : new String[]{"document_assessment_owners", "document_assessment_slots", "document_assessment_objects"}) {
            long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM " + table + " WHERE assessment_id=:id")
                    .setParameter("id", candidate.assessment()).getSingleResult()).longValue());
            assertThat(count).as(table).isEqualTo(expected);
        }
        long mirrors = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                .setParameter("id", candidate.assessment()).getSingleResult()).longValue());
        assertThat(mirrors).isEqualTo(expected);
    }

    private static boolean exclusive(Connection connection, UUID node) {
        try (var statement = connection.prepareStatement("SELECT pg_try_advisory_xact_lock(?)")) {
            statement.setLong(1, node.getMostSignificantBits() ^ node.getLeastSignificantBits());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                return result.getBoolean(1);
            }
        } catch (SQLException failure) { throw new AssertionError("Cannot inspect source fence", failure); }
    }
}
