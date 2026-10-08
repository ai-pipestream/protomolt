package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real canonical historical registration with two independent PostgreSQL sessions. No provider proof. */
@Testcontainers
class DocumentPreparationSourceFenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void exclusiveSourceFenceRollsBackEntireHistoricalRegistration() throws Exception {
        try (var c = context(POSTGRES); var competing = c.pool().getConnection()) {
            competing.setAutoCommit(false);
            var submitted = new AtomicReference<DocumentPublicationPreparationRecord>();
            var failure = catchThrowable(() -> {
                try (var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c, record -> {
                    submitted.set(record);
                    assertThat(exclusive(competing, record)).isTrue();
                }, () -> {})) {
                    fail("Registration must refuse the exclusive source fence");
                }
            });
            assertThat(failure).hasStackTraceContaining("Historical source acquisition conflicts with document mutation");
            Throwable cause = failure;
            while (cause != null && !(cause instanceof SQLException)) cause = cause.getCause();
            assertThat(cause).isInstanceOf(SQLException.class);
            assertThat(((SQLException) cause).getSQLState()).isEqualTo("40001");
            var record = submitted.get();
            assertThat(record).isNotNull();
            for (String table : new String[]{"repository_execution_claims", "repository_coordinator_bindings",
                    "repository_publication_preparations", "repository_preparation_history_sets",
                    "repository_preparation_history_roots", "repository_preparation_pin_batches"}) {
                long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM " + table + " WHERE operation_id=:operation")
                        .setParameter("operation", record.key().operationId()).getSingleResult()).longValue());
                assertThat(count).as(table).isZero();
            }
            competing.rollback();
        }
    }

    @Test void historicalRegistrationHoldsSharedSourceFenceThroughItsTransaction() throws Exception {
        try (var c = context(POSTGRES); var competing = c.pool().getConnection()) {
            competing.setAutoCommit(false);
            var submitted = new AtomicReference<DocumentPublicationPreparationRecord>();
            try (var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c, submitted::set,
                    () -> assertThat(exclusive(competing, submitted.get())).isFalse())) {
                assertThat(exclusive(competing, submitted.get())).isTrue();
                competing.rollback();
                c.tx().readOnly(em -> {
                    assertThat(DocumentPreparationHistoryRoots.coverage(em, rig.record(),
                            DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(rig.record()))))
                            .isEqualTo(DocumentPreparationHistoryRoots.Coverage.EXACT);
                    return null;
                });
            }
        }
    }

    private static boolean exclusive(Connection connection, DocumentPublicationPreparationRecord record) {
        var source = record.command().intent().getMembers(0).getParts(0).getHistoricalReuse().getSource();
        var node = DocumentIds.nodeId(source);
        try (var statement = connection.prepareStatement("SELECT pg_try_advisory_xact_lock(?)")) {
            statement.setLong(1, node.getMostSignificantBits() ^ node.getLeastSignificantBits());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                return result.getBoolean(1);
            }
        } catch (SQLException failure) { throw new AssertionError("Cannot inspect source fence", failure); }
    }
}
