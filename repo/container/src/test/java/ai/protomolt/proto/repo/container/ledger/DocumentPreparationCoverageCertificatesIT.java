package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPreparationCoverageCertificatesIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void newJournalCertifiesExactEmptyCoverageAndRetryKeepsOneCertificate() {
        try (var c = context(POSTGRES)) {
            var record = DocumentPublicationPreparationCodecIT.input(c);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var journal = new DocumentPublicationPreparationJournal(c.tx(), budget);
            var token = UUID.randomUUID();
            var claim = journal.acquireInitial(CALLER, record, token, NONE);
            assertThat(journal.acquireInitial(CALLER, record, token, NONE)).isEqualTo(claim);
            assertThat(count(c, record, "repository_preparation_coverage_certificates")).isEqualTo(1);
            assertThat(count(c, record, "repository_preparation_coverage_unresolved")).isZero();
            Object[] certificate = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT preparation_sha256,command_sha256,root_count,roots_sha256,verifier_version,proof_kind
                    FROM repository_preparation_coverage_certificates WHERE operation_id=:operation
                    """).setParameter("operation", record.key().operationId()).getSingleResult());
            assertThat((byte[]) certificate[0]).isEqualTo(DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record)));
            assertThat((byte[]) certificate[1]).isEqualTo(java.util.HexFormat.of().parseHex(record.command().sha256()));
            assertThat(((Number) certificate[2]).intValue()).isZero();
            assertThat((byte[]) certificate[3]).isEqualTo(DocumentPreparationHistoryRoots.digest(List.of()));
            assertThat(((Number) certificate[4]).intValue()).isEqualTo(1);
            assertThat(certificate[5]).isEqualTo("LIVE_ROOTS");
            for (String sql : List.of("DELETE FROM repository_preparation_coverage_certificates",
                    "UPDATE repository_preparation_coverage_certificates SET verifier_version=1"))
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    em.createNativeQuery(sql + " WHERE operation_id=:operation")
                            .setParameter("operation", record.key().operationId()).executeUpdate();
                })).hasStackTraceContaining("certificate is immutable");
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void oldWriterCannotCommitNewUncertifiedPreparation() {
        try (var c = context(POSTGRES)) {
            var record = DocumentPublicationPreparationCodecIT.input(c);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.acquireInitialInTransaction(em, record.key(), record.command(), UUID.randomUUID(), record.lease());
                LegacyPublicationPreparationFixture.insertProjected(em, record, List.of());
            })).hasStackTraceContaining("atomic canonical coverage certification");
            for (String table : List.of("repository_execution_claims", "repository_publication_preparations",
                    "repository_preparation_history_sets", "repository_preparation_coverage_unresolved",
                    "repository_preparation_coverage_certificates")) assertThat(count(c, record, table)).as(table).isZero();
        }
    }

    @Test void migrationRetainsUnresolvedOldRowsAndOrdinaryRetryDoesNotCertifyThem() {
        try (var c = context(POSTGRES, "117")) {
            var record = DocumentPublicationPreparationCodecIT.input(c);
            var token = UUID.randomUUID();
            var claim = c.tx().inTransaction(em -> {
                var acquired = RepositoryExecutionClaimLedger.acquireInitialInTransaction(em, record.key(), record.command(), token, record.lease());
                LegacyPublicationPreparationFixture.insertProjected(em, record, List.of());
                return acquired.claim();
            });
            migrate(c);
            var journal = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64L * 1024 * 1024));
            assertThat(journal.acquireInitial(CALLER, record, token, NONE)).isEqualTo(claim);
            assertThat(count(c, record, "repository_preparation_coverage_certificates")).isZero();
            assertThat(count(c, record, "repository_preparation_coverage_unresolved")).isEqualTo(1);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM repository_preparation_coverage_unresolved WHERE operation_id=:operation")
                        .setParameter("operation", record.key().operationId()).executeUpdate();
            })).hasStackTraceContaining("requires a coverage certificate");
        }
    }

    @Test void historicalRegistrationCertifiesCanonicalNonemptyRoots() throws Exception {
        try (var c = context(POSTGRES); var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c)) {
            assertThat(count(c, rig.record(), "repository_preparation_coverage_certificates")).isEqualTo(1);
            assertThat(count(c, rig.record(), "repository_preparation_coverage_unresolved")).isZero();
            int roots = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT root_count FROM repository_preparation_coverage_certificates WHERE operation_id=:operation")
                    .setParameter("operation", rig.record().key().operationId()).getSingleResult()).intValue());
            assertThat(roots).isEqualTo(1);
        }
    }

    @Test void internallyConsistentEmptyHeaderCannotCertifyHistoricalCommand() throws Exception {
        try (var c = context(POSTGRES, "117"); var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c)) {
            var command = new ai.protomolt.proto.repo.spi.DocumentPublicationCommand(rig.record().command().intent()
                    .toBuilder().setOperationId(UUID.randomUUID().toString()).build());
            var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
            var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                    rig.record().placements(), rig.record().lease(), 0);
            var digest = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record));
            try (var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(rig.history()), NONE)) {
                var claim = c.tx().inTransaction(em -> {
                    var acquired = RepositoryExecutionClaimLedger.acquireHistoricalInitialInTransaction(
                            em, key, command, UUID.randomUUID(), record.lease(), sources);
                    LegacyPublicationPreparationFixture.insert(em, record);
                    // Deliberately incorrect old SQL projection. No trigger is disabled:
                    // SQL can check its internal consistency but cannot decode the command.
                    em.createNativeQuery("""
                            INSERT INTO repository_preparation_history_sets(account_id,principal,operation_id,predecessor_generation,
                             preparation_sha256,command_sha256,expected_count,roots_sha256)
                            SELECT account_id,principal,operation_id,predecessor_generation,preparation_sha256,command_sha256,0,:roots
                            FROM repository_publication_preparations WHERE operation_id=:operation
                            """).setParameter("roots", DocumentPreparationHistoryRoots.digest(List.of()))
                            .setParameter("operation", key.operationId()).executeUpdate();
                    em.createNativeQuery("UPDATE repository_preparation_history_sets SET sealed=true WHERE operation_id=:operation")
                            .setParameter("operation", key.operationId()).executeUpdate();
                    return acquired.claim();
                });
                migrate(c);
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, claim);
                    DocumentPreparationCoverageCertificates.certifyNew(em, record, digest);
                })).isInstanceOf(RepositoryException.class).hasMessageContaining("differs from its canonical command");
                assertThat(count(c, record, "repository_preparation_coverage_certificates")).isZero();
                assertThat(count(c, record, "repository_preparation_coverage_unresolved")).isEqualTo(1);
            }
        }
    }

    static void migrate(Context c) {
        org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
    }

    static long count(Context c, DocumentPublicationPreparationRecord record, String table) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:operation")
                .setParameter("operation", record.key().operationId()).getSingleResult()).longValue());
    }
}
