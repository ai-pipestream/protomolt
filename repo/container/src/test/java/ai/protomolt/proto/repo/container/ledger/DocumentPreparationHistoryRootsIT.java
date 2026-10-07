package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;
import static ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE;

/** Real SQL registration and rollback. Historical execution remains disabled. */
@Testcontainers
class DocumentPreparationHistoryRootsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void newOrdinaryPreparationHasAnExplicitSealedEmptySetAndRetriesPreserveIt() {
        try (var c = context(POSTGRES)) {
            var value = DocumentPublicationPreparationCodecIT.input(c);
            var journal = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64L * 1024 * 1024));
            var token = UUID.randomUUID();
            var claim = journal.acquireInitial(CALLER, value, token, NONE);
            assertThat(journal.acquireInitial(CALLER, value, token, NONE)).isEqualTo(claim);
            c.tx().readOnly(em -> {
                assertThat(DocumentPreparationHistoryRoots.coverage(em, value,
                        DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(value))))
                        .isEqualTo(DocumentPreparationHistoryRoots.Coverage.EXACT);
                var rows = em.createNativeQuery("SELECT expected_count,roots_sha256,sealed FROM repository_preparation_history_sets WHERE operation_id=:id")
                        .setParameter("id", value.key().operationId()).getResultList();
                assertThat(rows).hasSize(1);
                Object[] row = (Object[]) rows.getFirst();
                assertThat(((Number) row[0]).intValue()).isZero();
                assertThat((byte[]) row[1]).isEqualTo(DocumentPreparationHistoryRoots.digest(java.util.List.of()));
                assertThat(row[2]).isEqualTo(true);
                return null;
            });
            for (String change : java.util.List.of(
                    "UPDATE repository_preparation_history_sets SET sealed=false WHERE operation_id=:id",
                    "UPDATE repository_preparation_history_sets SET expected_count=1 WHERE operation_id=:id",
                    "DELETE FROM repository_preparation_history_sets WHERE operation_id=:id")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, claim);
                    em.createNativeQuery(change).setParameter("id", value.key().operationId()).executeUpdate();
                })).isInstanceOf(RuntimeException.class);
            }
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                em.createNativeQuery("""
                        INSERT INTO repository_preparation_history_roots(account_id,principal,operation_id,predecessor_generation,node_id,revision_id)
                        SELECT account_id,principal,operation_id,predecessor_generation,:node,:revision
                        FROM repository_preparation_history_sets WHERE operation_id=:id
                        """).setParameter("id", value.key().operationId()).setParameter("node", UUID.randomUUID())
                        .setParameter("revision", UUID.randomUUID()).executeUpdate();
            })).hasStackTraceContaining("unsealed creation transaction");
        }
    }

    @Test void preMigrationPreparationRemainsUnknownOnExactRetry() {
        try (var c = context(POSTGRES, "102")) {
            var value = DocumentPublicationPreparationCodecIT.input(c);
            var token = UUID.randomUUID();
            var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(value.key(), value.command(), token, value.lease());
            var bytes = DocumentPublicationPreparationCodec.encode(value);
            var digest = DocumentPublicationPreparationJournal.digest(bytes);
            c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                em.createNativeQuery("""
                        INSERT INTO repository_publication_preparations(account_id,principal,operation_id,predecessor_generation,
                         owner_nonce,command_codec,command_version,command_bytes,command_sha256,preparation_bytes,preparation_sha256)
                        VALUES(:account,:principal,:operation,0,:owner,'document-publication',1,:command,:commandDigest,:bytes,:digest)
                        """).setParameter("account", value.key().account()).setParameter("principal", value.key().principal())
                        .setParameter("operation", value.key().operationId()).setParameter("owner", value.seeds().ownerNonce())
                        .setParameter("command", value.command().canonical().toByteArray())
                        .setParameter("commandDigest", java.util.HexFormat.of().parseHex(value.command().sha256()))
                        .setParameter("bytes", bytes.toByteArray()).setParameter("digest", digest).executeUpdate();
            });
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema()).defaultSchema(c.pool().getSchema())
                    .locations("classpath:db/migration/repo").target("103").load().migrate();
            var journal = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64L * 1024 * 1024));
            assertThat(journal.acquireInitial(CALLER, value, token, NONE)).isEqualTo(claim);
            c.tx().readOnly(em -> {
                assertThat(DocumentPreparationHistoryRoots.coverage(em, value, digest)).isEqualTo(DocumentPreparationHistoryRoots.Coverage.UNKNOWN);
                return null;
            });
            // A legacy row gives us an absent header without disabling any guard.
            // Every failed attempt must roll back, leaving coverage unknown.
            for (int variant = 0; variant < 3; variant++) {
                int attempt = variant;
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, claim);
                    em.createNativeQuery("""
                            INSERT INTO repository_preparation_history_sets(account_id,principal,operation_id,predecessor_generation,
                             preparation_sha256,command_sha256,expected_count,roots_sha256)
                            SELECT account_id,principal,operation_id,predecessor_generation,preparation_sha256,command_sha256,:count,:roots
                            FROM repository_publication_preparations WHERE operation_id=:id
                            """).setParameter("count", attempt == 1 ? 1 : 0)
                            .setParameter("roots", attempt == 2 ? new byte[32]
                                    : DocumentPreparationHistoryRoots.digest(java.util.List.of()))
                            .setParameter("id", value.key().operationId()).executeUpdate();
                    if (attempt != 0) {
                        em.createNativeQuery("UPDATE repository_preparation_history_sets SET sealed=true WHERE operation_id=:id")
                                .setParameter("id", value.key().operationId()).executeUpdate();
                    }
                })).hasStackTraceContaining(attempt == 0
                        ? "Preparation history source set must seal atomically"
                        : "Preparation history source count or digest differs");
                c.tx().readOnly(em -> {
                    assertThat(DocumentPreparationHistoryRoots.coverage(em, value, digest))
                            .isEqualTo(DocumentPreparationHistoryRoots.Coverage.UNKNOWN);
                    return null;
                });
            }
            // Deliberately forged SQL projection: its count/digest agree internally,
            // but this ordinary command has no historical selector. SQL is not a
            // protobuf decoder; a consumer must reconcile against canonical intent.
            c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                Object[] source = (Object[]) em.createNativeQuery("SELECT node_id,revision_id FROM document_revision_publications WHERE projection_sealed ORDER BY node_id,revision_id LIMIT 1")
                        .getSingleResult();
                var roots = java.util.List.of(new DocumentPreparationHistoryRoots.Root(source[0].toString(), source[1].toString()));
                em.createNativeQuery("""
                        INSERT INTO repository_preparation_history_sets(account_id,principal,operation_id,predecessor_generation,
                         preparation_sha256,command_sha256,expected_count,roots_sha256)
                        SELECT account_id,principal,operation_id,predecessor_generation,preparation_sha256,command_sha256,1,:roots
                        FROM repository_publication_preparations WHERE operation_id=:id
                        """).setParameter("roots", DocumentPreparationHistoryRoots.digest(roots))
                        .setParameter("id", value.key().operationId()).executeUpdate();
                em.createNativeQuery("""
                        INSERT INTO repository_preparation_history_roots(account_id,principal,operation_id,predecessor_generation,node_id,revision_id)
                        SELECT account_id,principal,operation_id,predecessor_generation,:node,:revision
                        FROM repository_preparation_history_sets WHERE operation_id=:id
                        """).setParameter("node", source[0]).setParameter("revision", source[1])
                        .setParameter("id", value.key().operationId()).executeUpdate();
                em.createNativeQuery("UPDATE repository_preparation_history_sets SET sealed=true WHERE operation_id=:id")
                        .setParameter("id", value.key().operationId()).executeUpdate();
            });
            assertThatThrownBy(() -> journal.acquireInitial(CALLER, value, token, NONE))
                    .hasMessageContaining("differs from its canonical command");
        }
    }

    @Test void failureToIndexRollsBackTheClaimAndPreparationTogether() {
        try (var c = context(POSTGRES)) {
            var value = DocumentPublicationPreparationCodecIT.input(c);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION fail_history_index() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'injected history index failure'; END; $$
                        """).executeUpdate();
                em.createNativeQuery("CREATE TRIGGER fail_history_index BEFORE INSERT ON repository_preparation_history_sets "
                        + "FOR EACH ROW EXECUTE FUNCTION fail_history_index()").executeUpdate();
            });
            var journal = new DocumentPublicationPreparationJournal(c.tx(), new PayloadBudget(64L * 1024 * 1024));
            var token = UUID.randomUUID();
            try {
                assertThatThrownBy(() -> journal.acquireInitial(CALLER, value, token, NONE))
                        .hasStackTraceContaining("injected history index failure");
                for (String table : java.util.List.of("repository_execution_claims", "repository_publication_preparations", "repository_preparation_history_sets")) {
                    long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                            .setParameter("id", value.key().operationId()).getSingleResult()).longValue());
                    assertThat(count).isZero();
                }
            } finally {
                c.tx().inTransaction(em -> {
                    em.createNativeQuery("DROP TRIGGER fail_history_index ON repository_preparation_history_sets").executeUpdate();
                    em.createNativeQuery("DROP FUNCTION fail_history_index()").executeUpdate();
                });
            }
            assertThat(journal.acquireInitial(CALLER, value, token, NONE)).isNotNull();
        }
    }
}
