package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real SQL lifecycle with synthetic snapshot bytes; canonical binding is tested in the runtime probe. */
@Testcontainers
@Timeout(60)
class DocumentAssessmentSlotSnapshotsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentAssessmentRetentionFixture fixture;
    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); fixture = new DocumentAssessmentRetentionFixture(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void oldOwnersSurviveMigrationWithoutInventedProvenance() throws Exception {
        try (var context = DocumentNativePublicationFixture.context(POSTGRES, "68")) {
            var prior = new DocumentAssessmentRetentionFixture(context.tx());
            var c = prior.candidate(120); prior.stage(c, 2);
            String before = context.tx().readOnly(em -> (String) em.createNativeQuery(
                    "SELECT to_jsonb(o)::text FROM document_assessment_owners o WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult());
            String schema = context.pool().getSchema();
            org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
            String after = context.tx().readOnly(em -> (String) em.createNativeQuery(
                    "SELECT to_jsonb(o)::text FROM document_assessment_owners o WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult());
            assertThat(after).isEqualTo(before);
            assertThat(count(context.tx(), c)).isZero();
            assertThatThrownBy(() -> context.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, c.owner()); insert(em, c, "sha256(decode('01','hex'))");
            })).hasStackTraceContaining("creation transaction");
            prior.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
            assertThat(prior.release(c)).isTrue();
        }
    }

    @Test void immutableSnapshotRequiresExplicitExpiredRecovery() {
        var c = fixture.candidate(120); stage(c, 2, "sha256(decode('01','hex'))");
        assertThat(count(tx, c)).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery(
                "UPDATE document_assessment_slot_snapshots SET snapshot_bytes=decode('02','hex') WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("snapshot is immutable");
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery(
                "DELETE FROM document_assessment_slot_snapshots WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("recovery fence");
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        assertThat(count(tx, c)).isEqualTo(1);
        assertThat(fixture.release(c)).isTrue(); assertThat(count(tx, c)).isZero();
        assertThat(fixture.release(c)).isFalse();
    }

    @Test void checksumFailureRollsBackPhysicalOwnerAndSnapshot() {
        var c = fixture.candidate(120);
        assertThatThrownBy(() -> stage(c, 120, "sha256(decode('02','hex'))"))
                .hasStackTraceContaining("document_assessment_slot_snapshot_checksum");
        assertThat(count(tx, c)).isZero();
        long owners = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_owners WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue());
        assertThat(owners).isZero();
        long objects = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_objects WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue());
        long references = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue());
        assertThat(objects).isZero(); assertThat(references).isZero();
    }

    @Test void snapshotCannotPrecedePhysicalSeal() {
        var c = fixture.candidate(120);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner()); fixture.insertOwner(em, c, 120, 2);
            insert(em, c, "sha256(decode('01','hex'))");
        })).hasStackTraceContaining("physical seal");
        assertThat(count(tx, c)).isZero();
    }

    @Test void recoveryFailureRestoresAlreadyDeletedSnapshot() {
        var c = fixture.candidate(120); stage(c, 1, "sha256(decode('01','hex'))");
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    CREATE FUNCTION test_stop_assessment_release() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN RAISE EXCEPTION 'injected assessment release failure'; END $$
                    """).executeUpdate();
            em.createNativeQuery("CREATE TRIGGER test_stop_assessment_release BEFORE DELETE ON document_assessment_slots "
                    + "FOR EACH ROW EXECUTE FUNCTION test_stop_assessment_release()").executeUpdate();
        });
        try {
            assertThatThrownBy(() -> fixture.release(c)).hasStackTraceContaining("injected assessment release failure");
            assertThat(count(tx, c)).isEqualTo(1);
            long references = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult()).longValue());
            assertThat(references).isEqualTo(2);
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER test_stop_assessment_release ON document_assessment_slots").executeUpdate();
                em.createNativeQuery("DROP FUNCTION test_stop_assessment_release()").executeUpdate();
            });
        }
        assertThat(fixture.release(c)).isTrue(); assertThat(count(tx, c)).isZero();
    }

    private static void stage(DocumentAssessmentRetentionFixture.Candidate c, int seconds, String digest) {
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner()); fixture.insertOwner(em, c, seconds, 2);
            fixture.insertSlots(em, c, "revision_ordinal", 1); fixture.seal(em, c); insert(em, c, digest);
        });
    }
    private static void insert(EntityManager em, DocumentAssessmentRetentionFixture.Candidate c, String digest) {
        em.createNativeQuery("INSERT INTO document_assessment_slot_snapshots VALUES(:id,'document-assessment-slots',1,decode('01','hex')," + digest + ")")
                .setParameter("id", c.assessment()).executeUpdate();
    }
    private static long count(Tx source, DocumentAssessmentRetentionFixture.Candidate c) {
        return source.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_assessment_slot_snapshots WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue());
    }
}
