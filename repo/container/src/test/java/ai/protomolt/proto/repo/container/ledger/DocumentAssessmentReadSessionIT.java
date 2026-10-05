package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real SQL protection using synthetic manifest/snapshot bytes, not reader admission or provider I/O. */
@Testcontainers
@Timeout(60)
class DocumentAssessmentReadSessionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentAssessmentRetentionFixture fixture;
    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); fixture = new DocumentAssessmentRetentionFixture(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void sessionsPreserveWholeAssessmentAcrossExpiryUntilEveryReaderQuiesces() {
        var c = staged(2, true);
        UUID first = reader(), second = reader(), one = UUID.randomUUID(), two = UUID.randomUUID();
        capture(c, one, first); capture(c, two, second);
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        assertThatThrownBy(() -> fixture.release(c)).hasStackTraceContaining("all reader sessions to drain");
        // Guard the raw release transition too, not only the recovery function.
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("SELECT fence_repository_operation_recovery('account','principal',:op)")
                    .setParameter("op", c.owner().key().operationId()).getSingleResult();
            em.createNativeQuery("UPDATE document_assessment_owners SET release_xid=pg_current_xact_id() WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate();
        })).hasStackTraceContaining("all reader sessions to drain");
        assertThat(references(c.assessment())).isEqualTo(2);
        assertThatThrownBy(() -> capture(c, UUID.randomUUID(), reader())).hasStackTraceContaining("available sealed owner");
        assertThatThrownBy(() -> recover(one, first, c.assessment())).hasStackTraceContaining("proven reader quiescence");
        fence(first);
        assertThatThrownBy(() -> recover(one, first, c.assessment())).hasStackTraceContaining("proven reader quiescence");
        quiesce(first);
        assertThatThrownBy(() -> recover(one, first, UUID.randomUUID())).hasStackTraceContaining("another reader or assessment");
        assertThat(recover(one, first, c.assessment())).isTrue();
        assertThat(recover(one, first, c.assessment())).isTrue();
        assertThatThrownBy(() -> fixture.release(c)).hasStackTraceContaining("all reader sessions to drain");
        fence(second); quiesce(second);
        assertThat(recover(two, second, c.assessment())).isTrue();
        assertThat(fixture.release(c)).isTrue();
        assertThat(references(c.assessment())).isZero();
        assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_slot_snapshots WHERE assessment_id=:id")
                .setParameter("id", c.assessment()).getSingleResult()).longValue())).isZero();
    }

    @Test void refusesMissingProvenanceFencedReadersAndIdentityChanges() {
        var legacy = staged(120, false);
        assertThatThrownBy(() -> capture(legacy, UUID.randomUUID(), reader())).hasStackTraceContaining("retained slot provenance");
        var c = staged(120, true); var reader = reader(); var session = UUID.randomUUID();
        capture(c, session, reader);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_assessment_read_sessions SET acquired_at=clock_timestamp() WHERE session_id=:id")
                    .setParameter("id", session).executeUpdate();
        })).hasStackTraceContaining("identity is immutable");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("DELETE FROM document_assessment_read_sessions WHERE session_id=:id")
                    .setParameter("id", session).executeUpdate();
        })).hasStackTraceContaining("proven reader quiescence");
        fence(reader);
        assertThatThrownBy(() -> capture(c, UUID.randomUUID(), reader)).hasStackTraceContaining("not ACTIVE");
    }

    @Test void existingAssessmentReferencesProtectRetiringObjectsWithoutNewMirrors() {
        var c = staged(120, true);
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT lock_repository_retention_set(array_agg(object_id)) FROM document_assessment_objects WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).getSingleResult();
            em.createNativeQuery("UPDATE repository_object_retention SET retiring=true WHERE object_id IN (SELECT object_id FROM document_assessment_objects WHERE assessment_id=:id)")
                    .setParameter("id", c.assessment()).executeUpdate();
        });
        capture(c, UUID.randomUUID(), reader());
        assertThat(references(c.assessment())).isEqualTo(2);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("SELECT fence_repository_object(object_id) FROM document_assessment_objects WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).getResultList();
        })).hasStackTraceContaining("Retained repository objects cannot be reclaimed");
        assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM document_assessment_objects a JOIN repository_object_retention r USING(object_id)
                WHERE a.assessment_id=:id AND r.retiring AND NOT r.reclaiming
                """).setParameter("id", c.assessment()).getSingleResult()).longValue())).isEqualTo(2);
    }

    private static DocumentAssessmentRetentionFixture.Candidate staged(int seconds, boolean snapshot) {
        var c = fixture.candidate(120);
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
            fixture.insertOwner(em, c, seconds, 2); fixture.insertSlots(em, c, "revision_ordinal", 1); fixture.seal(em, c);
            if (snapshot) em.createNativeQuery("""
                    INSERT INTO document_assessment_slot_snapshots(assessment_id,snapshot_codec,snapshot_version,snapshot_bytes,snapshot_sha256)
                    VALUES(:id,'document-assessment-slots',1,:bytes,sha256(:bytes))
                    """).setParameter("id", c.assessment()).setParameter("bytes", new byte[]{1}).executeUpdate();
        });
        return c;
    }
    private static UUID reader() {
        var id = UUID.randomUUID();
        tx.inTransaction(em -> { em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:id,'ACTIVE')")
                .setParameter("id", id).executeUpdate(); });
        return id;
    }
    private static void capture(DocumentAssessmentRetentionFixture.Candidate c, UUID session, UUID reader) {
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT require_active_repository_reader(:id)").setParameter("id", reader).getSingleResult();
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
            em.createNativeQuery("INSERT INTO document_assessment_read_sessions(session_id,reader_incarnation,assessment_id) VALUES(:id,:reader,:assessment)")
                    .setParameter("id", session).setParameter("reader", reader).setParameter("assessment", c.assessment()).executeUpdate();
        });
    }
    private static void fence(UUID reader) {
        tx.inTransaction(em -> { em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", reader).getSingleResult(); });
    }
    private static void quiesce(UUID reader) {
        // SQL fixture attestation; actual provider drain belongs to lifecycle integration.
        tx.inTransaction(em -> { em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)").setParameter("id", reader).getSingleResult(); });
    }
    private static boolean recover(UUID session, UUID reader, UUID assessment) {
        return tx.inTransaction(em -> (Boolean) em.createNativeQuery("SELECT recover_quiesced_document_assessment_read_session(:id,:reader,:assessment)")
                .setParameter("id", session).setParameter("reader", reader).setParameter("assessment", assessment).getSingleResult());
    }
    private static long references(UUID assessment) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                .setParameter("id", assessment).getSingleResult()).longValue());
    }
}
