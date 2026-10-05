package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** SQL lifecycle fixtures, not observed-manifest admission or provider verification. */
@Testcontainers
@Timeout(60)
class DocumentAssessmentRetentionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentAssessmentRetentionFixture fixture;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        fixture = new DocumentAssessmentRetentionFixture(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void sealsSparseFullOrdinalsAndRetainsWithoutPublishing() {
        var c = fixture.candidate(120);
        fixture.stage(c, 120);
        assertThat(count("document_assessment_slots", c.assessment())).isEqualTo(2);
        assertThat(count("document_assessment_objects", c.assessment())).isEqualTo(2);
        assertThat(references(c.assessment())).isEqualTo(2);
        long publications = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                .setParameter("op", c.owner().key().operationId()).getSingleResult()).longValue());
        assertThat(publications).isZero();
        assertThatThrownBy(() -> fixture.release(c)).hasStackTraceContaining("staging has not expired");
        assertThat(references(c.assessment())).isEqualTo(2);
    }

    @ParameterizedTest @ValueSource(strings = {"unsealed", "missing", "dense", "selection", "retired", "unfenced"})
    void refusesIncompleteOrWrongCandidateAndRollsBackEveryAssociation(String defect) {
        var c = fixture.candidate(120);
        if (defect.equals("retired")) tx.inTransaction(em -> { em.createNativeQuery("""
                SELECT fence_repository_object(physical_object_id) FROM document_part_attempt_objects WHERE attempt_id=:id
                """).setParameter("id", c.attempt()).getResultList(); });
        String expected = switch (defect) {
            case "unsealed" -> "finish sealed";
            case "missing" -> "complete declared candidate slot set";
            case "unfenced" -> "requires a live owner write fence";
            default -> "differs from selected verified uploads";
        };
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            if (!defect.equals("unfenced")) RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
            fixture.insertOwner(em, c, 120, defect.equals("missing") ? 3 : 2);
            fixture.insertSlots(em, c, defect.equals("dense") ? "ordinal" : "revision_ordinal", defect.equals("selection") ? 2 : 1);
            if (!defect.equals("unsealed")) fixture.seal(em, c);
        })).hasStackTraceContaining(expected);
        assertThat(count("document_assessment_owners", c.assessment())).isZero();
        assertThat(count("document_assessment_slots", c.assessment())).isZero();
        assertThat(references(c.assessment())).isZero();
    }

    @Test void directReferenceForgeryAndPostSealAssociationChangesFail() {
        var c = fixture.candidate(120);
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery("""
                INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
                SELECT physical_object_id,'ASSESSMENT',:owner,1 FROM document_part_attempt_objects WHERE attempt_id=:id
                """).setParameter("owner", c.assessment()).setParameter("id", c.attempt()).executeUpdate(); }))
                .hasStackTraceContaining("exact durable native owner");
        fixture.stage(c, 120);
        for (String table : new String[]{"document_assessment_slots", "document_assessment_objects", "document_assessment_owners"}) {
            assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery("DELETE FROM " + table + " WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("recovery fence");
        }
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
            fixture.insertSlots(em, c, "revision_ordinal+1", 1);
        })).hasStackTraceContaining("creation transaction");
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery(
                "DELETE FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                .setParameter("id", c.assessment()).executeUpdate(); })).hasStackTraceContaining("cannot release a retained native owner");
        assertThat(references(c.assessment())).isEqualTo(2);
    }

    @Test void loweredExpectedCountCannotHideAnOmittedSelectedUpload() {
        var c = fixture.candidate(120);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
            fixture.insertOwner(em, c, 120, 1);
            em.createNativeQuery("""
                    INSERT INTO document_assessment_slots(assessment_id,member_id,revision_ordinal,selection_revision,object_id,declaration)
                    SELECT :assessment,'member',revision_ordinal,1,physical_object_id,'NEW_CONTENT'
                    FROM document_part_attempt_objects WHERE attempt_id=:attempt AND ordinal=0
                    """).setParameter("assessment", c.assessment()).setParameter("attempt", c.attempt()).executeUpdate();
            fixture.seal(em, c);
        })).hasStackTraceContaining("omits a selected member or uploaded part");
        assertThat(references(c.assessment())).isZero();
    }

    @Test void generationBindingCannotBorrowTheCurrentOwnerFence() {
        var c = fixture.candidate(120);
        var owner = c.owner();
        var wrong = new DocumentAssessmentRetentionFixture.Candidate(new RepositoryOperationLedger.Owner(owner.key(),
                owner.generation() + 1, owner.token(), owner.leaseUntil()), c.attempt(), c.assessment());
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            fixture.insertOwner(em, wrong, 120, 2);
        })).hasStackTraceContaining("requires a live owner write fence");
        assertThat(count("document_assessment_owners", c.assessment())).isZero();
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void cleanupWaitsForAssessmentAcquisitionCommitOrRollback(boolean commit) throws Exception {
        var c = fixture.candidate(2);
        var acquired = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var blocker = new AtomicInteger();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var acquiring = executor.submit(() -> {
                try {
                    tx.inTransaction(em -> {
                        RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
                        fixture.insertOwner(em, c, 120, 2);
                        fixture.insertSlots(em, c, "revision_ordinal", 1);
                        fixture.seal(em, c);
                        blocker.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                        acquired.countDown();
                        try { assertThat(finish.await(15, TimeUnit.SECONDS)).isTrue(); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
                        if (!commit) throw new DeliberateRollback();
                    });
                    return true;
                } catch (DeliberateRollback expected) { return false; }
            });
            try {
                assertThat(acquired.await(10, TimeUnit.SECONDS)).isTrue();
                fixture.expire("document_part_attempts", "attempt_id", c.attempt(), "lease_until");
                var cleanup = executor.submit(() -> new DocumentAttemptCleanupLedger(tx).claim(c.attempt(), Duration.ofMinutes(1)));
                awaitBlockedBy(blocker.get());
                finish.countDown();
                assertThat(acquiring.get(10, TimeUnit.SECONDS)).isEqualTo(commit);
                assertThat(cleanup.get(10, TimeUnit.SECONDS).isPresent()).isEqualTo(!commit);
                assertThat(references(c.assessment())).isEqualTo(commit ? 2 : 0);
            } finally { finish.countDown(); }
        }
    }

    private static final class DeliberateRollback extends RuntimeException {}

    private static void awaitBlockedBy(int blocker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            boolean blocked = tx.readOnly(em -> (Boolean) em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock')
                    """).setParameter("blocker", blocker).getSingleResult());
            if (blocked) return;
            Thread.sleep(10);
        }
        fail("Cleanup did not reach the expected PostgreSQL assessment lock wait");
    }

    @Test void expiryDoesNotReleaseBytesUntilExplicitFencedRecoveryAndRetryIsSafe() {
        var c = fixture.candidate(2);
        fixture.stage(c, 2);
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        fixture.expire("document_part_attempts", "attempt_id", c.attempt(), "lease_until");
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        assertThat(references(c.assessment())).isEqualTo(2);
        assertThat(cleanup.candidates(Duration.ZERO, 1000, fixture.backend)).doesNotContain(c.attempt());
        assertThat(cleanup.claim(c.attempt(), Duration.ofMinutes(1))).isEmpty();
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery("""
                INSERT INTO document_part_attempt_cleanup(attempt_id,cleanup_token,claim_until,state)
                VALUES (:id,gen_random_uuid(),clock_timestamp()+interval '1 minute','DELETING')
                """).setParameter("id", c.attempt()).executeUpdate(); })).hasStackTraceContaining("remain retained until explicit release");
        assertThat(fixture.release(c)).isTrue();
        assertThat(fixture.release(c)).isFalse();
        assertThat(count("document_assessment_owners", c.assessment())).isZero();
        assertThat(count("document_assessment_slots", c.assessment())).isZero();
        assertThat(count("document_assessment_objects", c.assessment())).isZero();
        assertThat(references(c.assessment())).isZero();
        assertThat(cleanup.claim(c.attempt(), Duration.ofMinutes(1))).isPresent();
    }

    @Test void partialReleaseCannotCommitOrRemoveProtection() {
        var c = fixture.candidate(120);
        fixture.stage(c, 1);
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("SELECT fence_repository_operation_recovery('account','principal',:op)")
                    .setParameter("op", c.owner().key().operationId()).getSingleResult();
            em.createNativeQuery("UPDATE document_assessment_owners SET release_xid=pg_current_xact_id() WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate();
            em.createNativeQuery("DELETE FROM document_assessment_objects WHERE assessment_id=:id")
                    .setParameter("id", c.assessment()).executeUpdate();
        })).hasStackTraceContaining("finish sealed or fully released");
        assertThat(references(c.assessment())).isEqualTo(2);
        assertThat(fixture.release(c)).isTrue();
    }

    @Test void preexistingCleanupTombstoneCannotBeAdopted() {
        var c = fixture.candidate(1);
        fixture.expire("document_part_attempts", "attempt_id", c.attempt(), "lease_until");
        assertThat(new DocumentAttemptCleanupLedger(tx).claim(c.attempt(), Duration.ofMinutes(1))).isPresent();
        assertThatThrownBy(() -> fixture.stage(c, 120)).hasStackTraceContaining("differs from selected verified uploads");
        assertThat(references(c.assessment())).isZero();
    }

    private static long count(String table, UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE assessment_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
    private static long references(UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
}
