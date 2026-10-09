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

    @Test void externalRecoverySharesOneBudgetAndPreservesLocalDrainProvenance() throws Exception {
        try (var context = DocumentNativePublicationFixture.context(POSTGRES);
             var child = new ReaderHostTerminationIT.ManagedChild()) {
            var local = context.tx();
            var host = child.identity.execution();
            ReaderHostExecutions.register(local, host, child.identity.host(), child.identity.boot());
            var published = DocumentNativePublicationFixture.publish(context,
                    DocumentNativePublicationFixture.prepare(context, 1), DocumentNativePublicationFixture.Fault.NONE,
                    em -> {}).getMembers(0);
            var candidate = staged(local, new DocumentAssessmentRetentionFixture(local), 120, true);
            UUID reader = new UUID(0, 1), drainedReader = new UUID(0, 2), foreign = reader(local);
            var ledger = new DocumentReadLedger(local, reader, host, 1);
            var drained = new DocumentReadLedger(local, drainedReader, host, 1);
            ledger.captureHistorical(new ai.protomolt.proto.repo.spi.RepositoryCaller("reader", true),
                    published.getAddress(), UUID.fromString(published.getRevisionId())).close();
            // The native fixture publishes two parts, each retained by a separate pin.
            assertThat(local.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_read_pins WHERE reader_incarnation=:id")
                    .setParameter("id", reader).getSingleResult()).longValue())).isEqualTo(2);
            capture(local, candidate, UUID.randomUUID(), reader);
            capture(local, candidate, UUID.randomUUID(), drainedReader);
            capture(local, candidate, UUID.randomUUID(), foreign);
            var entry = UUID.randomUUID();
            var archive = new ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger(local)
                    .register(new ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger.Location(
                            entry, "account", "external-quiescence", "native-test", "bucket", "archive-" + UUID.randomUUID()));
            ArchiveExternalQuiescenceIT.createArchiveVersion(local, entry, archive.objectId());
            ArchiveExternalQuiescenceIT.insertArchivePin(local, UUID.randomUUID(), reader, entry, archive.objectId());
            var recovery = new DocumentReadRecovery(local);
            assertThatThrownBy(() -> recovery.recoverResourcesBatch(foreign, 1)).hasStackTraceContaining("quiescence");
            assertThatThrownBy(() -> recovery.recoverResourcesBatch(UUID.randomUUID(), 1)).hasStackTraceContaining("quiescence");
            for (int invalid : new int[]{0, -1, 10001})
                assertThatThrownBy(() -> recovery.recoverResourcesBatch(reader, invalid)).isInstanceOf(IllegalArgumentException.class);
            drained.fence(); drained.attestLocalQuiescence(); // Synthetic SQL fixture, no provider workers.
            ReaderHostExecutions.fence(local, host); child.stop();
            var termination = new ReaderHostTermination(local, java.util.Map.of("managed-child", child::verify))
                    .record(child.identity, child.proof());
            var nonce = local.readOnly(em -> (UUID) em.createNativeQuery(
                    "SELECT registration_nonce FROM repository_reader_incarnations WHERE incarnation=:id")
                    .setParameter("id", reader).getSingleResult());
            new ReaderExternalQuiescence(local).quiesce(reader, nonce, host, termination.id());
            var supervisor = new ReaderHostRecovery(local);
            var expected = java.util.List.of(new ReaderHostRecovery.Recovered(reader, 1, 0, 0),
                    new ReaderHostRecovery.Recovered(reader, 0, 1, 0),
                    new ReaderHostRecovery.Recovered(reader, 0, 1, 0),
                    new ReaderHostRecovery.Recovered(reader, 0, 0, 1),
                    new ReaderHostRecovery.Recovered(reader, 0, 0, 0));
            for (var result : expected) {
                var page = supervisor.recoverPage(host, termination.id(), java.util.Optional.empty(), 1, 1,
                        ai.protomolt.proto.repo.spi.RepositoryOperationControl.NONE);
                assertThat(page.recovered()).containsExactly(result);
                assertThat(page.recovered().getFirst().readerResourcesDrained())
                        .isEqualTo(result.archivePins() + result.documentPins() + result.assessmentSessions() == 0);
            }
            assertThat(recovery.recoverResourcesBatch(drainedReader, 1)).isEqualTo(new DocumentReadRecovery.Batch(0, 1));
            assertThat(local.<Object>readOnly(em -> em.createNativeQuery(
                    "SELECT quiescence_source FROM repository_reader_incarnations WHERE incarnation=:id")
                    .setParameter("id", drainedReader).getSingleResult())).isEqualTo("LOCAL_DRAIN");
            assertThat(local.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_read_sessions WHERE reader_incarnation=:id")
                    .setParameter("id", foreign).getSingleResult()).longValue())).isEqualTo(1);
        }
    }

    @Test void shutdownSharesOneBudgetAcrossDocumentPinsAndAssessmentSessions() throws Exception {
        try (var context = DocumentNativePublicationFixture.context(POSTGRES);
                var providerReader = new ai.protomolt.proto.repo.engine.DocumentPartReader((generation, profile) -> {
                    throw new AssertionError("SQL shutdown fixture must not perform provider reads");
                })) {
            var local = context.tx();
            var published = DocumentNativePublicationFixture.publish(context,
                    DocumentNativePublicationFixture.prepare(context, 1), DocumentNativePublicationFixture.Fault.NONE,
                    em -> {}).getMembers(0);
            var candidate = staged(local, new DocumentAssessmentRetentionFixture(local), 120, true);
            UUID reader = UUID.randomUUID();
            var ledger = new DocumentReadLedger(local, reader, 1);
            ledger.captureHistorical(new ai.protomolt.proto.repo.spi.RepositoryCaller("reader", true),
                    published.getAddress(), UUID.fromString(published.getRevisionId())).close();
            capture(local, candidate, UUID.randomUUID(), reader);
            var lifecycle = new DocumentReadLifecycle(ledger, providerReader, 1);
            assertThat(lifecycle.shutdownStep(java.time.Duration.ZERO)).isFalse();
            assertThat(lifecycle.shutdownStep(java.time.Duration.ZERO)).isFalse();
            assertThat(local.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_read_sessions WHERE reader_incarnation=:reader")
                    .setParameter("reader", reader).getSingleResult()).longValue())).isEqualTo(1);
            assertThat(lifecycle.shutdownStep(java.time.Duration.ZERO)).isFalse();
            assertThat(lifecycle.shutdownStep(java.time.Duration.ZERO)).isTrue();
            assertThat(ledger.outstandingReads()).isZero();
            assertThat(local.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_read_sessions WHERE reader_incarnation=:reader")
                    .setParameter("reader", reader).getSingleResult()).longValue())).isZero();
        }
    }

    @Test void boundedShutdownRecoveryIncludesAssessmentSessionsAndPreservesOtherReaders() {
        var c = staged(120, true);
        UUID reader = UUID.randomUUID(), other = reader();
        var ledger = new DocumentReadLedger(tx, reader, 1);
        capture(c, UUID.randomUUID(), reader);
        capture(c, UUID.randomUUID(), reader);
        capture(c, UUID.randomUUID(), other);
        var recovery = new DocumentAssessmentReadRecovery(tx);
        assertThatThrownBy(() -> recovery.recoverBatch(reader, 1)).hasMessageContaining("proven reader quiescence");
        assertThatThrownBy(() -> recovery.recoverBatch(reader, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> recovery.recoverBatch(reader, 10001)).isInstanceOf(IllegalArgumentException.class);
        ledger.fence();
        assertThatThrownBy(() -> recovery.recoverBatch(reader, 1)).hasMessageContaining("proven reader quiescence");
        ledger.attestLocalQuiescence(); // SQL fixture has no provider work; this does not prove provider drain.
        assertThat(ledger.recoverQuiescedPins(1)).isEqualTo(1);
        assertThat(ledger.recoverQuiescedPins(1)).isEqualTo(1);
        assertThat(ledger.recoverQuiescedPins(1)).isZero();
        assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_read_sessions WHERE reader_incarnation=:reader")
                .setParameter("reader", other).getSingleResult()).longValue())).isEqualTo(1);
        assertThat(references(c.assessment())).isEqualTo(2);
        fence(other); quiesce(other);
        assertThat(recovery.recoverBatch(other, 10)).isEqualTo(1);
        assertThat(recovery.recoverBatch(other, 10)).isZero();
    }

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

    @Test void exactLocalReleaseIsAtomicAfterAssessmentExpiryWithoutReaderQuiescence() {
        var c = staged(2, true); UUID reader = reader(), session = UUID.randomUUID();
        capture(c, session, reader);
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        fence(reader); // Local drain and incarnation quiescence are different authorities.
        assertThatThrownBy(() -> release(session, UUID.randomUUID(), c.assessment()))
                .hasStackTraceContaining("another reader or assessment");
        assertThatThrownBy(() -> release(session, reader, UUID.randomUUID()))
                .hasStackTraceContaining("another reader or assessment");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_assessment_read_sessions SET release_xid=pg_current_xact_id() WHERE session_id=:id")
                    .setParameter("id", session).executeUpdate();
        })).hasStackTraceContaining("must remove its session atomically");
        assertThatThrownBy(() -> tx.inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
            em.createNativeQuery("SELECT release_document_assessment_read_session(:id,:reader,:assessment)")
                    .setParameter("id", session).setParameter("reader", reader).setParameter("assessment", c.assessment()).getSingleResult();
            throw new DeliberateRollback();
        })).isInstanceOf(DeliberateRollback.class);
        assertThatThrownBy(() -> fixture.release(c)).hasStackTraceContaining("all reader sessions to drain");
        // Synthetic host drain assertion: this exercises SQL, not provider completion.
        assertThat(release(session, reader, c.assessment())).isTrue();
        assertThat(release(session, reader, c.assessment())).isTrue();
        assertThat(fixture.release(c)).isTrue();
        assertThat(references(c.assessment())).isZero();
    }

    @Test void releasedIdentityCannotBeReusedOrAcknowledgedByAnotherTuple() {
        var c = staged(120, true); UUID reader = reader(), session = UUID.randomUUID();
        capture(c, session, reader);
        assertThat(release(session, reader, c.assessment())).isTrue();
        assertThatThrownBy(() -> capture(c, session, reader)).hasStackTraceContaining("document_assessment_read_identities_pkey");
        assertThatThrownBy(() -> release(session, UUID.randomUUID(), c.assessment())).hasStackTraceContaining("another reader or assessment");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("DELETE FROM document_assessment_read_identities WHERE session_id=:id").setParameter("id", session).executeUpdate();
        })).hasStackTraceContaining("identities are permanent");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_assessment_read_identities SET released=false WHERE session_id=:id").setParameter("id", session).executeUpdate();
        })).hasStackTraceContaining("only completed release");
        fence(reader); quiesce(reader);
        assertThatThrownBy(() -> recover(session, reader, UUID.randomUUID())).hasStackTraceContaining("another reader or assessment");
        assertThat(recover(session, reader, c.assessment())).isTrue();
    }

    @Test void unknownCaptureAbsenceRequiresProvenQuiescence() {
        UUID reader = reader(), session = UUID.randomUUID(), assessment = UUID.randomUUID();
        assertThatThrownBy(() -> release(session, reader, assessment)).hasStackTraceContaining("outcome is not established");
        fence(reader);
        assertThatThrownBy(() -> release(session, reader, assessment)).hasStackTraceContaining("outcome is not established");
        assertThatThrownBy(() -> recover(session, reader, assessment)).hasStackTraceContaining("proven reader quiescence");
        quiesce(reader);
        assertThat(recover(session, reader, assessment)).isTrue();
    }

    @Test void migrationBackfillsExistingLiveSessionsWithoutInventingReleasedHistory() {
        try (var old = DocumentNativePublicationFixture.context(POSTGRES, "72")) {
            var localFixture = new DocumentAssessmentRetentionFixture(old.tx());
            var c = staged(old.tx(), localFixture, 120, true);
            UUID reader = reader(old.tx()), session = UUID.randomUUID();
            capture(old.tx(), c, session, reader);
            org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(old.pool().getSchema()).defaultSchema(old.pool().getSchema())
                    .locations("classpath:db/migration/repo").load().migrate();
            Object[] identity = old.tx().readOnly(em -> (Object[]) em.createNativeQuery(
                    "SELECT reader_incarnation,assessment_id,released FROM document_assessment_read_identities WHERE session_id=:id")
                    .setParameter("id", session).getSingleResult());
            assertThat(identity).containsExactly(reader, c.assessment(), false);
            boolean released = old.tx().inTransaction(em -> (Boolean) em.createNativeQuery(
                    "SELECT release_document_assessment_read_session(:id,:reader,:assessment)")
                    .setParameter("id", session).setParameter("reader", reader).setParameter("assessment", c.assessment()).getSingleResult());
            assertThat(released).isTrue();
            assertThat(old.tx().<Boolean>readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT released FROM document_assessment_read_identities WHERE session_id=:id")
                    .setParameter("id", session).getSingleResult())).isTrue();
        }
    }

    @Test void uncommittedCaptureCannotBeAcknowledgedAsReleased() throws Exception {
        var c = staged(120, true); UUID reader = reader(), session = UUID.randomUUID();
        var inserted = new java.util.concurrent.CountDownLatch(1);
        var allowCommit = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var capture = executor.submit(() -> tx.inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>) em -> {
                em.createNativeQuery("SELECT require_active_repository_reader(:id)").setParameter("id", reader).getSingleResult();
                RepositoryOperationLedger.fenceLiveOwner(em, c.owner());
                em.createNativeQuery("INSERT INTO document_assessment_read_sessions(session_id,reader_incarnation,assessment_id) VALUES(:id,:reader,:assessment)")
                        .setParameter("id", session).setParameter("reader", reader).setParameter("assessment", c.assessment()).executeUpdate();
                inserted.countDown();
                try {
                    if (!allowCommit.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("Capture commit gate timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted);
                }
            }));
            try {
                assertThat(inserted.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> release(session, reader, c.assessment())).hasStackTraceContaining("outcome is not established");
            } finally { allowCommit.countDown(); }
            capture.get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertThat(references(c.assessment())).isEqualTo(2);
        assertThat(release(session, reader, c.assessment())).isTrue();
    }

    private static final class DeliberateRollback extends RuntimeException {}

    @Test void committedCapturePreventsWaitingExpiredAssessmentRelease() throws Exception {
        var c = staged(2, true); UUID reader = reader(), session = UUID.randomUUID();
        try (var holder = database.entityManagerFactory().createEntityManager();
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            try {
                int pid = ((Number) holder.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                holder.createNativeQuery("SELECT require_active_repository_reader(:id)").setParameter("id", reader).getSingleResult();
                RepositoryOperationLedger.fenceLiveOwner(holder, c.owner());
                holder.createNativeQuery("INSERT INTO document_assessment_read_sessions(session_id,reader_incarnation,assessment_id) VALUES(:id,:reader,:assessment)")
                        .setParameter("id", session).setParameter("reader", reader).setParameter("assessment", c.assessment()).executeUpdate();
                fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
                var releasing = executor.submit(() -> fixture.release(c));
                awaitBlockedBy(pid);
                holder.getTransaction().commit();
                assertThatThrownBy(() -> releasing.get(10, java.util.concurrent.TimeUnit.SECONDS))
                        .hasStackTraceContaining("all reader sessions to drain");
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
        assertThat(references(c.assessment())).isEqualTo(2);
        assertThat(release(session, reader, c.assessment())).isTrue();
        assertThat(fixture.release(c)).isTrue();
        assertThat(references(c.assessment())).isZero();
    }

    @Test void committedAssessmentReleasePreventsWaitingCapture() throws Exception {
        var c = staged(2, true); UUID reader = reader(), session = UUID.randomUUID();
        fixture.expire("document_assessment_owners", "assessment_id", c.assessment(), "retain_until");
        try (var holder = database.entityManagerFactory().createEntityManager();
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            try {
                int pid = ((Number) holder.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                assertThat(holder.createNativeQuery("SELECT release_expired_document_assessment('account','principal',:op,:id)")
                        .setParameter("op", c.owner().key().operationId()).setParameter("id", c.assessment()).getSingleResult()).isEqualTo(true);
                var capturing = executor.submit(() -> capture(c, session, reader));
                awaitBlockedBy(pid);
                holder.getTransaction().commit();
                assertThatThrownBy(() -> capturing.get(10, java.util.concurrent.TimeUnit.SECONDS))
                        .hasStackTraceContaining("query returned no rows");
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
        assertThat(references(c.assessment())).isZero();
        assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_read_identities WHERE session_id=:id")
                .setParameter("id", session).getSingleResult()).longValue())).isZero();
        assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_assessment_read_sessions WHERE session_id=:id")
                .setParameter("id", session).getSingleResult()).longValue())).isZero();
    }

    private static void awaitBlockedBy(int holder) throws InterruptedException {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        do {
            boolean waiting = tx.readOnly(em -> !em.createNativeQuery("""
                    SELECT pid FROM pg_stat_activity WHERE :holder=ANY(pg_blocking_pids(pid))
                        AND wait_event_type='Lock'
                    """).setParameter("holder", holder).getResultList().isEmpty());
            if (waiting) return;
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Competing assessment transaction did not wait on the held transaction");
    }
    private static boolean release(UUID session, UUID reader, UUID assessment) {
        return tx.inTransaction(em -> (Boolean) em.createNativeQuery("SELECT release_document_assessment_read_session(:id,:reader,:assessment)")
                .setParameter("id", session).setParameter("reader", reader).setParameter("assessment", assessment).getSingleResult());
    }

    private static DocumentAssessmentRetentionFixture.Candidate staged(int seconds, boolean snapshot) {
        return staged(tx, fixture, seconds, snapshot);
    }
    private static DocumentAssessmentRetentionFixture.Candidate staged(Tx tx, DocumentAssessmentRetentionFixture fixture,
            int seconds, boolean snapshot) {
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
        return reader(tx);
    }
    private static UUID reader(Tx tx) {
        var id = UUID.randomUUID();
        tx.inTransaction(em -> { em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:id,'ACTIVE')")
                .setParameter("id", id).executeUpdate(); });
        return id;
    }
    private static void capture(DocumentAssessmentRetentionFixture.Candidate c, UUID session, UUID reader) {
        capture(tx, c, session, reader);
    }
    private static void capture(Tx tx, DocumentAssessmentRetentionFixture.Candidate c, UUID session, UUID reader) {
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
