package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.container.lifecycle.JdbcEventOutbox;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentAtomicPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentLedger documents;
    private static final String SHA = "ab".repeat(32);
    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); documents = new DocumentLedger(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void sharedRetentionTracksExactHistoryAndCurrentOwners() {
        var first = fixture(null);
        var saved = publish(first, (em,row) -> {});
        assertThat(sharedReferences(first.attempt.id())).isEqualTo(2);
        var next = fixture(saved);
        var replaced = publish(next, (em,row) -> {});
        assertThat(sharedReferences(first.attempt.id())).isEqualTo(1);
        assertThat(sharedReferences(next.attempt.id())).isEqualTo(2);
        documents.deleteByNodeId(replaced.nodeId);
        assertThat(sharedReferences(first.attempt.id())).isEqualTo(1);
        assertThat(sharedReferences(next.attempt.id())).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("DELETE FROM repository_object_references WHERE owner_kind='DOCUMENT_HISTORY' AND owner_id=:id")
                    .setParameter("id", first.attempt.id()).executeUpdate();
        })).hasStackTraceContaining("cannot release a retained native owner");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
                    SELECT physical_object_id,'DOCUMENT_CURRENT',:owner,1 FROM document_part_attempt_objects WHERE attempt_id=:id
                    """).setParameter("owner", UUID.randomUUID()).setParameter("id", first.attempt.id()).executeUpdate();
        })).hasStackTraceContaining("requires its exact durable native owner");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("""
                    UPDATE repository_object_retention SET reclaiming=true WHERE object_id IN
                    (SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=:id)
                    """).setParameter("id", first.attempt.id()).executeUpdate();
        })).hasStackTraceContaining("Retained repository objects cannot be reclaimed");
    }

    @Test void failedPublicationRollsBackSharedReferencesAndCleanupFencesRemainPermanent() {
        var f = fixture(null, Duration.ofSeconds(2));
        assertThatThrownBy(() -> publish(f, (em,row) -> { throw new IllegalStateException("abort publication"); }))
                .hasMessage("abort publication");
        assertThat(sharedReferences(f.attempt.id())).isZero();
        expireAttempt(f.attempt.id());
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        var claim = cleanup.claim(f.attempt.id(), Duration.ofSeconds(5)).orElseThrow();
        assertThat(cleanup.finish(claim, true, null)).isTrue();
        long fenced = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id
                WHERE o.attempt_id=:id AND r.reclaiming
                """).setParameter("id", f.attempt.id()).getSingleResult()).longValue());
        assertThat(fenced).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("""
                    UPDATE repository_object_retention SET reclaiming=false WHERE object_id IN
                    (SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=:id)
                    """).setParameter("id", f.attempt.id()).executeUpdate();
        })).hasStackTraceContaining("fence is permanent");
    }

    private static long sharedReferences(UUID attempt) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM repository_object_references r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id
                WHERE o.attempt_id=:id
                """).setParameter("id", attempt).getSingleResult()).longValue());
    }

    @Test void readSnapshotRetainsOriginalBindingAfterDriveChanges() {
        var f = fixture(null);
        var saved = publish(f, (em, row) -> {});
        var reader = new DocumentPublicationLedger(tx);
        var original = reader.findForRead(saved).orElseThrow();
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE drives SET bucket='replacement-container' WHERE drive_id=:id")
                    .setParameter("id", f.drive.driveId).executeUpdate();
        });
        assertThat(reader.findForRead(saved)).contains(original);
        assertThat(original.namespace()).isEqualTo("container");
        assertThat(original.attemptId()).isEqualTo(f.attempt.id());
        assertThat(original.parts()).hasSize(1);
        assertThat(original.parts().getFirst().providerVersion()).isEqualTo("v1");
    }

    @Test void staleOrDeletedAuthorizedRevisionCannotBecomeLegacyRead() {
        var first = publish(fixture(null), (em, row) -> {});
        var second = publish(fixture(first), (em, row) -> {});
        var reader = new DocumentPublicationLedger(tx);
        assertThatThrownBy(() -> reader.findForRead(first))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertThat(reader.findForRead(second)).isPresent();
        documents.deleteByNodeId(second.nodeId);
        assertThatThrownBy(() -> reader.findForRead(second))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
    }

    @Test void rowPinAndOutboxCommitTogetherWithOneRevision() {
        var f = fixture(null);
        var observedRevision = new java.util.concurrent.atomic.AtomicLong();
        var saved = publish(f, (em, row) -> {
            observedRevision.set(row.mutationRevision);
            new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()));
        });
        assertThat(saved.mutationRevision).isEqualTo(observedRevision.get());
        assertThat(documents.findByNodeId(saved.nodeId).orElseThrow().mutationRevision).isEqualTo(saved.mutationRevision);
        assertThat(pin(saved.nodeId)).isEqualTo(f.attempt.id());
        assertThat(historyCount(f.attempt.id())).isEqualTo(1);
        assertThat(eventCount(saved.docId)).isEqualTo(1);
        long revision = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT publication_revision FROM document_part_publication_history WHERE attempt_id=:id")
                .setParameter("id", f.attempt.id()).getSingleResult()).longValue());
        assertThat(revision).isEqualTo(saved.mutationRevision);
    }

    @Test void abandonedCleanupClaimsExpireAndAbsenceRemainsEligible() {
        var f = fixture(null, Duration.ofSeconds(1));
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        assertThat(cleanup.claim(f.attempt.id(), Duration.ofSeconds(5))).isEmpty();
        expireAttempt(f.attempt.id());
        var claim = cleanup.claim(f.attempt.id(), Duration.ofSeconds(5)).orElseThrow();
        assertThat(claim.keys()).containsExactly(f.row.readManifest().getParts(0).getObjectKey());
        assertThat(claim.generation()).isEqualTo(f.attempt.location().backendGeneration());
        assertThat(claim.namespace()).isEqualTo(f.attempt.location().namespace());
        assertThat(cleanup.claim(f.attempt.id(), Duration.ofSeconds(5))).isEmpty();
        assertThat(cleanup.candidates(Duration.ZERO, 1000)).doesNotContain(f.attempt.id());
        assertThat(cleanup.finish(claim, true, null)).isTrue();
        assertThat(cleanup.candidates(Duration.ZERO, 1000)).contains(f.attempt.id());
        assertThat(cleanup.candidates(Duration.ofHours(1), 1000)).doesNotContain(f.attempt.id());
        var again = cleanup.claim(f.attempt.id(), Duration.ofSeconds(5)).orElseThrow();
        assertThat(again.token()).isNotEqualTo(claim.token());
        assertThat(cleanup.finish(claim, true, null)).isFalse();
        assertThat(cleanup.renew(claim, Duration.ofSeconds(5))).isFalse();
        assertThat(cleanup.finish(again, false, "Provider cleanup failed")).isTrue();
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("DELETE FROM document_part_attempt_cleanup WHERE attempt_id=:id")
                    .setParameter("id", f.attempt.id()).executeUpdate();
        })).hasStackTraceContaining("tombstone cannot be deleted");
        assertThatThrownBy(() -> publish(f, (em, row) -> {}))
                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
    }

    @Test void cleanupNeverClaimsPublishedHistoryEvenAfterRowDeletion() {
        var f = fixture(null, Duration.ofSeconds(1));
        var saved = publish(f, (em, row) -> {});
        expireAttempt(f.attempt.id());
        documents.deleteByNodeId(saved.nodeId);
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        assertThat(cleanup.claim(f.attempt.id(), Duration.ofSeconds(5))).isEmpty();
        assertThat(cleanup.candidates(Duration.ZERO, 1000)).doesNotContain(f.attempt.id());
        assertThatThrownBy(() -> insertCleanup(f.attempt.id())).hasStackTraceContaining("Published document attempts are retained");
    }

    @Test void expiredCleanupOwnerCannotRecordResultAndCanBeReplaced() {
        var f = fixture(null, Duration.ofSeconds(1));
        expireAttempt(f.attempt.id());
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        var old = cleanup.claim(f.attempt.id(), Duration.ofSeconds(1)).orElseThrow();
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM claim_until-clock_timestamp()))+0.05)
                FROM document_part_attempt_cleanup WHERE attempt_id=:id
                """).setParameter("id", f.attempt.id()).getSingleResult());
        assertThat(cleanup.finish(old, true, null)).isFalse();
        assertThat(cleanup.renew(old, Duration.ofSeconds(5))).isFalse();
        var replacement = cleanup.claim(f.attempt.id(), Duration.ofSeconds(5)).orElseThrow();
        assertThat(cleanup.finish(replacement, true, null)).isTrue();
    }

    @Test void directSqlCannotClaimALiveWriter() {
        var f = fixture(null);
        assertThatThrownBy(() -> insertCleanup(f.attempt.id())).hasStackTraceContaining("expired sealed attempt");
    }

    @Test void publicationHoldingAttemptLockWinsAgainstExpiredCleanupScan() throws Exception {
        var f = fixture(null, Duration.ofSeconds(1));
        var publishing = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var claiming = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var publication = executor.submit(() -> publish(f, (em, row) -> {
                publishing.countDown();
                try { assertThat(release.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            }));
            try {
                assertThat(publishing.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                expireAttempt(f.attempt.id());
                var cleanup = executor.submit(() -> {
                    claiming.countDown();
                    return new DocumentAttemptCleanupLedger(tx).claim(f.attempt.id(), Duration.ofSeconds(5));
                });
                assertThat(claiming.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> cleanup.get(200, java.util.concurrent.TimeUnit.MILLISECONDS))
                        .isInstanceOf(java.util.concurrent.TimeoutException.class);
                release.countDown();
                publication.get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(cleanup.get(5, java.util.concurrent.TimeUnit.SECONDS)).isEmpty();
                assertThat(pin(f.row.nodeId)).isEqualTo(f.attempt.id());
            } finally { release.countDown(); }
        }
    }

    @Test void concurrentCleanupClaimsHaveOnlyOneOwnerAndFenceDirectPublication() throws Exception {
        var f = fixture(null, Duration.ofSeconds(1));
        expireAttempt(f.attempt.id());
        var start = new java.util.concurrent.CountDownLatch(1);
        var cleanup = new DocumentAttemptCleanupLedger(tx);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            java.util.concurrent.Callable<Boolean> compete = () -> {
                start.await();
                return cleanup.claim(f.attempt.id(), Duration.ofSeconds(5)).isPresent();
            };
            var one = executor.submit(compete); var two = executor.submit(compete); start.countDown();
            assertThat(List.of(one.get(5, java.util.concurrent.TimeUnit.SECONDS), two.get(5, java.util.concurrent.TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(true, false);
        }
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body)
                    VALUES (:attempt,:node,1,CAST('{}' AS jsonb))
                    """).setParameter("attempt", f.attempt.id()).setParameter("node", f.row.nodeId).executeUpdate();
        })).hasStackTraceContaining("permanently fenced");
    }

    private static void insertCleanup(UUID attempt) {
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO document_part_attempt_cleanup(attempt_id,cleanup_token,claim_until,state)
                    VALUES (:id,:token,clock_timestamp()+interval '5 seconds','DELETING')
                    """).setParameter("id", attempt).setParameter("token", UUID.randomUUID()).executeUpdate();
        });
    }

    private static void expireAttempt(UUID attempt) {
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.05)
                FROM document_part_attempts WHERE attempt_id=:id
                """).setParameter("id", attempt).getSingleResult());
    }

    @Test void callbackFailureRollsBackRowPinHistoryAndOutboxAndAllowsRetry() {
        var f = fixture(null);
        assertThatThrownBy(() -> publish(f, (em, row) -> {
            new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()));
            em.flush();
            throw new IllegalStateException("injected after outbox flush");
        })).hasMessage("injected after outbox flush");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(documents.hasPartPublication(f.row.nodeId)).isFalse();
        assertThat(historyCount(f.attempt.id())).isZero();
        assertThat(eventCount(f.row.docId)).isZero();
        assertThat(publish(f, (em, row) -> {}).nodeId).isEqualTo(f.row.nodeId);
    }

    @Test void ordinarySaveCannotExposeAdmittedPartsWithoutAPublication() {
        var f = fixture(null);
        assertThatThrownBy(() -> documents.saveIfRevision(f.row,null,(em,row)->{}))
                .hasStackTraceContaining("Admitted document parts require an atomic publication");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(publish(f,(em,row)->{}).nodeId).isEqualTo(f.row.nodeId);
    }

    @Test void replacingPublicationRetainsHistoryAndRetiredAttemptCannotReturn() {
        var first = fixture(null);
        var saved = publish(first, (em, row) -> {});
        var second = fixture(saved);
        var replaced = publish(second, (em, row) -> {});
        assertThat(pin(saved.nodeId)).isEqualTo(second.attempt.id());
        assertThat(replaced.readManifest().getDocVersion()).isEqualTo(2);
        assertThat(historyCount(first.attempt.id())).isEqualTo(1);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE document_part_publications SET attempt_id=:attempt WHERE node_id=:node")
                    .setParameter("attempt", first.attempt.id()).setParameter("node", saved.nodeId).executeUpdate();
        })).hasStackTraceContaining("requires a new live publication");
        assertThat(pin(saved.nodeId)).isEqualTo(second.attempt.id());
    }

    @ParameterizedTest @ValueSource(strings = {
        "DELETE FROM document_part_publications WHERE node_id=:id",
        "UPDATE documents SET part_manifest='{}'::jsonb WHERE node_id=:id",
        "UPDATE documents SET drive_name='other-drive' WHERE node_id=:id",
        "UPDATE documents SET checksum='other-checksum' WHERE node_id=:id",
        "DELETE FROM document_part_publication_history WHERE node_id=:id" })
    void databaseRejectsBypasses(String sql) {
        var f = fixture(null); var saved = publish(f, (em, row) -> {});
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery(sql).setParameter("id", saved.nodeId).executeUpdate();
        })).hasStackTraceContaining("ERROR:");
        assertThat(pin(saved.nodeId)).isEqualTo(f.attempt.id());
        assertThat(documents.findByNodeId(saved.nodeId).orElseThrow().readManifest()).isEqualTo(saved.readManifest());
    }

    @Test void statusOnlyChangeKeepsPinAndRowDeletionReleasesItButKeepsHistory() {
        var f = fixture(null); var saved = publish(f, (em, row) -> {});
        saved.status = DocumentStatus.PENDING_PURGE;
        saved.pendingPurgeId = UUID.randomUUID();
        documents.saveIfRevision(saved, saved.mutationRevision, (em, row) -> {});
        assertThat(pin(saved.nodeId)).isEqualTo(f.attempt.id());
        documents.deleteByNodeId(saved.nodeId);
        assertThat(documents.hasPartPublication(saved.nodeId)).isFalse();
        assertThat(historyCount(f.attempt.id())).isEqualTo(1);
    }

    @Test void staleRevisionAndWrongNextVersionCannotPublish() {
        var first = fixture(null); var saved = publish(first, (em, row) -> {});
        var f = fixture(saved);
        f.row.writeManifest(f.row.readManifest().toBuilder().setDocVersion(3).build());
        assertThatThrownBy(() -> publish(f, (em, row) -> {})).hasMessageContaining("next locked version");
        f.row.writeManifest(f.row.readManifest().toBuilder().setDocVersion(2).build());
        saved.reprocessCount++;
        documents.save(saved);
        assertThatThrownBy(() -> publish(f, (em, row) -> {})).isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertThat(pin(saved.nodeId)).isEqualTo(first.attempt.id());
        assertThat(historyCount(f.attempt.id())).isZero();
    }

    @Test void changedDriveAndWrongSelectedBackendCannotPublish() {
        var f = fixture(null);
        var wrong = new DocumentPublicationTarget(new DriveLedger(tx), f.drive, f.attempt.location().backendGeneration(),
                new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "https://different.example")));
        assertThatThrownBy(() -> documents.saveVerifiedAttempt(f.row, null, Map.of(), f.attempt.id(), f.attempt.token(), wrong, (em,row) -> {}))
                .hasMessageContaining("physical location");
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET bucket='moved' WHERE drive_id=:id")
                .setParameter("id", f.drive.driveId).executeUpdate(); });
        assertThatThrownBy(() -> publish(f, (em,row) -> {})).hasMessageContaining("drive changed");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
    }

    @Test void matchingNamespaceDoesNotPermitWrongDriveProviderOrBypassCompositionGate() {
        var f = fixture(null);
        var backend = new ManagedBackendLedger(tx).find(f.attempt.location().backendGeneration()).orElseThrow().identity();
        var gated = new DocumentPublicationTarget(new DriveLedger(tx,drive -> {
            throw new IllegalStateException("selected composition refuses this drive");
        }),f.drive,f.attempt.location().backendGeneration(),backend);
        assertThatThrownBy(() -> documents.saveVerifiedAttempt(f.row,null,Map.of(),f.attempt.id(),f.attempt.token(),gated,(em,row)->{}))
                .hasMessage("selected composition refuses this drive");
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET provider='redis' WHERE drive_id=:id")
                .setParameter("id",f.drive.driveId).executeUpdate(); });
        f.drive.provider = "redis";
        var wrongProvider = new DocumentPublicationTarget(new DriveLedger(tx),f.drive,f.attempt.location().backendGeneration(),backend);
        assertThatThrownBy(() -> documents.saveVerifiedAttempt(f.row,null,Map.of(),f.attempt.id(),f.attempt.token(),wrongProvider,(em,row)->{}))
                .hasMessageContaining("physical location");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
    }

    @Test void competingWritersCannotPublishAgainstTheSameRevision() throws Exception {
        var initial = publish(fixture(null), (em,row) -> {});
        var first = fixture(initial);
        var second = fixture(initial);
        var insideCommit = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var winner = executor.submit(() -> publish(first, (em,row) -> {
                insideCommit.countDown();
                try { assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            }));
            try {
                assertThat(insideCommit.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var loser = executor.submit(() -> publish(second,(em,row)->{}));
                release.countDown();
                winner.get(10,java.util.concurrent.TimeUnit.SECONDS);
                assertThatThrownBy(() -> loser.get(10,java.util.concurrent.TimeUnit.SECONDS))
                        .hasCauseInstanceOf(DocumentLedger.RevisionConflictException.class);
            } finally { release.countDown(); }
        }
        assertThat(pin(initial.nodeId)).isEqualTo(first.attempt.id());
        assertThat(historyCount(second.attempt.id())).isZero();
    }

    @Test void cancellationWhileWaitingForDriveLockPreventsPublication() throws Exception {
        var f = fixture(null);
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var callback = new java.util.concurrent.atomic.AtomicBoolean();
        var signal = new java.util.concurrent.CancellationException("Caller cancelled during lock wait");
        try (var blocker = database.dataSource().getConnection();
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            int blockerPid;
            try (var query = blocker.createStatement(); var rows = query.executeQuery("SELECT pg_backend_pid()")) {
                assertThat(rows.next()).isTrue(); blockerPid = rows.getInt(1);
            }
            try (var lock = blocker.prepareStatement("SELECT drive_id FROM drives WHERE drive_id=? FOR UPDATE")) {
                lock.setObject(1, f.drive.driveId);
                try (var rows = lock.executeQuery()) { assertThat(rows.next()).isTrue(); }
            }
            var publishing = executor.submit(() -> documents.saveVerifiedAttempt(f.row, f.expected, Map.of(),
                    f.attempt.id(), f.attempt.token(), f.target, () -> {
                        if (cancelled.get()) throw signal;
                    }, (em, row) -> callback.set(true)));
            try {
                boolean waiting = false;
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (!waiting && System.nanoTime() < deadline) {
                    waiting = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid)))")
                            .setParameter("pid", blockerPid).getSingleResult());
                    if (!waiting) Thread.sleep(10);
                }
                assertThat(waiting).as("publication is actually blocked on the held database lock").isTrue();
                cancelled.set(true);
            } finally { blocker.rollback(); }
            assertThatThrownBy(() -> publishing.get(5, java.util.concurrent.TimeUnit.SECONDS)).hasCause(signal);
        }
        assertThat(callback).isFalse();
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt.id())).isPresent();
    }

    @Test void cancellationAfterTransactionalCallbackRollsBackPublicationAndOutbox() {
        var f = fixture(null);
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var signal = new java.util.concurrent.CancellationException("Caller cancelled before commit");
        assertThatThrownBy(() -> documents.saveVerifiedAttempt(f.row, f.expected, Map.of(),
                f.attempt.id(), f.attempt.token(), f.target, () -> {
                    if (cancelled.get()) throw signal;
                }, (em, row) -> {
                    new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()));
                    em.flush();
                    cancelled.set(true);
                })).isSameAs(signal);
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
        assertThat(eventCount(f.row.docId)).isZero();
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt.id())).isPresent();
    }

    @Test void deferredGuardRollsBackAChangeMadeAfterPublicationCallback() {
        var f = fixture(null);
        assertThatThrownBy(() -> publish(f,(em,row)->{
            new JdbcEventOutbox(tx).enqueue(em,DocumentEventFactory.saved(row,Instant.now()));
            em.flush();
            em.createNativeQuery("UPDATE documents SET checksum='corrupted-after-binding' WHERE node_id=:id")
                    .setParameter("id",row.nodeId).executeUpdate();
        })).hasStackTraceContaining("Bound document body changed without a new publication");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
        assertThat(eventCount(f.row.docId)).isZero();
    }

    @Test void batchCommitsTwoPublicationsWithRetainedSourceAndOutbox() {
        var source = publish(fixture(null), (em, row) -> {});
        var snapshot = DocumentSourceSnapshot.bound(tx, source);
        var first = fixture(null, Duration.ofMinutes(5), Map.of(source.nodeId, source.mutationRevision));
        var second = fixture(null);
        var result = DocumentPublicationBatch.save(tx, List.of(entry(first, List.of(snapshot), (em, row) ->
                new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()))),
                entry(second, List.of(), (em, row) ->
                new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now())))));
        assertThat(result).extracting(row -> row.nodeId).containsExactly(first.row.nodeId, second.row.nodeId);
        for (var f : List.of(first, second)) {
            assertThat(pin(f.row.nodeId)).isEqualTo(f.attempt.id());
            assertThat(historyCount(f.attempt.id())).isEqualTo(1);
            assertThat(sharedReferences(f.attempt.id())).isEqualTo(2);
            assertThat(eventCount(f.row.docId)).isEqualTo(1);
        }
        assertThat(documents.findByNodeId(source.nodeId).orElseThrow().mutationRevision).isEqualTo(source.mutationRevision);
    }

    @Test void failureAfterBothMergesRollsBackEveryPublicationReferenceAndOutbox() {
        var first = fixture(null);
        var second = fixture(null);
        var callbacks = new java.util.concurrent.atomic.AtomicInteger();
        java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> failSecond = (em, row) -> {
            new JdbcEventOutbox(tx).enqueue(em, DocumentEventFactory.saved(row, Instant.now()));
            em.flush();
            if (callbacks.incrementAndGet() == 2) throw new IllegalStateException("abort whole batch");
        };
        assertThatThrownBy(() -> DocumentPublicationBatch.save(tx, List.of(
                entry(first, List.of(), failSecond), entry(second, List.of(), failSecond))))
                .hasMessage("abort whole batch");
        assertThat(callbacks.get()).isEqualTo(2);
        for (var f : List.of(first, second)) {
            assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
            assertThat(documents.hasPartPublication(f.row.nodeId)).isFalse();
            assertThat(historyCount(f.attempt.id())).isZero();
            assertThat(sharedReferences(f.attempt.id())).isZero();
            assertThat(eventCount(f.row.docId)).isZero();
            assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt.id())).isPresent();
        }
    }

    @Test void batchValidatesAllSourcesBeforePublishingAnyDestination() {
        var initial = fixture(null, Duration.ofMinutes(5), Map.of(), new UUID(0, 1));
        var source = publish(initial, (em, row) -> {});
        var snapshot = DocumentSourceSnapshot.bound(tx, source);
        var replacement = fixture(source);
        var copy = fixture(null, Duration.ofMinutes(5), Map.of(source.nodeId, source.mutationRevision), new UUID(0, 2));
        var result = DocumentPublicationBatch.save(tx, List.of(
                entry(replacement, List.of(), (em, row) -> {}), entry(copy, List.of(snapshot), (em, row) -> {})));
        assertThat(result.getFirst().readManifest().getDocVersion()).isEqualTo(2);
        assertThat(pin(copy.row.nodeId)).isEqualTo(copy.attempt.id());
        assertThat(historyCount(initial.attempt.id())).isEqualTo(1);
        assertThat(sharedReferences(initial.attempt.id())).isEqualTo(1);
    }

    @Test void staleSourceRejectsWholeBatchBeforeCallbacks() {
        var source = publish(fixture(null), (em, row) -> {});
        var snapshot = DocumentSourceSnapshot.bound(tx, source);
        var copy = fixture(null, Duration.ofMinutes(5), Map.of(source.nodeId, source.mutationRevision));
        var independent = fixture(null);
        source.reprocessCount++;
        documents.save(source);
        var callbacks = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentPublicationBatch.save(tx, List.of(
                entry(independent, List.of(), (em, row) -> callbacks.incrementAndGet()),
                entry(copy, List.of(snapshot), (em, row) -> callbacks.incrementAndGet()))))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertThat(callbacks.get()).isZero();
        assertThat(documents.findByNodeId(independent.row.nodeId)).isEmpty();
        assertThat(documents.findByNodeId(copy.row.nodeId)).isEmpty();
    }

    @Test void batchRejectsDuplicatesAndUnboundedDestinationLists() {
        var f = fixture(null);
        var p = entry(f, List.of(), (em, row) -> {});
        assertThatThrownBy(() -> DocumentPublicationBatch.save(tx, List.of(p, p)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> DocumentPublicationBatch.save(tx, List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 to 64");
        assertThatThrownBy(() -> DocumentPublicationBatch.save(tx, java.util.Collections.nCopies(65, p)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 to 64");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
    }

    @Test void oversizedAggregatePartsAreRejectedBeforePublication() {
        var first = fixture(null); var second = fixture(null);
        var manifest = first.row.readManifest();
        first.row.writeManifest(manifest.toBuilder().clearParts()
                .addAllParts(java.util.Collections.nCopies(10000, manifest.getParts(0))).build());
        assertThatThrownBy(() -> DocumentPublicationBatch.save(tx, List.of(
                entry(first, List.of(), (em, row) -> {}), entry(second, List.of(), (em, row) -> {}))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("10000 parts");
        assertThat(documents.findByNodeId(first.row.nodeId)).isEmpty();
        assertThat(documents.findByNodeId(second.row.nodeId)).isEmpty();
    }

    @ParameterizedTest @ValueSource(strings = {"destination", "drive", "cleanup"})
    void oneInvalidMemberRejectsBothBeforeCallbacks(String failure) {
        var original = publish(fixture(null), (em, row) -> {});
        var first = fixture(null);
        var second = fixture(original, failure.equals("cleanup") ? Duration.ofSeconds(2) : Duration.ofMinutes(5));
        switch (failure) {
            case "destination" -> { original.reprocessCount++; documents.save(original); }
            case "drive" -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET bucket='changed' WHERE drive_id=:id")
                    .setParameter("id", second.drive.driveId).executeUpdate(); });
            case "cleanup" -> {
                expireAttempt(second.attempt.id());
                assertThat(new DocumentAttemptCleanupLedger(tx).claim(second.attempt.id(), Duration.ofSeconds(5))).isPresent();
            }
        }
        var callbacks = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentPublicationBatch.save(tx, List.of(
                entry(first, List.of(), (em, row) -> callbacks.incrementAndGet()),
                entry(second, List.of(), (em, row) -> callbacks.incrementAndGet()))))
                .isInstanceOfAny(DocumentLedger.RevisionConflictException.class, DocumentPartAttemptLedger.FenceException.class);
        assertThat(callbacks.get()).isZero();
        assertThat(documents.findByNodeId(first.row.nodeId)).isEmpty();
        assertThat(historyCount(first.attempt.id())).isZero();
        assertThat(historyCount(second.attempt.id())).isZero();
        assertThat(documents.findByNodeId(original.nodeId).orElseThrow().readManifest().getDocVersion()).isEqualTo(1);
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void opposingBatchesSerializeWhileUnrelatedPublicationProgresses(boolean abortFirst) throws Exception {
        var a = publish(fixture(null), (em, row) -> {});
        var b = publish(fixture(null), (em, row) -> {});
        var firstA = fixture(a); var firstB = fixture(b);
        var secondA = fixture(a); var secondB = fixture(b);
        var unrelated = fixture(null);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var blockerPid = new java.util.concurrent.atomic.AtomicInteger();
        java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> hold = (em, row) -> {
            if (blockerPid.compareAndSet(0, ((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue())) {
                entered.countDown();
                try { assertThat(release.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                if (abortFirst) throw new IllegalStateException("abort first batch");
            }
        };
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> DocumentPublicationBatch.save(tx, List.of(
                    entry(firstA, List.of(), hold), entry(firstB, List.of(), hold))));
            try {
                assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var second = executor.submit(() -> DocumentPublicationBatch.save(tx, List.of(
                        entry(secondB, List.of(), (em, row) -> {}), entry(secondA, List.of(), (em, row) -> {}))));
                boolean waiting = false;
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (!waiting && System.nanoTime() < deadline) {
                    waiting = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid)))")
                            .setParameter("pid", blockerPid.get()).getSingleResult());
                    if (!waiting) Thread.sleep(10);
                }
                assertThat(waiting).as("opposing batch waits on the first batch in PostgreSQL").isTrue();
                var independent = executor.submit(() -> publish(unrelated, (em, row) -> {}));
                assertThat(independent.get(5, java.util.concurrent.TimeUnit.SECONDS).nodeId).isEqualTo(unrelated.row.nodeId);
                release.countDown();
                if (abortFirst) {
                    assertThatThrownBy(() -> first.get(5, java.util.concurrent.TimeUnit.SECONDS))
                            .hasCauseInstanceOf(IllegalStateException.class);
                    assertThat(second.get(5, java.util.concurrent.TimeUnit.SECONDS)).hasSize(2);
                    assertThat(pin(a.nodeId)).isEqualTo(secondA.attempt.id());
                    assertThat(pin(b.nodeId)).isEqualTo(secondB.attempt.id());
                } else {
                    assertThat(first.get(5, java.util.concurrent.TimeUnit.SECONDS)).hasSize(2);
                    assertThatThrownBy(() -> second.get(5, java.util.concurrent.TimeUnit.SECONDS))
                            .hasCauseInstanceOf(DocumentLedger.RevisionConflictException.class);
                    assertThat(pin(a.nodeId)).isEqualTo(firstA.attempt.id());
                    assertThat(pin(b.nodeId)).isEqualTo(firstB.attempt.id());
                }
            } finally { release.countDown(); }
        }
    }

    private static DocumentPublicationBatch.Publication entry(Fixture f, List<DocumentSourceSnapshot> snapshots,
            java.util.function.BiConsumer<jakarta.persistence.EntityManager, DocumentRecord> callback) {
        var sources = snapshots.stream().collect(java.util.stream.Collectors.toMap(
                DocumentSourceSnapshot::nodeId, DocumentSourceSnapshot::revision));
        return new DocumentPublicationBatch.Publication(f.row, f.expected, sources, f.attempt.id(), f.attempt.token(),
                f.target, snapshots, () -> {}, callback);
    }

    private record Fixture(DocumentRecord row, Long expected, DocumentPartAttemptLedger.Attempt attempt,
            DriveRecord drive, DocumentPublicationTarget target) {}
    private static Fixture fixture(DocumentRecord previous) {
        return fixture(previous, Duration.ofMinutes(5));
    }
    private static Fixture fixture(DocumentRecord previous, Duration lease) {
        return fixture(previous, lease, Map.of());
    }
    private static Fixture fixture(DocumentRecord previous, Duration lease, Map<UUID, Long> sources) {
        return fixture(previous, lease, sources, previous == null ? UUID.randomUUID() : previous.nodeId);
    }
    private static Fixture fixture(DocumentRecord previous, Duration lease, Map<UUID, Long> sources, UUID node) {
        String generation = "atomic-" + UUID.randomUUID();
        var backend = new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "https://storage.example"));
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(backend, generation));
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "drive-" + UUID.randomUUID(); drive.provider = "test-location"; drive.driveType = "CUSTOM"; drive.bucket = "container";
        new DriveLedger(tx).insert(drive);
        UUID attemptId = UUID.randomUUID();
        String prefix = "documents/account/" + node + "/attempts/" + attemptId + "/";
        var object = new DocumentPartAttemptLedger.PlannedObject(DocumentPart.DOCUMENT_PART_CORE,"",prefix+"core",3,SHA,"application/protobuf");
        var plan = new DocumentPartAttemptLedger.Plan(attemptId,
                new DocumentPartAttemptLedger.Location(node,"account",generation,"container"),
                previous == null ? 0 : previous.mutationRevision, sources, List.of(object));
        var attempts = new DocumentPartAttemptLedger(tx);
        var attempt = attempts.begin(plan, lease);
        attempts.verify(attempt.id(),attempt.token(),object.objectKey(),3,SHA,"v1","etag");
        var row = new DocumentRecord(); row.nodeId=node; row.accountId="account"; row.docId="doc-"+node;
        row.createdAt = previous == null ? Instant.now() : previous.createdAt;
        row.updatedAt = Instant.now();
        row.graphId="intake:account"; row.graphAddressId="source"; row.rowKind=DocumentRowKind.INTAKE; row.datasourceId="source";
        row.driveName=drive.name; row.objectKey=prefix; row.versionId="v1"; row.etag="etag"; row.sizeBytes=3L;
        var manifest = DocumentManifest.newBuilder().setDocVersion(previous == null ? 1 : previous.readManifest().getDocVersion()+1)
                .setAddress(NodeAddress.newBuilder().setAccountId(row.accountId).setDocId(row.docId).setGraphId(row.graphId).setGraphAddressId(row.graphAddressId))
                .addParts(PartManifestEntry.newBuilder().setPart(object.part()).setObjectKey(object.objectKey())
                        .setState(PartState.PART_STATE_PRESENT).setSizeBytes(3).setSha256(SHA)).build();
        row.writeManifest(manifest); row.checksum=DocumentPartCodec.rootChecksumFromManifest(manifest);
        return new Fixture(row, previous == null ? null : previous.mutationRevision, attempt, drive, new DocumentPublicationTarget(new DriveLedger(tx), drive,generation,backend));
    }
    private static DocumentRecord publish(Fixture f, java.util.function.BiConsumer<jakarta.persistence.EntityManager,DocumentRecord> callback) {
        return documents.saveVerifiedAttempt(f.row,f.expected,Map.of(),f.attempt.id(),f.attempt.token(),f.target,callback);
    }

    @ParameterizedTest @ValueSource(strings = {"size", "checksum", "state", "version"})
    void directSqlCannotAdmitFalseBodyFacts(String defect) {
        var f = fixture(null);
        switch (defect) {
            case "size" -> f.row.sizeBytes = 4L;
            case "checksum" -> f.row.checksum = "cd".repeat(32);
            case "version" -> f.row.versionId = "another-version";
            case "state" -> f.row.writeManifest(f.row.readManifest().toBuilder().addParts(
                    PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).setStateValue(999)).build());
        }
        assertThatThrownBy(() -> documents.saveIfRevision(f.row, null, (em, row) -> {
            em.createNativeQuery("""
                    INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body)
                    SELECT :attempt,node_id,mutation_revision,document_publication_body(documents) FROM documents WHERE node_id=:id
                    """).setParameter("attempt",f.attempt.id()).setParameter("id",row.nodeId).executeUpdate();
            em.createNativeQuery("INSERT INTO document_part_publications VALUES (:id,:attempt)")
                    .setParameter("id",row.nodeId).setParameter("attempt",f.attempt.id()).executeUpdate();
        })).hasStackTraceContaining("ERROR: Document publication");
        assertThat(documents.findByNodeId(f.row.nodeId)).isEmpty();
        assertThat(historyCount(f.attempt.id())).isZero();
    }
    private static UUID pin(UUID node) {
        return tx.readOnly(em -> (UUID) em.createNativeQuery("SELECT attempt_id FROM document_part_publications WHERE node_id=:id")
                .setParameter("id",node).getSingleResult());
    }
    private static long historyCount(UUID attempt) {
        return tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_part_publication_history WHERE attempt_id=:id")
                .setParameter("id",attempt).getSingleResult()).longValue());
    }
    private static long eventCount(String doc) {
        return tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_events_outbox WHERE kafka_key=:id")
                .setParameter("id",doc).getSingleResult()).longValue());
    }
}
