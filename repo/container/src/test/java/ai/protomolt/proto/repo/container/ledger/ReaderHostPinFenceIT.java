package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL host-fence ordering for native reader inserts; fixture bytes are synthetic SQL evidence. */
@Testcontainers
class ReaderHostPinFenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void hostFenceWaitsForAndRejectsEveryNativeReadInsertWithoutReleasingAdmittedRows() throws Exception {
        try (var c = context(POSTGRES); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var readerTx = c.tx();
            var publication = prepare(c, 1);
            var published = publish(c, publication, Fault.NONE, em -> {}).getMembers(0);
            UUID documentRevision = UUID.fromString(published.getRevisionId());
            UUID documentObject = UUID.fromString(publication.sources().getFirst().identities().getFirst().getObjectId());
            UUID documentNode = publication.sources().getFirst().row().nodeId;

            UUID entry = UUID.randomUUID();
            var backend = "host-pin-" + UUID.randomUUID();
            new ManagedBackendLedger(readerTx).bind(backend, new ManagedBackendLedger.Profile(
                    S3BackendIdentity.of("https://storage.example", "us-east-1", true), backend));
            var archiveBinding = new ArchiveObjectLedger(readerTx).register(new ArchiveObjectLedger.Location(
                    entry, "account", "host-fence", backend, "bucket", "key-" + UUID.randomUUID()));
            stageArchiveVersion(readerTx, entry, archiveBinding.objectId());

            var assessmentFixture = new DocumentAssessmentRetentionFixture(readerTx);
            var assessment = assessmentFixture.candidate(120);
            readerTx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, assessment.owner());
                assessmentFixture.insertOwner(em, assessment, 120, 2);
                assessmentFixture.insertSlots(em, assessment, "revision_ordinal", 1);
                assessmentFixture.seal(em, assessment);
                em.createNativeQuery("""
                        INSERT INTO document_assessment_slot_snapshots(assessment_id,snapshot_codec,snapshot_version,snapshot_bytes,snapshot_sha256)
                        VALUES(:id,'document-assessment-slots',1,:bytes,sha256(:bytes))
                        """).setParameter("id", assessment.assessment()).setParameter("bytes", new byte[] {1}).executeUpdate();
            });

            var archiveCase = new InsertCase("archive_read_pins", (em, pin, reader) -> em.createNativeQuery("""
                    INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
                    VALUES(:pin,:reader,:object,:entry,1)
                    """).setParameter("pin", pin)
                    .setParameter("reader", reader)
                    .setParameter("object", archiveBinding.objectId()).setParameter("entry", entry).executeUpdate(),
                    "SELECT count(*) FROM archive_read_pins WHERE pin_id=:pin", """
                    SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id=:pin
                    """);
            var currentCase = documentCase("CURRENT", documentRevision, documentNode, documentObject);
            var historicalCase = documentCase("HISTORICAL", documentRevision, documentNode, documentObject);
            var assessmentCase = new InsertCase("document_assessment_read_sessions", (em, pin, reader) -> { RepositoryOperationLedger.fenceLiveOwner(em, assessment.owner()); em.createNativeQuery("""
                    INSERT INTO document_assessment_read_sessions(session_id,reader_incarnation,assessment_id)
                    VALUES(:pin,:reader,:assessment)
                    """).setParameter("pin", pin)
                    .setParameter("reader", reader)
                    .setParameter("assessment", assessment.assessment()).executeUpdate(); },
                    "SELECT count(*) FROM document_assessment_read_sessions WHERE session_id=:pin", null);

            runPositiveThenFenceRace(c, workers, archiveCase);
            runPositiveThenFenceRace(c, workers, currentCase);
            runPositiveThenFenceRace(c, workers, historicalCase);
            runPositiveThenFenceRace(c, workers, assessmentCase);
        }
    }

    private static InsertCase documentCase(String scope, UUID revision, UUID node, UUID object) {
        return new InsertCase("document_read_pins " + scope, (em, pin, reader) -> em.createNativeQuery("""
                INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision,read_scope)
                SELECT :pin,:reader,:object,r.node_id,r.revision_id,r.publication_revision,:scope
                FROM document_revision_publications r WHERE r.revision_id=:revision
                """).setParameter("pin", pin)
                .setParameter("reader", reader)
                .setParameter("object", object).setParameter("scope", scope).setParameter("revision", revision)
                .executeUpdate(), "SELECT count(*) FROM document_read_pins WHERE pin_id=:pin",
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='DOCUMENT_READER' AND owner_id=:pin");
    }

    private static void runPositiveThenFenceRace(Context c, java.util.concurrent.ExecutorService workers, InsertCase insert)
            throws Exception {
        UUID host = UUID.randomUUID(), reader = UUID.randomUUID();
        register(c.tx(), host, reader);
        UUID existingPin = UUID.randomUUID();
        insertOnce(c.tx(), insert, existingPin, reader);
        assertThat(countFor(c.tx(), insert.nativeCountSql(), existingPin)).isEqualTo(1);
        if (insert.mirrorCountSql() != null)
            assertThat(countFor(c.tx(), insert.mirrorCountSql(), existingPin)).isEqualTo(1);

        UUID attemptedPin = UUID.randomUUID();
        try (var blocker = c.pool().getConnection()) {
            blocker.setAutoCommit(false);
            int blockerPid;
            try (var statement = blocker.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                result.next();
                blockerPid = result.getInt(1);
            }
            try (var statement = blocker.prepareStatement("SELECT fence_repository_reader_host(?)")) {
                statement.setObject(1, host);
                statement.execute();
            }
            var workerPid = new AtomicInteger();
            var waiting = new CountDownLatch(1);
            Future<Throwable> attempt = workers.submit(() -> {
                try {
                    c.tx().inTransaction(em -> {
                        workerPid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                        waiting.countDown();
                        insert.insert().insert(em, attemptedPin, reader);
                    });
                    return null;
                } catch (Throwable failure) { return failure; }
            });
            try {
                assertThat(waiting.await(10, TimeUnit.SECONDS)).as("%s backend started", insert.description()).isTrue();
                awaitBlocked(c.tx(), workerPid.get(), blockerPid);
                assertThat(attempt).isNotDone();
                blocker.commit();
                Throwable rejected = attempt.get(10, TimeUnit.SECONDS);
                assertThat(rejected).as("%s insert after host fence", insert.description())
                        .hasStackTraceContaining("Reader host execution is not ACTIVE");
            } finally { if (!blocker.getAutoCommit()) blocker.rollback(); }
        }

        assertThat(countFor(c.tx(), insert.nativeCountSql(), existingPin)).isEqualTo(1);
        assertThat(countFor(c.tx(), insert.nativeCountSql(), attemptedPin)).isZero();
        if (insert.mirrorCountSql() != null) {
            assertThat(countFor(c.tx(), insert.mirrorCountSql(), existingPin)).isEqualTo(1);
            assertThat(countFor(c.tx(), insert.mirrorCountSql(), attemptedPin)).isZero();
        }
        assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery(
                "SELECT state FROM repository_reader_host_executions WHERE execution=:id")
                .setParameter("id", host).getSingleResult())).isEqualTo("FENCED");
    }

    private static void insertOnce(Tx tx, InsertCase insert, UUID pin, UUID reader) {
        tx.inTransaction(em -> { insert.insert().insert(em, pin, reader); });
    }

    private static long countFor(Tx tx, String sql, UUID pin) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(sql).setParameter("pin", pin).getSingleResult()).longValue());
    }

    private static void register(Tx tx, UUID host, UUID reader) {
        tx.inTransaction(em -> {
            em.createNativeQuery("INSERT INTO repository_reader_host_executions(execution,host_identity,boot_identity,state) VALUES(:host,'test-host','test-boot','ACTIVE')")
                    .setParameter("host", host).executeUpdate();
            em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state,host_execution) VALUES(:reader,'ACTIVE',:host)")
                    .setParameter("reader", reader).setParameter("host", host).executeUpdate();
        });
    }

    private static void stageArchiveVersion(Tx tx, UUID entry, UUID object) {
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO archive_object_uploads(object_id,expected_size,content_type,lease_token,lease_until,state)
                    VALUES(:object,0,'text/plain',gen_random_uuid(),clock_timestamp()+interval '1 hour','STAGING')
                    """).setParameter("object", object).executeUpdate();
            em.createNativeQuery("UPDATE archive_object_uploads SET state='VERIFIED',sha256=:sha WHERE object_id=:object")
                    .setParameter("sha", "a".repeat(64)).setParameter("object", object).executeUpdate();
            em.createNativeQuery("UPDATE archive_object_uploads SET state='LIVE' WHERE object_id=:object")
                    .setParameter("object", object).executeUpdate();
            em.createNativeQuery("INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version) VALUES(:entry,'account','host-fence',:entry,1)")
                    .setParameter("entry", entry).executeUpdate();
            em.createNativeQuery("INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes) VALUES(:entry,1,'{}','fixture-root',0)")
                    .setParameter("entry", entry).executeUpdate();
            em.createNativeQuery("INSERT INTO archive_version_object_refs(entry_uuid,version,object_id) VALUES(:entry,1,:object)")
                    .setParameter("entry", entry).setParameter("object", object).executeUpdate();
        });
    }

    private static void awaitBlocked(Tx tx, int waitingPid, int blockerPid) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        do {
            int blocked = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM pg_stat_activity WHERE pid=:waiting AND :blocker=ANY(pg_blocking_pids(pid))")
                    .setParameter("waiting", waitingPid).setParameter("blocker", blockerPid).getSingleResult()).intValue());
            if (blocked > 0) return;
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        fail("Expected an actual PostgreSQL host row lock wait");
    }

    @FunctionalInterface private interface NativeInsert { void insert(EntityManager em, UUID pin, UUID reader); }
    private record InsertCase(String description, NativeInsert insert,
            String nativeCountSql, String mirrorCountSql) {}
}
