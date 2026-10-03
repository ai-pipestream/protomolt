package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.container.archive.ArchiveCleanupLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** SQL lifecycle fixtures: no provider upload or byte-verification claim. */
@Testcontainers
@Timeout(60)
class ArchiveRetentionConcurrencyIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private record ObjectFixture(UUID objectId, UUID entryId, String generation) {}

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void referenceTransactionFencesCleanupWithoutBlockingOtherObjects(boolean commitReference) throws Exception {
        try (var database = database(); var owner = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            var cleanup = new ArchiveCleanupLedger(tx);
            var retained = liveObject(tx);
            var independent = liveObject(tx, retained.generation());
            owner.setAutoCommit(false);
            try {
                insertReference(owner, retained);
                int ownerPid = backendPid(owner);
                var contender = executor.submit(() -> cleanup.claim(retained.objectId(), Instant.now().plusSeconds(60)));
                awaitDatabaseWait(ownerPid);
                // Progress on a different row is required while the first row remains locked.
                var unrelated = executor.submit(() -> cleanup.claim(independent.objectId(), Instant.now().plusSeconds(60)));
                assertThat(unrelated.get(10, TimeUnit.SECONDS)).isPresent();
                if (commitReference) owner.commit(); else owner.rollback();
                assertThat(contender.get(10, TimeUnit.SECONDS).isPresent()).isEqualTo(!commitReference);
                assertThat(referenceCount(retained)).isEqualTo(commitReference ? 1 : 0);
                assertThat(state(retained)).isEqualTo(commitReference ? "LIVE" : "DELETING");
            } finally {
                owner.rollback();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cleanupTransactionFencesNewReferenceUntilItsOutcomeIsKnown(boolean commitCleanup) throws Exception {
        try (var database = database(); var owner = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var object = liveObject(new Tx(database.entityManagerFactory()));
            owner.setAutoCommit(false);
            try {
                // Hold the same state transition made by ArchiveCleanupLedger before commit.
                try (var statement = owner.prepareStatement("""
                        UPDATE archive_object_uploads SET state='DELETING',cleanup_token=?,cleanup_attempts=1
                        WHERE object_id=?
                        """)) {
                    statement.setObject(1, UUID.randomUUID());
                    statement.setObject(2, object.objectId());
                    assertThat(statement.executeUpdate()).isEqualTo(1);
                }
                int ownerPid = backendPid(owner);
                var contender = executor.submit(() -> {
                    try (var writer = connection()) {
                        insertReference(writer, object);
                        return true;
                    } catch (SQLException failure) {
                        assertThat(failure.getSQLState()).isEqualTo("P0001");
                        assertThat((Throwable) failure).hasMessageContaining("requires a live object");
                        return false;
                    }
                });
                awaitDatabaseWait(ownerPid);
                if (commitCleanup) owner.commit(); else owner.rollback();
                assertThat(contender.get(10, TimeUnit.SECONDS)).isEqualTo(!commitCleanup);
                assertThat(referenceCount(object)).isEqualTo(commitCleanup ? 0 : 1);
                assertThat(state(object)).isEqualTo(commitCleanup ? "DELETING" : "LIVE");
            } finally {
                owner.rollback();
            }
        }
    }

    private static LedgerDatabase database() {
        return new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    private static Connection connection() throws SQLException {
        var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        try (var statement = connection.createStatement()) {
            statement.execute("SET statement_timeout='15s'");
        } catch (SQLException failure) {
            try { connection.close(); } catch (SQLException closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
        return connection;
    }

    private static ObjectFixture liveObject(Tx tx) {
        String generation = "retention-race-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                S3BackendIdentity.of("https://storage.example", "us-east-1", true), generation));
        return liveObject(tx, generation);
    }

    private static ObjectFixture liveObject(Tx tx, String generation) {
        var uploads = new ArchiveUploadLedger(tx);
        UUID entry = UUID.randomUUID();
        var admitted = uploads.begin(new ArchiveObjectLedger.Location(entry, "account", "archive",
                generation, "namespace", "object-" + entry), 0, "application/octet-stream", Duration.ofMinutes(1));
        UUID id = admitted.binding().objectId();
        var catalogLocation = new PhysicalObjectLedger(tx).find(id).orElseThrow();
        assertThat(catalogLocation.objectId()).isEqualTo(id);
        assertThat(catalogLocation.backendGeneration()).isEqualTo(generation);
        assertThat(catalogLocation.namespace()).isEqualTo("namespace");
        assertThat(catalogLocation.key()).isEqualTo(admitted.binding().location().objectKey());
        // Synthetic SQL metadata only. This isolates the retention trigger from provider I/O.
        uploads.verify(id, admitted.upload().leaseToken(), 0,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", null, null);
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE archive_object_uploads SET state='LIVE' WHERE object_id=:id")
                    .setParameter("id", id).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version)
                    VALUES (:id,'account','archive',:name,1)
                    """).setParameter("id", entry).setParameter("name", entry.toString()).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes)
                    VALUES (:id,1,'{}','sql-fixture',0)
                    """).setParameter("id", entry).executeUpdate();
        });
        return new ObjectFixture(id, entry, generation);
    }

    private static void insertReference(Connection connection, ObjectFixture object) throws SQLException {
        try (var statement = connection.prepareStatement(
                "INSERT INTO archive_version_object_refs(entry_uuid,version,object_id) VALUES (?,1,?)")) {
            statement.setObject(1, object.entryId());
            statement.setObject(2, object.objectId());
            statement.executeUpdate();
        }
    }

    private static int backendPid(Connection connection) throws SQLException {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
            result.next();
            return result.getInt(1);
        }
    }

    private static void awaitDatabaseWait(int blocker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try (var observer = connection(); var statement = observer.prepareStatement("""
                SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                WHERE ? = ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock')
                """)) {
            statement.setInt(1, blocker);
            while (System.nanoTime() < deadline) {
                try (var result = statement.executeQuery()) {
                    result.next();
                    if (result.getBoolean(1)) return;
                }
                Thread.sleep(10);
            }
        }
        fail("Competing transaction did not reach the expected PostgreSQL lock wait");
    }

    private static int referenceCount(ObjectFixture object) throws SQLException {
        try (var reader = connection(); var statement = reader.prepareStatement(
                "SELECT count(*) FROM archive_version_object_refs WHERE object_id=?")) {
            statement.setObject(1, object.objectId());
            try (var result = statement.executeQuery()) { result.next(); return result.getInt(1); }
        }
    }

    private static String state(ObjectFixture object) throws SQLException {
        try (var reader = connection(); var statement = reader.prepareStatement(
                "SELECT state FROM archive_object_uploads WHERE object_id=?")) {
            statement.setObject(1, object.objectId());
            try (var result = statement.executeQuery()) { result.next(); return result.getString(1); }
        }
    }
}
