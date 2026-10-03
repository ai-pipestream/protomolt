package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.container.archive.ArchiveCleanupLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveReadLedger;
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
import org.junit.jupiter.api.Test;
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

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void committedReadPinSurvivesARacingLogicalMutation(boolean commitPin) throws Exception {
        try (var database = database(); var owner = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            var object = liveObject(tx);
            insertReference(owner, object);
            UUID pin = UUID.randomUUID();
            owner.setAutoCommit(false);
            try {
                try (var statement = owner.prepareStatement("INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version) VALUES(?,?,?,?,1)")) {
                    statement.setObject(1, pin); statement.setObject(2, UUID.randomUUID());
                    statement.setObject(3, object.objectId()); statement.setObject(4, object.entryId());
                    statement.executeUpdate();
                }
                int pid = backendPid(owner);
                var mutation = executor.submit(() -> {
                    try (var writer = connection()) {
                        writer.setAutoCommit(false);
                        try {
                            deleteVersion(writer, object);
                            insertTarget(writer, object);
                            writer.commit();
                        } finally { writer.rollback(); }
                    }
                    return true;
                });
                awaitDatabaseWait(pid);
                if (commitPin) owner.commit(); else owner.rollback();
                assertThat(mutation.get(10, TimeUnit.SECONDS)).isTrue();
                assertThat(readerCount(object)).isEqualTo(commitPin ? 1 : 0);
                assertThat(retiring(object)).isTrue();
                assertThat(new ArchiveCleanupLedger(tx).claim(object.objectId(), Instant.now().plusSeconds(60)).isPresent())
                        .isEqualTo(!commitPin);
            } finally {
                owner.rollback();
                owner.setAutoCommit(true);
                try (var statement = owner.prepareStatement("DELETE FROM archive_read_pins WHERE pin_id=?")) {
                    statement.setObject(1, pin); statement.executeUpdate();
                }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void logicalMutationOutcomeDeterminesWhetherNewReaderCanAcquire(boolean commitMutation) throws Exception {
        try (var database = database(); var owner = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            var object = liveObject(tx);
            insertReference(owner, object);
            owner.setAutoCommit(false);
            try {
                deleteVersion(owner, object);
                insertTarget(owner, object);
                int pid = backendPid(owner);
                var read = executor.submit(() -> new ArchiveReadLedger(tx, UUID.randomUUID())
                        .acquire(object.entryId(), 1, object.objectId()));
                awaitDatabaseWait(pid);
                if (commitMutation) owner.commit(); else owner.rollback();
                var admitted = read.get(10, TimeUnit.SECONDS);
                try {
                    assertThat(admitted.isPresent()).isEqualTo(!commitMutation);
                    assertThat(readerCount(object)).isEqualTo(commitMutation ? 0 : 1);
                    assertThat(retiring(object)).isEqualTo(commitMutation);
                } finally { admitted.ifPresent(ArchiveReadLedger.Pin::close); }
            } finally { owner.rollback(); }
        }
    }

    private static void deleteVersion(Connection connection, ObjectFixture object) throws SQLException {
        try (var statement = connection.prepareStatement("DELETE FROM archive_versions WHERE entry_uuid=? AND version=1")) {
            statement.setObject(1, object.entryId()); statement.executeUpdate();
        }
    }

    @Test void readersSurviveLogicalDeletionAndCleanupWaitsForEveryIncarnation() throws Exception {
        try (var database = database(); var connection = connection()) {
            var tx = new Tx(database.entityManagerFactory());
            var object = liveObject(tx);
            var firstReader = new ArchiveReadLedger(tx, UUID.randomUUID());
            var secondReader = new ArchiveReadLedger(tx, UUID.randomUUID());
            assertThat(firstReader.acquire(object.entryId(), 1, object.objectId())).isEmpty();
            insertReference(connection, object);
            assertThat(firstReader.acquire(object.entryId(), 2, object.objectId())).isEmpty();
            assertThat(firstReader.acquire(UUID.randomUUID(), 1, object.objectId())).isEmpty();
            var first = firstReader.acquire(object.entryId(), 1, object.objectId()).orElseThrow();
            var second = secondReader.acquire(object.entryId(), 1, object.objectId()).orElseThrow();
            try {
                assertThat(first.readable().binding().location().backendGeneration()).isEqualTo(object.generation());
                assertThat(readerCount(object)).isEqualTo(2);
                try (var statement = connection.createStatement()) {
                    assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM repository_object_references WHERE object_id='"
                            + object.objectId() + "' AND owner_kind='ARCHIVE_READER'"))
                            .hasMessageContaining("cannot release a retained native owner");
                    statement.executeUpdate("DELETE FROM archive_versions WHERE entry_uuid='" + object.entryId() + "'");
                }
                insertTarget(connection, object);
                assertThat(retiring(object)).isTrue();
                assertThat(reclaiming(object)).isFalse();
                assertThat(secondReader.acquire(object.entryId(), 1, object.objectId())).isEmpty();
                var cleanup = new ArchiveCleanupLedger(tx);
                var distantCutoff = Instant.now().plus(Duration.ofDays(36500));
                assertThat(cleanup.claim(object.objectId(), distantCutoff)).isEmpty();
                assertThat(cleanup.candidates(distantCutoff, 1000)).doesNotContain(object.objectId());
                assertThat(cleanup.mutationCandidates(distantCutoff, distantCutoff, 1000)).doesNotContain(object.objectId());
                first.close();
                first.close();
                assertThat(readerCount(object)).isEqualTo(1);
                assertThat(cleanup.claim(object.objectId(), distantCutoff)).isEmpty();
                second.close();
                assertThat(readerCount(object)).isZero();
                assertThat(cleanup.claim(object.objectId(), distantCutoff)).isPresent();
            } finally { first.close(); second.close(); }
        }
    }

    @Test void failedReleaseRetainsProtectionAndTheSameHandleCanRetry() throws Exception {
        try (var database = database(); var connection = connection(); var statement = connection.createStatement()) {
            var tx = new Tx(database.entityManagerFactory());
            var object = liveObject(tx);
            insertReference(connection, object);
            var pin = new ArchiveReadLedger(tx, UUID.randomUUID())
                    .acquire(object.entryId(), 1, object.objectId()).orElseThrow();
            statement.executeUpdate("DELETE FROM archive_versions WHERE entry_uuid='" + object.entryId() + "'");
            insertTarget(connection, object);
            // A real SQL failure at release; no fake ledger or successful provider stub.
            statement.executeUpdate("""
                    CREATE FUNCTION injected_read_release_failure() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN RAISE EXCEPTION 'Injected read release failure'; END; $$
                    """);
            statement.executeUpdate("CREATE TRIGGER injected_read_release_failure BEFORE DELETE ON archive_read_pins FOR EACH ROW EXECUTE FUNCTION injected_read_release_failure()");
            try {
                assertThatThrownBy(pin::close).hasStackTraceContaining("Injected read release failure");
                assertThat(readerCount(object)).isEqualTo(1);
                assertThat(new ArchiveCleanupLedger(tx).claim(object.objectId(), Instant.now().plusSeconds(60))).isEmpty();
            } finally {
                statement.executeUpdate("DROP TRIGGER injected_read_release_failure ON archive_read_pins");
                statement.executeUpdate("DROP FUNCTION injected_read_release_failure()");
                pin.close();
            }
            assertThat(readerCount(object)).isZero();
            assertThat(new ArchiveCleanupLedger(tx).claim(object.objectId(), Instant.now().plusSeconds(60))).isPresent();
        }
    }

    private static int readerCount(ObjectFixture object) throws SQLException {
        try (var reader = connection(); var statement = reader.prepareStatement(
                "SELECT count(*) FROM archive_read_pins WHERE object_id=?")) {
            statement.setObject(1, object.objectId());
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getInt(1); }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void retirementClosesAcquisitionOnlyAfterCommitWithoutStartingPhysicalCleanup(boolean commitRetirement) throws Exception {
        try (var database = database(); var owner = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            var object = liveObject(tx);
            var independent = liveObject(tx, object.generation());
            owner.setAutoCommit(false);
            try {
                insertTarget(owner, object);
                int ownerPid = backendPid(owner);
                var contender = executor.submit(() -> {
                    try (var writer = connection()) {
                        insertReference(writer, object);
                        return true;
                    } catch (SQLException failure) {
                        assertThat(failure.getSQLState()).isEqualTo("P0001");
                        assertThat((Throwable) failure).hasMessageContaining("admitted mutation target");
                        return false;
                    }
                });
                awaitDatabaseWait(ownerPid);
                var unrelated = executor.submit(() -> new ArchiveCleanupLedger(tx)
                        .claim(independent.objectId(), Instant.now().plusSeconds(60)));
                assertThat(unrelated.get(10, TimeUnit.SECONDS)).isPresent();
                if (commitRetirement) owner.commit(); else owner.rollback();
                assertThat(contender.get(10, TimeUnit.SECONDS)).isEqualTo(!commitRetirement);
                assertThat(referenceCount(object)).isEqualTo(commitRetirement ? 0 : 1);
                assertThat(state(object)).isEqualTo("LIVE");
                assertThat(reclaiming(object)).isFalse();
                assertThat(retiring(object)).isEqualTo(commitRetirement);
            } finally { owner.rollback(); }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void referenceOutcomeControlsRetirementAdmission(boolean commitReference) throws Exception {
        try (var database = database(); var owner = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var object = liveObject(new Tx(database.entityManagerFactory()));
            owner.setAutoCommit(false);
            try {
                insertReference(owner, object);
                int ownerPid = backendPid(owner);
                var contender = executor.submit(() -> {
                    try (var writer = connection()) {
                        writer.setAutoCommit(false);
                        try {
                            insertTarget(writer, object);
                            writer.commit();
                            return true;
                        } catch (SQLException failure) {
                            writer.rollback();
                            assertThat(failure.getSQLState()).isEqualTo("P0001");
                            assertThat((Throwable) failure).hasMessageContaining("retained references");
                            return false;
                        }
                    }
                });
                awaitDatabaseWait(ownerPid);
                if (commitReference) owner.commit(); else owner.rollback();
                assertThat(contender.get(10, TimeUnit.SECONDS)).isEqualTo(!commitReference);
                assertThat(referenceCount(object)).isEqualTo(commitReference ? 1 : 0);
                assertThat(retiring(object)).isEqualTo(!commitReference);
                assertThat(reclaiming(object)).isFalse();
            } finally { owner.rollback(); }
        }
    }

    private static void insertTarget(Connection connection, ObjectFixture object) throws SQLException {
        UUID operation = UUID.randomUUID();
        try (var statement = connection.prepareStatement("""
                INSERT INTO archive_mutations(account_id,principal,operation_id,command_sha256,command,admission_receipt,sampled_revision)
                VALUES('account','sql-fixture',? ,?,decode('01','hex'),decode('01','hex'),0)
                """)) {
            statement.setObject(1, operation); statement.setString(2, "a".repeat(64)); statement.executeUpdate();
        }
        try (var statement = connection.prepareStatement(
                "INSERT INTO archive_mutation_targets VALUES('account','sql-fixture',?,?)")) {
            statement.setObject(1, operation); statement.setObject(2, object.objectId()); statement.executeUpdate();
        }
    }

    private static boolean retiring(ObjectFixture object) throws SQLException {
        try (var reader = connection(); var statement = reader.prepareStatement(
                "SELECT retiring FROM repository_object_retention WHERE object_id=?")) {
            statement.setObject(1, object.objectId());
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }

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
                assertThat(reclaiming(retained)).isEqualTo(!commitReference);
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
                assertThat(reclaiming(object)).isEqualTo(commitCleanup);
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

    private static boolean reclaiming(ObjectFixture object) throws SQLException {
        try (var reader = connection(); var statement = reader.prepareStatement(
                "SELECT reclaiming FROM repository_object_retention WHERE object_id=?")) {
            statement.setObject(1, object.objectId());
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }
}
