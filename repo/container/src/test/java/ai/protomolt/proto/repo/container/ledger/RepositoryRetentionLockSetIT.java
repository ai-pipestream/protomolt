package ai.protomolt.proto.repo.container.ledger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.RepositoryRetentionMigrationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL locks over synthetic SQL metadata; no provider/admission claim. */
@Testcontainers
@Timeout(60)
class RepositoryRetentionLockSetIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static UUID archiveObject;
    private static UUID unrelatedObject;
    private static UUID documentObject;
    private static UUID attempt;

    @BeforeAll static void migrateExistingOwners() throws Exception {
        flyway("25").migrate();
        try (var connection = connection()) {
            execute(connection, """
                    INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                    VALUES('retention-original','s3','https://storage.example','us-east-1',true,'retention-realm')
                    """);
            archiveObject = archive(connection, false);
            unrelatedObject = archive(connection, false);
            attempt = abandonedDocument(connection);
            try (var statement = connection.prepareStatement(
                    "SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=?")) {
                statement.setObject(1, attempt);
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue(); documentObject = rows.getObject(1, UUID.class);
                }
            }
        }
        flyway("64").migrate();
        try (var connection = connection()) {
            String before = snapshot(connection);
            flyway(null).migrate();
            assertThat(snapshot(connection)).isEqualTo(before);
        }
    }

    @Test void deduplicatesMixedSourcesAndDoesNotReopenRetiredObjects() throws Exception {
        try (var connection = connection()) {
            String before = snapshot(connection);
            assertThat(lock(connection, documentObject, archiveObject, documentObject)).isEqualTo(2);
            assertThat(lock(connection)).isZero();
            assertThat(snapshot(connection)).isEqualTo(before);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention WHERE retiring AND reclaiming"))
                    .isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(strings = {
            "NULL::uuid[]", "ARRAY[NULL]::uuid[]", "ARRAY[[gen_random_uuid()]]",
            "array_fill(gen_random_uuid(),ARRAY[10001])"})
    void rejectsInvalidOrUnboundedInput(String input) throws Exception {
        try (var connection = connection()) {
            assertThatThrownBy(() -> number(connection, "SELECT lock_repository_retention_set(" + input + ")"))
                    .isInstanceOf(SQLException.class).hasMessageContaining("Repository retention lock set");
        }
    }

    @Test void missingObjectRefusesTheWholeSetBeforeLockingKnownObjects() throws Exception {
        try (var connection = connection(); var observer = connection()) {
            observer.setAutoCommit(false);
            nowait(observer, "archive_object_uploads", "object_id", archiveObject);
            try {
                execute(connection, "SET lock_timeout='250ms'");
                // If validation tried to lock the known object first, this would
                // time out instead of reporting the missing physical location.
                assertThatThrownBy(() -> lock(connection, archiveObject, UUID.randomUUID()))
                        .hasMessageContaining("requires every physical location");
            } finally { observer.rollback(); }
        }
    }

    @Test void ordersOwnersByPostgresUuidRatherThanInputOrder() throws Exception {
        // UUID.compareTo uses signed longs; compare textual UUIDs here to match
        // PostgreSQL's unsigned byte order for the fixture expectation.
        UUID first = archiveObject.toString().compareTo(unrelatedObject.toString()) < 0 ? archiveObject : unrelatedObject;
        UUID second = first.equals(archiveObject) ? unrelatedObject : archiveObject;
        try (var blocker = connection(); var contender = connection(); var observer = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            nowait(blocker, "archive_object_uploads", "object_id", second);
            int contenderPid = (int) number(contender, "SELECT pg_backend_pid()");
            int blockerPid = (int) number(blocker, "SELECT pg_backend_pid()");
            try {
                var pending = executor.submit(() -> lock(contender, second, first));
                awaitWait(observer, contenderPid, blockerPid);
                assertLocked(observer, "archive_object_uploads", "object_id", first);
                nowait(observer, "repository_object_retention", "object_id", first);
                blocker.rollback();
                assertThat(pending.get(10, TimeUnit.SECONDS)).isEqualTo(2);
            } finally { blocker.rollback(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void holdsSourcesAndPhysicalRowsUntilCommitOrRollback(boolean commit) throws Exception {
        try (var owner = connection(); var observer = connection()) {
            owner.setAutoCommit(false);
            try {
                assertThat(lock(owner, documentObject, archiveObject)).isEqualTo(2);
                assertLocked(observer, "archive_object_uploads", "object_id", archiveObject);
                assertLocked(observer, "document_part_attempts", "attempt_id", attempt);
                assertLocked(observer, "repository_object_retention", "object_id", archiveObject);
                assertLocked(observer, "repository_object_retention", "object_id", documentObject);
                if (commit) owner.commit(); else owner.rollback();
                nowait(observer, "archive_object_uploads", "object_id", archiveObject);
                nowait(observer, "document_part_attempts", "attempt_id", attempt);
                nowait(observer, "repository_object_retention", "object_id", documentObject);
            } finally { owner.rollback(); }
        }
    }

    @Test void locksEverySourceBeforeAnyPhysicalRowAndDoesNotBlockUnrelatedObjects() throws Exception {
        try (var blocker = connection(); var contender = connection(); var observer = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            nowait(blocker, "document_part_attempts", "attempt_id", attempt);
            int contenderPid = (int) number(contender, "SELECT pg_backend_pid()");
            int blockerPid = (int) number(blocker, "SELECT pg_backend_pid()");
            try {
                var pending = executor.submit(() -> {
                    contender.setAutoCommit(false);
                    try {
                        int result = lock(contender, documentObject, archiveObject);
                        contender.commit();
                        return result;
                    } finally { contender.rollback(); }
                });
                awaitWait(observer, contenderPid, blockerPid);
                // Despite reversed input, ARCHIVE is already held while DOCUMENT_PART waits.
                assertLocked(observer, "archive_object_uploads", "object_id", archiveObject);
                nowait(observer, "repository_object_retention", "object_id", archiveObject);
                nowait(observer, "repository_object_retention", "object_id", documentObject);
                assertThat(lock(observer, unrelatedObject)).isEqualTo(1);
                blocker.rollback();
                assertThat(pending.get(10, TimeUnit.SECONDS)).isEqualTo(2);
            } finally { blocker.rollback(); }
        }
    }

    @Test void refusesSnapshotIsolationEvenForEmptySet() throws Exception {
        try (var connection = connection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try {
                assertThatThrownBy(() -> lock(connection)).hasMessageContaining("requires READ COMMITTED");
            } finally { connection.rollback(); }
        }
    }

    private static int lock(Connection connection, UUID... ids) throws SQLException {
        return lockSet(connection, "lock_repository_retention_set", ids);
    }

    private static int share(Connection connection, UUID... ids) throws SQLException {
        return lockSet(connection, "share_repository_retention_set", ids);
    }

    private static int lockSet(Connection connection, String function, UUID... ids) throws SQLException {
        var array = connection.createArrayOf("uuid", ids);
        try (var statement = connection.prepareStatement("SELECT " + function + "(?)")) {
            statement.setArray(1, array);
            try (var rows = statement.executeQuery()) { assertThat(rows.next()).isTrue(); return rows.getInt(1); }
        } finally { array.free(); }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void sharedSetsOverlapButExcludeWritersUntilBothTransactionsFinish(boolean commit) throws Exception {
        try (var first = connection(); var second = connection(); var observer = connection()) {
            String before = snapshot(observer);
            first.setAutoCommit(false); second.setAutoCommit(false);
            try {
                assertThat(share(first, documentObject, archiveObject, documentObject)).isEqualTo(2);
                // A conflicting lock implementation would time out here.
                execute(second, "SET lock_timeout='500ms'");
                assertThat(share(second, archiveObject, documentObject)).isEqualTo(2);
                assertLocked(observer, "archive_object_uploads", "object_id", archiveObject);
                assertLocked(observer, "document_part_attempts", "attempt_id", attempt);
                assertLocked(observer, "repository_object_retention", "object_id", documentObject);
                assertThat(lock(observer, unrelatedObject)).isEqualTo(1);
                if (commit) first.commit(); else first.rollback();
                assertLocked(observer, "repository_object_retention", "object_id", documentObject);
                if (commit) second.commit(); else second.rollback();
                nowait(observer, "repository_object_retention", "object_id", documentObject);
                assertThat(snapshot(observer)).isEqualTo(before);
            } finally { first.rollback(); second.rollback(); }
        }
    }

    @Test void sharedSetLocksAllSourcesBeforePhysicalRows() throws Exception {
        try (var blocker = connection(); var contender = connection(); var observer = connection();
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            nowait(blocker, "document_part_attempts", "attempt_id", attempt);
            int contenderPid = (int) number(contender, "SELECT pg_backend_pid()");
            int blockerPid = (int) number(blocker, "SELECT pg_backend_pid()");
            try {
                var pending = executor.submit(() -> share(contender, documentObject, archiveObject));
                awaitWait(observer, contenderPid, blockerPid);
                assertLocked(observer, "archive_object_uploads", "object_id", archiveObject);
                nowait(observer, "repository_object_retention", "object_id", archiveObject);
                nowait(observer, "repository_object_retention", "object_id", documentObject);
                assertThat(share(observer, unrelatedObject)).isEqualTo(1);
                blocker.rollback();
                assertThat(pending.get(10, TimeUnit.SECONDS)).isEqualTo(2);
            } finally { blocker.rollback(); }
        }
    }

    @Test void sharedSetRejectsMalformedMissingAndSnapshotInputs() throws Exception {
        try (var connection = connection()) {
            for (String input : new String[]{"NULL::uuid[]", "ARRAY[NULL]::uuid[]", "ARRAY[[gen_random_uuid()]]",
                    "array_fill(gen_random_uuid(),ARRAY[10001])"})
                assertThatThrownBy(() -> number(connection, "SELECT share_repository_retention_set(" + input + ")"))
                        .isInstanceOf(SQLException.class).hasMessageContaining("Repository retention shared lock set");
            assertThatThrownBy(() -> share(connection, archiveObject, UUID.randomUUID()))
                    .hasMessageContaining("requires every physical location");
            assertThat(share(connection)).isZero();
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try {
                assertThatThrownBy(() -> share(connection)).hasMessageContaining("requires READ COMMITTED");
            } finally { connection.rollback(); }
        }
    }

    private static void nowait(Connection connection, String table, String column, UUID id) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT 1 FROM " + table + " WHERE " + column + "=? FOR UPDATE NOWAIT")) {
            statement.setObject(1, id);
            try (var rows = statement.executeQuery()) { assertThat(rows.next()).isTrue(); }
        }
    }

    private static void assertLocked(Connection connection, String table, String column, UUID id) {
        assertThatThrownBy(() -> nowait(connection, table, column, id))
                .isInstanceOfSatisfying(SQLException.class, failure -> assertThat(failure.getSQLState()).isEqualTo("55P03"));
    }

    private static void awaitWait(Connection observer, int waiter, int blocker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try (var statement = observer.prepareStatement("SELECT ? = ANY(pg_blocking_pids(?))")) {
            statement.setInt(1, blocker); statement.setInt(2, waiter);
            while (System.nanoTime() < deadline) {
                try (var rows = statement.executeQuery()) {
                    rows.next(); if (rows.getBoolean(1)) return;
                }
                Thread.sleep(10);
            }
        }
        fail("Retention lock set did not reach the expected PostgreSQL source lock wait");
    }

    private static String snapshot(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery("""
                SELECT (SELECT jsonb_agg(to_jsonb(r) ORDER BY object_id) FROM repository_object_retention r)::text
                    || (SELECT jsonb_agg(to_jsonb(r) ORDER BY object_id,owner_kind,owner_id,owner_revision)
                        FROM repository_object_references r)::text
                """)) {
            rows.next(); return rows.getString(1);
        }
    }

    private static Flyway flyway(String target) {
        var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo");
        if (target != null) config.target(target);
        return config.load();
    }

    private static Connection connection() throws SQLException {
        var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        try (var statement = connection.createStatement()) { statement.execute("SET statement_timeout='15s'"); }
        catch (SQLException failure) {
            try { connection.close(); } catch (SQLException closing) { failure.addSuppressed(closing); }
            throw failure;
        }
        return connection;
    }
}
