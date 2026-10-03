package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveReadLedger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.RepositoryRetentionMigrationIT.*;
import static org.assertj.core.api.Assertions.*;

/** V31 incarnation admission and fencing against real PostgreSQL row locks. */
@Testcontainers
class ArchiveReaderIncarnationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;

    @BeforeAll static void migrate() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").load().migrate();
        try (var connection = connection()) { profile(connection); }
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    @AfterAll static void closeDatabase() { if (database != null) database.close(); }

    @Test void populatedV30PinsMigrateAsUnknownWithoutChangingEitherReference() throws Exception {
        // A separate database keeps this migration fixture independent of the migrated test database.
        String schema = "incarnation_migration_" + UUID.randomUUID().toString().replace("-", "");
        try (var admin = connection()) { execute(admin, "CREATE SCHEMA " + schema); }
        try {
            var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).locations("classpath:db/migration/repo");
            config.target("30").load().migrate();
            try (var connection = connection()) {
                execute(connection, "SET search_path TO " + schema);
                profile(connection);
                UUID object = archive(connection, false), reader = UUID.randomUUID(), pin = UUID.randomUUID();
                execute(connection, """
                        INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
                        SELECT '%s','%s',object_id,entry_uuid,version FROM archive_version_object_refs WHERE object_id='%s'
                        """.formatted(pin, reader, object));
                assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id='" + pin + "' ")).isEqualTo(1);
                Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                        .schemas(schema).locations("classpath:db/migration/repo").load().migrate();
                assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "' AND reader_incarnation='" + reader + "'")).isEqualTo(1);
                assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id='" + pin + "' ")).isEqualTo(1);
                assertThat(number(connection, "SELECT count(*) FROM archive_version_object_refs WHERE object_id='" + object + "'")).isEqualTo(1);
                assertThat(state(connection, reader)).isEqualTo("UNKNOWN");
                assertThatThrownBy(() -> execute(connection,
                        "INSERT INTO repository_reader_incarnations(incarnation,state) VALUES ('" + reader + "','ACTIVE')"))
                        .hasMessageContaining("duplicate key");
                assertThatThrownBy(() -> fence(connection, reader)).hasMessageContaining("unknown");
                assertThatThrownBy(() -> execute(connection,
                        "UPDATE repository_reader_incarnations SET state='ACTIVE' WHERE incarnation='" + reader + "'"))
                        .hasMessageContaining("permanent");
                assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isEqualTo(1);
            }
        } finally {
            try (var admin = connection()) { execute(admin, "DROP SCHEMA " + schema + " CASCADE"); }
        }
    }

    @Test void registrationRejectsReuseAndFenceIsIdempotent() throws Exception {
        UUID reader = UUID.randomUUID();
        new ArchiveReadLedger(tx(), reader);
        try (var connection = connection()) {
            assertThat(state(connection, reader)).isEqualTo("ACTIVE");
            assertThatThrownBy(() -> new ArchiveReadLedger(tx(), reader))
                    .satisfies(error -> assertThat(rootCause(error)).hasMessageContaining("duplicate key"));
            assertThat(fence(connection, reader)).isTrue();
            assertThat(fence(connection, reader)).isTrue();
            assertThat(state(connection, reader)).isEqualTo("FENCED");
            assertThatThrownBy(() -> new ArchiveReadLedger(tx(), reader))
                    .satisfies(error -> assertThat(rootCause(error)).hasMessageContaining("duplicate key"));
            assertThatThrownBy(() -> fence(connection, UUID.randomUUID())).hasMessageContaining("unknown");
            assertThatThrownBy(() -> execute(connection,
                    "UPDATE repository_reader_incarnations SET state='ACTIVE' WHERE incarnation='" + reader + "'"))
                    .hasMessageContaining("permanent");
            assertThatThrownBy(() -> execute(connection,
                    "DELETE FROM repository_reader_incarnations WHERE incarnation='" + reader + "'"))
                    .hasMessageContaining("permanent");
        }
    }

    @Test void admittedPinSurvivesFenceAndReleaseStillWorks() throws Exception {
        UUID reader = UUID.randomUUID(), other = UUID.randomUUID();
        new ArchiveReadLedger(tx(), reader);
        new ArchiveReadLedger(tx(), other);
        try (var connection = connection()) {
            UUID object = archive(connection, false), entry = entry(connection, object), pin = UUID.randomUUID();
            assertThat(acquire(connection, pin, reader, entry, object)).isTrue();
            assertThat(fence(connection, reader)).isTrue();
            assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isEqualTo(1);
            assertThatThrownBy(() -> acquire(connection, UUID.randomUUID(), reader, entry, object))
                    .hasMessageContaining("ACTIVE");
            assertThatThrownBy(() -> directInsert(connection, UUID.randomUUID(), reader, object))
                    .hasMessageContaining("ACTIVE");
            assertThat(acquire(connection, UUID.randomUUID(), other, entry, object)).isTrue();
            assertThat(release(connection, pin, reader, object)).isTrue();
            assertThat(release(connection, pin, reader, object)).isTrue();
            assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isZero();
        }
    }

    @Test void admissionHoldingSharedLockMakesFenceWaitUntilCommit() throws Exception {
        admissionFirst(true);
    }

    @Test void rolledBackAdmissionStillLetsWaitingFenceCommit() throws Exception {
        admissionFirst(false);
    }

    private static void admissionFirst(boolean commitAdmission) throws Exception {
        UUID reader = UUID.randomUUID();
        new ArchiveReadLedger(tx(), reader);
        try (var admission = connection(); var fencing = connection(); var observer = connection()) {
            UUID object = archive(admission, false), entry = entry(admission, object), pin = UUID.randomUUID();
            admission.setAutoCommit(false);
            try {
                assertThat(acquire(admission, pin, reader, entry, object)).isTrue();
                int fencingPid = pid(fencing);
                var fenceResult = CompletableFuture.supplyAsync(() -> uncheckedFence(fencing, reader));
                awaitBlocked(observer, fencingPid);
                if (commitAdmission) admission.commit(); else admission.rollback();
                assertThat(fenceResult.get(10, TimeUnit.SECONDS)).isTrue();
                assertThat(state(observer, reader)).isEqualTo("FENCED");
                assertThat(number(observer, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'"))
                        .isEqualTo(commitAdmission ? 1 : 0);
                assertThatThrownBy(() -> acquire(observer, UUID.randomUUID(), reader, entry, object))
                        .hasMessageContaining("ACTIVE");
            } finally { admission.rollback(); admission.setAutoCommit(true); }
        }
    }

    @Test void fenceHoldingExclusiveLockMakesAdmissionWaitAndCommitRejectsIt() throws Exception {
        fenceFirst(true);
    }

    @Test void rolledBackFenceLetsWaitingAdmissionCommit() throws Exception {
        fenceFirst(false);
    }

    private static void fenceFirst(boolean commitFence) throws Exception {
        UUID reader = UUID.randomUUID(), otherReader = UUID.randomUUID();
        new ArchiveReadLedger(tx(), reader);
        new ArchiveReadLedger(tx(), otherReader);
        try (var fencing = connection(); var admission = connection(); var otherAdmission = connection();
                var observer = connection()) {
            UUID object = archive(fencing, false), entry = entry(fencing, object), pin = UUID.randomUUID();
            fencing.setAutoCommit(false);
            try {
                assertThat(fence(fencing, reader)).isTrue();
                int admissionPid = pid(admission);
                var acquireResult = CompletableFuture.supplyAsync(() -> {
                    try { return acquire(admission, pin, reader, entry, object); }
                    catch (SQLException e) { throw new RuntimeException(e); }
                });
                awaitBlocked(observer, admissionPid);
                UUID otherPin = UUID.randomUUID();
                var unrelatedResult = CompletableFuture.supplyAsync(() -> {
                    try {
                        boolean admitted = acquire(otherAdmission, otherPin, otherReader, entry, object);
                        return admitted && release(otherAdmission, otherPin, otherReader, object);
                    } catch (SQLException e) { throw new RuntimeException(e); }
                });
                assertThat(unrelatedResult.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(number(observer, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + otherPin + "'")).isZero();
                if (commitFence) fencing.commit(); else fencing.rollback();
                if (commitFence) {
                    assertThatThrownBy(() -> acquireResult.get(10, TimeUnit.SECONDS))
                            .satisfies(error -> assertThat(rootCause(error)).hasMessageContaining("ACTIVE"));
                    assertThat(number(observer, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isZero();
                } else {
                    assertThat(acquireResult.get(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(state(observer, reader)).isEqualTo("ACTIVE");
                    assertThat(number(observer, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isEqualTo(1);
                }
            } finally { fencing.rollback(); fencing.setAutoCommit(true); }
        }
    }

    private static Tx tx() {
        return new Tx(database.entityManagerFactory());
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void profile(Connection connection) throws Exception {
        execute(connection, """
                INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                VALUES('retention-original','s3','https://storage.example','us-east-1',true,'retention-realm')
                """);
    }

    private static String state(Connection connection, UUID reader) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT state FROM repository_reader_incarnations WHERE incarnation=?")) {
            statement.setObject(1, reader);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getString(1); }
        }
    }

    private static boolean fence(Connection connection, UUID reader) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT fence_repository_reader(?)")) {
            statement.setObject(1, reader);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }

    private static boolean uncheckedFence(Connection connection, UUID reader) {
        try { return fence(connection, reader); } catch (SQLException e) { throw new RuntimeException(e); }
    }

    private static boolean acquire(Connection connection, UUID pin, UUID reader, UUID entry, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM acquire_archive_read_pin(?,?,?,?,?)")) {
            statement.setObject(1, pin); statement.setObject(2, reader); statement.setObject(3, entry);
            statement.setLong(4, 1); statement.setObject(5, object);
            try (var result = statement.executeQuery()) { return result.next(); }
        }
    }

    private static boolean release(Connection connection, UUID pin, UUID reader, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT release_archive_read_pin(?,?,?)")) {
            statement.setObject(1, pin); statement.setObject(2, reader); statement.setObject(3, object);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }

    private static void directInsert(Connection connection, UUID pin, UUID reader, UUID object) throws Exception {
        execute(connection, """
                INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
                SELECT '%s','%s',object_id,entry_uuid,version FROM archive_version_object_refs WHERE object_id='%s'
                """.formatted(pin, reader, object));
    }

    private static UUID entry(Connection connection, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT entry_uuid FROM archive_object_bindings WHERE object_id=?")) {
            statement.setObject(1, object);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getObject(1, UUID.class); }
        }
    }

    private static int pid(Connection connection) throws SQLException {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(result.next()).isTrue(); return result.getInt(1);
        }
    }

    private static void awaitBlocked(Connection observer, int blockedPid) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (System.nanoTime() < deadline) {
            if (number(observer, "SELECT cardinality(pg_blocking_pids(" + blockedPid + "))") > 0) return;
            Thread.sleep(20);
        }
        fail("PostgreSQL backend " + blockedPid + " did not wait for the incarnation lock");
    }

    private static Throwable rootCause(Throwable error) {
        while (error.getCause() != null) error = error.getCause();
        return error;
    }
}
