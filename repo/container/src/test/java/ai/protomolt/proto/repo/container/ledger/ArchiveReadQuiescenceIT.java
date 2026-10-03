package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveReadLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveReadRecovery;
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
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.RepositoryRetentionMigrationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Local lifetime evidence and bounded recovery against PostgreSQL retention triggers. */
@Testcontainers
@Timeout(60)
class ArchiveReadQuiescenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;

    @BeforeAll static void migrate() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").load().migrate();
        try (var connection = connection()) { profile(connection); }
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    @AfterAll static void closeDatabase() { if (database != null) database.close(); }

    @Test void failingFirstPageDoesNotStarveLaterPinsAndIsRetriedAfterWrap() throws Exception {
        UUID id = UUID.randomUUID();
        var ledger = new ArchiveReadLedger(tx(), id);
        var recovery = new ArchiveReadRecovery(tx());
        try (var connection = connection()) {
            UUID object = archive(connection, false), entry = entry(connection, object);
            var handles = java.util.List.of(ledger.acquire(entry, 1, object).orElseThrow(),
                    ledger.acquire(entry, 1, object).orElseThrow(), ledger.acquire(entry, 1, object).orElseThrow());
            String trigger = "fair_release_" + id.toString().replace("-", "");
            execute(connection, "CREATE FUNCTION " + trigger + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + "IF OLD.reader_incarnation='" + id + "' THEN RAISE EXCEPTION 'injected fairness failure'; END IF; RETURN OLD; END $$");
            execute(connection, "CREATE TRIGGER " + trigger + " BEFORE DELETE ON archive_read_pins FOR EACH ROW EXECUTE FUNCTION " + trigger + "()");
            try {
                ledger.fence();
                for (var handle : handles) assertThatThrownBy(handle::close).hasStackTraceContaining("injected fairness failure");
                ledger.attestLocalQuiescence();
                UUID first;
                try (var statement = connection.createStatement(); var result = statement.executeQuery(
                        "SELECT pin_id FROM archive_read_pins WHERE reader_incarnation='" + id + "' ORDER BY object_id,pin_id LIMIT 1")) {
                    assertThat(result.next()).isTrue(); first = result.getObject(1, UUID.class);
                }
                execute(connection, "CREATE OR REPLACE FUNCTION " + trigger + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                        + "IF OLD.pin_id='" + first + "' THEN RAISE EXCEPTION 'injected fairness failure'; END IF; RETURN OLD; END $$");
                assertThatThrownBy(() -> recovery.recover(1)).hasStackTraceContaining("injected fairness failure");
                assertThat(pinCount(connection, id)).isEqualTo(3);
                assertThat(recovery.recover(1)).isEqualTo(1);
                assertThat(pinCount(connection, id)).isEqualTo(2);
                assertThat(recovery.recover(1)).isEqualTo(1);
                assertThat(pinCount(connection, id)).isEqualTo(1);
                assertThatThrownBy(() -> recovery.recover(1)).hasStackTraceContaining("injected fairness failure");
                assertThat(pinCount(connection, id)).isEqualTo(1);
                // A restart resets scheduling, not durable retention or failure state.
                assertThatThrownBy(() -> new ArchiveReadRecovery(tx()).recover(1))
                        .hasStackTraceContaining("injected fairness failure");
                assertThat(pinCount(connection, id)).isEqualTo(1);
            } finally {
                execute(connection, "DROP TRIGGER " + trigger + " ON archive_read_pins");
                execute(connection, "DROP FUNCTION " + trigger + "()");
            }
            assertThat(recovery.recover(1)).isEqualTo(1);
            assertThat(pinCount(connection, id)).isZero();
        }
    }

    @Test void emptyAndSqlFailedAcquisitionsCompleteTheirLocalLifetimes() throws Exception {
        UUID id = UUID.randomUUID();
        var ledger = new ArchiveReadLedger(tx(), id);
        try (var connection = connection()) {
            UUID object = archive(connection, false), entry = entry(connection, object);
            assertThat(ledger.acquire(UUID.randomUUID(), 1, object)).isEmpty();
            String trigger = "fail_acquire_" + id.toString().replace("-", "");
            execute(connection, "CREATE FUNCTION " + trigger + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + "IF NEW.reader_incarnation='" + id + "' THEN RAISE EXCEPTION 'injected admission failure'; END IF; "
                    + "RETURN NEW; END $$");
            execute(connection, "CREATE TRIGGER " + trigger + " BEFORE INSERT ON archive_read_pins "
                    + "FOR EACH ROW EXECUTE FUNCTION " + trigger + "()");
            try {
                assertThatThrownBy(() -> ledger.acquire(entry, 1, object)).satisfies(error ->
                        assertThat(rootCause(error)).hasMessageContaining("injected admission failure"));
            } finally {
                execute(connection, "DROP TRIGGER " + trigger + " ON archive_read_pins");
                execute(connection, "DROP FUNCTION " + trigger + "()");
            }
            assertThat(pinCount(connection, id)).isZero();
            ledger.fence();
            ledger.attestLocalQuiescence();
            assertThat(state(connection, id)).isEqualTo("QUIESCED");
        }
    }

    @Test void livePinAndAnotherReaderSharingTheLedgerPreventAttestation() throws Exception {
        UUID id = UUID.randomUUID();
        var ledger = new ArchiveReadLedger(tx(), id);
        try (var connection = connection()) {
            UUID object = archive(connection, false);
            var first = ledger.acquire(entry(connection, object), 1, object).orElseThrow();
            var second = ledger.acquire(entry(connection, object), 1, object).orElseThrow();
            assertThatThrownBy(ledger::attestLocalQuiescence).isInstanceOf(IllegalStateException.class);
            ledger.fence();
            assertThatThrownBy(ledger::attestLocalQuiescence).isInstanceOf(IllegalStateException.class);
            first.close();
            assertThatThrownBy(ledger::attestLocalQuiescence).isInstanceOf(IllegalStateException.class);
            assertThat(pinCount(connection, id)).isEqualTo(1);
            second.close();
            ledger.attestLocalQuiescence();
            assertThat(state(connection, id)).isEqualTo("QUIESCED");
            assertThat(number(connection, "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation='" + id
                    + "' AND quiesced_at IS NOT NULL AND quiescence_source='LOCAL_DRAIN'")).isEqualTo(1);
            assertThatThrownBy(() -> ledger.acquire(entry(connection, object), 1, object))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        }
    }

    @Test void pendingSqlAcquisitionCannotBeAttestedAndFenceWaitsForIt() throws Exception {
        UUID id = UUID.randomUUID();
        var ledger = new ArchiveReadLedger(tx(), id);
        try (var owner = connection(); var observer = connection()) {
            UUID object = archive(owner, false), entry = entry(owner, object);
            owner.setAutoCommit(false);
            try {
                try (var statement = owner.prepareStatement(
                        "SELECT 1 FROM archive_object_uploads WHERE object_id=? FOR UPDATE")) {
                    statement.setObject(1, object);
                    try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); }
                }
                var acquiring = CompletableFuture.supplyAsync(() -> ledger.acquire(entry, 1, object).orElseThrow());
                awaitBlockedAcquisition(observer);
                assertThatThrownBy(ledger::attestLocalQuiescence).isInstanceOf(IllegalStateException.class);
                var fencing = CompletableFuture.runAsync(ledger::fence);
                owner.commit();
                var pin = acquiring.get(10, TimeUnit.SECONDS);
                fencing.get(10, TimeUnit.SECONDS);
                assertThatThrownBy(ledger::attestLocalQuiescence).isInstanceOf(IllegalStateException.class);
                pin.close();
                ledger.attestLocalQuiescence();
                assertThat(state(observer, id)).isEqualTo("QUIESCED");
            } finally { owner.rollback(); owner.setAutoCommit(true); }
        }
    }

    @Test void failedReleaseLeavesBothReferencesForBoundedRecovery() throws Exception {
        UUID id = UUID.randomUUID();
        var ledger = new ArchiveReadLedger(tx(), id);
        var recovery = new ArchiveReadRecovery(tx());
        try (var connection = connection()) {
            UUID first = archive(connection, false), second = archive(connection, false);
            var firstPin = ledger.acquire(entry(connection, first), 1, first).orElseThrow();
            var secondPin = ledger.acquire(entry(connection, second), 1, second).orElseThrow();
            String trigger = "fail_release_" + id.toString().replace("-", "");
            execute(connection, "CREATE FUNCTION " + trigger + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + "IF OLD.reader_incarnation='" + id + "' THEN RAISE EXCEPTION 'injected release failure'; END IF; "
                    + "RETURN OLD; END $$");
            execute(connection, "CREATE TRIGGER " + trigger + " BEFORE DELETE ON archive_read_pins "
                    + "FOR EACH ROW EXECUTE FUNCTION " + trigger + "()");
            try {
                ledger.fence();
                assertThatThrownBy(firstPin::close).satisfies(error ->
                        assertThat(rootCause(error)).hasMessageContaining("injected release failure"));
                assertThatThrownBy(secondPin::close).satisfies(error ->
                        assertThat(rootCause(error)).hasMessageContaining("injected release failure"));
                ledger.attestLocalQuiescence();
                assertThat(pinCount(connection, id)).isEqualTo(2);
                assertThat(mirrorCount(connection, first, second)).isEqualTo(2);
                assertThatThrownBy(() -> recovery.recover(1)).satisfies(error ->
                        assertThat(rootCause(error)).hasMessageContaining("injected release failure"));
                assertThat(pinCount(connection, id)).isEqualTo(2);
            } finally {
                execute(connection, "DROP TRIGGER " + trigger + " ON archive_read_pins");
                execute(connection, "DROP FUNCTION " + trigger + "()");
            }
            execute(connection, "CREATE FUNCTION " + trigger + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + "IF OLD.object_id='" + first + "' THEN RAISE EXCEPTION 'one object still fails'; END IF; "
                    + "RETURN OLD; END $$");
            execute(connection, "CREATE TRIGGER " + trigger + " BEFORE DELETE ON archive_read_pins "
                    + "FOR EACH ROW EXECUTE FUNCTION " + trigger + "()");
            try {
                // A fresh pass selects both pins to exercise partial-batch failure.
                assertThatThrownBy(() -> new ArchiveReadRecovery(tx()).recover(2)).satisfies(error ->
                        assertThat(rootCause(error)).hasMessageContaining("one object still fails"));
                assertThat(pinCount(connection, id)).isEqualTo(1);
                assertThat(mirrorCount(connection, first, second)).isEqualTo(1);
            } finally {
                execute(connection, "DROP TRIGGER " + trigger + " ON archive_read_pins");
                execute(connection, "DROP FUNCTION " + trigger + "()");
            }
            var normalRelease = CompletableFuture.runAsync(firstPin::close);
            int recovered = recovery.recover(1);
            normalRelease.get(10, TimeUnit.SECONDS);
            assertThat(recovered).isBetween(0, 1);
            assertThat(pinCount(connection, id)).isZero();
            assertThat(recovery.recover(1)).isZero();
            assertThat(pinCount(connection, id)).isZero();
            assertThat(mirrorCount(connection, first, second)).isZero();
            firstPin.close();
            secondPin.close();
            ledger.attestLocalQuiescence();
            assertThat(state(connection, id)).isEqualTo("QUIESCED");
        }
    }

    @Test void fencedAndUnknownPinsRemainProtected() throws Exception {
        UUID fencedId = UUID.randomUUID();
        var ledger = new ArchiveReadLedger(tx(), fencedId);
        var recovery = new ArchiveReadRecovery(tx());
        try (var connection = connection()) {
            UUID object = archive(connection, false);
            var pin = ledger.acquire(entry(connection, object), 1, object).orElseThrow();
            ledger.fence();
            assertThat(recovery.recover(1)).isZero();
            assertThat(pinCount(connection, fencedId)).isEqualTo(1);
            assertThatThrownBy(() -> recoverSql(connection, pinId(connection, fencedId), fencedId, object))
                    .hasMessageContaining("proven quiescence");
            pin.close();
            ledger.attestLocalQuiescence();
        }

        String schema = "quiescence_unknown_" + UUID.randomUUID().toString().replace("-", "");
        try (var admin = connection()) { execute(admin, "CREATE SCHEMA " + schema); }
        try {
            var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).locations("classpath:db/migration/repo");
            config.target("30").load().migrate();
            UUID unknown = UUID.randomUUID(), pin = UUID.randomUUID(), object;
            try (var connection = connection()) {
                execute(connection, "SET search_path TO " + schema);
                profile(connection);
                object = archive(connection, false);
                execute(connection, "INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version) "
                        + "SELECT '" + pin + "','" + unknown + "',object_id,entry_uuid,version "
                        + "FROM archive_version_object_refs WHERE object_id='" + object + "'");
            }
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).locations("classpath:db/migration/repo").load().migrate();
            try (var connection = connection()) {
                execute(connection, "SET search_path TO " + schema);
                assertThat(state(connection, unknown)).isEqualTo("UNKNOWN");
                assertThatThrownBy(() -> attestSql(connection, unknown)).hasMessageContaining("fenced incarnation");
                assertThatThrownBy(() -> recoverSql(connection, pin, unknown, object))
                        .hasMessageContaining("proven quiescence");
                assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isEqualTo(1);
                assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id='" + pin + "'")).isEqualTo(1);
            }
        } finally {
            try (var admin = connection()) { execute(admin, "DROP SCHEMA " + schema + " CASCADE"); }
        }
    }

    @Test void evidenceIsImmutableAndRepeatedAttestationIsIdempotent() throws Exception {
        UUID id = UUID.randomUUID();
        var ledger = new ArchiveReadLedger(tx(), id);
        var recovery = new ArchiveReadRecovery(tx());
        try (var connection = connection()) {
            UUID object = archive(connection, false);
            var pin = ledger.acquire(entry(connection, object), 1, object).orElseThrow();
            ledger.fence();
            pin.close();
            ledger.attestLocalQuiescence();
            ledger.attestLocalQuiescence();
            assertThat(attestSql(connection, id)).isTrue();
            assertThatThrownBy(() -> execute(connection, "UPDATE repository_reader_incarnations "
                    + "SET quiescence_source='OTHER' WHERE incarnation='" + id + "'"))
                    .hasMessageContaining("immutable");
            assertThatThrownBy(() -> execute(connection, "UPDATE repository_reader_incarnations "
                    + "SET quiesced_at=clock_timestamp()+interval '1 second' WHERE incarnation='" + id + "'"))
                    .hasMessageContaining("immutable");
            assertThatThrownBy(() -> execute(connection, "UPDATE repository_reader_incarnations "
                    + "SET state='FENCED' WHERE incarnation='" + id + "'"))
                    .hasMessageContaining("permanent");
            assertThatThrownBy(() -> execute(connection, "DELETE FROM repository_reader_incarnations WHERE incarnation='" + id + "'"))
                    .hasMessageContaining("permanent");
            assertThat(recovery.recover(1)).isZero();
        }
    }

    @Test void v31FencedIdentityMigratesWithoutImpliedQuiescence() throws Exception {
        String schema = "quiescence_fenced_" + UUID.randomUUID().toString().replace("-", "");
        try (var admin = connection()) { execute(admin, "CREATE SCHEMA " + schema); }
        try {
            var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).locations("classpath:db/migration/repo");
            config.target("31").load().migrate();
            UUID id = UUID.randomUUID();
            try (var connection = connection()) {
                execute(connection, "SET search_path TO " + schema);
                execute(connection, "INSERT INTO repository_reader_incarnations(incarnation,state) VALUES('" + id + "','ACTIVE')");
                assertThat(fenceSql(connection, id)).isTrue();
            }
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(schema).locations("classpath:db/migration/repo").load().migrate();
            try (var connection = connection()) {
                execute(connection, "SET search_path TO " + schema);
                assertThat(state(connection, id)).isEqualTo("FENCED");
                assertThat(number(connection, "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation='" + id
                        + "' AND quiesced_at IS NULL AND quiescence_source IS NULL")).isEqualTo(1);
            }
        } finally {
            try (var admin = connection()) { execute(admin, "DROP SCHEMA " + schema + " CASCADE"); }
        }
    }

    private static Tx tx() { return new Tx(database.entityManagerFactory()); }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void profile(Connection connection) throws Exception {
        execute(connection, "INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm) "
                + "VALUES('retention-original','s3','https://storage.example','us-east-1',true,'retention-realm')");
    }

    private static UUID entry(Connection connection, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT entry_uuid FROM archive_object_bindings WHERE object_id=?")) {
            statement.setObject(1, object);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getObject(1, UUID.class); }
        }
    }

    private static String state(Connection connection, UUID id) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT state FROM repository_reader_incarnations WHERE incarnation=?")) {
            statement.setObject(1, id);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getString(1); }
        }
    }

    private static long pinCount(Connection connection, UUID id) throws Exception {
        return number(connection, "SELECT count(*) FROM archive_read_pins WHERE reader_incarnation='" + id + "'");
    }

    private static long mirrorCount(Connection connection, UUID first, UUID second) throws Exception {
        return number(connection, "SELECT count(*) FROM repository_object_references "
                + "WHERE owner_kind='ARCHIVE_READER' AND object_id IN ('" + first + "','" + second + "')");
    }

    private static UUID pinId(Connection connection, UUID id) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT pin_id FROM archive_read_pins WHERE reader_incarnation=?")) {
            statement.setObject(1, id);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getObject(1, UUID.class); }
        }
    }

    private static boolean attestSql(Connection connection, UUID id) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT attest_local_reader_quiescence(?)")) {
            statement.setObject(1, id);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }

    private static boolean fenceSql(Connection connection, UUID id) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT fence_repository_reader(?)")) {
            statement.setObject(1, id);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }

    private static boolean recoverSql(Connection connection, UUID pin, UUID reader, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT recover_quiesced_archive_read_pin(?,?,?)")) {
            statement.setObject(1, pin); statement.setObject(2, reader); statement.setObject(3, object);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }

    private static void awaitBlockedAcquisition(Connection observer) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (System.nanoTime() < deadline) {
            long blocked = number(observer, "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%acquire_archive_read_pin%' "
                    + "AND wait_event_type='Lock' AND pid<>pg_backend_pid()");
            if (blocked > 0) return;
            Thread.sleep(20);
        }
        fail("Archive pin admission did not reach the PostgreSQL lock");
    }

    private static Throwable rootCause(Throwable error) {
        while (error.getCause() != null) error = error.getCause();
        return error;
    }
}
