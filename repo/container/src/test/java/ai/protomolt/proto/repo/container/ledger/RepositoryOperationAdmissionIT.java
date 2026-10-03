package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import java.sql.Connection;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Opaque fixture bytes test storage identity, not semantic validation or authorization. */
@Testcontainers
class RepositoryOperationAdmissionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static RepositoryOperationLedger ledger;
    private static final Duration LEASE = Duration.ofSeconds(30);

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); ledger = new RepositoryOperationLedger(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void exactReplaySurvivesNewLedgerWithoutRenewalOrExposingAnotherOwner() {
        var key = key(); var command = command("one"); var nonce = UUID.randomUUID();
        var admitted = ledger.admit(key, command, nonce, LEASE);
        var reconnected = new RepositoryOperationLedger(new Tx(database.entityManagerFactory()));
        assertThat(reconnected.admit(key, command, nonce, Duration.ofHours(1))).isEqualTo(admitted);
        assertThat(reconnected.admit(key, command, UUID.randomUUID(), LEASE).owner()).isEmpty();
        var observed = reconnected.find(key).orElseThrow();
        assertThat(observed).isEqualTo(admitted.snapshot());
        assertThat(observed.toString()).doesNotContain(nonce.toString(), "one");
        assertThat(admitted.owner().orElseThrow().toString()).doesNotContain(nonce.toString());
    }

    @ParameterizedTest @ValueSource(strings = {"bytes", "codec", "version"})
    void differentCommandConflictsWhilePending(String change) {
        var key = key(); var original = command("original");
        ledger.admit(key, original, UUID.randomUUID(), LEASE);
        var changed = switch (change) {
            case "codec" -> new RepositoryOperationLedger.EncodedCommand("another.fixture", 1, original.bytes());
            case "version" -> new RepositoryOperationLedger.EncodedCommand(original.codec(), 2, original.bytes());
            default -> command("changed");
        };
        assertThatThrownBy(() -> ledger.admit(key, changed, UUID.randomUUID(), LEASE))
                .isInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
        assertThat(ledger.find(key).orElseThrow().command()).isEqualTo(original);
    }

    @Test void scopeUsesAccountPrincipalAndOperationTogether() {
        var id = UUID.randomUUID();
        var first = new RepositoryOperationLedger.Key("account-a", "principal-a", id);
        var otherAccount = new RepositoryOperationLedger.Key("account-b", "principal-a", id);
        var otherPrincipal = new RepositoryOperationLedger.Key("account-a", "principal-b", id);
        ledger.admit(first, command("first"), UUID.randomUUID(), LEASE);
        assertThat(ledger.find(otherAccount)).isEmpty();
        assertThat(ledger.find(otherPrincipal)).isEmpty();
        ledger.admit(otherAccount, command("second"), UUID.randomUUID(), LEASE);
        ledger.admit(otherPrincipal, command("third"), UUID.randomUUID(), LEASE);
        assertThat(ledger.find(first).orElseThrow().command()).isEqualTo(command("first"));
    }

    @Test void expiredOwnerCannotRenewAndTakeoverReplayDoesNotExtendLease() {
        var key = key(); var originalNonce = UUID.randomUUID(); var command = command("takeover");
        var original = ledger.admit(key, command, originalNonce, Duration.ofSeconds(1)).owner().orElseThrow();
        expire(key);
        assertThatThrownBy(() -> ledger.renew(original, LEASE)).isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        assertThat(ledger.admit(key, command, originalNonce, LEASE).owner()).isEmpty();
        var nonce = UUID.randomUUID();
        var taken = ledger.takeOver(key, 1, nonce, LEASE);
        assertThat(taken.generation()).isEqualTo(2);
        assertThat(ledger.takeOver(key, 1, nonce, Duration.ofHours(1))).isEqualTo(taken);
        assertThatThrownBy(() -> ledger.renew(original, LEASE)).isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        assertThatThrownBy(() -> ledger.takeOver(key, 2, UUID.randomUUID(), LEASE))
                .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        var renewed = ledger.renew(taken, Duration.ofMinutes(1));
        assertThat(renewed.generation()).isEqualTo(2);
        assertThat(renewed.leaseUntil()).isAfter(taken.leaseUntil());
    }

    @Test void sizeAndLeaseBoundsAreEnforced() {
        assertThatThrownBy(() -> command("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RepositoryOperationLedger.EncodedCommand("fixture", 1,
                ByteString.copyFrom(new byte[1048577]))).isInstanceOf(IllegalArgumentException.class);
        var maximum = new RepositoryOperationLedger.EncodedCommand("fixture", 1, ByteString.copyFrom(new byte[1048576]));
        var key = key();
        assertThat(ledger.admit(key, maximum, UUID.randomUUID(), LEASE).snapshot().command()).isEqualTo(maximum);
        assertThatThrownBy(() -> ledger.admit(key(), command("x"), UUID.randomUUID(), Duration.ofMillis(999)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ledger.takeOver(key, Long.MAX_VALUE, UUID.randomUUID(), LEASE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void admissionWaitSeesWinnerCommitOrRollback(boolean commit, boolean sameBytes) throws Exception {
        var key = key(); var first = command("first"); var requested = sameBytes ? first : command("second");
        try (var connection = database.dataSource().getConnection(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            connection.setAutoCommit(false);
            int pid = pid(connection);
            insert(connection, key, first, UUID.randomUUID());
            // Absence while this INSERT is uncommitted is not proof of rollback.
            assertThat(ledger.find(key)).isEmpty();
            var waiting = executor.submit(() -> ledger.admit(key, requested, UUID.randomUUID(), LEASE));
            try {
                awaitBlocked(pid);
                assertThat(ledger.admit(key(), command("independent"), UUID.randomUUID(), LEASE).owner()).isPresent();
                if (commit) connection.commit(); else connection.rollback();
                if (commit && !sameBytes) {
                    assertThatThrownBy(() -> waiting.get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
                } else {
                    var result = waiting.get(5, TimeUnit.SECONDS);
                    assertThat(result.snapshot().command()).isEqualTo(requested);
                    assertThat(result.owner().isPresent()).isEqualTo(!commit);
                }
                assertThat(ledger.find(key).orElseThrow().command()).isEqualTo(commit ? first : requested);
            } finally { connection.rollback(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void takeoverWaitsForRenewalCommitOrRollback(boolean commitRenewal) throws Exception {
        var key = key(); ledger.admit(key, command("renew-race"), UUID.randomUUID(), Duration.ofSeconds(2));
        try (var connection = database.dataSource().getConnection(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            connection.setAutoCommit(false);
            int pid = pid(connection);
            try (var statement = connection.prepareStatement("UPDATE repository_operation_owners SET lease_until=clock_timestamp()+interval '30 seconds' WHERE operation_id=?")) {
                statement.setObject(1, key.operationId()); statement.executeUpdate();
            }
            var takeover = executor.submit(() -> ledger.takeOver(key, 1, UUID.randomUUID(), LEASE));
            try {
                awaitBlocked(pid);
                expire(key); // Sees the old committed lease, not the held renewal.
                if (commitRenewal) connection.commit(); else connection.rollback();
                if (commitRenewal) assertThatThrownBy(() -> takeover.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                else assertThat(takeover.get(5, TimeUnit.SECONDS).generation()).isEqualTo(2);
            } finally { connection.rollback(); }
        }
    }

    @Test void renewalRechecksDatabaseClockAfterLockWait() throws Exception {
        var key = key(); var owner = ledger.admit(key, command("expire-in-wait"), UUID.randomUUID(), Duration.ofSeconds(2)).owner().orElseThrow();
        try (var connection = database.dataSource().getConnection(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            connection.setAutoCommit(false);
            int pid = pid(connection);
            try (var statement = connection.prepareStatement("SELECT operation_id FROM repository_operation_owners WHERE operation_id=? FOR UPDATE")) {
                statement.setObject(1, key.operationId()); statement.executeQuery().close();
            }
            var renewal = executor.submit(() -> ledger.renew(owner, LEASE));
            try {
                awaitBlocked(pid); expire(key); connection.commit();
                assertThatThrownBy(() -> renewal.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
                assertThat(ledger.takeOver(key, 1, UUID.randomUUID(), LEASE).generation()).isEqualTo(2);
            } finally { connection.rollback(); }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"command", "codec", "version", "digest", "token", "generation", "created", "delete", "delete-owner", "lease"})
    void sqlCannotRewriteIdentityOrBypassLiveOwner(String change) {
        var key = key(); var snapshot = ledger.admit(key, command("guarded"), UUID.randomUUID(), LEASE).snapshot();
        String mutation = switch (change) {
            case "command" -> "UPDATE repository_operations SET command=decode('01','hex')";
            case "codec" -> "UPDATE repository_operations SET command_codec='different'";
            case "version" -> "UPDATE repository_operations SET command_version=2";
            case "digest" -> "UPDATE repository_operations SET command_sha256=decode('01','hex')";
            case "token" -> "UPDATE repository_operation_owners SET owner_token=gen_random_uuid()";
            case "generation" -> "UPDATE repository_operation_owners SET owner_generation=owner_generation+2,owner_token=gen_random_uuid()";
            case "created" -> "UPDATE repository_operations SET created_at=created_at+interval '1 second'";
            case "delete-owner" -> "DELETE FROM repository_operation_owners";
            case "lease" -> "UPDATE repository_operation_owners SET lease_until=clock_timestamp()+interval '2 days'";
            default -> "DELETE FROM repository_operations";
        };
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery(mutation + " WHERE operation_id=:id").setParameter("id", key.operationId()).executeUpdate();
        })).hasStackTraceContaining("Repository operation");
        assertThat(ledger.find(key).orElseThrow()).isEqualTo(snapshot);
    }

    @Test void commandWithoutOwnerCannotCommit() {
        var key = key();
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO repository_operations(account_id,principal,operation_id,command_codec,
                        command_version,command,command_sha256)
                    VALUES ('account','principal',:id,'fixture',1,decode('01','hex'),sha256(decode('01','hex')))
                    """).setParameter("id", key.operationId()).executeUpdate();
        })).hasStackTraceContaining("requires atomic owner admission");
        assertThat(ledger.find(key)).isEmpty();
        long remaining = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_operations WHERE operation_id=:id")
                .setParameter("id", key.operationId()).getSingleResult()).longValue());
        assertThat(remaining).isZero();
    }

    @Test void conflictedAdmissionStillRequiresReadCommitted() throws Exception {
        var key = key(); ledger.admit(key, command("isolation"), UUID.randomUUID(), LEASE);
        try (var connection = database.dataSource().getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("""
                    INSERT INTO repository_operations(account_id,principal,operation_id,command_codec,
                        command_version,command,command_sha256)
                    SELECT account_id,principal,operation_id,command_codec,command_version,command,command_sha256
                    FROM repository_operations WHERE operation_id=?
                    ON CONFLICT(account_id,principal,operation_id) DO NOTHING
                    """)) {
                statement.setObject(1, key.operationId());
                assertThatThrownBy(statement::executeUpdate).hasMessageContaining("READ COMMITTED");
            } finally { connection.rollback(); }
        }
    }

    @Test void maximumCommandHeartbeatHasOneTransactionAndBoundedClientStatements() {
        var large = new RepositoryOperationLedger.EncodedCommand("fixture", 1, ByteString.copyFrom(new byte[1048576]));
        var owner = ledger.admit(key(), large, UUID.randomUUID(), LEASE).owner().orElseThrow();
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        statistics.clear();
        try {
            assertThat(ledger.renew(owner, Duration.ofMinutes(1)).generation()).isEqualTo(1);
            assertThat(statistics.getTransactionCount()).isEqualTo(1);
            assertThat(statistics.getPrepareStatementCount()).isBetween(1L, 4L);
        } finally { statistics.setStatisticsEnabled(false); }
    }

    @Test void migrationPreservesExistingReaderIdentity() throws Exception {
        String schema = "admission_migration";
        org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("33").load().migrate();
        UUID incarnation = UUID.randomUUID();
        try (var connection = database.dataSource().getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO admission_migration.repository_reader_incarnations(incarnation,state) VALUES (?,'ACTIVE')")) {
            statement.setObject(1, incarnation); statement.executeUpdate();
        }
        org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
        try (var connection = database.dataSource().getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("SELECT incarnation,state FROM admission_migration.repository_reader_incarnations")) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getObject(1)).isEqualTo(incarnation);
                assertThat(rows.getString(2)).isEqualTo("ACTIVE"); assertThat(rows.next()).isFalse();
            }
            try (var rows = statement.executeQuery("SELECT count(*) FROM admission_migration.repository_operations")) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getLong(1)).isZero();
            }
        }
    }

    private static RepositoryOperationLedger.Key key() { return new RepositoryOperationLedger.Key("account", "principal", UUID.randomUUID()); }
    private static RepositoryOperationLedger.EncodedCommand command(String value) {
        return new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8(value));
    }
    private static int pid(Connection connection) throws Exception {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
            assertThat(rows.next()).isTrue(); return rows.getInt(1);
        }
    }
    private static void insert(Connection connection, RepositoryOperationLedger.Key key,
            RepositoryOperationLedger.EncodedCommand command, UUID owner) throws Exception {
        try (var statement = connection.prepareStatement("""
                INSERT INTO repository_operations(account_id,principal,operation_id,command_codec,command_version,
                    command,command_sha256) VALUES (?,?,?,?,?,?,sha256(?))
                """)) {
            statement.setString(1, key.account()); statement.setString(2, key.principal()); statement.setObject(3, key.operationId());
            statement.setString(4, command.codec()); statement.setInt(5, command.version());
            statement.setBytes(6, command.bytes().toByteArray()); statement.setBytes(7, command.bytes().toByteArray());
            statement.executeUpdate();
        }
        try (var statement = connection.prepareStatement("""
                INSERT INTO repository_operation_owners(account_id,principal,operation_id,owner_token,owner_generation,lease_until)
                VALUES (?,?,?,?,1,clock_timestamp()+interval '30 seconds')
                """)) {
            statement.setString(1, key.account()); statement.setString(2, key.principal()); statement.setObject(3, key.operationId());
            statement.setObject(4, owner); statement.executeUpdate();
        }
    }
    private static void expire(RepositoryOperationLedger.Key key) {
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                FROM repository_operation_owners WHERE operation_id=:id
                """).setParameter("id", key.operationId()).getSingleResult());
    }
    private static void awaitBlocked(int pid) throws Exception {
        boolean waiting = false; long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!waiting && System.nanoTime() < end) {
            waiting = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid)))")
                    .setParameter("pid", pid).getSingleResult());
            if (!waiting) Thread.sleep(10);
        }
        assertThat(waiting).as("contender waits on the held PostgreSQL transaction").isTrue();
    }
}
