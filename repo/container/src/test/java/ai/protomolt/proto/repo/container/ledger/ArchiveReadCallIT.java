package ai.protomolt.proto.repo.container.ledger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.RepositoryRetentionMigrationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL admission/release contracts; synthetic lifecycle metadata, no byte-verification claim. */
@Testcontainers
class ArchiveReadCallIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @BeforeAll static void migrate() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").load().migrate();
        try (var connection = connection()) {
            execute(connection, """
                    INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                    VALUES('retention-original','s3','https://storage.example','us-east-1',true,'retention-realm')
                    """);
        }
    }

    @Test void ledgerUsesOneStatementPerAdmissionAndRelease() throws Exception {
        try (var connection = connection(); var database = new LedgerDatabase(new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()))) {
            UUID object = archive(connection, false), entry = entry(connection, object);
            var reads = new ai.protomolt.proto.repo.container.archive.ArchiveReadLedger(
                    new Tx(database.entityManagerFactory()), UUID.randomUUID());
            var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
            statistics.setStatisticsEnabled(true);
            statistics.clear();
            var pin = reads.acquire(entry, 1, object).orElseThrow();
            try {
                assertThat(statistics.getPrepareStatementCount()).isEqualTo(1);
                assertThat(statistics.getSuccessfulTransactionCount()).isEqualTo(1);
                assertThat(pin.readable().binding().objectId()).isEqualTo(object);
            } finally { pin.close(); }
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(2);
            assertThat(statistics.getSuccessfulTransactionCount()).isEqualTo(2);
            pin.close();
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(2);
        }
    }

    @Test void admissionReturnsOriginalIdentityAndRollsBackThePinWithTheTransaction() throws Exception {
        try (var connection = connection()) {
            UUID object = archive(connection, false), entry = entry(connection, object), pin = UUID.randomUUID(), reader = registeredReader(connection);
            connection.setAutoCommit(false);
            try {
                assertThat(acquire(connection, pin, reader, entry, 1, object)).isTrue();
                assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isEqualTo(1);
                assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id='" + pin + "'")).isEqualTo(1);
                connection.rollback();
                assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isZero();
                assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id='" + pin + "'")).isZero();
            } finally { connection.rollback(); connection.setAutoCommit(true); }
            assertThat(acquire(connection, pin, reader, entry, 1, object)).isTrue();
            assertThat(release(connection, pin, reader, object)).isTrue();
        }
    }

    @Test void unavailableIdentitiesNeverCreatePins() throws Exception {
        try (var connection = connection()) {
            UUID object = archive(connection, false), entry = entry(connection, object), pin = UUID.randomUUID(), reader = registeredReader(connection);
            assertThat(acquire(connection, pin, reader, UUID.randomUUID(), 1, object)).isFalse();
            assertThat(acquire(connection, pin, reader, entry, 2, object)).isFalse();
            assertThat(acquire(connection, pin, reader, entry, 1, UUID.randomUUID())).isFalse();
            assertThatThrownBy(() -> acquire(connection, pin, reader, entry, 0, object)).hasMessageContaining("positive version");
            assertThatThrownBy(() -> acquire(connection, pin, null, entry, 1, object)).hasMessageContaining("complete identity");
            execute(connection, "UPDATE repository_object_retention SET retiring=true WHERE object_id='" + object + "'");
            assertThat(acquire(connection, pin, reader, entry, 1, object)).isFalse();
            assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isZero();
        }
    }

    @Test void releaseChecksIncarnationAndObjectAndCanRepeatAfterACommittedRelease() throws Exception {
        try (var connection = connection()) {
            UUID object = archive(connection, false), other = archive(connection, false), entry = entry(connection, object);
            UUID pin = UUID.randomUUID(), reader = registeredReader(connection);
            assertThat(acquire(connection, pin, reader, entry, 1, object)).isTrue();
            assertThatThrownBy(() -> release(connection, pin, UUID.randomUUID(), object)).hasMessageContaining("another incarnation or object");
            assertThatThrownBy(() -> release(connection, pin, reader, other)).hasMessageContaining("another incarnation or object");
            assertThat(number(connection, "SELECT count(*) FROM archive_read_pins WHERE pin_id='" + pin + "'")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE owner_kind='ARCHIVE_READER' AND owner_id='" + pin + "'")).isEqualTo(1);
            assertThat(release(connection, pin, reader, object)).isTrue();
            assertThat(release(connection, pin, reader, object)).isTrue();
            assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE object_id='" + object + "'")).isEqualTo(1);
        }
    }

    private static UUID registeredReader(Connection connection) throws Exception {
        UUID id = UUID.randomUUID();
        execute(connection, "INSERT INTO repository_reader_incarnations VALUES('" + id + "','ACTIVE')");
        return id;
    }

    private static boolean acquire(Connection connection, UUID pin, UUID reader, UUID entry, long version, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM acquire_archive_read_pin(?,?,?,?,?)")) {
            statement.setObject(1, pin); statement.setObject(2, reader); statement.setObject(3, entry);
            statement.setLong(4, version); statement.setObject(5, object);
            try (var result = statement.executeQuery()) {
                if (!result.next()) return false;
                assertThat(result.getObject("object_id", UUID.class)).isEqualTo(object);
                assertThat(result.getObject("entry_uuid", UUID.class)).isEqualTo(entry);
                assertThat(result.getString("backend_generation")).isEqualTo("retention-original");
                assertThat(result.getString("storage_realm")).isEqualTo("retention-realm");
                assertThat(result.getString("bucket")).isEqualTo("namespace");
                assertThat(result.getString("object_key")).isEqualTo("object-" + object);
                assertThat(result.getLong("expected_size")).isZero();
                assertThat(result.getString("sha256")).isEqualTo("a".repeat(64));
                assertThat(result.getString("provider_version")).isNull();
                assertThat(result.next()).isFalse();
                return true;
            }
        }
    }

    private static boolean release(Connection connection, UUID pin, UUID reader, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT release_archive_read_pin(?,?,?)")) {
            statement.setObject(1, pin); statement.setObject(2, reader); statement.setObject(3, object);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getBoolean(1); }
        }
    }

    private static UUID entry(Connection connection, UUID object) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT entry_uuid FROM archive_object_bindings WHERE object_id=?")) {
            statement.setObject(1, object);
            try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); return result.getObject(1, UUID.class); }
        }
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
