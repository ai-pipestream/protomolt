package ai.protomolt.proto.repo.container.ledger;

import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Fresh migration and restart keep the configured runtime pool at one connection. */
@Testcontainers
class LedgerSingleConnectionStartupIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test @Timeout(45) void migratesRestartsAndClosesWithoutBorrowingExtraRuntimeConnections() throws Exception {
        var config = new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(),
                1, LedgerConfig.DEFAULT_MIGRATION_LOCATION);
        try (var database = new LedgerDatabase(config)) {
            assertThat(((com.zaxxer.hikari.HikariDataSource) database.dataSource()).getMaximumPoolSize()).isEqualTo(1);
            var tx = new Tx(database.entityManagerFactory());
            tx.inTransaction(em -> {
                em.createNativeQuery("SELECT count(*) FROM document_operation_selection_current").getSingleResult();
                em.createNativeQuery("CREATE TABLE single_pool_probe(id integer PRIMARY KEY)").executeUpdate();
                em.createNativeQuery("INSERT INTO single_pool_probe VALUES (1)").executeUpdate();
                assertThat(((Number) em.createNativeQuery("""
                        SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                        """).getSingleResult()).longValue()).as("migration connections were closed").isZero();
            });
        }
        try (var database = new LedgerDatabase(config)) {
            assertThat(((com.zaxxer.hikari.HikariDataSource) database.dataSource()).getMaximumPoolSize()).isEqualTo(1);
            var tx = new Tx(database.entityManagerFactory());
            assertThat(tx.<Long>readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM single_pool_probe")
                    .getSingleResult()).longValue())).isEqualTo(1);
        }
        try (var observer = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = observer.createStatement();
                var result = statement.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getLong(1)).as("no runtime or migration sessions survive close").isZero();
        }
    }

    @Test @Timeout(30) void failedMigrationClosesBothMigrationAndRuntimeConnections(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path migrations) throws Exception {
        try (var admin = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = admin.createStatement()) {
            statement.execute("CREATE SCHEMA failed_migration_probe");
        }
        java.nio.file.Files.writeString(migrations.resolve("V1__intentional_failure.sql"), "SELECT 1 / 0;\n");
        String url = POSTGRES.getJdbcUrl() + (POSTGRES.getJdbcUrl().contains("?") ? "&" : "?")
                + "currentSchema=failed_migration_probe";
        var config = new LedgerConfig(url, POSTGRES.getUsername(), POSTGRES.getPassword(), 1,
                "filesystem:" + migrations);
        assertThatThrownBy(() -> new LedgerDatabase(config)).hasStackTraceContaining("division by zero");
        try (var observer = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = observer.createStatement();
                var result = statement.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getLong(1)).as("failed startup retained no connections").isZero();
        }
    }
}
