package ai.protomolt.proto.repo.container.archive;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@Timeout(60)
class ArchiveStatisticsMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest
    @ValueSource(strings = {"entries", "versions", "retained_bytes", "current_bytes", "object_count", "total_bytes"})
    void refusesEveryNegativeExistingCounterWithoutRepairingIt(String column) throws Exception {
        String schema = "counter_" + UUID.randomUUID().toString().replace("-", "");
        migrate(schema, "74");
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var sql = connection.createStatement()) {
            connection.setSchema(schema);
            boolean rendition = column.equals("object_count") || column.equals("total_bytes");
            String table = rendition ? "archive_rendition_stats" : "archive_stats";
            sql.executeUpdate("INSERT INTO " + table + "(account_id,archive," + (rendition ? "rendition_name," : "")
                    + column + ") VALUES('account','records'," + (rendition ? "'original'," : "") + "-1)");
            assertThatThrownBy(() -> migrate(schema, "75"))
                    .hasStackTraceContaining("reconcile before migration");
            try (var rows = sql.executeQuery("SELECT " + column + " FROM " + table)) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getLong(1)).isEqualTo(-1);
            }
            // Explicit fixture repair, not an automatic production migration.
            sql.executeUpdate("UPDATE " + table + " SET " + column + "=0");
            migrate(schema, "75");
            assertThatThrownBy(() -> sql.executeUpdate("UPDATE " + table + " SET " + column + "=-1"))
                    .isInstanceOfSatisfying(SQLException.class, failure -> assertThat(failure.getSQLState()).isEqualTo("23514"));
        }
    }

    @Test void preservesExistingCountersAndGuardsDirectSqlWrites() throws Exception {
        String schema = "counter_" + UUID.randomUUID().toString().replace("-", "");
        migrate(schema, "74");
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var sql = connection.createStatement()) {
            connection.setSchema(schema);
            sql.executeUpdate("INSERT INTO archive_stats VALUES('account','records',1,2,3,4)");
            sql.executeUpdate("INSERT INTO archive_rendition_stats VALUES('account','records','original',5,6)");
            migrate(schema, "75");
            try (var rows = sql.executeQuery("SELECT entries,versions,retained_bytes,current_bytes FROM archive_stats")) {
                assertThat(rows.next()).isTrue();
                assertThat(new long[]{rows.getLong(1), rows.getLong(2), rows.getLong(3), rows.getLong(4)})
                        .containsExactly(1, 2, 3, 4);
            }
            try (var rows = sql.executeQuery("SELECT object_count,total_bytes FROM archive_rendition_stats")) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getLong(1)).isEqualTo(5); assertThat(rows.getLong(2)).isEqualTo(6);
            }
            for (String update : new String[]{
                    "UPDATE archive_stats SET entries=-1",
                    "UPDATE archive_rendition_stats SET total_bytes=-1",
                    "INSERT INTO archive_stats(account_id,archive,versions) VALUES('other','records',-1)",
                    "INSERT INTO archive_rendition_stats VALUES('other','records','original',-1,0)"})
                assertThatThrownBy(() -> sql.executeUpdate(update)).isInstanceOfSatisfying(SQLException.class,
                        failure -> assertThat(failure.getSQLState()).isEqualTo("23514"));
        }
    }

    private static void migrate(String schema, String target) {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target(target).load().migrate();
    }
}
