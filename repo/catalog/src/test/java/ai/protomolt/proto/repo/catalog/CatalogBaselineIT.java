package ai.protomolt.proto.repo.catalog;

import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** The fresh baseline initializes an empty catalog, continues its own, and refuses anything else. */
@Testcontainers
class CatalogBaselineIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void initializesAnEmptySchemaAndReopensItsOwnCatalog() throws Exception {
        var url = database("fresh");
        try (var catalog = new PostgresCatalog(config(url))) {
            assertThat(catalog.outcome("acct", "op")).isEmpty();
        }
        try (var reopened = new PostgresCatalog(config(url))) {
            assertThat(reopened.outcome("acct", "op")).isEmpty();
        }
        try (var connection = DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var rows = connection.createStatement().executeQuery(
                     "SELECT count(*) FROM " + CatalogBaseline.HISTORY_TABLE + " WHERE success")) {
            rows.next();
            assertThat(rows.getInt(1)).isEqualTo(3);
        }
    }

    @Test void refusesASchemaThatAlreadyHoldsTablesEvenWithoutFlywayHistory() throws Exception {
        var url = database("occupied");
        try (var connection = DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.createStatement().execute("CREATE TABLE documents (node_id uuid PRIMARY KEY)");
        }
        assertThatThrownBy(() -> new PostgresCatalog(config(url)))
                .isInstanceOf(CatalogBaseline.Refused.class).hasMessageContaining("documents")
                .hasMessageContaining("never migrated in place");
        assertNoCatalogTables(url);
    }

    @Test void refusesALedgerWithItsFlywayHistory() throws Exception {
        var url = database("ledger");
        try (var connection = DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.createStatement().execute("""
                    CREATE TABLE flyway_schema_history (installed_rank int PRIMARY KEY, version varchar(50));
                    INSERT INTO flyway_schema_history VALUES (120, '120')""");
        }
        assertThatThrownBy(() -> new PostgresCatalog(config(url)))
                .isInstanceOf(CatalogBaseline.Refused.class).hasMessageContaining("flyway_schema_history");
        assertNoCatalogTables(url);
    }

    @Test void refusesASchemaHoldingOnlyFunctions() throws Exception {
        var url = database("functions");
        try (var connection = DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.createStatement().execute(
                    "CREATE FUNCTION require_repository_read_committed() RETURNS void LANGUAGE sql AS 'SELECT 1'");
        }
        assertThatThrownBy(() -> new PostgresCatalog(config(url)))
                .isInstanceOf(CatalogBaseline.Refused.class).hasMessageContaining("require_repository_read_committed");
    }

    private static void assertNoCatalogTables(String url) throws Exception {
        try (var connection = DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var rows = connection.createStatement().executeQuery(
                     "SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'catalog_%'")) {
            rows.next();
            assertThat(rows.getInt(1)).isZero();
        }
    }

    static String database(String name) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.createStatement().execute("CREATE DATABASE " + name);
        }
        return POSTGRES.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + name + "$1");
    }

    static CatalogConfig config(String url) {
        return new CatalogConfig(url, POSTGRES.getUsername(), POSTGRES.getPassword(), 4);
    }
}
