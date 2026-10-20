package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.catalog.CatalogBaseline;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.container.ledger.LedgerDatabase;
import org.junit.jupiter.api.Test;

/** The catalog baseline refuses a real V1-V120 repository ledger, migrated by the ledger itself. */
class CatalogLedgerRefusalIT {
    @Test void aMigratedLedgerDatabaseIsRefusedAndLeftUnchanged() throws Exception {
        AccessionFixture.start();
        var postgres = AccessionFixture.POSTGRES;
        try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword())) {
            connection.createStatement().execute("CREATE DATABASE legacy_ledger");
        }
        var url = postgres.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/legacy_ledger$1");
        try (var ledger = new LedgerDatabase(new LedgerConfig(url, postgres.getUsername(), postgres.getPassword()))) {
            assertThat(ledger.entityManagerFactory()).isNotNull();
        }
        assertThatThrownBy(() -> new PostgresCatalog(new CatalogConfig(url, postgres.getUsername(), postgres.getPassword(), 2)))
                .isInstanceOf(CatalogBaseline.Refused.class).hasMessageContaining("flyway_schema_history");
        try (var connection = java.sql.DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
             var rows = connection.createStatement().executeQuery(
                     "SELECT count(*) FROM pg_tables WHERE tablename LIKE 'catalog_%' OR tablename = '"
                             + CatalogBaseline.HISTORY_TABLE + "'")) {
            rows.next();
            assertThat(rows.getInt(1)).isZero();
        }
    }
}
