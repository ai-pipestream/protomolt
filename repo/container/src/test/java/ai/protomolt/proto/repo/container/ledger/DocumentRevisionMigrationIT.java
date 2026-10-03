package ai.protomolt.proto.repo.container.ledger;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.sql.DriverManager;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentRevisionMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void migratesExistingRowsAndTracksDirectSqlMutations() throws Exception {
        var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo");
        config.target("7").load().migrate();
        UUID id = UUID.randomUUID();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO documents(node_id, doc_id, graph_address_id, graph_id, row_kind,
                        account_id, datasource_id, checksum, drive_name, object_key, etag, size_bytes)
                    VALUES (?, 'legacy', 'source', 'intake:legacy', 'INTAKE', 'legacy', 'source',
                        'hash', 'drive', 'object', 'etag', 1)
                    """)) {
                insert.setObject(1, id);
                insert.executeUpdate();
            }
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            long migrated = revision(connection, id);
            assertThat(migrated).isPositive();
            try (var sql = connection.createStatement()) {
                sql.executeUpdate("UPDATE documents SET security = '{}'::jsonb");
                long policyChanged = revision(connection, id);
                assertThat(policyChanged).isGreaterThan(migrated);
                sql.executeUpdate("UPDATE documents SET status = 'PENDING_PURGE'");
                assertThat(revision(connection, id)).isGreaterThan(policyChanged);
                // A writer cannot reset the revision to an earlier value.
                sql.executeUpdate("UPDATE documents SET mutation_revision = 0");
                assertThat(revision(connection, id)).isGreaterThan(policyChanged);
            }
        }
    }

    private static long revision(java.sql.Connection connection, UUID id) throws Exception {
        try (var query = connection.prepareStatement("SELECT mutation_revision FROM documents WHERE node_id = ?")) {
            query.setObject(1, id);
            try (var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }
}
