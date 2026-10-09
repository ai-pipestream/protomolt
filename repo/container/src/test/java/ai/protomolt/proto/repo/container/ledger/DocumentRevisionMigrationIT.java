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
            try (var insert = connection.prepareStatement("""
                    INSERT INTO document_purges(purge_id, node_id, doc_id, graph_address_id,
                        account_id, graph_id, drive_name, object_keys, requested_at)
                    VALUES (?, ?, 'legacy', 'source', 'legacy', 'intake:legacy', 'drive', '["object"]', now())
                    """)) {
                insert.setObject(1, UUID.randomUUID());
                insert.setObject(2, id);
                insert.executeUpdate();
            }
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            try (var query = connection.createStatement();
                 var rows = query.executeQuery("SELECT completion_mode, generation_id, content_checksum, object_keys FROM document_purges")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("ASYNC");
                assertThat(rows.getObject(2)).isNull();
                assertThat(rows.getString(3)).isNull();
                assertThat(rows.getString(4)).isEqualTo("[\"object\"]");
            }
            long migrated = revision(connection, id);
            assertThat(migrated).isPositive();
            // V10 creates managed storage state without adopting legacy keys or
            // manufacturing deletion authority for an existing document.
            try (var query = connection.createStatement();
                 var rows = query.executeQuery("""
                         SELECT (SELECT count(*) FROM raw_objects), (SELECT count(*) FROM document_raw_refs),
                         (SELECT count(*) FROM document_part_attempts), (SELECT count(*) FROM document_part_publications),
                         (SELECT count(*) FROM document_part_publication_history)
                         """)) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isZero();
                assertThat(rows.getLong(2)).isZero();
                assertThat(rows.getLong(3)).isZero();
                assertThat(rows.getLong(4)).isZero();
                assertThat(rows.getLong(5)).isZero();
            }
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
