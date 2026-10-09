package ai.protomolt.proto.repo.container.ledger;

import java.sql.DriverManager;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentKeyReservationMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void backfillsDuplicateLegacyAndPurgeKeysWithoutChangingRowsOrReservingRawBlobs() throws Exception {
        var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())
                .locations("classpath:db/migration/repo");
        config.target("22").load().migrate();
        try (var connection=DriverManager.getConnection(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())) {
            for (int i=0;i<2;i++) try (var insert=connection.prepareStatement("""
                    INSERT INTO documents(node_id,doc_id,graph_address_id,graph_id,row_kind,account_id,datasource_id,
                        checksum,drive_name,object_key,etag,size_bytes,part_manifest)
                    VALUES (?,?,'source','intake:account','INTAKE','account','source','legacy','drive','documents/shared','etag',3,
                        '{"parts":[{"objectKey":"documents/shared"}]}'::jsonb)
                    """)) {
                insert.setObject(1,UUID.randomUUID()); insert.setString(2,"doc-"+i); insert.executeUpdate();
            }
            try (var insert=connection.prepareStatement("""
                    INSERT INTO document_purges(purge_id,node_id,doc_id,graph_address_id,account_id,graph_id,
                        drive_name,object_keys,requested_at)
                    VALUES (?,?,'gone','source','account','intake:account','drive',
                        '["documents/orphaned-purge","documents/shared","blobs/raw-object"]'::jsonb,now())
                    """)) {
                insert.setObject(1,UUID.randomUUID()); insert.setObject(2,UUID.randomUUID()); insert.executeUpdate();
            }
            String before;
            try (var query=connection.createStatement(); var rows=query.executeQuery("SELECT jsonb_agg(to_jsonb(documents) ORDER BY doc_id)::text FROM documents")) {
                assertThat(rows.next()).isTrue(); before=rows.getString(1);
            }
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            try (var query=connection.createStatement(); var rows=query.executeQuery("SELECT jsonb_agg(to_jsonb(documents) ORDER BY doc_id)::text FROM documents")) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getString(1)).isEqualTo(before);
            }
            try (var query=connection.createStatement(); var rows=query.executeQuery("SELECT object_key,attempt_id FROM document_part_key_reservations ORDER BY object_key")) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getString(1)).isEqualTo("documents/orphaned-purge"); assertThat(rows.getObject(2)).isNull();
                assertThat(rows.next()).isTrue(); assertThat(rows.getString(1)).isEqualTo("documents/shared"); assertThat(rows.getObject(2)).isNull();
                assertThat(rows.next()).isFalse();
            }
            try (var statement=connection.createStatement()) {
                assertThatThrownBy(() -> statement.executeUpdate("DELETE FROM document_part_key_reservations"))
                        .hasMessageContaining("immutable");
                assertThatThrownBy(() -> statement.executeUpdate("UPDATE document_part_key_reservations SET object_key='documents/redirected'"))
                        .hasMessageContaining("immutable");
            }
        }
    }
}
