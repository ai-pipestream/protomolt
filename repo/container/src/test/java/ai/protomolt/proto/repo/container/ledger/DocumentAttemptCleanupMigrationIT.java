package ai.protomolt.proto.repo.container.ledger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentAttemptCleanupMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void existingSealedAttemptAndReservationsSurviveWithoutAutomaticCleanup() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").target("23").load().migrate();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            UUID attempt = UUID.randomUUID(), node = UUID.randomUUID();
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("""
                        INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                        VALUES ('original','s3','https://storage.example','us-east-1',true,'realm')
                        """);
            }
            try (var insert = connection.prepareStatement("""
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,
                        backend_generation,storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                    VALUES (?,?,'account',0,'original','realm','namespace',1,0,?,clock_timestamp()+interval '5 minutes','PLANNING')
                    """)) {
                insert.setObject(1, attempt); insert.setObject(2, node); insert.setObject(3, UUID.randomUUID()); insert.executeUpdate();
            }
            try (var insert = connection.prepareStatement("""
                    INSERT INTO document_part_attempt_objects(attempt_id,ordinal,part,sub_key,storage_realm,
                        storage_namespace,object_key,expected_size,expected_sha256,content_type)
                    VALUES (?,0,1,'','realm','namespace',?,3,?,'application/x-protobuf')
                    """)) {
                insert.setObject(1, attempt);
                insert.setString(2, "documents/account/" + node + "/attempts/" + attempt + "/core");
                insert.setString(3, "ab".repeat(32)); insert.executeUpdate();
            }
            try (var statement = connection.createStatement()) {
                statement.executeUpdate("UPDATE document_part_attempts SET state='STAGING'");
            }
            connection.commit();
            connection.setAutoCommit(true);
            var before = new LinkedHashMap<String, String>();
            for (String table : new String[] {"managed_backend_profiles", "document_part_attempts",
                    "document_part_attempt_objects", "document_part_key_reservations"}) before.put(table, snapshot(connection, table));
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            for (var entry : before.entrySet()) assertThat(snapshot(connection, entry.getKey())).isEqualTo(entry.getValue());
            try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT count(*) FROM document_part_attempt_cleanup")) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getLong(1)).isZero();
            }
        }
    }

    private static String snapshot(Connection connection, String table) throws Exception {
        // Callers supply only the fixed table names above.
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                "SELECT jsonb_agg(to_jsonb(t)-'physical_object_id' ORDER BY (to_jsonb(t)-'physical_object_id')::text)::text FROM " + table + " t")) {
            assertThat(rows.next()).isTrue(); return rows.getString(1);
        }
    }
}
