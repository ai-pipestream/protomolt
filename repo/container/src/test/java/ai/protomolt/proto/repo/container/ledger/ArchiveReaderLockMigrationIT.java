package ai.protomolt.proto.repo.container.ledger;

import java.sql.DriverManager;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.RepositoryRetentionMigrationIT.*;
import static org.assertj.core.api.Assertions.*;

/** SQL lifecycle fixtures; this migration must preserve existing reader protection. */
@Testcontainers
class ArchiveReaderLockMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void populatedReaderPinsRemainProtectedAcrossTheLockChange() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").target("28").load().migrate();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            execute(connection, """
                    INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                    VALUES('retention-original','s3','https://storage.example','us-east-1',true,'retention-realm')
                    """);
            UUID object = archive(connection, false), reader = UUID.randomUUID();
            execute(connection, """
                    INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
                    SELECT gen_random_uuid(),'%s',object_id,entry_uuid,version FROM archive_version_object_refs WHERE object_id='%s'
                    """.formatted(reader, object));
            String before;
            try (var statement = connection.createStatement(); var result = statement.executeQuery(
                    "SELECT row_to_json(p)::text FROM archive_read_pins p WHERE object_id='" + object + "'")) {
                assertThat(result.next()).isTrue(); before = result.getString(1);
            }
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            try (var statement = connection.createStatement(); var result = statement.executeQuery(
                    "SELECT row_to_json(p)::text FROM archive_read_pins p WHERE object_id='" + object + "'")) {
                assertThat(result.next()).isTrue(); assertThat(result.getString(1)).isEqualTo(before);
            }
            assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE object_id='" + object + "'")).isEqualTo(2);
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
            assertThatThrownBy(() -> execute(connection, """
                    INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
                    SELECT gen_random_uuid(),gen_random_uuid(),object_id,entry_uuid,version FROM archive_version_object_refs WHERE object_id='%s'
                    """.formatted(object))).hasMessageContaining("requires READ COMMITTED isolation");
            assertThatThrownBy(() -> execute(connection,
                    "UPDATE repository_object_retention SET retiring=true WHERE object_id='" + object + "'"))
                    .hasMessageContaining("requires READ COMMITTED isolation");
            assertThatThrownBy(() -> abandonedDocument(connection)).hasMessageContaining("requires READ COMMITTED isolation");
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
            execute(connection, "DELETE FROM archive_version_object_refs WHERE object_id='" + object + "'");
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
            assertThatThrownBy(() -> execute(connection,
                    "UPDATE archive_object_uploads SET state='DELETING',cleanup_token=gen_random_uuid(),cleanup_attempts=1 WHERE object_id='" + object + "'"))
                    .hasMessageContaining("requires READ COMMITTED isolation");
            connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
            assertThatThrownBy(() -> execute(connection,
                    "UPDATE archive_object_uploads SET state='DELETING',cleanup_token=gen_random_uuid(),cleanup_attempts=1 WHERE object_id='" + object + "'"))
                    .hasMessageContaining("Retained repository objects cannot be reclaimed");
            assertThatThrownBy(() -> execute(connection, "DELETE FROM repository_object_references WHERE object_id='" + object + "'"))
                    .hasMessageContaining("cannot release a retained native owner");
            execute(connection, "DELETE FROM archive_read_pins WHERE object_id='" + object + "' AND reader_incarnation='" + reader + "'");
            execute(connection, "UPDATE archive_object_uploads SET state='DELETING',cleanup_token=gen_random_uuid(),cleanup_attempts=1 WHERE object_id='" + object + "'");
            assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE object_id='" + object + "'")).isZero();
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention WHERE object_id='" + object + "' AND reclaiming")).isEqualTo(1);
        }
    }
}
