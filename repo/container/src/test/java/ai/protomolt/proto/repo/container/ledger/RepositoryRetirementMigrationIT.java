package ai.protomolt.proto.repo.container.ledger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.RepositoryRetentionMigrationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Populated SQL lifecycle fixtures; no provider verification is asserted. */
@Testcontainers
class RepositoryRetirementMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void reclassifiesOnlyUnclaimedLiveTargetsAndPreservesPhysicalCleanupFences() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").target("26").load().migrate();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            execute(connection, """
                    INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                    VALUES('retention-original','s3','https://storage.example','us-east-1',true,'retention-realm')
                    """);
            UUID ordinary = archive(connection, false);
            UUID retired = target(connection);
            UUID deleting = target(connection);
            UUID deleted = target(connection);
            UUID document = abandonedDocument(connection);
            for (UUID id : new UUID[]{deleting, deleted}) {
                execute(connection, "UPDATE archive_object_uploads SET state='DELETING',cleanup_token=gen_random_uuid(),cleanup_attempts=1 WHERE object_id='" + id + "'");
            }
            execute(connection, "UPDATE archive_object_uploads SET cleanup_error='PROVIDER_UNAVAILABLE' WHERE object_id='" + deleting + "'");
            execute(connection, "UPDATE archive_object_uploads SET state='DELETED' WHERE object_id='" + deleted + "'");
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention WHERE reclaiming")).isEqualTo(4);
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            assertState(connection, ordinary, false, false);
            assertState(connection, retired, true, false);
            assertState(connection, deleting, true, true);
            assertState(connection, deleted, true, true);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id WHERE o.attempt_id='" + document + "' AND r.retiring AND r.reclaiming")).isEqualTo(1);
            for (UUID id : new UUID[]{retired, deleting, deleted}) {
                assertThatThrownBy(() -> execute(connection, "UPDATE repository_object_retention SET retiring=false WHERE object_id='" + id + "'"))
                        .hasMessageContaining("fence is permanent");
                assertThatThrownBy(() -> execute(connection, "INSERT INTO repository_object_references VALUES ('" + id + "','ARCHIVE_VERSION',gen_random_uuid(),1)"))
                        .hasMessageContaining("permanently fenced");
            }
            assertThatThrownBy(() -> execute(connection, "UPDATE repository_object_retention SET reclaiming=false WHERE object_id='" + deleting + "'"))
                    .hasMessageContaining("fence is permanent");
            execute(connection, "UPDATE archive_object_uploads SET state='DELETING',cleanup_token=gen_random_uuid(),cleanup_attempts=1 WHERE object_id='" + retired + "'");
            assertState(connection, retired, true, true);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE object_id='" + ordinary + "'")).isEqualTo(1);
        }
    }

    private static UUID target(Connection connection) throws Exception {
        UUID id = archive(connection, false), operation = UUID.randomUUID();
        execute(connection, "DELETE FROM archive_version_object_refs WHERE object_id='" + id + "'");
        execute(connection, """
                INSERT INTO archive_mutations(account_id,principal,operation_id,command_sha256,command,admission_receipt,sampled_revision)
                VALUES('account','sql-fixture','%s','%s',decode('01','hex'),decode('01','hex'),0)
                """.formatted(operation, "a".repeat(64)));
        execute(connection, "INSERT INTO archive_mutation_targets VALUES('account','sql-fixture','" + operation + "','" + id + "')");
        return id;
    }

    private static void assertState(Connection connection, UUID id, boolean retiring, boolean reclaiming) throws Exception {
        try (var statement = connection.prepareStatement("SELECT retiring,reclaiming FROM repository_object_retention WHERE object_id=?")) {
            statement.setObject(1, id);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getBoolean(1)).isEqualTo(retiring);
                assertThat(result.getBoolean(2)).isEqualTo(reclaiming);
            }
        }
    }
}
