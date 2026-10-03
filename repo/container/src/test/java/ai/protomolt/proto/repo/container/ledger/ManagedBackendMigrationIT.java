package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import java.sql.DriverManager;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ManagedBackendMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void legacyGenerationAndObjectBindingsSurviveWithoutRewritingTheirLocation() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").target("16").load().migrate();
        UUID object = UUID.randomUUID();
        UUID entry = UUID.randomUUID();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            try (var sql = connection.createStatement()) {
                sql.executeUpdate("INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm) "
                        + "VALUES ('legacy','s3','https://original.example','us-east-1',true,'original-realm')");
            }
            try (var insert = connection.prepareStatement("""
                    INSERT INTO archive_object_bindings(object_id,entry_uuid,account_id,archive,backend_generation,storage_realm,bucket,object_key)
                    VALUES (?,?,'account','archive','legacy','original-realm','bucket','key')
                    """)) {
                insert.setObject(1, object);
                insert.setObject(2, entry);
                insert.executeUpdate();
            }
        }
        try (var database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()))) {
            var tx = new Tx(database.entityManagerFactory());
            var ledger = new ManagedBackendLedger(tx);
            var expected = new ManagedBackendLedger.Profile(S3BackendIdentity.of("https://original.example", "us-east-1", true), "original-realm");
            assertThat(ledger.find("legacy")).contains(expected);
            ledger.bind("legacy", expected);
            assertThat(new ArchiveObjectLedger(tx).find(object).orElseThrow().location().backendGeneration()).isEqualTo("legacy");
            tx.readOnly(em -> {
                var row = (Object[]) em.createNativeQuery("SELECT endpoint,region,path_style,identity_schema,identity_json FROM managed_backend_profiles WHERE generation='legacy'")
                        .getSingleResult();
                assertThat(row).containsExactly("https://original.example", "us-east-1", true, null, null);
                return null;
            });
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery("UPDATE managed_backend_profiles SET endpoint='https://other.example' WHERE generation='legacy'").executeUpdate();
            })).hasStackTraceContaining("Managed backend generations are immutable");
        }
    }
}
