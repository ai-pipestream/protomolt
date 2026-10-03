package ai.protomolt.proto.repo.container.ledger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Populated SQL lifecycle fixtures; no provider verification is asserted. */
@Testcontainers
class RepositoryRetentionMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void backfillsRetainedOwnersAndCleanupTombstonesWithoutReopeningObjects() throws Exception {
        var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo");
        config.target("25").load().migrate();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            execute(connection, """
                    INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                    VALUES('retention-original','s3','https://storage.example','us-east-1',true,'retention-realm')
                    """);
            UUID retained = archive(connection, false);
            UUID deleted = archive(connection, true);
            UUID document = abandonedDocument(connection);
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention")).isEqualTo(3);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention WHERE reclaiming")).isEqualTo(2);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_references WHERE object_id='" + retained + "'")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention WHERE object_id='" + deleted + "' AND reclaiming")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id WHERE o.attempt_id='" + document + "' AND r.reclaiming")).isEqualTo(1);
            assertThatThrownBy(() -> execute(connection, "DELETE FROM repository_object_references WHERE object_id='" + retained + "'"))
                    .hasMessageContaining("cannot release a retained native owner");
            assertThatThrownBy(() -> execute(connection, "UPDATE repository_object_retention SET reclaiming=false WHERE object_id='" + deleted + "'"))
                    .hasMessageContaining("fence is permanent");
            assertThatThrownBy(() -> execute(connection, "INSERT INTO repository_object_references VALUES ('" + deleted + "','ARCHIVE_VERSION',gen_random_uuid(),1)"))
                    .hasMessageContaining("permanently fenced");
            execute(connection, "DELETE FROM archive_version_object_refs WHERE object_id='" + retained + "'");
            assertThat(number(connection, "SELECT count(*) FROM repository_object_references")).isZero();
            execute(connection, "UPDATE archive_object_uploads SET state='DELETING',cleanup_token=gen_random_uuid(),cleanup_attempts=1 WHERE object_id='" + retained + "'");
            assertThat(number(connection, "SELECT count(*) FROM repository_object_retention WHERE reclaiming")).isEqualTo(3);
        }
    }

    static UUID archive(Connection connection, boolean deleted) throws Exception {
        UUID object = UUID.randomUUID(), entry = UUID.randomUUID();
        try (var insert = connection.prepareStatement("""
                INSERT INTO archive_object_bindings(object_id,entry_uuid,account_id,archive,backend_generation,storage_realm,bucket,object_key)
                VALUES (?,?,'account','retention','retention-original','retention-realm','namespace',?)
                """)) {
            insert.setObject(1, object); insert.setObject(2, entry); insert.setString(3, "object-" + object); insert.executeUpdate();
        }
        execute(connection, "INSERT INTO archive_object_uploads(object_id,expected_size,content_type,lease_token,lease_until,state) VALUES ('" + object + "',0,'text/plain',gen_random_uuid(),clock_timestamp()+interval '1 hour','STAGING')");
        if (deleted) {
            execute(connection, "UPDATE archive_object_uploads SET state='DELETING',cleanup_token=gen_random_uuid(),cleanup_attempts=1 WHERE object_id='" + object + "'");
            execute(connection, "UPDATE archive_object_uploads SET state='DELETED' WHERE object_id='" + object + "'");
        } else {
            execute(connection, "UPDATE archive_object_uploads SET state='VERIFIED',sha256='" + "a".repeat(64) + "' WHERE object_id='" + object + "'");
            execute(connection, "UPDATE archive_object_uploads SET state='LIVE' WHERE object_id='" + object + "'");
            execute(connection, "INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version) VALUES ('" + entry + "','account','retention','" + entry + "',1)");
            execute(connection, "INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes) VALUES ('" + entry + "',1,'{}','sql-fixture',0)");
            execute(connection, "INSERT INTO archive_version_object_refs VALUES ('" + entry + "',1,'" + object + "')");
        }
        return object;
    }

    static UUID abandonedDocument(Connection connection) throws Exception {
        UUID attempt = UUID.randomUUID(), node = UUID.randomUUID();
        connection.setAutoCommit(false);
        try {
            execute(connection, """
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                        storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                    VALUES ('%s','%s','account',0,'retention-original','retention-realm','namespace',1,0,
                        gen_random_uuid(),clock_timestamp()+interval '1 second','PLANNING')
                    """.formatted(attempt, node));
            execute(connection, """
                    INSERT INTO document_part_attempt_objects(attempt_id,ordinal,part,sub_key,storage_realm,
                        storage_namespace,object_key,expected_size,expected_sha256,content_type)
                    VALUES ('%s',0,1,'','retention-realm','namespace','documents/account/%s/attempts/%s/core',0,'%s','text/plain')
                    """.formatted(attempt, node, attempt, "a".repeat(64)));
            execute(connection, "UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id='" + attempt + "'");
            connection.commit();
        } finally { connection.rollback(); connection.setAutoCommit(true); }
        try (var statement = connection.createStatement(); var result = statement.executeQuery(
                "SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.05) FROM document_part_attempts WHERE attempt_id='" + attempt + "'")) {
            assertThat(result.next()).isTrue();
        }
        execute(connection, "INSERT INTO document_part_attempt_cleanup(attempt_id,cleanup_token,claim_until,state) VALUES ('" + attempt + "',gen_random_uuid(),clock_timestamp()+interval '1 minute','DELETING')");
        return attempt;
    }

    static void execute(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement()) { statement.executeUpdate(sql); }
    }

    static long number(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue(); return result.getLong(1);
        }
    }
}
