package ai.protomolt.proto.repo.container.ledger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Populated SQL fixtures prove location migration, not provider qualification. */
@Testcontainers
class PhysicalLocationMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void sameKeyInDifferentKnownNamespacesDoesNotSerialize() throws Exception {
        String schema = schema();
        migrate(schema, null);
        try (var owner = connection(schema); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            profile(owner);
            // Establish the permanent key row before testing its shared lock.
            archiveBinding(owner, "common-key", "seed");
            owner.setAutoCommit(false);
            try {
                archiveBinding(owner, "common-key", "first");
                var unrelated = executor.submit(() -> {
                    try (var writer = connection(schema)) { return archiveBinding(writer, "common-key", "second"); }
                });
                assertThat(unrelated.get(10, TimeUnit.SECONDS)).isNotNull();
                owner.commit();
                assertThat(number(owner, "SELECT count(*) FROM repository_physical_locations")).isEqualTo(3);
            } finally {
                owner.rollback();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void knownRegistrationAndUnknownQuarantineCannotBothCommit(boolean knownFirst, boolean commitFirst) throws Exception {
        String schema = schema();
        migrate(schema, null);
        try (var owner = connection(schema); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            profile(owner);
            owner.setAutoCommit(false);
            try {
                if (knownFirst) archiveBinding(owner, "contended"); else raw(owner, "contended");
                int ownerPid = (int) number(owner, "SELECT pg_backend_pid()");
                var contender = executor.submit(() -> {
                    try (var writer = connection(schema)) {
                        if (knownFirst) raw(writer, "contended"); else archiveBinding(writer, "contended");
                        return true;
                    } catch (SQLException failure) {
                        assertThat(failure.getSQLState()).isEqualTo("P0001");
                        assertThat(failure.getMessage()).contains(knownFirst
                                ? "conflicts with a managed key" : "unknown legacy storage identity");
                        return false;
                    }
                });
                awaitLock(schema, ownerPid);
                if (commitFirst) owner.commit(); else owner.rollback();
                assertThat(contender.get(10, TimeUnit.SECONDS)).isEqualTo(!commitFirst);
                boolean knownWins = knownFirst == commitFirst;
                assertThat(number(owner, "SELECT count(*) FROM repository_physical_locations")).isEqualTo(knownWins ? 1 : 0);
                assertThat(number(owner, "SELECT count(*) FROM archive_object_bindings")).isEqualTo(knownWins ? 1 : 0);
                assertThat(number(owner, "SELECT count(*) FROM raw_objects")).isEqualTo(knownWins ? 0 : 1);
                assertThat(number(owner, "SELECT count(*) FROM repository_object_keys WHERE quarantined")).isEqualTo(knownWins ? 0 : 1);
            } finally {
                owner.rollback();
            }
        }
    }

    @Test void backfillsBareArchiveAndStagedDocumentWithoutInventingLegacyIdentity() throws Exception {
        String schema = schema();
        migrate(schema, "24");
        try (var connection = connection(schema)) {
            profile(connection);
            UUID archive = archiveBinding(connection, "archive-known");
            UUID attempt = documentPart(connection);
            raw(connection, "raw-unknown");
            legacyArchive(connection, "archive-unknown");
            migrate(schema, null);
            assertThat(number(connection, "SELECT count(*) FROM repository_physical_locations")).isEqualTo(2);
            assertThat(number(connection, "SELECT count(*) FROM repository_object_keys WHERE quarantined")).isEqualTo(2);
            assertThat(number(connection, "SELECT count(*) FROM repository_physical_locations WHERE object_id='" + archive + "' AND source_kind='ARCHIVE'")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM document_part_attempt_objects o JOIN repository_physical_locations p ON p.object_id=o.physical_object_id WHERE o.attempt_id='" + attempt + "'")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM archive_object_uploads")).isZero();
            assertThat(number(connection, "SELECT count(*) FROM document_part_attempt_cleanup")).isZero();
            assertThatThrownBy(() -> archiveBinding(connection, "raw-unknown"))
                    .hasMessageContaining("unknown legacy storage identity");
            assertThatThrownBy(() -> archiveBinding(connection, "archive-unknown"))
                    .hasMessageContaining("unknown legacy storage identity");
            assertThatThrownBy(() -> raw(connection, "archive-known"))
                    .hasMessageContaining("conflicts with a managed key");
            UUID fresh = archiveBinding(connection, "archive-after-migration");
            assertThat(number(connection, "SELECT count(*) FROM repository_physical_locations WHERE object_id='" + fresh + "'")).isEqualTo(1);
            assertThatThrownBy(() -> execute(connection, "DELETE FROM repository_physical_locations"))
                    .hasMessageContaining("immutable");
            assertThatThrownBy(() -> execute(connection, "UPDATE repository_object_keys SET quarantined=false WHERE quarantined"))
                    .hasMessageContaining("cannot be removed");
            assertThatThrownBy(() -> execute(connection, "UPDATE document_part_attempt_objects SET physical_object_id=gen_random_uuid()"))
                    .hasMessageContaining("identity is immutable");
            raw(connection, "late-raw");
            legacyArchive(connection, "late-legacy-archive");
            assertThatThrownBy(() -> archiveBinding(connection, "late-raw"))
                    .hasMessageContaining("unknown legacy storage identity");
            assertThatThrownBy(() -> archiveBinding(connection, "late-legacy-archive"))
                    .hasMessageContaining("unknown legacy storage identity");
        }
    }

    @Test void conflictingRawLocationAbortsMigrationWithoutChangingSourceRows() throws Exception {
        String schema = schema();
        migrate(schema, "24");
        try (var connection = connection(schema)) {
            profile(connection);
            archiveBinding(connection, "conflicting-key");
            raw(connection, "conflicting-key");
            assertThatThrownBy(() -> migrate(schema, null)).hasStackTraceContaining("conflicts with a managed key");
            assertThat(number(connection, "SELECT count(*) FROM archive_object_bindings")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM raw_objects")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM information_schema.tables WHERE table_schema='" + schema + "' AND table_name='repository_physical_locations'")).isZero();
        }
    }

    @Test void distinctDomainOwnersCannotAdoptTheSameKnownCoordinates() throws Exception {
        String schema = schema();
        migrate(schema, "24");
        try (var connection = connection(schema)) {
            profile(connection);
            UUID attempt = documentPart(connection);
            String key;
            try (var query = connection.prepareStatement("SELECT object_key FROM document_part_attempt_objects WHERE attempt_id=?")) {
                query.setObject(1, attempt);
                try (var rows = query.executeQuery()) { assertThat(rows.next()).isTrue(); key = rows.getString(1); }
            }
            archiveBinding(connection, key);
            assertThatThrownBy(() -> migrate(schema, null)).hasStackTraceContaining("duplicate key value violates unique constraint");
            assertThat(number(connection, "SELECT count(*) FROM document_part_attempt_objects")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM archive_object_bindings")).isEqualTo(1);
        }
    }

    @Test void unboundArchiveOccurrenceCannotBorrowAnotherVersionsKnownKey() throws Exception {
        String schema = schema();
        migrate(schema, "24");
        try (var connection = connection(schema)) {
            profile(connection);
            archiveBinding(connection, "ambiguous-archive");
            legacyArchive(connection, "ambiguous-archive");
            assertThatThrownBy(() -> migrate(schema, null)).hasStackTraceContaining("conflicts with a managed key");
        }
    }

    @Test void deletedRenditionKeepsProvenLocationWithoutAcquiringRetention() throws Exception {
        String schema = schema();
        migrate(schema, "24");
        try (var connection = connection(schema)) {
            profile(connection);
            UUID id = archiveBinding(connection, "deleted-known");
            execute(connection, "INSERT INTO archive_object_uploads(object_id,expected_size,content_type,lease_token,lease_until,state) VALUES ('" + id + "',0,'text/plain',gen_random_uuid(),clock_timestamp()+interval '1 hour','STAGING')");
            execute(connection, "UPDATE archive_object_uploads SET state='VERIFIED',sha256='" + "a".repeat(64) + "' WHERE object_id='" + id + "'");
            try (var insert = connection.prepareStatement("""
                    INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version)
                    SELECT entry_uuid,account_id,archive,'deleted',1 FROM archive_object_bindings WHERE object_id=?
                    """)) { insert.setObject(1, id); insert.executeUpdate(); }
            try (var insert = connection.prepareStatement("""
                    INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes)
                    SELECT entry_uuid,1,jsonb_build_object('renditions',jsonb_build_array(jsonb_build_object(
                        'objectKey','deleted-known','storageObjectId',object_id::text,'sha256',?,
                        'state','RENDITION_STATE_DELETED','sizeBytes','0'))),'sql-fixture',0
                    FROM archive_object_bindings WHERE object_id=?
                    """)) { insert.setString(1, "a".repeat(64)); insert.setObject(2, id); insert.executeUpdate(); }
            migrate(schema, null);
            assertThat(number(connection, "SELECT count(*) FROM repository_physical_locations")).isEqualTo(1);
            assertThat(number(connection, "SELECT count(*) FROM archive_version_object_refs")).isZero();
            assertThat(number(connection, "SELECT count(*) FROM repository_object_keys WHERE quarantined")).isZero();
        }
    }

    private static String schema() { return "catalog_" + UUID.randomUUID().toString().replace("-", ""); }

    private static void migrate(String schema, String target) {
        var config = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").schemas(schema).defaultSchema(schema);
        if (target != null) config.target(target);
        config.load().migrate();
    }

    private static Connection connection(String schema) throws Exception {
        var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        try {
            connection.setSchema(schema);
            execute(connection, "SET statement_timeout='15s'");
        } catch (Exception failure) {
            try { connection.close(); } catch (SQLException closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
        return connection;
    }

    private static void awaitLock(String schema, int blocker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try (var observer = connection(schema)) {
            while (System.nanoTime() < deadline) {
                if (number(observer, "SELECT count(*) FROM pg_stat_activity WHERE " + blocker
                        + "=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'") > 0) return;
                Thread.sleep(10);
            }
        }
        fail("Contender did not reach the PostgreSQL key guard");
    }

    private static void profile(Connection connection) throws Exception {
        execute(connection, """
                INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                VALUES('original','s3','https://storage.example','us-east-1',true,'realm')
                """);
    }

    private static UUID archiveBinding(Connection connection, String key) throws Exception {
        return archiveBinding(connection, key, "namespace");
    }

    private static UUID archiveBinding(Connection connection, String key, String namespace) throws Exception {
        UUID object = UUID.randomUUID();
        try (var insert = connection.prepareStatement("""
                INSERT INTO archive_object_bindings(object_id,entry_uuid,account_id,archive,backend_generation,storage_realm,bucket,object_key)
                VALUES (?,?,'account','archive','original','realm',?,?)
                """)) {
            insert.setObject(1, object); insert.setObject(2, UUID.randomUUID());
            insert.setString(3, namespace); insert.setString(4, key); insert.executeUpdate();
        }
        return object;
    }

    private static UUID documentPart(Connection connection) throws Exception {
        UUID attempt = UUID.randomUUID(), node = UUID.randomUUID();
        connection.setAutoCommit(false);
        try {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                        storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                    VALUES (?,?,'account',0,'original','realm','namespace',1,0,?,clock_timestamp()+interval '1 hour','PLANNING')
                    """)) {
                insert.setObject(1, attempt); insert.setObject(2, node); insert.setObject(3, UUID.randomUUID()); insert.executeUpdate();
            }
            try (var insert = connection.prepareStatement("""
                    INSERT INTO document_part_attempt_objects(attempt_id,ordinal,part,sub_key,storage_realm,
                        storage_namespace,object_key,expected_size,expected_sha256,content_type)
                    VALUES (?,0,1,'','realm','namespace',?,0,?,'application/x-protobuf')
                    """)) {
                insert.setObject(1, attempt); insert.setString(2, "documents/account/" + node + "/attempts/" + attempt + "/core");
                insert.setString(3, "a".repeat(64)); insert.executeUpdate();
            }
            execute(connection, "UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id='" + attempt + "'");
            connection.commit();
        } finally {
            connection.rollback();
            connection.setAutoCommit(true);
        }
        return attempt;
    }

    private static void raw(Connection connection, String key) throws Exception {
        try (var insert = connection.prepareStatement("""
                INSERT INTO raw_objects(raw_id,account_id,backend_identity,drive_id,drive_name,bucket,object_key,
                    expected_size,content_type,state,lease_token,lease_until,created_at,updated_at)
                VALUES (?,'account','original',?,'drive','namespace',?,0,'text/plain','STAGING',?,
                    clock_timestamp()+interval '1 hour',clock_timestamp(),clock_timestamp())
                """)) {
            insert.setObject(1, UUID.randomUUID()); insert.setObject(2, UUID.randomUUID()); insert.setString(3, key);
            insert.setObject(4, UUID.randomUUID()); insert.executeUpdate();
        }
    }

    private static void legacyArchive(Connection connection, String key) throws Exception {
        UUID entry = UUID.randomUUID();
        try (var insert = connection.prepareStatement("INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version) VALUES (?,'account','legacy',?,1)")) {
            insert.setObject(1, entry); insert.setString(2, entry.toString()); insert.executeUpdate();
        }
        try (var insert = connection.prepareStatement("""
                INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes)
                VALUES (?,1,jsonb_build_object('renditions',jsonb_build_array(jsonb_build_object('objectKey',?::text))),'sql-fixture',0)
                """)) { insert.setObject(1, entry); insert.setString(2, key); insert.executeUpdate(); }
    }

    private static void execute(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement()) { statement.executeUpdate(sql); }
    }

    private static long number(Connection connection, String sql) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue(); return result.getLong(1);
        }
    }
}
