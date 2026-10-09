package ai.protomolt.proto.repo.container.ledger;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchiveRevisionMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void existingArchiveRowsGainRevisionsAndEveryRewriteGetsANewIdentity() throws Exception {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").target("11").load().migrate();
        UUID id = UUID.randomUUID();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            try (var insert = connection.prepareStatement("""
                    INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version,title)
                    VALUES (?, 'account','records','entry',1,'Original title')
                    """)) {
                insert.setObject(1, id);
                insert.executeUpdate();
            }
            insertVersion(connection, id);
            Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration/repo").load().migrate();
            long entry = revision(connection, "archive_entries", id);
            long version = revision(connection, "archive_versions", id);
            assertThat(entry).isPositive();
            try (var query = connection.createStatement(); var rows = query.executeQuery(
                    "SELECT count(*) FROM archive_object_bindings")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isZero();
            }
            assertThat(version).isGreaterThan(entry);
            try (var sql = connection.createStatement()) {
                sql.executeUpdate("UPDATE archive_entries SET title='Metadata edit'");
                assertThat(revision(connection, "archive_entries", id)).isGreaterThan(entry);
                sql.executeUpdate("UPDATE archive_versions SET manifest='{\"renditions\":[]}'::jsonb");
                long rewritten = revision(connection, "archive_versions", id);
                assertThat(rewritten).isGreaterThan(version);
                sql.executeUpdate("UPDATE archive_versions SET mutation_revision=1");
                long guarded = revision(connection, "archive_versions", id);
                assertThat(guarded).isGreaterThan(rewritten);
                sql.executeUpdate("DELETE FROM archive_versions");
                insertVersion(connection, id);
                assertThat(revision(connection, "archive_versions", id)).isGreaterThan(guarded);
            }
            try (var query = connection.createStatement(); var rows = query.executeQuery(
                    "SELECT current_version,title FROM archive_entries")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isEqualTo(1);
                assertThat(rows.getString(2)).isEqualTo("Metadata edit");
            }
            long beforeReinsert = revision(connection, "archive_entries", id);
            try (var delete = connection.prepareStatement("DELETE FROM archive_entries WHERE entry_uuid=?")) {
                delete.setObject(1, id);
                delete.executeUpdate();
            }
            try (var insert = connection.prepareStatement("""
                    INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version,title)
                    VALUES (?, 'account','records','entry',1,'Replacement')
                    """)) {
                insert.setObject(1, id);
                insert.executeUpdate();
            }
            assertThat(revision(connection, "archive_entries", id)).isGreaterThan(beforeReinsert);
        }
    }

    private static void insertVersion(Connection connection, UUID id) throws Exception {
        try (var insert = connection.prepareStatement("""
                INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes)
                VALUES (?,1,'{}','retained-hash',7)
                """)) {
            insert.setObject(1, id);
            insert.executeUpdate();
        }
    }

    private static long revision(Connection connection, String table, UUID id) throws Exception {
        // Table names are test constants; identity values remain bound parameters.
        try (var query = connection.prepareStatement("SELECT mutation_revision FROM " + table + " WHERE entry_uuid=?")) {
            query.setObject(1, id);
            try (var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }
}
