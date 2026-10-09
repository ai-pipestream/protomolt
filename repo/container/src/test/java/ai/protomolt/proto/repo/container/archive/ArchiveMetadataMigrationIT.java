package ai.protomolt.proto.repo.container.archive;

import java.sql.DriverManager;
import java.sql.SQLException;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@Timeout(120)
class ArchiveMetadataMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void preservesLegacyAbsenceAndFreezesNewSnapshots() throws Exception {
        migrate("101");
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var sql = connection.createStatement()) {
            // Synthetic ledger rows isolate migration semantics; no publication is claimed.
            String id = "a2000000-0000-4000-8000-000000000001";
            sql.executeUpdate("INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version) "
                    + "VALUES('" + id + "','account','records','entry',1)");
            String legacy = "{\"address\":{\"accountId\":\"account\",\"archive\":\"records\",\"entryId\":\"entry\"},\"version\":\"1\"}";
            sql.executeUpdate("INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes) VALUES('"
                    + id + "',1,'" + legacy + "','" + "ab".repeat(32) + "',0)");
            migrate("102");
            try (var rows = sql.executeQuery("SELECT manifest ? 'metadataSnapshot' FROM archive_versions")) {
                assertThat(rows.next()).isTrue(); assertThat(rows.getBoolean(1)).isFalse();
            }
            assertThat(sql.executeUpdate("UPDATE archive_versions SET manifest=manifest || '{\"renditions\":[]}'::jsonb"))
                    .isEqualTo(1);
            rejects(sql, "UPDATE archive_versions SET manifest=manifest || '{\"metadataSnapshot\":{}}'::jsonb");
            rejects(sql, "UPDATE archive_versions SET manifest=manifest || '{\"metadataSnapshot\":null}'::jsonb");
            String snapshot = "{\"address\":{\"accountId\":\"account\",\"archive\":\"records\",\"entryId\":\"entry\"},"
                    + "\"currentVersion\":\"2\",\"entryUuid\":\"" + id + "\",\"title\":\"Original title\"}";
            String manifest = legacy.replace("\"version\":\"1\"", "\"version\":\"2\"");
            manifest = manifest.substring(0, manifest.length() - 1) + ",\"metadataSnapshot\":" + snapshot + "}";
            String insert = "INSERT INTO archive_versions(entry_uuid,version,manifest,root_checksum,total_bytes) VALUES('"
                    + id + "',2,'" + manifest + "','" + "cd".repeat(32) + "',0)";
            rejects(sql, insert.replace("\"currentVersion\":\"2\"", "\"currentVersion\":\"3\""));
            String address = "{\"accountId\":\"account\",\"archive\":\"records\",\"entryId\":\"entry\"}";
            for (String malformed : new String[]{"null", "{}", "{\"accountId\":\"\",\"archive\":\"records\",\"entryId\":\"entry\"}"}) {
                rejects(sql, insert.replace(address, malformed));
            }
            rejects(sql, insert.replace("\"entryId\":\"entry\"", "\"entryId\":\"another-entry\""));
            rejects(sql, insert.replace("\"entryUuid\":\"" + id + "\"", "\"entryUuid\":\"a2000000-0000-4000-8000-000000000099\""));
            assertThat(sql.executeUpdate(insert)).isEqualTo(1);
            rejects(sql, "UPDATE archive_versions SET manifest=manifest-'metadataSnapshot' WHERE version=2");
            rejects(sql, "UPDATE archive_versions SET manifest=jsonb_set(manifest,'{metadataSnapshot,title}','\"Changed\"') WHERE version=2");
            rejects(sql, "UPDATE archive_versions SET version=3 WHERE version=2");
            sql.executeUpdate("INSERT INTO archive_entries(entry_uuid,account_id,archive,entry_id,current_version) "
                    + "VALUES('a2000000-0000-4000-8000-000000000002','account','records','other',1)");
            rejects(sql, "UPDATE archive_versions SET entry_uuid='a2000000-0000-4000-8000-000000000002' WHERE version=2");
            rejects(sql, "UPDATE archive_versions SET manifest=jsonb_set(manifest,'{address,entryId}','\"other\"') WHERE version=2");
            assertThat(sql.executeUpdate("UPDATE archive_versions SET manifest=manifest || '{\"renditions\":[]}'::jsonb WHERE version=2"))
                    .isEqualTo(1);
            assertThat(sql.executeUpdate("DELETE FROM archive_versions")).isEqualTo(2);
        }
    }

    private static void rejects(java.sql.Statement sql, String command) {
        assertThatThrownBy(() -> sql.executeUpdate(command)).isInstanceOfSatisfying(SQLException.class,
                failure -> assertThat(failure.getSQLState()).isEqualTo("23514"));
    }

    private static void migrate(String target) {
        Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration/repo").target(target).load().migrate();
    }
}
