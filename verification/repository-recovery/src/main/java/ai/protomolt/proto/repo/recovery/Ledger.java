package ai.protomolt.proto.repo.recovery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/** Read-only identity queries over plain JDBC: the rehearsal records what the ledger says, it never inserts outcomes. */
final class Ledger implements AutoCloseable {
    private final Connection connection;

    Ledger(String jdbcUrl, String user, String password) {
        try {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            connection.setReadOnly(true);
            connection.setAutoCommit(true);
        } catch (SQLException failure) { throw new RehearsalFailure("Cannot connect to " + jdbcUrl, failure); }
    }

    /** Provider identities of every physical object bound to the given document revisions. */
    List<Map<String, Object>> documentObjects(List<UUID> revisions) {
        var rows = new ArrayList<Map<String, Object>>();
        for (var revision : revisions) {
            query("""
                    SELECT rp.revision_ordinal, rp.part, rp.sub_key, p.object_id::text, p.backend_generation, p.storage_realm,
                           p.storage_namespace, p.object_key, o.provider_version, o.etag, o.expected_sha256, o.expected_size, o.content_type
                    FROM document_revision_parts rp
                    JOIN repository_physical_locations p ON p.object_id = rp.object_id
                    JOIN document_part_attempt_objects o ON o.physical_object_id = p.object_id
                    WHERE rp.revision_id = ?::uuid ORDER BY rp.revision_ordinal
                    """, List.of(revision.toString()), result -> {
                var row = new LinkedHashMap<String, Object>();
                row.put("revisionId", revision.toString());
                row.put("revisionOrdinal", (long) result.getInt(1));
                row.put("part", (long) result.getInt(2));
                row.put("subKey", result.getString(3));
                row.put("objectId", result.getString(4));
                row.put("generation", result.getString(5));
                row.put("realm", result.getString(6));
                row.put("namespace", result.getString(7));
                row.put("key", result.getString(8));
                row.put("providerVersion", result.getString(9));
                row.put("etag", result.getString(10));
                row.put("sha256", result.getString(11));
                row.put("size", result.getLong(12));
                row.put("contentType", result.getString(13));
                rows.add(row);
            });
        }
        return rows;
    }

    /** Provider identities of every object an archive entry's versions reference. */
    List<Map<String, Object>> archiveObjects(UUID entry) {
        var rows = new ArrayList<Map<String, Object>>();
        query("""
                SELECT r.version, b.object_id::text, b.backend_generation, b.storage_realm, b.bucket, b.object_key,
                       u.provider_version, u.etag, u.sha256, u.expected_size, u.content_type, u.state
                FROM archive_version_object_refs r
                JOIN archive_object_bindings b ON b.object_id = r.object_id
                JOIN archive_object_uploads u ON u.object_id = r.object_id
                WHERE r.entry_uuid = ?::uuid ORDER BY r.version, b.object_key
                """, List.of(entry.toString()), result -> {
            var row = new LinkedHashMap<String, Object>();
            row.put("version", result.getLong(1));
            row.put("objectId", result.getString(2));
            row.put("generation", result.getString(3));
            row.put("realm", result.getString(4));
            row.put("namespace", result.getString(5));
            row.put("key", result.getString(6));
            row.put("providerVersion", result.getString(7));
            row.put("etag", result.getString(8));
            row.put("sha256", result.getString(9));
            row.put("size", result.getLong(10));
            row.put("contentType", result.getString(11));
            row.put("state", result.getString(12));
            rows.add(row);
        });
        return rows;
    }

    /** Normalized schema catalog digests (account, sha256 -> size); the bytes stay in SQL. */
    TreeMap<String, Long> schemaCatalog() {
        var catalog = new TreeMap<String, Long>();
        query("SELECT account_id, encode(artifact_sha256,'hex'), octet_length(artifact_bytes) FROM repository_schema_artifacts ORDER BY 1,2",
                List.of(), result -> catalog.put(result.getString(1) + "/" + result.getString(2), result.getLong(3)));
        return catalog;
    }

    Map<String, Long> sequences() {
        var values = new TreeMap<String, Long>();
        for (String name : List.of("document_mutation_revision_seq", "archive_mutation_revision_seq")) {
            query("SELECT last_value, is_called FROM " + name, List.of(), result ->
                    values.put(name, result.getBoolean(2) ? result.getLong(1) : result.getLong(1) - 1));
        }
        return values;
    }

    Map<String, Long> rowCounts() {
        var counts = new TreeMap<String, Long>();
        for (String table : List.of("documents", "document_revision_publications", "document_revision_parts", "document_revision_commits",
                "repository_physical_locations", "repository_schema_artifacts", "document_revision_schema_artifacts",
                "document_revision_schema_evidence", "document_revision_schema_assets", "managed_backend_profiles",
                "repository_operations", "repository_operation_success", "archives", "archive_entries", "archive_versions",
                "archive_object_bindings", "archive_object_uploads", "archive_version_object_refs", "repository_reader_incarnations",
                "repository_execution_claims", "document_part_attempts", "document_part_attempt_cleanup", "flyway_schema_history")) {
            query("SELECT count(*) FROM " + table, List.of(), result -> counts.put(table, result.getLong(1)));
        }
        return counts;
    }

    long migrationLevel() {
        var level = new long[1];
        query("SELECT max(version::int) FROM flyway_schema_history WHERE success", List.of(), result -> level[0] = result.getLong(1));
        return level[0];
    }

    Map<String, Object> backendProfile(String generation) {
        var profile = new LinkedHashMap<String, Object>();
        query("SELECT provider, storage_realm, identity_schema, identity_json::text FROM managed_backend_profiles WHERE generation = ?",
                List.of(generation), result -> {
                    profile.put("generation", generation);
                    profile.put("provider", result.getString(1));
                    profile.put("realm", result.getString(2));
                    profile.put("identitySchema", result.getString(3));
                    profile.put("identityJson", result.getString(4));
                });
        return profile;
    }

    /** Canonical retained evidence selecting the root Any occurrence of a revision; digests only, as the SPI requires. */
    Map<String, Object> materializationSelection(UUID revision) {
        var selection = new LinkedHashMap<String, Object>();
        query("SELECT revision_ordinal, encode(root_locator_sha256,'hex'), evidence_bytes FROM document_revision_schema_evidence WHERE revision_id = ?::uuid ORDER BY revision_ordinal LIMIT 1",
                List.of(revision.toString()), result -> {
                    try {
                        var evidence = ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence.parseFrom(result.getBytes(3));
                        var path = evidence.getOccurrencesList().stream().filter(value -> value.getStepsCount() == 1).findFirst()
                                .orElseThrow(() -> new RehearsalFailure("Evidence has no root occurrence"));
                        selection.put("revisionOrdinal", (long) result.getInt(1));
                        selection.put("rootSha256", result.getString(2));
                        selection.put("pathSha256", ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(path.toByteArray()));
                        selection.put("artifactSha256", path.getSteps(0).getAnyBoundary().getResolved().getArtifactSha256());
                    } catch (com.google.protobuf.InvalidProtocolBufferException failure) { throw new RehearsalFailure("Corrupt evidence", failure); }
                });
        if (selection.isEmpty()) throw new RehearsalFailure("No schema evidence for revision " + revision);
        return selection;
    }

    long readerIncarnations(String state) {
        var count = new long[1];
        query("SELECT count(*) FROM repository_reader_incarnations WHERE state = ?", List.of(state), result -> count[0] = result.getLong(1));
        return count[0];
    }

    /** Backends connected to this database other than the caller: writer quiescence evidence. */
    List<String> otherBackends() {
        var backends = new ArrayList<String>();
        query("SELECT coalesce(application_name,'') || '/' || coalesce(client_addr::text,'local') || '/' || state FROM pg_stat_activity "
                + "WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
                List.of(), result -> backends.add(result.getString(1)));
        return backends;
    }

    /** Largest transaction id stored in any xid8 column, as a 64-bit value (epoch in the high half). */
    long maxStoredXid() {
        var columns = new ArrayList<String>();
        query("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = current_schema() AND data_type = 'xid8' ORDER BY 1,2",
                List.of(), result -> columns.add(result.getString(1) + "." + result.getString(2)));
        long max = 0;
        for (String column : columns) {
            int dot = column.indexOf('.');
            var value = new long[1];
            query("SELECT coalesce(max(" + column.substring(dot + 1) + ")::text::numeric, 0)::bigint FROM " + column.substring(0, dot),
                    List.of(), result -> value[0] = result.getLong(1));
            max = Math.max(max, value[0]);
        }
        return max;
    }

    /**
     * Consume transaction ids on the source cluster before seeding, so the seeded xid8 values exceed
     * what a fresh restored cluster allocates and the restore has to advance the counter.
     */
    void burnTransactions(long count) {
        try {
            connection.setReadOnly(false);
            try (var statement = connection.createStatement()) {
                for (long i = 0; i < count; i++) {
                    try (var result = statement.executeQuery("SELECT pg_current_xact_id()")) { result.next(); }
                }
            }
        } catch (SQLException failure) { throw new RehearsalFailure("Cannot consume transaction ids", failure); }
        finally { try { connection.setReadOnly(true); } catch (SQLException ignored) { /* best effort */ } }
    }

    long currentXid() {
        var value = new long[1];
        query("SELECT pg_current_xact_id()::text::numeric::bigint", List.of(), result -> value[0] = result.getLong(1));
        return value[0];
    }

    long xidEpoch() {
        var value = new long[1];
        query("SELECT (pg_current_xact_id()::text::numeric / 4294967296)::bigint", List.of(), result -> value[0] = result.getLong(1));
        return value[0];
    }

    interface RowConsumer { void accept(ResultSet result) throws SQLException; }

    void query(String sql, List<String> parameters, RowConsumer consumer) {
        try (var statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) statement.setString(i + 1, parameters.get(i));
            try (var result = statement.executeQuery()) { while (result.next()) consumer.accept(result); }
        } catch (SQLException failure) { throw new RehearsalFailure("Query failed: " + sql, failure); }
    }

    /** Damage injection owned by a negative case; never used on the positive rehearsal or a source. */
    void execute(String sql) {
        try {
            connection.setReadOnly(false);
            try (var statement = connection.createStatement()) { statement.execute(sql); }
        } catch (SQLException failure) { throw new RehearsalFailure("Statement failed: " + sql, failure); }
        finally { try { connection.setReadOnly(true); } catch (SQLException ignored) { /* best effort */ } }
    }

    @Override public void close() {
        try { connection.close(); } catch (SQLException failure) { throw new RehearsalFailure("Cannot close ledger connection", failure); }
    }
}
