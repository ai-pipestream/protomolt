package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
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

/**
 * Read-only identity queries over plain JDBC: the hosts record what the ledger says and never
 * insert outcomes. Damage injection for negative modes is the one explicit exception and is
 * only ever run against a disposable restored copy. Adapted from GitHub PR #413 (4862f35f9).
 */
final class RepositoryBackupRehearsalLedger implements AutoCloseable {
    private final Connection connection;

    RepositoryBackupRehearsalLedger(String jdbcUrl, String user, String password) {
        try {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            connection.setReadOnly(true);
            connection.setAutoCommit(true);
        } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Cannot connect to " + jdbcUrl, failure); }
    }

    static RepositoryBackupRehearsalLedger fromEnvironment() {
        return new RepositoryBackupRehearsalLedger(RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_JDBC"),
                RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_DB_USER"), RepositoryBackupRehearsalFixture.env("PROTOMOLT_REHEARSAL_DB_PASSWORD"));
    }

    /** Provider identities of every physical object bound to the given document revisions. */
    List<Map<String, Object>> documentObjects(List<UUID> revisions) {
        var rows = new ArrayList<Map<String, Object>>();
        for (var revision : revisions) {
            query("""
                    SELECT rp.revision_ordinal, rp.part, rp.sub_key, p.object_id::text, p.backend_generation, p.storage_realm,
                           p.storage_namespace, p.object_key, o.provider_version, o.etag, o.expected_sha256, o.expected_size, o.content_type, o.verified
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
                row.put("verified", result.getBoolean(14));
                rows.add(row);
            });
        }
        return rows;
    }

    /** Normalized schema catalog digests (account/sha256 -> size); the bytes stay in SQL. */
    TreeMap<String, Long> schemaCatalog() {
        var catalog = new TreeMap<String, Long>();
        query("SELECT account_id, encode(artifact_sha256,'hex'), octet_length(artifact_bytes) FROM repository_schema_artifacts ORDER BY 1,2",
                List.of(), result -> catalog.put(result.getString(1) + "/" + result.getString(2), result.getLong(3)));
        return catalog;
    }

    Map<String, Long> sequences() {
        var values = new TreeMap<String, Long>();
        query("SELECT sequencename FROM pg_sequences WHERE schemaname = current_schema() ORDER BY 1", List.of(), result -> values.put(result.getString(1), 0L));
        for (String name : List.copyOf(values.keySet())) {
            query("SELECT last_value, is_called FROM " + name, List.of(), result ->
                    values.put(name, result.getBoolean(2) ? result.getLong(1) : result.getLong(1) - 1));
        }
        return values;
    }

    /** Row counts of every base table in the schema; the catalog fingerprints are the driver's stronger comparison. */
    Map<String, Long> rowCounts() {
        var counts = new TreeMap<String, Long>();
        query("SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' ORDER BY 1",
                List.of(), result -> counts.put(result.getString(1), 0L));
        for (String table : List.copyOf(counts.keySet()))
            query("SELECT count(*) FROM " + table, List.of(), result -> counts.put(table, result.getLong(1)));
        return counts;
    }

    long migrationLevel() {
        var level = new long[1];
        query("SELECT max(version::int) FROM flyway_schema_history WHERE success", List.of(), result -> level[0] = result.getLong(1));
        return level[0];
    }

    /** The nonsecret physical profile bound to a generation, as the ledger stores it. */
    String backendProfile(String generation) {
        var profile = new String[1];
        query("SELECT to_jsonb(p)::text FROM managed_backend_profiles p WHERE generation = ?", List.of(generation), result -> profile[0] = result.getString(1));
        if (profile[0] == null) throw new RepositoryBackupRehearsalFailure("No managed backend profile for generation " + generation);
        return profile[0];
    }

    /** Every retained occurrence selection of a revision: the root and each nested Any boundary. */
    List<Map<String, Object>> materializationSelections(UUID revision) {
        var selections = new ArrayList<Map<String, Object>>();
        query("SELECT revision_ordinal, encode(root_locator_sha256,'hex'), evidence_bytes FROM document_revision_schema_evidence WHERE revision_id = ?::uuid ORDER BY revision_ordinal",
                List.of(revision.toString()), result -> {
                    try {
                        var evidence = DocumentRootSchemaEvidence.parseFrom(result.getBytes(3));
                        for (var path : evidence.getOccurrencesList()) {
                            var boundary = path.getSteps(path.getStepsCount() - 1).getAnyBoundary();
                            var selection = new LinkedHashMap<String, Object>();
                            selection.put("revisionOrdinal", (long) result.getInt(1));
                            selection.put("rootSha256", result.getString(2));
                            selection.put("pathSha256", DocumentPartCodec.sha256Hex(path.toByteArray()));
                            selection.put("steps", (long) path.getStepsCount());
                            selection.put("typeUrl", boundary.getTypeUrl());
                            selection.put("artifactSha256", boundary.getResolved().getArtifactSha256());
                            selections.add(selection);
                        }
                    } catch (com.google.protobuf.InvalidProtocolBufferException failure) { throw new RepositoryBackupRehearsalFailure("Corrupt evidence", failure); }
                });
        if (selections.isEmpty()) throw new RepositoryBackupRehearsalFailure("No schema evidence for revision " + revision);
        return selections;
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

    long currentXid() {
        var value = new long[1];
        query("SELECT pg_current_xact_id()::text::numeric::bigint", List.of(), result -> value[0] = result.getLong(1));
        return value[0];
    }

    /** Coverage state of one operation across the V118/V119 tables and the successor edges. */
    Map<String, Object> coverageState(UUID operation) {
        var state = new LinkedHashMap<String, Object>();
        for (String table : List.of("repository_publication_preparations", "repository_preparation_coverage_certificates",
                "repository_preparation_coverage_unresolved", "repository_preparation_coverage_lineage", "repository_successor_installs",
                "repository_successor_executions", "repository_preparation_history_sets", "repository_operation_success", "repository_execution_claims")) {
            var generations = new ArrayList<Long>();
            String column = table.equals("repository_successor_installs") || table.equals("repository_preparation_coverage_certificates")
                    || table.equals("repository_preparation_coverage_unresolved") || table.equals("repository_preparation_coverage_lineage")
                    || table.equals("repository_publication_preparations") || table.equals("repository_preparation_history_sets")
                    ? "predecessor_generation" : table.equals("repository_successor_executions") ? "owner_generation"
                    : table.equals("repository_operation_success") ? "owner_generation" : "claim_epoch";
            query("SELECT " + column + " FROM " + table + " WHERE operation_id = ?::uuid ORDER BY 1", List.of(operation.toString()),
                    result -> generations.add(result.getLong(1)));
            state.put(table, generations);
        }
        var lineage = new ArrayList<Map<String, Object>>();
        query("SELECT predecessor_generation, anchor_generation, depth, root_count, encode(preparation_sha256,'hex'), encode(anchor_sha256,'hex') "
                + "FROM repository_preparation_coverage_lineage WHERE operation_id = ?::uuid ORDER BY 1", List.of(operation.toString()), result -> {
            var row = new LinkedHashMap<String, Object>();
            row.put("generation", result.getLong(1)); row.put("anchorGeneration", result.getLong(2)); row.put("depth", result.getLong(3));
            row.put("rootCount", result.getLong(4)); row.put("preparationSha256", result.getString(5)); row.put("anchorSha256", result.getString(6));
            lineage.add(row);
        });
        state.put("lineageRows", lineage);
        var certificates = new ArrayList<Map<String, Object>>();
        query("SELECT predecessor_generation, proof_kind, root_count, encode(preparation_sha256,'hex'), encode(command_sha256,'hex'), encode(roots_sha256,'hex') "
                + "FROM repository_preparation_coverage_certificates WHERE operation_id = ?::uuid ORDER BY 1", List.of(operation.toString()), result -> {
            var row = new LinkedHashMap<String, Object>();
            row.put("generation", result.getLong(1)); row.put("proofKind", result.getString(2)); row.put("rootCount", result.getLong(3));
            row.put("preparationSha256", result.getString(4)); row.put("commandSha256", result.getString(5)); row.put("rootsSha256", result.getString(6));
            certificates.add(row);
        });
        state.put("certificateRows", certificates);
        var installs = new ArrayList<Map<String, Object>>();
        query("SELECT predecessor_generation, successor_epoch, install_xid::text, encode(preparation_sha256,'hex'), encode(predecessor_preparation_sha256,'hex') "
                + "FROM repository_successor_installs WHERE operation_id = ?::uuid ORDER BY 1", List.of(operation.toString()), result -> {
            var row = new LinkedHashMap<String, Object>();
            row.put("generation", result.getLong(1)); row.put("successorEpoch", result.getLong(2)); row.put("installXid", result.getString(3));
            row.put("preparationSha256", result.getString(4)); row.put("predecessorPreparationSha256", result.getString(5));
            installs.add(row);
        });
        state.put("installRows", installs);
        return state;
    }

    interface RowConsumer { void accept(ResultSet result) throws SQLException; }

    void query(String sql, List<String> parameters, RowConsumer consumer) {
        try (var statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) statement.setString(i + 1, parameters.get(i));
            try (var result = statement.executeQuery()) { while (result.next()) consumer.accept(result); }
        } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Query failed: " + sql, failure); }
    }

    /** Damage injection owned by a negative mode; never used on the positive rehearsal or a source. */
    void execute(String sql) {
        try {
            connection.setReadOnly(false);
            try (var statement = connection.createStatement()) { statement.execute(sql); }
        } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Statement failed: " + sql, failure); }
        finally { try { connection.setReadOnly(true); } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Cannot restore read-only", failure); } }
    }

    @Override public void close() {
        try { connection.close(); } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Cannot close ledger connection", failure); }
    }
}
