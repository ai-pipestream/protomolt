package ai.protomolt.proto.repo.container.ledger;

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
 * Read-only archive identity queries over plain JDBC: physical bindings, provider identities,
 * retained references, cleanup state and mutation admissions as the ledger stores them. The
 * hosts record and compare these rows; they never insert outcomes. The generic catalog
 * queries (sequences, row counts, incarnations, xid8) come from the reused document rehearsal
 * ledger helper.
 */
final class ArchiveBackupQualificationLedger implements AutoCloseable {
    private final Connection connection;

    ArchiveBackupQualificationLedger(String jdbcUrl, String user, String password) {
        try {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            connection.setReadOnly(true);
            connection.setAutoCommit(true);
        } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Cannot connect to " + jdbcUrl, failure); }
    }

    static ArchiveBackupQualificationLedger fromEnvironment() {
        return new ArchiveBackupQualificationLedger(ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_JDBC"),
                ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_DB_USER"), ArchiveBackupQualificationFixture.env("PROTOMOLT_REHEARSAL_DB_PASSWORD"));
    }

    /**
     * Every physical object bound to an entry, referenced or not: the original backend identity,
     * the verified byte identity with its provider version id and etag, the cleanup state and the
     * retention fence. Ordered by object key so the record is stable across runs.
     */
    List<Map<String, Object>> bindings(UUID entry) {
        var rows = new ArrayList<Map<String, Object>>();
        query("""
                SELECT b.object_id::text, b.entry_uuid::text, b.account_id, b.archive, b.backend_generation, b.storage_realm, b.bucket, b.object_key,
                       u.state, u.sha256, u.expected_size, u.content_type, u.provider_version, u.etag, u.cleanup_attempts, u.cleanup_error,
                       (SELECT count(*) FROM archive_version_object_refs r WHERE r.object_id = b.object_id),
                       (SELECT string_agg(r.version::text, ',' ORDER BY r.version) FROM archive_version_object_refs r WHERE r.object_id = b.object_id),
                       (SELECT count(*) FROM repository_object_references s WHERE s.object_id = b.object_id AND s.owner_kind = 'ARCHIVE_VERSION'),
                       t.retiring, t.reclaiming,
                       EXISTS (SELECT 1 FROM archive_mutation_targets m WHERE m.object_id = b.object_id)
                FROM archive_object_bindings b
                JOIN archive_object_uploads u ON u.object_id = b.object_id
                LEFT JOIN repository_object_retention t ON t.object_id = b.object_id
                WHERE b.entry_uuid = ?::uuid ORDER BY b.object_key
                """, List.of(entry.toString()), result -> {
            var row = new LinkedHashMap<String, Object>();
            row.put("objectId", result.getString(1));
            row.put("entryUuid", result.getString(2));
            row.put("account", result.getString(3));
            row.put("archive", result.getString(4));
            row.put("generation", result.getString(5));
            row.put("realm", result.getString(6));
            row.put("namespace", result.getString(7));
            row.put("key", result.getString(8));
            row.put("state", result.getString(9));
            row.put("sha256", result.getString(10));
            row.put("size", result.getLong(11));
            row.put("contentType", result.getString(12));
            row.put("providerVersion", result.getString(13));
            row.put("etag", result.getString(14));
            row.put("cleanupAttempts", result.getLong(15));
            row.put("cleanupError", result.getString(16));
            row.put("references", result.getLong(17));
            row.put("referencedVersions", result.getString(18) == null ? "" : result.getString(18));
            row.put("sharedReferences", result.getLong(19));
            row.put("retiring", result.getObject(20) == null ? null : result.getBoolean(20));
            row.put("reclaiming", result.getObject(21) == null ? null : result.getBoolean(21));
            row.put("mutationTarget", result.getBoolean(22));
            rows.add(row);
        });
        return rows;
    }

    /** The entry row's database-assigned revision and version cursor. */
    Map<String, Object> entry(UUID entry) {
        var row = new LinkedHashMap<String, Object>();
        query("SELECT current_version, mutation_revision, classification_state FROM archive_entries WHERE entry_uuid = ?::uuid",
                List.of(entry.toString()), result -> {
                    row.put("currentVersion", result.getLong(1));
                    row.put("mutationRevision", result.getLong(2));
                    row.put("classificationState", result.getString(3));
                });
        if (row.isEmpty()) throw new RepositoryBackupRehearsalFailure("Archive entry is missing: " + entry);
        return row;
    }

    /** Retained version rows: version number, retained-manifest revision, root checksum, byte total. */
    List<Map<String, Object>> versions(UUID entry) {
        var rows = new ArrayList<Map<String, Object>>();
        query("SELECT version, mutation_revision, root_checksum, total_bytes FROM archive_versions WHERE entry_uuid = ?::uuid ORDER BY version",
                List.of(entry.toString()), result -> {
                    var row = new LinkedHashMap<String, Object>();
                    row.put("version", result.getLong(1));
                    row.put("mutationRevision", result.getLong(2));
                    row.put("rootChecksum", result.getString(3));
                    row.put("totalBytes", result.getLong(4));
                    rows.add(row);
                });
        return rows;
    }

    /** The immutable admission of one mutation and its current observation revision. */
    Map<String, Object> mutation(String account, String principal, UUID operation) {
        var row = new LinkedHashMap<String, Object>();
        query("""
                SELECT m.command_sha256, encode(sha256(m.command), 'hex'), encode(sha256(m.admission_receipt), 'hex'), m.sampled_revision,
                       (SELECT o.status_revision FROM archive_mutation_observations o
                         WHERE o.account_id = m.account_id AND o.principal = m.principal AND o.operation_id = m.operation_id),
                       (SELECT string_agg(t.object_id::text, ',' ORDER BY t.object_id) FROM archive_mutation_targets t
                         WHERE t.account_id = m.account_id AND t.principal = m.principal AND t.operation_id = m.operation_id)
                FROM archive_mutations m WHERE m.account_id = ? AND m.principal = ? AND m.operation_id = ?::uuid
                """, List.of(account, principal, operation.toString()), result -> {
            row.put("commandSha256", result.getString(1));
            row.put("commandBytesSha256", result.getString(2));
            row.put("admissionReceiptSha256", result.getString(3));
            row.put("sampledRevision", result.getLong(4));
            row.put("observationRevision", result.getObject(5) == null ? 0L : result.getLong(5));
            row.put("targets", result.getString(6) == null ? "" : result.getString(6));
        });
        if (row.isEmpty()) throw new RepositoryBackupRehearsalFailure("Archive mutation admission is missing: " + operation);
        return row;
    }

    long archiveReadPins() {
        var count = new long[1];
        query("SELECT count(*) FROM archive_read_pins", List.of(), result -> count[0] = result.getLong(1));
        return count[0];
    }

    /** Upload rows by lifecycle state; at a quiescent cut only LIVE and DELETED may remain. */
    Map<String, Long> uploadStates() {
        var states = new TreeMap<String, Long>();
        query("SELECT state, count(*) FROM archive_object_uploads GROUP BY state ORDER BY state", List.of(),
                result -> states.put(result.getString(1), result.getLong(2)));
        return states;
    }

    interface RowConsumer { void accept(ResultSet result) throws SQLException; }

    void query(String sql, List<String> parameters, RowConsumer consumer) {
        try (var statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) statement.setString(i + 1, parameters.get(i));
            try (var result = statement.executeQuery()) { while (result.next()) consumer.accept(result); }
        } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Query failed: " + sql, failure); }
    }

    @Override public void close() {
        try { connection.close(); } catch (SQLException failure) { throw new RepositoryBackupRehearsalFailure("Cannot close ledger connection", failure); }
    }
}
