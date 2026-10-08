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

/**
 * Driver-side catalog identity over plain JDBC. The comparison between source and restored
 * catalogs is a per-table fingerprint of every row's text, not a row count, so a restored
 * value that differs in any column (including xid8 proofs, timestamps and digests) is found.
 * Damage injection exists only for negative cases against disposable restored copies.
 */
final class RepositoryBackupRehearsalCatalog implements AutoCloseable {
    private final Connection connection;

    RepositoryBackupRehearsalCatalog(String jdbcUrl, String user, String password) {
        try {
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            connection.setReadOnly(true);
            connection.setAutoCommit(true);
        } catch (SQLException failure) { throw new IllegalStateException("Cannot connect to " + jdbcUrl + ": " + failure.getMessage(), failure); }
    }

    /** Every base table in the schema: row count and an order-independent digest of all row text. */
    TreeMap<String, String> tableFingerprints() {
        var tables = new ArrayList<String>();
        query("SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' ORDER BY 1",
                List.of(), result -> tables.add(result.getString(1)));
        var fingerprints = new TreeMap<String, String>();
        for (String table : tables) {
            query("SELECT count(*), coalesce(md5(string_agg(t::text, E'\\n' ORDER BY t::text)), 'empty') FROM " + table + " t", List.of(),
                    result -> fingerprints.put(table, result.getLong(1) + ":" + result.getString(2)));
        }
        return fingerprints;
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

    Map<String, Long> rowCounts() {
        var counts = new TreeMap<String, Long>();
        for (var entry : tableFingerprints().entrySet()) counts.put(entry.getKey(), Long.parseLong(entry.getValue().substring(0, entry.getValue().indexOf(':'))));
        return counts;
    }

    long migrationLevel() {
        var level = new long[1];
        query("SELECT max(version::int) FROM flyway_schema_history WHERE success", List.of(), result -> level[0] = result.getLong(1));
        return level[0];
    }

    /** Normalized schema catalog digests (account/sha256 -> size). */
    TreeMap<String, Long> schemaCatalog() {
        var catalog = new TreeMap<String, Long>();
        query("SELECT account_id, encode(artifact_sha256,'hex'), octet_length(artifact_bytes) FROM repository_schema_artifacts ORDER BY 1,2",
                List.of(), result -> catalog.put(result.getString(1) + "/" + result.getString(2), result.getLong(3)));
        return catalog;
    }

    /** Backends connected to this database other than the caller: writer quiescence evidence. */
    List<String> otherBackends() {
        var backends = new ArrayList<String>();
        query("SELECT coalesce(application_name,'') || '/' || coalesce(client_addr::text,'local') || '/' || state FROM pg_stat_activity "
                + "WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
                List.of(), result -> backends.add(result.getString(1)));
        return backends;
    }

    /** Open part-upload attempts and unreleased read pins would mean undrained work at the cut. */
    Map<String, Long> inFlightWork() {
        var work = new LinkedHashMap<String, Long>();
        query("SELECT count(*) FROM repository_reader_incarnations WHERE state = 'ACTIVE'", List.of(), result -> work.put("activeReaderIncarnations", result.getLong(1)));
        query("SELECT count(*) FROM document_read_pins", List.of(), result -> work.put("documentReadPins", result.getLong(1)));
        query("SELECT count(*) FROM document_part_attempts WHERE state IN ('PLANNING','STAGING')", List.of(),
                result -> work.put("stagingPartAttempts", result.getLong(1)));
        return work;
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

    int xidColumns() {
        var count = new int[1];
        query("SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND data_type = 'xid8'", List.of(), result -> count[0] = result.getInt(1));
        return count[0];
    }

    long currentXid() {
        var value = new long[1];
        query("SELECT pg_current_xact_id()::text::numeric::bigint", List.of(), result -> value[0] = result.getLong(1));
        return value[0];
    }

    /**
     * V118/V119 relational invariants that must hold in any consistent catalog: unresolved and proven
     * sets are disjoint, every lineage row has its committed install edge and certified anchor, every
     * certificate has a sealed header, and no restored install_xid is reachable by a new transaction.
     */
    Map<String, Object> coverageInvariants() {
        var invariants = new LinkedHashMap<String, Object>();
        query("""
                SELECT (SELECT count(*) FROM repository_preparation_coverage_certificates c JOIN repository_preparation_coverage_unresolved u
                         USING (account_id,principal,operation_id,predecessor_generation)),
                       (SELECT count(*) FROM repository_preparation_coverage_lineage l JOIN repository_preparation_coverage_unresolved u
                         USING (account_id,principal,operation_id,predecessor_generation)),
                       (SELECT count(*) FROM repository_preparation_coverage_lineage l LEFT JOIN repository_successor_installs i
                         USING (account_id,principal,operation_id,predecessor_generation) WHERE i.install_xid IS NULL),
                       (SELECT count(*) FROM repository_preparation_coverage_lineage l WHERE NOT EXISTS (SELECT 1 FROM repository_preparation_coverage_certificates c
                         WHERE c.account_id=l.account_id AND c.principal=l.principal AND c.operation_id=l.operation_id AND c.predecessor_generation=l.anchor_generation)),
                       (SELECT count(*) FROM repository_preparation_coverage_certificates c LEFT JOIN repository_preparation_history_sets h
                         USING (account_id,principal,operation_id,predecessor_generation) WHERE h.sealed IS DISTINCT FROM true),
                       (SELECT count(*) FROM repository_publication_preparations p WHERE NOT EXISTS (SELECT 1 FROM repository_preparation_coverage_certificates c
                         WHERE (c.account_id,c.principal,c.operation_id,c.predecessor_generation)=(p.account_id,p.principal,p.operation_id,p.predecessor_generation))
                         AND NOT EXISTS (SELECT 1 FROM repository_preparation_coverage_lineage l
                         WHERE (l.account_id,l.principal,l.operation_id,l.predecessor_generation)=(p.account_id,p.principal,p.operation_id,p.predecessor_generation))
                         AND NOT EXISTS (SELECT 1 FROM repository_preparation_coverage_unresolved u
                         WHERE (u.account_id,u.principal,u.operation_id,u.predecessor_generation)=(p.account_id,p.principal,p.operation_id,p.predecessor_generation))),
                       (SELECT count(*) FROM repository_successor_installs WHERE install_xid >= pg_current_xact_id()),
                       (SELECT count(*) FROM repository_preparation_coverage_certificates),
                       (SELECT count(*) FROM repository_preparation_coverage_lineage),
                       (SELECT count(*) FROM repository_preparation_coverage_unresolved),
                       (SELECT count(*) FROM repository_successor_installs),
                       (SELECT count(*) FROM repository_successor_executions)
                """, List.of(), result -> {
            invariants.put("certifiedAndUnresolved", result.getLong(1));
            invariants.put("lineageAndUnresolved", result.getLong(2));
            invariants.put("lineageWithoutInstallEdge", result.getLong(3));
            invariants.put("lineageWithoutCertifiedAnchor", result.getLong(4));
            invariants.put("certificateWithoutSealedHeader", result.getLong(5));
            invariants.put("preparationWithoutAnyState", result.getLong(6));
            invariants.put("installXidNotBelowCurrent", result.getLong(7));
            invariants.put("certificates", result.getLong(8));
            invariants.put("lineage", result.getLong(9));
            invariants.put("unresolved", result.getLong(10));
            invariants.put("successorInstalls", result.getLong(11));
            invariants.put("successorExecutions", result.getLong(12));
        });
        return invariants;
    }

    interface RowConsumer { void accept(ResultSet result) throws SQLException; }

    void query(String sql, List<String> parameters, RowConsumer consumer) {
        try (var statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) statement.setString(i + 1, parameters.get(i));
            try (var result = statement.executeQuery()) { while (result.next()) consumer.accept(result); }
        } catch (SQLException failure) { throw new IllegalStateException("Query failed: " + sql + ": " + failure.getMessage(), failure); }
    }

    /** Damage injection owned by a negative case; never used on the positive rehearsal or a source. */
    void execute(String sql) {
        try {
            connection.setReadOnly(false);
            try (var statement = connection.createStatement()) { statement.execute(sql); }
        } catch (SQLException failure) { throw new IllegalStateException("Statement failed: " + sql + ": " + failure.getMessage(), failure); }
        finally { try { connection.setReadOnly(true); } catch (SQLException failure) { throw new IllegalStateException("Cannot restore read-only", failure); } }
    }

    @Override public void close() {
        try { connection.close(); } catch (SQLException failure) { throw new IllegalStateException("Cannot close catalog connection", failure); }
    }
}
