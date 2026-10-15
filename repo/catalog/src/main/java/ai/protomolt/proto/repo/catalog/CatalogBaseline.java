package ai.protomolt.proto.repo.catalog;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/**
 * Applies the catalog's fresh Flyway baseline (requirements §14). The baseline
 * initializes only an empty schema, or continues a catalog it created itself
 * (recognized by its own history table). Any other content, a V1-V120
 * repository ledger included, is refused before Flyway runs: a missing Flyway
 * history table alone is not proof that a database is empty.
 */
public final class CatalogBaseline {
    /** The catalog's own Flyway history table; never the ledger's flyway_schema_history. */
    public static final String HISTORY_TABLE = "repo_catalog_schema_history";
    private static final String LOCATION = "classpath:db/repo-catalog";

    private CatalogBaseline() {}

    /** The schema already holds objects the catalog did not create. */
    public static final class Refused extends IllegalStateException {
        Refused(String message) { super(message); }
    }

    public static void apply(DataSource dataSource) {
        String schema;
        try (var connection = dataSource.getConnection()) {
            schema = connection.getSchema();
            if (!hasHistory(connection)) {
                var found = foreignObjects(connection);
                if (hasTable(connection, "flyway_schema_history"))
                    found.addFirst("flyway_schema_history (a repository ledger's history)");
                connection.rollback();
                if (!found.isEmpty())
                    throw new Refused("The repository catalog baseline applies only to an empty catalog; schema "
                            + schema + " already holds " + String.join(", ", found)
                            + ". An existing V1-V120 repository database is never migrated in place.");
            } else {
                connection.rollback();
            }
        } catch (SQLException failure) {
            throw CatalogException.from("Catalog baseline inspection", failure);
        }
        Flyway.configure().dataSource(dataSource).locations(LOCATION).table(HISTORY_TABLE)
                .schemas(schema).baselineOnMigrate(false).cleanDisabled(true).validateMigrationNaming(true)
                .load().migrate();
    }

    private static boolean hasHistory(Connection connection) throws SQLException {
        return hasTable(connection, HISTORY_TABLE);
    }

    private static boolean hasTable(Connection connection, String table) throws SQLException {
        try (var statement = connection.prepareStatement("""
                SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema() AND c.relname = ? AND c.relkind IN ('r', 'p')""")) {
            statement.setString(1, table);
            try (var rows = statement.executeQuery()) { return rows.next(); }
        }
    }

    /** Relations, sequences, functions and types in the current schema, by name. */
    private static List<String> foreignObjects(Connection connection) throws SQLException {
        var found = new ArrayList<String>();
        try (var statement = connection.prepareStatement("""
                SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
                UNION ALL
                SELECT p.proname FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = current_schema()
                UNION ALL
                SELECT t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = current_schema() AND t.typtype IN ('e', 'd', 'r', 'm')
                   OR (n.nspname = current_schema() AND t.typtype = 'c' AND NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_class r WHERE r.oid = t.typrelid AND r.relkind <> 'c'))
                ORDER BY 1 LIMIT 20""")) {
            try (var rows = statement.executeQuery()) {
                while (rows.next()) found.add(rows.getString(1));
            }
        }
        return found;
    }
}
