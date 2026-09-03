package ai.protomolt.proto.metric.iceberg;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.MetricExecutor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

/**
 * The lake-native metric backend: one {@code SELECT ... GROUP BY} rendered
 * from the compiled query and run by DuckDB over the Parquet files of the
 * table the Iceberg sink wrote. DuckDB is an in-process reader here, never
 * a warehouse product; Trino and Spark stay external consumers of the same
 * table. Columns are addressed by the compiled query's {@code fieldPath}
 * (the table keeps the message's nesting as structs), and the rendered SQL
 * is the {@code physical_plan}, evidence a human or agent can read.
 *
 * <p>{@code COUNT_DISTINCT} is supported here (the reduction spills), the
 * one aggregate the Lucene backend refuses. Rows missing a dimension value
 * ({@code NULL}, or the empty string on term dimensions) are excluded from
 * group-by, matching the doc-values backend. Tables with delete files are
 * refused loudly: the sink appends, and this reader trusts that.</p>
 */
public final class IcebergMetricExecutor implements MetricExecutor {

    /** The subjects this executor can reach: one Iceberg table each. */
    public interface SubjectTables {

        /** The subject's table; throws for a subject not mounted here. */
        Table table(String subject);
    }

    private static final Capabilities CAPABILITIES = new Capabilities(
            Set.of(Aggregate.AGGREGATE_COUNT, Aggregate.AGGREGATE_SUM,
                    Aggregate.AGGREGATE_AVG, Aggregate.AGGREGATE_MIN,
                    Aggregate.AGGREGATE_MAX, Aggregate.AGGREGATE_COUNT_DISTINCT),
            true, true);

    private final SubjectTables tables;

    public IcebergMetricExecutor(SubjectTables tables) {
        if (tables == null) {
            throw new IllegalArgumentException("tables must not be null");
        }
        this.tables = tables;
    }

    @Override
    public MetricBackend backend() {
        return MetricBackend.METRIC_BACKEND_ICEBERG;
    }

    @Override
    public Capabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Result execute(CompiledMetricQuery query) {
        Table table = tables.table(query.subject());
        List<String> files = dataFiles(query.subject(), table);
        // Object-store locations materialize through the table's own
        // FileIO for the query's duration; local files pass through.
        try (LocalizedScan scan = LocalizedScan.of(files, table.io())) {
            DuckDbSql sql = DuckDbSql.render(query, scan.localPaths());
            if (files.isEmpty()) {
                // Nothing appended yet: no files to read, no rows to answer.
                return new Result(List.of(), sql.text() + " -- empty table, not executed");
            }
            return new Result(run(query, sql), sql.text() + scan.note());
        }
    }

    /** The current snapshot's data files; refuses tables carrying deletes. */
    private static List<String> dataFiles(String subject, Table table) {
        List<String> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.deletes().isEmpty()) {
                    throw new IllegalStateException("subject '" + subject + "' carries delete"
                            + " files; this reader trusts the sink's append-only tables");
                }
                files.add(task.file().location());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "cannot plan the files of subject '" + subject + "'", e);
        }
        return files;
    }

    private static List<MetricRow> run(CompiledMetricQuery query, DuckDbSql sql) {
        List<MetricRow> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                PreparedStatement statement = connection.prepareStatement(sql.text())) {
            List<String> parameters = sql.parameters();
            for (int i = 0; i < parameters.size(); i++) {
                statement.setString(i + 1, parameters.get(i));
            }
            try (ResultSet results = statement.executeQuery()) {
                int dimensions = query.dimensions().size();
                while (results.next()) {
                    MetricRow.Builder row = MetricRow.newBuilder();
                    for (int i = 0; i < dimensions; i++) {
                        row.putDimensions(query.dimensions().get(i).member(),
                                results.getString(i + 1));
                    }
                    for (int i = 0; i < query.measures().size(); i++) {
                        double value = results.getDouble(dimensions + i + 1);
                        if (!results.wasNull()) {
                            row.putMeasures(query.measures().get(i).member(), value);
                        }
                    }
                    rows.add(row.build());
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("the DuckDB reduction failed: " + e.getMessage(), e);
        }
        return rows;
    }
}
