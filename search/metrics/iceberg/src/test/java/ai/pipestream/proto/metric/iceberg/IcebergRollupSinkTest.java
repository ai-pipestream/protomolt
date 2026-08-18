package ai.pipestream.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.lake.iceberg.LocalFileIO;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.spi.RollupSink;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The rollup sink against a real catalog: a rebuild REPLACES the table
 * (the second rollup's rows are the whole answer, the first run's rows
 * are gone), the synthesized schema lands dimension strings and measure
 * doubles readable straight off the Parquet, an empty rollup commits an
 * empty table, and a member that cannot be a column refuses by name.
 */
class IcebergRollupSinkTest {

    @TempDir
    static Path warehouse;

    static JdbcCatalog catalog;
    static IcebergRollupSink sink;

    @BeforeAll
    static void boot() {
        catalog = new JdbcCatalog();
        catalog.initialize("rollups", Map.of(
                CatalogProperties.URI, "jdbc:sqlite:" + warehouse.resolve("catalog.db"),
                CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString(),
                CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
        catalog.createNamespace(Namespace.of("protomolt"));
        sink = new IcebergRollupSink(catalog, "protomolt");
    }

    @AfterAll
    static void shutdown() throws Exception {
        catalog.close();
    }

    static MetricRow row(String segment, double revenue) {
        return MetricRow.newBuilder()
                .putDimensions("segment", segment)
                .putMeasures("revenue", revenue)
                .build();
    }

    /** Reads the table's rows back through DuckDB, straight off the Parquet. */
    static Map<String, Double> revenueBySegment(String table) throws Exception {
        Table committed = catalog.loadTable(TableIdentifier.of("protomolt", table));
        List<String> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = committed.newScan().planFiles()) {
            tasks.forEach(task -> files.add(task.file().location()));
        }
        Map<String, Double> rows = new LinkedHashMap<>();
        if (files.isEmpty()) {
            return rows;
        }
        String list = String.join(", ",
                files.stream().map(file -> "'" + file + "'").toList());
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(
                        "SELECT segment, revenue FROM read_parquet([" + list + "])"
                                + " ORDER BY segment")) {
            while (results.next()) {
                rows.put(results.getString(1), results.getDouble(2));
            }
        }
        return rows;
    }

    @Test
    void aRebuildReplacesTheTableInsteadOfAppending() throws Exception {
        RollupSink.Written first = sink.replace("revenue_by_segment",
                List.of("segment"), List.of("revenue"),
                List.of(row("mid", 200.0), row("smb", 180.0)));
        assertThat(first.table()).isEqualTo("protomolt.revenue_by_segment");
        assertThat(first.rowsWritten()).isEqualTo(2);
        assertThat(first.snapshotId()).isNotZero();
        assertThat(revenueBySegment("revenue_by_segment"))
                .containsExactly(Map.entry("mid", 200.0), Map.entry("smb", 180.0));

        RollupSink.Written second = sink.replace("revenue_by_segment",
                List.of("segment"), List.of("revenue"),
                List.of(row("ent", 999.0)));
        assertThat(second.snapshotId()).isNotEqualTo(first.snapshotId());
        assertThat(revenueBySegment("revenue_by_segment"))
                .as("the rebuild replaced the whole answer")
                .containsExactly(Map.entry("ent", 999.0));
    }

    @Test
    void anEmptyRollupCommitsAnEmptyTable() throws Exception {
        sink.replace("empty_rollup", List.of("segment"), List.of("revenue"), List.of());
        assertThat(revenueBySegment("empty_rollup")).isEmpty();
    }

    @Test
    void aMemberThatCannotBeAColumnRefusesByName() {
        assertThatThrownBy(() -> sink.replace("bad", List.of("Weird-Name"),
                List.of("revenue"), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Weird-Name")
                .hasMessageContaining("rollup column");
    }

}
