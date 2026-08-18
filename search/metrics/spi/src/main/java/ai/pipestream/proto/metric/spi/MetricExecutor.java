package ai.pipestream.proto.metric.spi;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricRow;
import java.util.List;
import java.util.Set;

/**
 * One analytics engine behind the metric contract. Implementations are
 * engine adapters (Lucene collectors, DuckDB over Iceberg) or a gRPC client
 * pointed at a remote metrics node — the seam is identical either way,
 * because everything is gRPC and the compiled query is engine-neutral.
 *
 * <p>An executor receives a {@link CompiledMetricQuery} that the SPI has
 * already validated and resolved; it never sees raw requests and never makes
 * policy choices. What an engine cannot run it declares up front via
 * {@link #capabilities()}, and the compiler refuses such queries by name
 * before execution — capabilities differ per engine and are never flattened
 * to a common denominator.</p>
 */
public interface MetricExecutor {

    /** The backend this executor runs. */
    MetricBackend backend();

    /** What this engine can run; the compiler refuses the rest by name. */
    Capabilities capabilities();

    /**
     * Runs one compiled, validated query.
     *
     * @param query the compiled query; every name already resolved
     * @return the rows and the engine's physical plan
     */
    Result execute(CompiledMetricQuery query);

    /**
     * What one engine can run.
     *
     * @param aggregates the aggregates this engine executes
     * @param dateGrains whether calendar-grain date bucketing is supported
     * @param measureRowFilters whether per-measure row filters (filter_cel)
     *        are supported
     */
    record Capabilities(
            Set<Aggregate> aggregates, boolean dateGrains, boolean measureRowFilters) {
        public Capabilities {
            aggregates = Set.copyOf(aggregates);
        }
    }

    /**
     * One execution's product.
     *
     * @param rows the result rows, at most the query's limit
     * @param physicalPlan the engine's plan (collector description, SQL):
     *        evidence for humans and agents, never input to a later query
     */
    record Result(List<MetricRow> rows, String physicalPlan) {
        public Result {
            rows = List.copyOf(rows);
        }
    }
}
