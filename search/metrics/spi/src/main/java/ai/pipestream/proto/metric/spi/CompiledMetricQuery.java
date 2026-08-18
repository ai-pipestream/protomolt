package ai.pipestream.proto.metric.spi;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.TimeGrain;
import java.util.List;

/**
 * One aggregate query after the SPI has resolved every name and refused
 * everything refusable: an executor receives this, never raw request JSON,
 * and never makes a policy choice. Field names are the engine's physical
 * names (the index field name, the table column), resolved from the
 * mapping's members by the compiler.
 *
 * @param subject the mapping subject, for evidence and errors
 * @param backend the engine that will run the reduction, already resolved
 * @param measures the measures to compute, in request order
 * @param dimensions the group-by dimensions, in request order
 * @param filters the query-wide row filters (AND), in request order
 * @param limit maximum result rows; already bounds-checked
 */
public record CompiledMetricQuery(
        String subject,
        MetricBackend backend,
        List<Measure> measures,
        List<Dimension> dimensions,
        List<EqualsFilter> filters,
        int limit) {

    public CompiledMetricQuery {
        measures = List.copyOf(measures);
        dimensions = List.copyOf(dimensions);
        filters = List.copyOf(filters);
    }

    /**
     * One measure to compute.
     *
     * @param member the public member name (the result-row key)
     * @param fieldName the engine's physical field name; empty for COUNT
     * @param fieldPath the proto field path (dot-joined field names), the
     *        column address for engines that keep the message's nesting
     *        (an Iceberg struct column); empty for COUNT
     * @param aggregate the reduction to run
     * @param rowFilters this measure's extra row filters (from filter_cel),
     *        ANDed with the query-wide filters; empty = all rows
     */
    public record Measure(
            String member, String fieldName, String fieldPath, Aggregate aggregate,
            List<EqualsFilter> rowFilters) {
        public Measure {
            rowFilters = List.copyOf(rowFilters);
        }
    }

    /**
     * One group-by dimension.
     *
     * @param member the public member name (the result-row key)
     * @param fieldName the engine's physical field name
     * @param fieldPath the proto field path (dot-joined field names)
     * @param kind how the engine buckets it
     * @param grain the resolved calendar grain; UNSPECIFIED unless DATE
     */
    public record Dimension(
            String member, String fieldName, String fieldPath, DimensionKind kind,
            TimeGrain grain) {
    }

    /** How an engine buckets a dimension. */
    public enum DimensionKind {
        /** Term buckets over a keyword-like field. */
        TERM,
        /** Boolean buckets. */
        BOOLEAN,
        /** Calendar buckets over an epoch field, under the grain. */
        DATE
    }

    /**
     * One equality filter: a row matches when the field equals any value.
     *
     * @param member the public member name, for evidence and errors
     * @param fieldName the engine's physical field name
     * @param fieldPath the proto field path (dot-joined field names)
     * @param kind how the engine matches it
     * @param values the legal values; never empty
     */
    public record EqualsFilter(
            String member, String fieldName, String fieldPath, DimensionKind kind,
            List<String> values) {
        public EqualsFilter {
            values = List.copyOf(values);
        }
    }
}
