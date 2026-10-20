package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.Measure;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.lucene.search.Query;

/**
 * The accumulation state the collector fills and the decoding of that state
 * into sorted, limited {@link MetricRow}s plus the rendered physical plan.
 * Missing-value semantics live here: a group's measure that saw no value
 * decodes to no row entry, exactly as before the extraction.
 */
final class LuceneMetricResults {

    private LuceneMetricResults() {
    }

    /** The finished rows: group keys sorted, limited, measures decoded. */
    static List<MetricRow> rows(CompiledMetricQuery query, Map<List<String>, GroupState> groups) {
        List<List<String>> keys = new ArrayList<>(groups.keySet());
        keys.sort(Comparator.comparing(List::toString));
        List<MetricRow> rows = new ArrayList<>();
        for (List<String> key : keys.subList(0, Math.min(keys.size(), query.limit()))) {
            MetricRow.Builder row = MetricRow.newBuilder();
            for (int d = 0; d < query.dimensions().size(); d++) {
                row.putDimensions(query.dimensions().get(d).member(), key.get(d));
            }
            GroupState group = groups.get(key);
            for (int m = 0; m < query.measures().size(); m++) {
                Measure measure = query.measures().get(m);
                Double value = group.value(m, measure.aggregate());
                if (value != null) {
                    row.putMeasures(measure.member(), value);
                }
            }
            rows.add(row.build());
        }
        return rows;
    }

    /** The human-readable physical plan the result carries. */
    static String plan(CompiledMetricQuery query, Query filter, int groups,
            boolean truncated, int distinctBound) {
        StringJoiner measures = new StringJoiner(", ");
        for (Measure measure : query.measures()) {
            String base = measure.aggregate().name().substring("AGGREGATE_".length())
                    .toLowerCase(Locale.ROOT) + (measure.fieldName().isEmpty()
                            ? "()" : "(" + measure.fieldName() + ")");
            if (!measure.rowFilters().isEmpty()) {
                StringJoiner where = new StringJoiner(" and ");
                measure.rowFilters().forEach(f ->
                        where.add(f.fieldName() + " in " + f.values()));
                base += " where " + where;
            }
            measures.add(measure.member() + "=" + base);
        }
        StringJoiner dims = new StringJoiner(", ");
        query.dimensions().forEach(d -> dims.add(d.member()
                + (d.kind() == CompiledMetricQuery.DimensionKind.DATE
                        ? " by " + d.grain().name().substring("TIME_GRAIN_".length())
                                .toLowerCase(Locale.ROOT)
                        : "")));
        boolean distinct = query.measures().stream().anyMatch(
                m -> m.aggregate() == Aggregate.AGGREGATE_COUNT_DISTINCT);
        return "lucene collector: filter=" + filter + "; group-by=[" + dims + "]; measures=["
                + measures + "]; groups=" + groups + (truncated ? " (truncated to "
                + query.limit() + ")" : "")
                + (distinct ? "; count_distinct exact, bound=" + distinctBound : "")
                + "; docs missing a dimension value are excluded";
    }

    /** One group's accumulation slots, one per measure. */
    static final class GroupState {

        final MeasureState[] slots;

        GroupState(int measures) {
            slots = new MeasureState[measures];
            for (int i = 0; i < measures; i++) {
                slots[i] = new MeasureState();
            }
        }

        /** The finished value of slot {@code m}; null when nothing landed. */
        Double value(int m, Aggregate aggregate) {
            MeasureState state = slots[m];
            return switch (aggregate) {
                case AGGREGATE_COUNT -> (double) state.count;
                case AGGREGATE_SUM -> state.present ? state.sum : null;
                case AGGREGATE_AVG -> state.count > 0 && state.present
                        ? state.sum / state.count : null;
                case AGGREGATE_MIN -> state.present ? state.min : null;
                case AGGREGATE_MAX -> state.present ? state.max : null;
                case AGGREGATE_COUNT_DISTINCT ->
                        (double) (state.distinct == null ? 0 : state.distinct.size());
                default -> null;
            };
        }
    }

    /** One measure's accumulation state within one group. */
    static final class MeasureState {
        long count;
        double sum;
        double min;
        double max;
        boolean present;
        Set<Object> distinct;

        void addDistinct(Object value, LuceneMetricLeaf.DistinctBudget budget) {
            if (distinct == null) {
                distinct = new HashSet<>();
            }
            if (distinct.add(value)) {
                budget.spend();
            }
        }
    }
}
