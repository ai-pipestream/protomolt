package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.Dimension;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.Measure;
import ai.protomolt.proto.metric.spi.MetricRefusal;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Per-leaf doc-value cursors for a query's dimensions and measures: the
 * decoding half of the collector. Cursors are opened on the leaf reader the
 * executor's collector hands in and live for that leaf only; this class
 * never opens or closes a reader or searcher, so reader ownership stays
 * where it was.
 */
final class LuceneMetricLeaf {

    private final List<DimensionCursor> dimensions = new ArrayList<>();
    private final List<MeasureCursor> measures = new ArrayList<>();

    LuceneMetricLeaf(LeafReader leafReader, CompiledMetricQuery query,
            Map<String, ResolvedFieldHint> hints, DistinctBudget[] budgets)
            throws IOException {
        for (Dimension dimension : query.dimensions()) {
            dimensions.add(new DimensionCursor(leafReader, dimension, hints));
        }
        for (int m = 0; m < query.measures().size(); m++) {
            measures.add(new MeasureCursor(
                    leafReader, query.measures().get(m), hints, budgets[m]));
        }
    }

    /** The doc's group labels, or null when a dimension value is absent. */
    List<String> groupKey(int doc) throws IOException {
        List<String> key = new ArrayList<>(dimensions.size());
        for (DimensionCursor dimension : dimensions) {
            String label = dimension.label(doc);
            if (label == null) {
                return null;
            }
            key.add(label);
        }
        return key;
    }

    void accumulate(int doc, LuceneMetricResults.GroupState group) throws IOException {
        for (int m = 0; m < measures.size(); m++) {
            measures.get(m).accumulate(doc, group.slots[m]);
        }
    }

    /**
     * Requires the doc-value type a field needs, naming the fix when the
     * field is present without them. A leaf with no trace of the field is
     * data, not misconfiguration: proto3's implicit presence drops
     * default-valued scalars, so a segment of all-default documents simply
     * never wrote the field.
     */
    private static void requireDocValues(
            LeafReader leafReader, String field, DocValuesType... accepted) {
        var info = leafReader.getFieldInfos().fieldInfo(field);
        if (info == null) {
            return;
        }
        for (DocValuesType type : accepted) {
            if (info.getDocValuesType() == type) {
                return;
            }
        }
        throw new IllegalStateException("field '" + field + "' has no "
                + accepted[0] + " doc values (found " + info.getDocValuesType()
                + "); declare facetable or sortable on its indexing hint and re-index");
    }

    private static final class DimensionCursor {

        private final Dimension dimension;
        private final SortedSetDocValues terms;
        private final SortedNumericDocValues numbers;
        private final boolean dateSeconds;

        DimensionCursor(LeafReader leafReader, Dimension dimension,
                Map<String, ResolvedFieldHint> hints) throws IOException {
            this.dimension = dimension;
            if (dimension.kind() == CompiledMetricQuery.DimensionKind.DATE) {
                requireDocValues(leafReader, dimension.fieldName(),
                        DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
                this.terms = null;
                this.numbers = DocValues.getSortedNumeric(leafReader, dimension.fieldName());
                ResolvedFieldHint hint = hints.get(dimension.fieldName());
                this.dateSeconds = hint != null && hint.dateResolution() != null
                        && "SECONDS".equals(hint.dateResolution().name());
            } else {
                requireDocValues(leafReader, dimension.fieldName(),
                        DocValuesType.SORTED_SET, DocValuesType.SORTED);
                this.terms = DocValues.getSortedSet(leafReader, dimension.fieldName());
                this.numbers = null;
                this.dateSeconds = false;
            }
        }

        /** The doc's bucket label; null when the doc carries no value. */
        String label(int doc) throws IOException {
            if (terms != null) {
                if (!terms.advanceExact(doc)) {
                    return null;
                }
                if (dimension.kind() == CompiledMetricQuery.DimensionKind.TREE_PATH) {
                    // The doc values hold the whole ancestor chain; the leaf
                    // labels the bucket. Every ancestor is a strict prefix of
                    // the full path, so the leaf is the chain's last ord.
                    BytesRef last = null;
                    for (int i = 0; i < terms.docValueCount(); i++) {
                        last = terms.lookupOrd(terms.nextOrd());
                    }
                    return last.utf8ToString();
                }
                BytesRef term = terms.lookupOrd(terms.nextOrd());
                return term.utf8ToString();
            }
            if (!numbers.advanceExact(doc)) {
                return null;
            }
            long epoch = numbers.nextValue();
            long millis = dateSeconds ? epoch * 1000 : epoch;
            return bucket(millis, dimension.grain());
        }

        private static String bucket(long epochMillis, TimeGrain grain) {
            LocalDate date = Instant.ofEpochMilli(epochMillis)
                    .atZone(ZoneOffset.UTC).toLocalDate();
            return switch (grain) {
                case TIME_GRAIN_DAY -> date.toString();
                case TIME_GRAIN_WEEK -> "%d-W%02d".formatted(
                        date.get(IsoFields.WEEK_BASED_YEAR),
                        date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
                case TIME_GRAIN_MONTH -> "%d-%02d".formatted(
                        date.getYear(), date.getMonthValue());
                case TIME_GRAIN_QUARTER -> "%d-Q%d".formatted(
                        date.getYear(), (date.getMonthValue() - 1) / 3 + 1);
                default -> Integer.toString(date.getYear());
            };
        }
    }

    private static final class MeasureCursor {

        private final Measure measure;
        private final SortedNumericDocValues values;
        private final SortedSetDocValues distinctTerms;
        private final DistinctBudget budget;
        private final boolean floatEncoded;
        private final boolean doubleEncoded;
        private final List<RowFilterCursor> rowFilters = new ArrayList<>();

        MeasureCursor(LeafReader leafReader, Measure measure,
                Map<String, ResolvedFieldHint> hints, DistinctBudget budget)
                throws IOException {
            this.measure = measure;
            this.budget = budget;
            switch (measure.aggregate()) {
                case AGGREGATE_COUNT -> {
                    this.values = null;
                    this.distinctTerms = null;
                    this.floatEncoded = false;
                    this.doubleEncoded = false;
                }
                case AGGREGATE_COUNT_DISTINCT -> {
                    // Distinct runs over keyword terms or raw numeric values.
                    // Raw is enough for numerics: the sortable encodings are
                    // bijective, so cardinality is identical undecoded.
                    requireDocValues(leafReader, measure.fieldName(),
                            DocValuesType.SORTED_SET, DocValuesType.SORTED,
                            DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
                    var info = leafReader.getFieldInfos().fieldInfo(measure.fieldName());
                    boolean keyword = info != null
                            && (info.getDocValuesType() == DocValuesType.SORTED_SET
                                    || info.getDocValuesType() == DocValuesType.SORTED);
                    this.distinctTerms = keyword
                            ? DocValues.getSortedSet(leafReader, measure.fieldName())
                            : null;
                    this.values = keyword || info == null
                            ? null
                            : DocValues.getSortedNumeric(leafReader, measure.fieldName());
                    this.floatEncoded = false;
                    this.doubleEncoded = false;
                }
                default -> {
                    requireDocValues(leafReader, measure.fieldName(),
                            DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
                    this.values = DocValues.getSortedNumeric(leafReader, measure.fieldName());
                    this.distinctTerms = null;
                    ResolvedFieldHint hint = hints.get(measure.fieldName());
                    String kind = hint == null ? "" : hint.type().name();
                    // The mapper writes FLOAT and DOUBLE facetable doc values in
                    // their sortable encoding; raw ints and dates stay raw.
                    this.floatEncoded = kind.endsWith("FLOAT");
                    this.doubleEncoded = kind.endsWith("DOUBLE");
                }
            }
            for (EqualsFilter filter : measure.rowFilters()) {
                rowFilters.add(new RowFilterCursor(leafReader, filter));
            }
        }

        void accumulate(int doc, LuceneMetricResults.MeasureState state) throws IOException {
            for (RowFilterCursor filter : rowFilters) {
                if (!filter.matches(doc)) { // zero-regex: allowed
                    return;
                }
            }
            if (measure.aggregate() == Aggregate.AGGREGATE_COUNT_DISTINCT) {
                if (distinctTerms != null && distinctTerms.advanceExact(doc)) {
                    for (int i = 0; i < distinctTerms.docValueCount(); i++) {
                        state.addDistinct(distinctTerms
                                .lookupOrd(distinctTerms.nextOrd()).utf8ToString(), budget);
                    }
                } else if (values != null && values.advanceExact(doc)) {
                    for (int i = 0; i < values.docValueCount(); i++) {
                        state.addDistinct(values.nextValue(), budget);
                    }
                }
                return;
            }
            if (values == null) {
                state.count++;
                return;
            }
            if (!values.advanceExact(doc)) {
                return;
            }
            long raw = values.nextValue();
            double value = doubleEncoded
                    ? NumericUtils.sortableLongToDouble(raw)
                    : floatEncoded
                            ? NumericUtils.sortableIntToFloat((int) raw)
                            : raw;
            state.count++;
            state.sum += value;
            state.min = state.present ? Math.min(state.min, value) : value;
            state.max = state.present ? Math.max(state.max, value) : value;
            state.present = true;
        }
    }

    /**
     * The exact-count budget one COUNT_DISTINCT measure spends across all
     * groups. Passing it refuses the query by name, never an estimate and
     * never a silently truncated count, because the sets live in memory
     * for the query's duration and the Iceberg backend runs the same
     * query unbounded.
     */
    static final class DistinctBudget {

        private final String member;
        private final int bound;
        private int remaining;

        DistinctBudget(String member, int bound) {
            this.member = member;
            this.bound = bound;
            this.remaining = bound;
        }

        void spend() {
            if (--remaining < 0) {
                throw new MetricRefusal(MetricRefusal.DISTINCT_BOUND,
                        "count_distinct over '" + member + "' passed this engine's bound"
                                + " of " + bound + " tracked values; the Iceberg backend"
                                + " runs count_distinct unbounded",
                        List.of());
            }
        }
    }

    /** One measure's row filter, resolved to this leaf's term ordinals. */
    private static final class RowFilterCursor {

        private final SortedSetDocValues terms;
        private final Set<Long> accepted = new HashSet<>();

        RowFilterCursor(LeafReader leafReader, EqualsFilter filter) throws IOException {
            requireDocValues(leafReader, filter.fieldName(),
                    DocValuesType.SORTED_SET, DocValuesType.SORTED);
            this.terms = DocValues.getSortedSet(leafReader, filter.fieldName());
            for (String value : filter.values()) {
                long ord = terms.lookupTerm(new BytesRef(value));
                if (ord >= 0) {
                    accepted.add(ord);
                }
            }
        }

        boolean matches(int doc) throws IOException {
            if (accepted.isEmpty() || !terms.advanceExact(doc)) {
                return false;
            }
            for (int i = 0; i < terms.docValueCount(); i++) {
                if (accepted.contains(terms.nextOrd())) {
                    return true;
                }
            }
            return false;
        }
    }
}
