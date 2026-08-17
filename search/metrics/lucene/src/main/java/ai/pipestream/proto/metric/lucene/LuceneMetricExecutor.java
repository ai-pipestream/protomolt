package ai.pipestream.proto.metric.lucene;

import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Dimension;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Measure;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * The shipped default metric engine: single-pass collectors over the doc
 * values the search door already writes, so aggregation is a read path over
 * existing storage with no extra infrastructure. Group-by members read
 * sorted-set (keyword, bool) and sorted-numeric (date, numeric) doc values;
 * FLOAT and DOUBLE doc values are sortable-encoded by the mapper and are
 * decoded here.
 *
 * <p>A field without the doc values a query needs fails loudly naming the
 * indexing hint to declare, never silently answering zero. Documents
 * missing a selected dimension's value are excluded from that query's
 * groups, and the physical plan says so.</p>
 */
public final class LuceneMetricExecutor implements MetricExecutor {

    /** The subjects this executor can reach: a mapping and a borrowed searcher. */
    public interface SubjectReader {

        /** The subject's index mapping, for field shapes and date units. */
        IndexMapping mapping(String subject);

        /** Runs one aggregation over the subject's live searcher. */
        Result read(String subject, Aggregation aggregation);
    }

    /** One aggregation over a borrowed searcher. */
    @FunctionalInterface
    public interface Aggregation {
        Result run(IndexSearcher searcher) throws IOException;
    }

    private static final Capabilities CAPABILITIES = new Capabilities(
            Set.of(Aggregate.AGGREGATE_COUNT, Aggregate.AGGREGATE_SUM, Aggregate.AGGREGATE_AVG,
                    Aggregate.AGGREGATE_MIN, Aggregate.AGGREGATE_MAX),
            true, true);

    private final SubjectReader reader;

    public LuceneMetricExecutor(SubjectReader reader) {
        if (reader == null) {
            throw new IllegalArgumentException("reader must not be null");
        }
        this.reader = reader;
    }

    @Override
    public MetricBackend backend() {
        return MetricBackend.METRIC_BACKEND_LUCENE;
    }

    /**
     * COUNT_DISTINCT is deliberately absent: no bounded collector exists
     * yet, and an unbounded one could hold every term in memory.
     */
    @Override
    public Capabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public Result execute(CompiledMetricQuery query) {
        IndexMapping mapping = reader.mapping(query.subject());
        return reader.read(query.subject(), searcher -> aggregate(searcher, mapping, query));
    }

    // ------------------------------------------------------------- collection

    private Result aggregate(
            IndexSearcher searcher, IndexMapping mapping, CompiledMetricQuery query)
            throws IOException {
        Map<String, ResolvedFieldHint> hints = hintsByFieldName(mapping);
        Query filter = filterQuery(query.filters());
        Map<List<String>, GroupState> groups = new HashMap<>();

        searcher.search(filter, new SimpleCollector() {
            private LeafState leaf;

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                leaf = new LeafState(context.reader(), query, hints);
            }

            @Override
            public void collect(int doc) throws IOException {
                List<String> key = leaf.groupKey(doc);
                if (key == null) {
                    return;
                }
                GroupState group = groups.computeIfAbsent(
                        key, k -> new GroupState(query.measures().size()));
                leaf.accumulate(doc, group);
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });

        List<List<String>> keys = new ArrayList<>(groups.keySet());
        keys.sort(Comparator.comparing(List::toString));
        boolean truncated = keys.size() > query.limit();
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
        return new Result(rows, plan(query, filter, groups.size(), truncated));
    }

    private static String plan(
            CompiledMetricQuery query, Query filter, int groups, boolean truncated) {
        StringJoiner measures = new StringJoiner(", ");
        for (Measure measure : query.measures()) {
            String base = measure.aggregate().name().substring("AGGREGATE_".length())
                    .toLowerCase() + (measure.fieldName().isEmpty()
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
                                .toLowerCase()
                        : "")));
        return "lucene collector: filter=" + filter + "; group-by=[" + dims + "]; measures=["
                + measures + "]; groups=" + groups + (truncated ? " (truncated to "
                + query.limit() + ")" : "")
                + "; docs missing a dimension value are excluded";
    }

    private static Query filterQuery(List<EqualsFilter> filters) {
        if (filters.isEmpty()) {
            return new MatchAllDocsQuery();
        }
        BooleanQuery.Builder all = new BooleanQuery.Builder();
        for (EqualsFilter filter : filters) {
            BooleanQuery.Builder any = new BooleanQuery.Builder();
            for (String value : filter.values()) {
                any.add(new TermQuery(new Term(filter.fieldName(), value)),
                        BooleanClause.Occur.SHOULD);
            }
            all.add(any.build(), BooleanClause.Occur.MUST);
        }
        return all.build();
    }

    private static Map<String, ResolvedFieldHint> hintsByFieldName(IndexMapping mapping) {
        Map<String, ResolvedFieldHint> hints = new HashMap<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            hints.put(field.fieldName(), field.hint());
        }
        return hints;
    }

    // ------------------------------------------------------------- leaf state

    /** Per-leaf doc-value cursors for the query's dimensions and measures. */
    private static final class LeafState {

        private final CompiledMetricQuery query;
        private final Map<String, ResolvedFieldHint> hints;
        private final List<DimensionCursor> dimensions = new ArrayList<>();
        private final List<MeasureCursor> measures = new ArrayList<>();

        LeafState(LeafReader leafReader, CompiledMetricQuery query,
                Map<String, ResolvedFieldHint> hints) throws IOException {
            this.query = query;
            this.hints = hints;
            for (Dimension dimension : query.dimensions()) {
                dimensions.add(new DimensionCursor(leafReader, dimension, hints));
            }
            for (Measure measure : query.measures()) {
                measures.add(new MeasureCursor(leafReader, measure, hints));
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

        void accumulate(int doc, GroupState group) throws IOException {
            for (int m = 0; m < measures.size(); m++) {
                measures.get(m).accumulate(doc, group.slots[m]);
            }
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
        private final boolean floatEncoded;
        private final boolean doubleEncoded;
        private final List<RowFilterCursor> rowFilters = new ArrayList<>();

        MeasureCursor(LeafReader leafReader, Measure measure,
                Map<String, ResolvedFieldHint> hints) throws IOException {
            this.measure = measure;
            if (measure.aggregate() == Aggregate.AGGREGATE_COUNT) {
                this.values = null;
                this.floatEncoded = false;
                this.doubleEncoded = false;
            } else {
                requireDocValues(leafReader, measure.fieldName(),
                        DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
                this.values = DocValues.getSortedNumeric(leafReader, measure.fieldName());
                ResolvedFieldHint hint = hints.get(measure.fieldName());
                String kind = hint == null ? "" : hint.type().name();
                // The mapper writes FLOAT and DOUBLE facetable doc values in
                // their sortable encoding; raw ints and dates stay raw.
                this.floatEncoded = kind.endsWith("FLOAT");
                this.doubleEncoded = kind.endsWith("DOUBLE");
            }
            for (EqualsFilter filter : measure.rowFilters()) {
                rowFilters.add(new RowFilterCursor(leafReader, filter));
            }
        }

        void accumulate(int doc, MeasureState state) throws IOException {
            for (RowFilterCursor filter : rowFilters) {
                if (!filter.matches(doc)) {
                    return;
                }
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

    // ------------------------------------------------------------- group state

    private static final class GroupState {

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
                default -> null;
            };
        }
    }

    private static final class MeasureState {
        long count;
        double sum;
        double min;
        double max;
        boolean present;
    }
}
