package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.Dimension;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.Measure;
import ai.protomolt.proto.metric.spi.MetricExecutor;
import ai.protomolt.proto.metric.spi.MetricRefusal;
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
import java.util.Locale;
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
 * values the search service already writes, so aggregation is a read path over
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
                    Aggregate.AGGREGATE_MIN, Aggregate.AGGREGATE_MAX,
                    Aggregate.AGGREGATE_COUNT_DISTINCT),
            true, true);

    /** Distinct values one COUNT_DISTINCT measure may track, by default. */
    public static final int DEFAULT_DISTINCT_BOUND = 100_000;

    private final SubjectReader reader;
    private final int distinctBound;

    public LuceneMetricExecutor(SubjectReader reader) {
        this(reader, DEFAULT_DISTINCT_BOUND);
    }

    /**
     * @param reader the subjects this executor can reach
     * @param distinctBound the most distinct values one COUNT_DISTINCT
     *        measure may track across all groups; past it the query is
     *        refused (never estimated, never silently truncated) naming
     *        the Iceberg backend as the engine that spills
     */
    public LuceneMetricExecutor(SubjectReader reader, int distinctBound) {
        if (reader == null) {
            throw new IllegalArgumentException("reader must not be null");
        }
        if (distinctBound <= 0) {
            throw new IllegalArgumentException("distinctBound must be positive");
        }
        this.reader = reader;
        this.distinctBound = distinctBound;
    }

    @Override
    public MetricBackend backend() {
        return MetricBackend.METRIC_BACKEND_LUCENE;
    }

    /**
     * COUNT_DISTINCT counts exactly up to {@link #DEFAULT_DISTINCT_BOUND}
     * tracked values per measure (the sets live in memory for the query's
     * duration); a query that passes the bound is refused by name, never
     * estimated, and the Iceberg backend runs the same query unbounded.
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
        Query filter = filterQuery(query, hints);
        Map<List<String>, GroupState> groups = new HashMap<>();
        DistinctBudget[] budgets = new DistinctBudget[query.measures().size()];
        for (int m = 0; m < query.measures().size(); m++) {
            if (query.measures().get(m).aggregate() == Aggregate.AGGREGATE_COUNT_DISTINCT) {
                budgets[m] = new DistinctBudget(
                        query.measures().get(m).member(), distinctBound);
            }
        }

        searcher.search(filter, new SimpleCollector() {
            private LeafState leaf;

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                leaf = new LeafState(context.reader(), query, hints, budgets);
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
        return new Result(rows, plan(query, filter, groups.size(), truncated, distinctBound));
    }

    private static String plan(CompiledMetricQuery query, Query filter, int groups,
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

    private static Query filterQuery(
            CompiledMetricQuery query, Map<String, ResolvedFieldHint> hints) {
        if (query.filters().isEmpty() && query.dateRanges().isEmpty()
                && query.pathPrefixes().isEmpty()) {
            return new MatchAllDocsQuery();
        }
        BooleanQuery.Builder all = new BooleanQuery.Builder();
        for (CompiledMetricQuery.PathPrefixFilter prefix : query.pathPrefixes()) {
            // The mapper indexed the whole ancestor chain as terms, so
            // descendant-or-self is one exact term match, never a scan.
            all.add(new TermQuery(new Term(prefix.fieldName(), prefix.path())),
                    BooleanClause.Occur.MUST);
        }
        for (EqualsFilter filter : query.filters()) {
            BooleanQuery.Builder any = new BooleanQuery.Builder();
            for (String value : filter.values()) {
                any.add(new TermQuery(new Term(filter.fieldName(), value)),
                        BooleanClause.Occur.SHOULD);
            }
            all.add(any.build(), BooleanClause.Occur.MUST);
        }
        for (CompiledMetricQuery.DateRangeFilter range : query.dateRanges()) {
            // The bounds arrive as inclusive UTC epoch millis; a field that
            // stores seconds compares in seconds (day bounds divide evenly
            // downward, so inclusiveness survives the floor).
            ResolvedFieldHint hint = hints.get(range.fieldName());
            boolean seconds = hint != null && hint.dateResolution() != null
                    && "SECONDS".equals(hint.dateResolution().name());
            long lower = range.gteEpochMillis() == null
                    ? Long.MIN_VALUE
                    : seconds ? Math.floorDiv(range.gteEpochMillis(), 1000)
                            : range.gteEpochMillis();
            long upper = range.lteEpochMillis() == null
                    ? Long.MAX_VALUE
                    : seconds ? Math.floorDiv(range.lteEpochMillis(), 1000)
                            : range.lteEpochMillis();
            all.add(org.apache.lucene.document.SortedNumericDocValuesField
                    .newSlowRangeQuery(range.fieldName(), lower, upper),
                    BooleanClause.Occur.MUST);
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
                Map<String, ResolvedFieldHint> hints, DistinctBudget[] budgets)
                throws IOException {
            this.query = query;
            this.hints = hints;
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

        void accumulate(int doc, MeasureState state) throws IOException {
            for (RowFilterCursor filter : rowFilters) {
                if (!filter.matches(doc)) {
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
     * groups. Passing it refuses the query by name — never an estimate,
     * never a silently truncated count — because the sets live in memory
     * for the query's duration and the Iceberg backend runs the same
     * query unbounded.
     */
    private static final class DistinctBudget {

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
                case AGGREGATE_COUNT_DISTINCT ->
                        (double) (state.distinct == null ? 0 : state.distinct.size());
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
        Set<Object> distinct;

        void addDistinct(Object value, DistinctBudget budget) {
            if (distinct == null) {
                distinct = new HashSet<>();
            }
            if (distinct.add(value)) {
                budget.spend();
            }
        }
    }
}
