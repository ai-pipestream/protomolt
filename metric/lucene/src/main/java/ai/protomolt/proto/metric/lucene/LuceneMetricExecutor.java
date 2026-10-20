package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.MetricExecutor;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

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
 *
 * <p>Collaborators, all package-private: {@link LuceneMetricQueries} builds
 * the filter query and the field-hint index, {@link LuceneMetricLeaf} owns
 * the per-leaf doc-value cursors, and {@link LuceneMetricResults} holds the
 * accumulation state and decodes it into rows and the physical plan.</p>
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
        Map<String, ResolvedFieldHint> hints = LuceneMetricQueries.hintsByFieldName(mapping);
        Query filter = LuceneMetricQueries.filterQuery(query, hints);
        Map<List<String>, LuceneMetricResults.GroupState> groups = new HashMap<>();
        LuceneMetricLeaf.DistinctBudget[] budgets =
                new LuceneMetricLeaf.DistinctBudget[query.measures().size()];
        for (int m = 0; m < query.measures().size(); m++) {
            if (query.measures().get(m).aggregate() == Aggregate.AGGREGATE_COUNT_DISTINCT) {
                budgets[m] = new LuceneMetricLeaf.DistinctBudget(
                        query.measures().get(m).member(), distinctBound);
            }
        }

        searcher.search(filter, new SimpleCollector() {
            private LuceneMetricLeaf leaf;

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                leaf = new LuceneMetricLeaf(context.reader(), query, hints, budgets);
            }

            @Override
            public void collect(int doc) throws IOException {
                List<String> key = leaf.groupKey(doc);
                if (key == null) {
                    return;
                }
                LuceneMetricResults.GroupState group = groups.computeIfAbsent(
                        key, k -> new LuceneMetricResults.GroupState(query.measures().size()));
                leaf.accumulate(doc, group);
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });

        boolean truncated = groups.size() > query.limit();
        List<MetricRow> rows = LuceneMetricResults.rows(query, groups);
        return new Result(rows, LuceneMetricResults.plan(
                query, filter, groups.size(), truncated, distinctBound));
    }
}
