package ai.pipestream.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.DateRangeFilter;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Dimension;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Measure;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.PathPrefixFilter;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The statement this renders is the query. Two things about it are load-bearing and neither
 * is visible from a passing end-to-end run: every value reaches DuckDB as a bind parameter
 * rather than as text spliced into the statement, and the parameters are handed over in the
 * order their placeholders appear. Get the order wrong and the query still runs, still
 * returns rows, and answers a different question than the one asked.
 */
class DuckDbSqlTest {

    private static final List<String> ONE_FILE = List.of("/lake/data/part-0.parquet");

    private static CompiledMetricQuery query(List<Measure> measures, List<Dimension> dimensions,
            List<EqualsFilter> filters, List<DateRangeFilter> dateRanges,
            List<PathPrefixFilter> prefixes) {
        return new CompiledMetricQuery("subject", MetricBackend.METRIC_BACKEND_UNSPECIFIED,
                measures, dimensions, filters, dateRanges, prefixes, 50);
    }

    private static Measure count() {
        return new Measure("hits", "", "", Aggregate.AGGREGATE_COUNT, List.of());
    }

    private static Dimension term(String member, String path) {
        return new Dimension(member, path, path, DimensionKind.TERM,
                TimeGrain.TIME_GRAIN_UNSPECIFIED);
    }

    private static EqualsFilter equals(String path, DimensionKind kind, String... values) {
        return new EqualsFilter(path, path, path, kind, List.of(values));
    }

    // --- shape ------------------------------------------------------------------

    @Test
    void aPlainCountReadsTheFilesAndGroupsNothing() {
        DuckDbSql sql = DuckDbSql.render(
                query(List.of(count()), List.of(), List.of(), List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).isEqualTo(
                "SELECT COUNT(*) FROM read_parquet(['/lake/data/part-0.parquet'])");
        assertThat(sql.parameters()).isEmpty();
    }

    @Test
    void groupingAddsGroupOrderAndLimitByPosition() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()),
                List.of(term("a", "alpha"), term("b", "beta")),
                List.of(), List.of(), List.of()), ONE_FILE);

        assertThat(sql.text())
                .contains("SELECT \"alpha\", \"beta\", COUNT(*)")
                .contains(" GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 50");
    }

    @Test
    void aGroupedDimensionSkipsRowsWithNothingInIt() {
        // An empty string is a value the writer left blank, not a bucket worth reporting.
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()), List.of(term("a", "alpha")),
                List.of(), List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).contains("WHERE (\"alpha\" IS NOT NULL AND \"alpha\" <> '')");
    }

    @Test
    void severalFilesBecomeOneList() {
        DuckDbSql sql = DuckDbSql.render(
                query(List.of(count()), List.of(), List.of(), List.of(), List.of()),
                List.of("/a.parquet", "/b.parquet"));

        assertThat(sql.text()).contains("read_parquet(['/a.parquet', '/b.parquet'])");
    }

    // --- columns ----------------------------------------------------------------

    @Test
    void aNestedPathBecomesStructExtraction() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()),
                List.of(term("m", "outer.middle.leaf")), List.of(), List.of(), List.of()),
                ONE_FILE);

        assertThat(sql.text()).contains("\"outer\"['middle']['leaf']");
    }

    /**
     * Field paths come from a descriptor, not from a caller, but the escaping is the only
     * thing standing between a quote in a name and a statement that means something else.
     */
    @Test
    void quotesInAPathAreEscapedRatherThanClosingTheIdentifier() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()),
                List.of(term("m", "we\"ird.od'd")), List.of(), List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).contains("\"we\"\"ird\"['od''d']");
    }

    @Test
    void quotesInAFilePathAreEscaped() {
        DuckDbSql sql = DuckDbSql.render(
                query(List.of(count()), List.of(), List.of(), List.of(), List.of()),
                List.of("/lake/it's/part-0.parquet"));

        assertThat(sql.text()).contains("['/lake/it''s/part-0.parquet']");
    }

    // --- values bind, never interpolate -----------------------------------------

    @Test
    void filterValuesBindAsParameters() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()), List.of(),
                List.of(equals("status", DimensionKind.TERM, "open", "closed")),
                List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).contains("WHERE \"status\" IN (?, ?)");
        assertThat(sql.parameters()).containsExactly("open", "closed");
    }

    @Test
    void aValueThatLooksLikeSqlStaysAValue() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()), List.of(),
                List.of(equals("status", DimensionKind.TERM, "') OR 1=1 --")),
                List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).doesNotContain("OR 1=1");
        assertThat(sql.parameters()).containsExactly("') OR 1=1 --");
    }

    @Test
    void booleanFiltersCastTheirBoundValue() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()), List.of(),
                List.of(equals("flag", DimensionKind.BOOLEAN, "true")),
                List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).contains("\"flag\" IN (CAST(? AS BOOLEAN))");
        assertThat(sql.parameters()).containsExactly("true");
    }

    /**
     * The select list is rendered before the where clause and its placeholders come first
     * in the statement, so a measure's own filters have to come first in the parameter
     * list. Nothing about a wrongly-ordered run looks wrong: it just answers differently.
     */
    @Test
    void measureFiltersBindAheadOfQueryFilters() {
        Measure filtered = new Measure("open_hits", "", "",
                Aggregate.AGGREGATE_COUNT,
                List.of(equals("status", DimensionKind.TERM, "measure-value")));
        DuckDbSql sql = DuckDbSql.render(query(List.of(filtered), List.of(),
                List.of(equals("region", DimensionKind.TERM, "query-value")),
                List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).contains("COUNT(*) FILTER (WHERE \"status\" IN (?))");
        assertThat(sql.parameters()).containsExactly("measure-value", "query-value");
        assertThat(sql.text().indexOf("FILTER (WHERE"))
                .isLessThan(sql.text().indexOf("WHERE \"region\""));
    }

    // --- aggregates -------------------------------------------------------------

    @Test
    void numericAggregatesCastTheirColumn() {
        for (Aggregate aggregate : List.of(Aggregate.AGGREGATE_SUM, Aggregate.AGGREGATE_AVG,
                Aggregate.AGGREGATE_MIN, Aggregate.AGGREGATE_MAX)) {
            DuckDbSql sql = DuckDbSql.render(query(
                    List.of(new Measure("m", "size", "size", aggregate, List.of())),
                    List.of(), List.of(), List.of(), List.of()), ONE_FILE);

            String name = aggregate.name().substring("AGGREGATE_".length());
            assertThat(sql.text()).contains(name + "(CAST(\"size\" AS DOUBLE))");
        }
    }

    @Test
    void countDistinctCountsTheColumnNotTheRows() {
        DuckDbSql sql = DuckDbSql.render(query(
                List.of(new Measure("m", "user", "user", Aggregate.AGGREGATE_COUNT_DISTINCT,
                        List.of())), List.of(), List.of(), List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).contains("COUNT(DISTINCT \"user\")");
    }

    // --- date grains ------------------------------------------------------------

    private static String labelFor(TimeGrain grain) {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()),
                List.of(new Dimension("d", "ts", "ts", DimensionKind.DATE, grain)),
                List.of(), List.of(), List.of()), ONE_FILE);
        return sql.text();
    }

    @Test
    void everyGrainLabelsItsBucketsInUtc() {
        assertThat(labelFor(TimeGrain.TIME_GRAIN_DAY))
                .contains("strftime(timezone('UTC', \"ts\"), '%Y-%m-%d')");
        assertThat(labelFor(TimeGrain.TIME_GRAIN_MONTH))
                .contains("strftime(timezone('UTC', \"ts\"), '%Y-%m')");
        assertThat(labelFor(TimeGrain.TIME_GRAIN_YEAR))
                .contains("CAST(year(timezone('UTC', \"ts\")) AS VARCHAR)");
        assertThat(labelFor(TimeGrain.TIME_GRAIN_QUARTER)).contains("'%d-Q%d'");
    }

    /**
     * A week belongs to the ISO year that owns it, which is not always the calendar year
     * the date falls in. Pairing weekofyear with year instead of isoyear mislabels the
     * turn of every year that straddles one.
     */
    @Test
    void weekBucketsUseTheIsoYearNotTheCalendarYear() {
        assertThat(labelFor(TimeGrain.TIME_GRAIN_WEEK))
                .contains("CAST(isoyear(timezone('UTC', \"ts\")) AS INT)")
                .contains("weekofyear(timezone('UTC', \"ts\"))");
    }

    @Test
    void aDateDimensionOnlySkipsNullsNotEmptyStrings() {
        assertThat(labelFor(TimeGrain.TIME_GRAIN_DAY)).contains("WHERE \"ts\" IS NOT NULL");
    }

    // --- date ranges ------------------------------------------------------------

    @Test
    void aClosedRangeBindsBothBoundsInOrder() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()), List.of(), List.of(),
                List.of(new DateRangeFilter("d", "ts", "ts", 1000L, 2000L)), List.of()),
                ONE_FILE);

        assertThat(sql.text())
                .contains("epoch_ms(timezone('UTC', \"ts\")) >= CAST(? AS BIGINT)")
                .contains("epoch_ms(timezone('UTC', \"ts\")) <= CAST(? AS BIGINT)");
        assertThat(sql.parameters()).containsExactly("1000", "2000");
    }

    @Test
    void anOpenSidedRangeBindsOnlyTheBoundItHas() {
        DuckDbSql lower = DuckDbSql.render(query(List.of(count()), List.of(), List.of(),
                List.of(new DateRangeFilter("d", "ts", "ts", 1000L, null)), List.of()),
                ONE_FILE);
        assertThat(lower.parameters()).containsExactly("1000");
        assertThat(lower.text()).contains(">=").doesNotContain("<=");

        DuckDbSql upper = DuckDbSql.render(query(List.of(count()), List.of(), List.of(),
                List.of(new DateRangeFilter("d", "ts", "ts", null, 2000L)), List.of()),
                ONE_FILE);
        assertThat(upper.parameters()).containsExactly("2000");
        assertThat(upper.text()).contains("<=").doesNotContain(">=");
    }

    // --- tree paths -------------------------------------------------------------

    @Test
    void aTreePathDimensionRendersTheSegmentsRatherThanAChainColumn() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()),
                List.of(new Dimension("p", "path", "path", DimensionKind.TREE_PATH,
                        TimeGrain.TIME_GRAIN_UNSPECIFIED)),
                List.of(), List.of(), List.of()), ONE_FILE);

        assertThat(sql.text()).contains("array_to_string(\"path\"['segments'], '/')");
    }

    /**
     * Descendant-or-self, and the slash in the second arm is the whole reason it is
     * correct: a plain prefix test would pull "a/b" into the subtree of "a/bc".
     */
    @Test
    void aPathPrefixMatchesTheNodeOrItsSubtreeAndNothingAdjacent() {
        DuckDbSql sql = DuckDbSql.render(query(List.of(count()), List.of(), List.of(),
                List.of(), List.of(new PathPrefixFilter("p", "path", "path", "a/b"))),
                ONE_FILE);

        String rendered = "array_to_string(\"path\"['segments'], '/')";
        assertThat(sql.text())
                .contains("(" + rendered + " = ? OR starts_with(" + rendered + ", ? || '/'))");
        assertThat(sql.parameters()).containsExactly("a/b", "a/b");
    }

    // --- everything at once -----------------------------------------------------

    /**
     * The parameter order across all four sources that contribute them, which is the one
     * property no single-feature test can check.
     */
    @Test
    void parametersFollowThePlaceholdersAcrossEverySource() {
        Measure filtered = new Measure("m", "", "", Aggregate.AGGREGATE_COUNT,
                List.of(equals("kind", DimensionKind.TERM, "p1")));
        DuckDbSql sql = DuckDbSql.render(query(
                List.of(filtered),
                List.of(term("d", "region")),
                List.of(equals("status", DimensionKind.TERM, "p2")),
                List.of(new DateRangeFilter("d", "ts", "ts", 3L, 4L)),
                List.of(new PathPrefixFilter("p", "path", "path", "p5"))), ONE_FILE);

        assertThat(sql.parameters()).containsExactly("p1", "p2", "p5", "p5", "3", "4");
        assertThat(countPlaceholders(sql.text())).isEqualTo(sql.parameters().size());
    }

    private static int countPlaceholders(String text) {
        return (int) text.chars().filter(c -> c == '?').count();
    }
}
