package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.DescribeMappingResponse;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricFilter;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The query compiler's contract: a compiled query carries resolved physical
 * names and the calculated measures' inputs, every refusal names its code
 * and the legal set, and calculated measures evaluate over the engine's
 * rows with unrequested inputs trimmed.
 */
class MetricQueriesTest {

    /** Records what it was asked and answers canned rows. */
    static final class FakeExecutor implements MetricExecutor {
        final MetricBackend backend;
        final Capabilities capabilities;
        final List<MetricRow> rows;
        CompiledMetricQuery executed;

        FakeExecutor(MetricBackend backend, Capabilities capabilities, List<MetricRow> rows) {
            this.backend = backend;
            this.capabilities = capabilities;
            this.rows = rows;
        }

        static FakeExecutor lucene(List<MetricRow> rows) {
            return new FakeExecutor(MetricBackend.METRIC_BACKEND_LUCENE,
                    new Capabilities(Set.of(Aggregate.AGGREGATE_COUNT, Aggregate.AGGREGATE_SUM,
                            Aggregate.AGGREGATE_AVG, Aggregate.AGGREGATE_MIN,
                            Aggregate.AGGREGATE_MAX), true, true),
                    rows);
        }

        @Override
        public MetricBackend backend() {
            return backend;
        }

        @Override
        public Capabilities capabilities() {
            return capabilities;
        }

        @Override
        public Result execute(CompiledMetricQuery query) {
            executed = query;
            return new Result(rows, "fake-plan");
        }
    }

    static MetricMapping mapping() throws Exception {
        return MetricMappings.build("", MetricMappingsTest.orders(),
                MetricMappingsTest.OPTIONS);
    }

    static QueryMetricsRequest.Builder request(String... measures) {
        QueryMetricsRequest.Builder builder = QueryMetricsRequest.newBuilder()
                .setMappingSubject("orders").setLimit(10);
        for (String measure : measures) {
            builder.addMeasures(measure);
        }
        return builder;
    }

    static MetricRefusal refusalOf(Runnable call) {
        try {
            call.run();
        } catch (MetricRefusal refusal) {
            return refusal;
        }
        throw new AssertionError("expected a MetricRefusal");
    }

    // ------------------------------------------------------------- date ranges

    @Test
    void aDateRangeCompilesToInclusiveUtcEpochBounds() throws Exception {
        FakeExecutor executor = FakeExecutor.lucene(List.of());
        MetricQueries.query(mapping(), Map.of(executor.backend(), executor),
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder()
                                        .setBegin("2026-07-01")
                                        .setEnd("2026-07-31")))
                        .build());
        CompiledMetricQuery.DateRangeFilter range = executor.executed.dateRanges().get(0);
        assertThat(range.member()).isEqualTo("created_at");
        assertThat(range.fieldName()).isEqualTo("created_at");
        assertThat(range.gteEpochMillis()).isEqualTo(
                java.time.Instant.parse("2026-07-01T00:00:00Z").toEpochMilli());
        assertThat(range.lteEpochMillis()).isEqualTo(
                java.time.Instant.parse("2026-08-01T00:00:00Z").toEpochMilli() - 1);

        // One open side is legal.
        MetricQueries.query(mapping(), Map.of(executor.backend(), executor),
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder().setBegin("2026-07-01")))
                        .build());
        assertThat(executor.executed.dateRanges().get(0).lteEpochMillis()).isNull();
    }

    @Test
    void excludedBoundDaysAreDroppedWhole() throws Exception {
        // The canonical DateRange semantics: an explicit include_head/include_tail
        // false drops that entire day, and the compiled bounds stay inclusive
        // millis so the executors never see the flags.
        FakeExecutor executor = FakeExecutor.lucene(List.of());
        MetricQueries.query(mapping(), Map.of(executor.backend(), executor),
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder()
                                        .setBegin("2026-07-01").setIncludeHead(false)
                                        .setEnd("2026-07-31").setIncludeTail(false)))
                        .build());
        CompiledMetricQuery.DateRangeFilter range = executor.executed.dateRanges().get(0);
        assertThat(range.gteEpochMillis()).isEqualTo(
                java.time.Instant.parse("2026-07-02T00:00:00Z").toEpochMilli());
        assertThat(range.lteEpochMillis()).isEqualTo(
                java.time.Instant.parse("2026-07-31T00:00:00Z").toEpochMilli() - 1);

        // Excluding both days of a two-day window leaves no day at all.
        MetricMapping mapping = mapping();
        assertThat(refusalOf(() -> MetricQueries.query(mapping,
                Map.of(executor.backend(), executor),
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder()
                                        .setBegin("2026-07-01").setIncludeHead(false)
                                        .setEnd("2026-07-02").setIncludeTail(false)))
                        .build())).getMessage())
                .contains("inverted or empty");
    }

    @Test
    void rangesRefuseEverythingButAWellFormedDateWindow() throws Exception {
        FakeExecutor executor = FakeExecutor.lucene(List.of());
        Map<ai.protomolt.proto.metric.MetricBackend, MetricExecutor> executors =
                Map.of(executor.backend(), executor);
        MetricMapping mapping = mapping();

        assertThat(refusalOf(() -> MetricQueries.query(mapping, executors,
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("segment")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder().setBegin("2026-07-01")))
                        .build())).getMessage())
                .contains("needs a DATE dimension");

        assertThat(refusalOf(() -> MetricQueries.query(mapping, executors,
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .addEquals("2026-07")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder().setBegin("2026-07-01")))
                        .build())).getMessage())
                .contains("more than one form");

        assertThat(refusalOf(() -> MetricQueries.query(mapping, executors,
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder()))
                        .build())).getMessage())
                .contains("no bounds");

        assertThat(refusalOf(() -> MetricQueries.query(mapping, executors,
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder()
                                        .setBegin("2026-08-01").setEnd("2026-07-01")))
                        .build())).getMessage())
                .contains("inverted");

        assertThat(refusalOf(() -> MetricQueries.query(mapping, executors,
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.protomolt.proto.types.DateRange
                                        .newBuilder().setBegin("July 1st")))
                        .build())).getMessage())
                .contains("not an ISO-8601 date");

        // A DATE dimension with an equality set points at the range form.
        assertThat(refusalOf(() -> MetricQueries.query(mapping, executors,
                request("revenue")
                        .addFilters(ai.protomolt.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .addEquals("2026-07"))
                        .build())).getMessage())
                .contains("a DATE dimension filters by range");
    }

    // ------------------------------------------------------------- happy path

    @Test
    void aCompiledQueryCarriesResolvedPhysicalNames() throws Exception {
        FakeExecutor executor = FakeExecutor.lucene(List.of(
                MetricRow.newBuilder().putDimensions("segment", "smb")
                        .putMeasures("revenue", 420.0).putMeasures("orders", 4.0).build()));

        QueryMetricsResponse response = MetricQueries.query(mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor),
                request("revenue", "orders")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("segment").addEquals("smb").addEquals("mid"))
                        .build());

        CompiledMetricQuery compiled = executor.executed;
        assertThat(compiled.subject()).isEqualTo("orders");
        assertThat(compiled.limit()).isEqualTo(10);
        assertThat(compiled.measures()).hasSize(2);
        assertThat(compiled.measures().get(0).fieldName()).isEqualTo("amount_cents");
        // COUNT needs no field: rows are counted, not read.
        assertThat(compiled.measures().get(1).fieldName()).isEmpty();
        assertThat(compiled.measures().get(1).rowFilters()).hasSize(2);
        assertThat(compiled.dimensions()).hasSize(1);
        assertThat(compiled.dimensions().get(0).fieldName()).isEqualTo("segment");
        assertThat(compiled.dimensions().get(0).kind()).isEqualTo(DimensionKind.TERM);
        assertThat(compiled.filters()).hasSize(1);
        assertThat(compiled.filters().get(0).values()).containsExactly("smb", "mid");

        assertThat(response.getBackend()).isEqualTo(MetricBackend.METRIC_BACKEND_LUCENE);
        assertThat(response.getRowCount()).isEqualTo(1);
        assertThat(response.getPhysicalPlan()).isEqualTo("fake-plan");
    }

    @Test
    void anUnsetBackendOnASingleEngineMountIsConfigurationNotAGuess() throws Exception {
        FakeExecutor executor = FakeExecutor.lucene(List.of());
        QueryMetricsResponse response = MetricQueries.query(mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor),
                request("revenue").build());
        assertThat(response.getBackend()).isEqualTo(MetricBackend.METRIC_BACKEND_LUCENE);
    }

    @Test
    void dateDimensionsResolveTheRequestedGrainOverTheDeclaredDefault() throws Exception {
        FakeExecutor executor = FakeExecutor.lucene(List.of());
        MetricQueries.query(mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor),
                request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("created_at")
                                .setGrain(TimeGrain.TIME_GRAIN_DAY))
                        .build());
        assertThat(executor.executed.dimensions().get(0).grain())
                .isEqualTo(TimeGrain.TIME_GRAIN_DAY);

        MetricQueries.query(mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor),
                request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("created_at"))
                        .build());
        assertThat(executor.executed.dimensions().get(0).grain())
                .isEqualTo(TimeGrain.TIME_GRAIN_MONTH);
    }

    @Test
    void calculatedMeasuresEvaluateOverTheEnginesRowsAndTrimTheirInputs() throws Exception {
        FakeExecutor executor = FakeExecutor.lucene(List.of(
                MetricRow.newBuilder().putDimensions("segment", "smb")
                        .putMeasures("revenue", 100.0).putMeasures("orders", 4.0).build(),
                MetricRow.newBuilder().putDimensions("segment", "mid")
                        .putMeasures("revenue", 90.0).putMeasures("orders", 3.0).build()));

        QueryMetricsResponse response = MetricQueries.query(mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor),
                request("average_order")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());

        // The engine computed the inputs the calculation reads.
        assertThat(executor.executed.measures())
                .extracting(CompiledMetricQuery.Measure::member)
                .containsExactly("revenue", "orders");
        // The response carries only what was requested.
        assertThat(response.getRows(0).getMeasuresMap())
                .containsOnlyKeys("average_order")
                .containsEntry("average_order", 25.0);
        assertThat(response.getRows(1).getMeasuresMap())
                .containsEntry("average_order", 30.0);
    }

    // ------------------------------------------------------------- refusals

    @Test
    void everyRefusalNamesItsCodeAndTheLegalSet() throws Exception {
        MetricMapping mapping = mapping();
        FakeExecutor lucene = FakeExecutor.lucene(List.of());
        Map<MetricBackend, MetricExecutor> single =
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, lucene);

        MetricRefusal unknownMember = refusalOf(() -> MetricQueries.query(
                mapping, single, request("nope").build()));
        assertThat(unknownMember.code()).isEqualTo(MetricRefusal.UNKNOWN_MEMBER);
        assertThat(unknownMember.legal()).contains("revenue", "segment");

        MetricRefusal roleMismatch = refusalOf(() -> MetricQueries.query(
                mapping, single, request("segment").build()));
        assertThat(roleMismatch.code()).isEqualTo(MetricRefusal.ROLE_MISMATCH);
        assertThat(roleMismatch.legal()).contains("revenue", "orders");

        MetricRefusal measureAsDimension = refusalOf(() -> MetricQueries.query(
                mapping, single, request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("revenue")).build()));
        assertThat(measureAsDimension.code()).isEqualTo(MetricRefusal.ROLE_MISMATCH);

        MetricRefusal empty = refusalOf(() -> MetricQueries.query(
                mapping, single, QueryMetricsRequest.newBuilder()
                        .setMappingSubject("orders").setLimit(10).build()));
        assertThat(empty.code()).isEqualTo(MetricRefusal.EMPTY_MEASURES);

        MetricRefusal overCap = refusalOf(() -> MetricQueries.query(
                mapping, single, request("revenue").setLimit(1001).build()));
        assertThat(overCap.code()).isEqualTo(MetricRefusal.INVALID_LIMIT);

        MetricRefusal unknownBackend = refusalOf(() -> MetricQueries.query(
                mapping, single, request("revenue")
                        .setBackend(MetricBackend.METRIC_BACKEND_ICEBERG).build()));
        assertThat(unknownBackend.code()).isEqualTo(MetricRefusal.UNKNOWN_BACKEND);
        assertThat(unknownBackend.legal()).containsExactly("METRIC_BACKEND_LUCENE");

        Map<MetricBackend, MetricExecutor> both = Map.of(
                MetricBackend.METRIC_BACKEND_LUCENE, lucene,
                MetricBackend.METRIC_BACKEND_ICEBERG,
                new FakeExecutor(MetricBackend.METRIC_BACKEND_ICEBERG,
                        lucene.capabilities(), List.of()));
        MetricRefusal ambiguous = refusalOf(() -> MetricQueries.query(
                mapping, both, request("revenue").build()));
        assertThat(ambiguous.code()).isEqualTo(MetricRefusal.AMBIGUOUS_BACKEND);
        assertThat(ambiguous.legal()).containsExactly(
                "METRIC_BACKEND_ICEBERG", "METRIC_BACKEND_LUCENE");

        MetricRefusal grainOnKeyword = refusalOf(() -> MetricQueries.query(
                mapping, single, request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("segment")
                                .setGrain(TimeGrain.TIME_GRAIN_DAY)).build()));
        assertThat(grainOnKeyword.code()).isEqualTo(MetricRefusal.INVALID_GRAIN);

        MetricRefusal filterOnMeasure = refusalOf(() -> MetricQueries.query(
                mapping, single, request("revenue")
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("revenue").addEquals("1")).build()));
        assertThat(filterOnMeasure.code()).isEqualTo(MetricRefusal.ROLE_MISMATCH);

        MetricRefusal emptyEquals = refusalOf(() -> MetricQueries.query(
                mapping, single, request("revenue")
                        .addFilters(MetricFilter.newBuilder().setMember("segment")).build()));
        assertThat(emptyEquals.code()).isEqualTo(MetricRefusal.UNSUPPORTED_FILTER);

        MetricRefusal dateFilter = refusalOf(() -> MetricQueries.query(
                mapping, single, request("revenue")
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("created_at").addEquals("2026-08")).build()));
        assertThat(dateFilter.code()).isEqualTo(MetricRefusal.UNSUPPORTED_FILTER);
    }

    @Test
    void capabilitiesRefuseWhatTheMountedEngineCannotRun() throws Exception {
        MetricMapping mapping = mapping();
        FakeExecutor narrow = new FakeExecutor(MetricBackend.METRIC_BACKEND_LUCENE,
                new MetricExecutor.Capabilities(Set.of(Aggregate.AGGREGATE_SUM), false, false),
                List.of());
        Map<MetricBackend, MetricExecutor> mounted =
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, narrow);

        MetricRefusal aggregate = refusalOf(() -> MetricQueries.query(
                mapping, mounted, request("orders").build()));
        assertThat(aggregate.code()).isEqualTo(MetricRefusal.UNSUPPORTED_AGGREGATE);
        assertThat(aggregate.legal()).containsExactly("AGGREGATE_SUM");

        MetricRefusal filtered = refusalOf(() -> MetricQueries.query(
                mapping, mounted, request("revenue", "orders").build()));
        // 'orders' fails its aggregate before its filter; a SUM-capable but
        // filter-less engine refuses the row filter by name.
        FakeExecutor sumOnly = new FakeExecutor(MetricBackend.METRIC_BACKEND_LUCENE,
                new MetricExecutor.Capabilities(
                        Set.of(Aggregate.AGGREGATE_SUM, Aggregate.AGGREGATE_COUNT),
                        false, false),
                List.of());
        MetricRefusal rowFilter = refusalOf(() -> MetricQueries.query(
                mapping, Map.of(MetricBackend.METRIC_BACKEND_LUCENE, sumOnly),
                request("orders").build()));
        assertThat(rowFilter.code()).isEqualTo(MetricRefusal.UNSUPPORTED_FILTER);

        MetricRefusal grain = refusalOf(() -> MetricQueries.query(
                mapping, Map.of(MetricBackend.METRIC_BACKEND_LUCENE, sumOnly),
                request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("created_at")).build()));
        assertThat(grain.code()).isEqualTo(MetricRefusal.INVALID_GRAIN);
        assertThat(filtered.code()).isEqualTo(MetricRefusal.UNSUPPORTED_AGGREGATE);
    }

    // ------------------------------------------------------------- describe

    @Test
    void describeAnswersTheDeclaredSurfaceAndTheMountedBackends() throws Exception {
        DescribeMappingResponse response = MetricQueries.describe(mapping(),
                List.of(MetricBackend.METRIC_BACKEND_LUCENE));
        assertThat(response.getMappingSubject()).isEqualTo("orders");
        assertThat(response.getMessageType()).isEqualTo("test.Order");
        assertThat(response.getBackendsList())
                .containsExactly(MetricBackend.METRIC_BACKEND_LUCENE);
        assertThat(response.getMembersList())
                .extracting(m -> m.getName())
                .containsExactly("segment", "created_at", "revenue", "orders", "average_order");
        // A calculated member has no physical field path.
        assertThat(response.getMembers(4).getFieldPath()).isEmpty();
        assertThat(response.getMembers(1).getDefaultGrain())
                .isEqualTo(TimeGrain.TIME_GRAIN_MONTH);
    }
}
