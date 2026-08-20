package ai.pipestream.proto.metric.service;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.RebuildRollupRequest;
import ai.pipestream.proto.metric.RebuildRollupResponse;
import ai.pipestream.proto.metric.RollupEnrichment;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMapping.FieldKind;
import ai.pipestream.proto.metric.spi.MetricMapping.MetricMember;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.metric.spi.RollupSink;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Rebuild-time enrichment, the metric-joins design as landed: a lookup
 * against a second subject, strictly one to at most one. A fan-out
 * refuses the whole rebuild before anything lands, a miss leaves the
 * pulled columns empty and is counted as evidence, the lookup is a
 * dimensions-only aggregate needing no declared measure, and the pulled
 * members become ordinary dimension columns of the rollup.
 */
class RollupEnrichmentTest {

    static final class FakeExecutor implements MetricExecutor {
        final List<MetricRow> rows;
        CompiledMetricQuery executed;

        FakeExecutor(List<MetricRow> rows) {
            this.rows = rows;
        }

        @Override
        public MetricBackend backend() {
            return MetricBackend.METRIC_BACKEND_LUCENE;
        }

        @Override
        public Capabilities capabilities() {
            return new Capabilities(
                    Set.of(Aggregate.AGGREGATE_COUNT, Aggregate.AGGREGATE_SUM), true, true);
        }

        @Override
        public Result execute(CompiledMetricQuery query) {
            executed = query;
            return new Result(rows, "fake-plan");
        }
    }

    static final class FakeSink implements RollupSink {
        String table;
        List<String> dimensions;
        List<MetricRow> rows;

        @Override
        public Written replace(String sourceSubject, String table, List<String> dimensions,
                List<MeasureColumn> measures, List<MetricRow> rows) {
            this.table = table;
            this.dimensions = dimensions;
            this.rows = rows;
            return new Written("protomolt." + table, rows.size(), 42L);
        }
    }

    static MetricMember dimension(String name) {
        return new MetricMember(name, MemberRole.MEMBER_ROLE_DIMENSION,
                Aggregate.AGGREGATE_UNSPECIFIED, name, name, FieldKind.KEYWORD,
                List.of(), "", List.of(), TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "");
    }

    static MetricMember sumMeasure(String name) {
        return new MetricMember(name, MemberRole.MEMBER_ROLE_MEASURE,
                Aggregate.AGGREGATE_SUM, "amount", "amount", FieldKind.NUMERIC,
                List.of(), "", List.of(), TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "");
    }

    static ServedMetricSubject subjectOf(
            String subject, FakeExecutor executor, MetricMember... members) {
        Map<String, MetricMember> byName = new LinkedHashMap<>();
        for (MetricMember member : members) {
            byName.put(member.name(), member);
        }
        return new ServedMetricSubject(
                new MetricMapping(subject, "test.T", byName),
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor));
    }

    static MetricRow orderRow(String segment, double revenue) {
        return MetricRow.newBuilder()
                .putDimensions("segment", segment)
                .putMeasures("revenue", revenue)
                .build();
    }

    static MetricRow tierRow(String segment, String tier) {
        return MetricRow.newBuilder()
                .putDimensions("segment", segment)
                .putDimensions("tier", tier)
                .putMeasures("_rows", 1.0)
                .build();
    }

    static RebuildRollupRequest.Builder request() {
        return RebuildRollupRequest.newBuilder()
                .setMappingSubject("orders")
                .addMeasures("revenue")
                .addDimensions(MemberRef.newBuilder().setName("segment"))
                .setTable("orders_by_segment");
    }

    static RollupEnrichment.Builder enrichment() {
        return RollupEnrichment.newBuilder()
                .setSubject("segments").setJoinKey("segment").addMembers("tier");
    }

    /** A primary subject, an enrichment subject, a sink, and the resolve seam. */
    record Rig(ServedMetricSubject orders, FakeExecutor ordersExecutor,
            FakeExecutor segmentsExecutor, FakeSink sink,
            Function<String, ServedMetricSubject> resolve) {

        static Rig of(List<MetricRow> orderRows, List<MetricRow> tierRows) {
            FakeExecutor ordersExecutor = new FakeExecutor(orderRows);
            FakeExecutor segmentsExecutor = new FakeExecutor(tierRows);
            ServedMetricSubject orders = subjectOf("orders", ordersExecutor,
                    dimension("segment"), sumMeasure("revenue"));
            ServedMetricSubject segments = subjectOf("segments", segmentsExecutor,
                    dimension("segment"), dimension("tier"), dimension("owner"),
                    sumMeasure("volume"));
            Map<String, ServedMetricSubject> served =
                    Map.of("orders", orders, "segments", segments);
            return new Rig(orders, ordersExecutor, segmentsExecutor, new FakeSink(),
                    name -> Subjects.find(served, null, name));
        }

        RebuildRollupResponse rebuild(RebuildRollupRequest built) {
            return Rollups.rebuild(orders, sink, built, resolve);
        }
    }

    @Test
    void anEnrichedRebuildDenormalizesTheLookupColumns() {
        Rig rig = Rig.of(
                List.of(orderRow("smb", 180.0), orderRow("mid", 200.0)),
                List.of(tierRow("smb", "low"), tierRow("mid", "high")));
        RebuildRollupResponse response = rig.rebuild(
                request().setEnrichment(enrichment()).build());

        assertThat(rig.sink().dimensions).containsExactly("segment", "tier");
        assertThat(rig.sink().rows).extracting(row -> row.getDimensionsOrThrow("tier"))
                .containsExactly("low", "high");
        assertThat(response.getEnrichedFrom()).isEqualTo("segments");
        assertThat(response.getRowsUnenriched()).isZero();

        // The lookup was a dimensions-only aggregate: a synthetic COUNT over
        // the join key and the pulled members, no declared measure needed.
        CompiledMetricQuery lookup = rig.segmentsExecutor().executed;
        assertThat(lookup.measures()).singleElement().satisfies(measure ->
                assertThat(measure.aggregate()).isEqualTo(Aggregate.AGGREGATE_COUNT));
        assertThat(lookup.dimensions())
                .extracting(CompiledMetricQuery.Dimension::member)
                .containsExactly("segment", "tier");
    }

    @Test
    void aMissingKeyLeavesTheColumnsEmptyAndCounted() {
        Rig rig = Rig.of(
                List.of(orderRow("smb", 180.0), orderRow("xl", 5.0)),
                List.of(tierRow("smb", "low")));
        RebuildRollupResponse response = rig.rebuild(
                request().setEnrichment(enrichment()).build());

        assertThat(response.getRowsUnenriched()).isEqualTo(1);
        MetricRow unmatched = rig.sink().rows.stream()
                .filter(row -> row.getDimensionsOrThrow("segment").equals("xl"))
                .findFirst().orElseThrow();
        assertThat(unmatched.getDimensionsMap()).doesNotContainKey("tier");
    }

    @Test
    void aFanoutRefusesTheWholeRebuild() {
        Rig rig = Rig.of(
                List.of(orderRow("smb", 180.0)),
                List.of(tierRow("smb", "low"), tierRow("smb", "high")));
        assertThatThrownBy(() -> rig.rebuild(request().setEnrichment(enrichment()).build()))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.JOIN_FANOUT);
                    assertThat(refusal.getMessage()).contains("smb").contains("segments");
                });
        assertThat(rig.sink().rows).as("nothing lands on a refusal").isNull();
    }

    @Test
    void aJoinKeyOutsideThePrimaryDimensionsRefuses() {
        // "owner" is a legal enrichment dimension and no pulled member, so
        // the only thing wrong is its absence from the primary dimensions:
        // exactly the check under test, nothing else masking it.
        Rig rig = Rig.of(List.of(orderRow("smb", 180.0)), List.of());
        assertThatThrownBy(() -> rig.rebuild(request()
                .setEnrichment(enrichment().setJoinKey("owner")).build()))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code())
                            .isEqualTo(MetricRefusal.INVALID_ENRICHMENT);
                    assertThat(refusal.getMessage()).contains("not among");
                });
    }

    @Test
    void aCollidingEnrichmentMemberRefuses() {
        Rig rig = Rig.of(List.of(orderRow("smb", 180.0)), List.of());
        assertThatThrownBy(() -> rig.rebuild(request()
                .setEnrichment(enrichment().clearMembers().addMembers("segment")).build()))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.INVALID_ENRICHMENT);
                    assertThat(refusal.getMessage()).contains("collides");
                });
    }

    @Test
    void aMeasureAsAPulledMemberRefusesTowardDimensions() {
        Rig rig = Rig.of(List.of(orderRow("smb", 180.0)), List.of());
        assertThatThrownBy(() -> rig.rebuild(request()
                .setEnrichment(enrichment().clearMembers().addMembers("volume")).build()))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.ROLE_MISMATCH);
                    assertThat(refusal.getMessage()).contains("lookup pulls dimensions");
                });
    }

    @Test
    void aLookupFillingTheBudgetRefusesTheJoin() {
        List<MetricRow> flood = new ArrayList<>();
        for (int i = 0; i < Rollups.GROUP_BUDGET; i++) {
            flood.add(tierRow("s-" + i, "t-" + i));
        }
        Rig rig = Rig.of(List.of(orderRow("smb", 180.0)), flood);
        assertThatThrownBy(() -> rig.rebuild(request().setEnrichment(enrichment()).build()))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.ROLLUP_BUDGET);
                    assertThat(refusal.getMessage()).contains("lookup");
                });
    }

    @Test
    void anUnknownEnrichmentSubjectRefusesWithTheServedList() {
        Rig rig = Rig.of(List.of(orderRow("smb", 180.0)), List.of());
        assertThatThrownBy(() -> rig.rebuild(request()
                .setEnrichment(enrichment().setSubject("nope")).build()))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.UNKNOWN_SUBJECT);
                    assertThat(refusal.getMessage()).contains("segments");
                });
    }

    @Test
    void anUnenrichedRebuildIsUntouched() {
        Rig rig = Rig.of(List.of(orderRow("smb", 180.0)), List.of());
        RebuildRollupResponse response = rig.rebuild(request().build());
        assertThat(rig.sink().dimensions).containsExactly("segment");
        assertThat(response.getEnrichedFrom()).isEmpty();
        assertThat(response.getRowsUnenriched()).isZero();
        assertThat(rig.segmentsExecutor().executed)
                .as("no lookup runs without an enrichment").isNull();
    }
}
