package ai.pipestream.proto.metric.door;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.DescribeMappingRequest;
import ai.pipestream.proto.metric.DescribeMappingResponse;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMapping.FieldKind;
import ai.pipestream.proto.metric.spi.MetricMapping.MetricMember;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The metric door's contract: the validating interceptor answers shape
 * violations before the handler, membership refusals carry the stable code
 * in brackets with an honest status, and the two catalog verbs round-trip
 * proto3 JSON with refusal codes and legal sets in details.
 */
class MetricDoorServicesTest {

    static class FakeExecutor implements MetricExecutor {
        CompiledMetricQuery executed;

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
            return new Result(List.of(MetricRow.newBuilder()
                    .putDimensions("segment", "smb")
                    .putMeasures("revenue", 180.0).build()), "fake-plan");
        }
    }

    static MetricDoorServices door;
    static ManagedChannel channel;
    static MetricServiceGrpc.MetricServiceBlockingStub stub;
    static Map<String, ServedMetricSubject> subjects;

    @BeforeAll
    static void boot() throws Exception {
        Map<String, MetricMember> members = new LinkedHashMap<>();
        members.put("segment", new MetricMember("segment",
                MemberRole.MEMBER_ROLE_DIMENSION, Aggregate.AGGREGATE_UNSPECIFIED,
                "segment", "segment", FieldKind.KEYWORD, List.of(), "", List.of(),
                TimeGrain.TIME_GRAIN_UNSPECIFIED, "Sales segment", "internal"));
        members.put("revenue", new MetricMember("revenue",
                MemberRole.MEMBER_ROLE_MEASURE, Aggregate.AGGREGATE_SUM,
                "amount_cents", "amount_cents", FieldKind.NUMERIC, List.of(), "", List.of(),
                TimeGrain.TIME_GRAIN_UNSPECIFIED, "", ""));
        MetricMapping mapping = new MetricMapping("orders", "test.Order", members);
        subjects = Map.of("orders", new ServedMetricSubject(mapping,
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, new FakeExecutor())));

        door = MetricDoorServices.build(subjects);
        String name = InProcessServerBuilder.generateName();
        door.startInProcess(name);
        channel = InProcessChannelBuilder.forName(name).build();
        stub = MetricServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void shutdown() {
        channel.shutdownNow();
        door.close();
    }

    static QueryMetricsRequest.Builder query() {
        return QueryMetricsRequest.newBuilder()
                .setMappingSubject("orders").addMeasures("revenue").setLimit(10);
    }

    @Test
    void theValidatingInterceptorAnswersShapeViolationsBeforeTheHandler() {
        // limit 0 violates the request proto's own declared bound; the
        // refusal wording is the interceptor's, not this door's.
        assertThatThrownBy(() -> stub.queryMetrics(query().setLimit(0).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription())
                            .contains("violates the schema's declared rules")
                            .contains("limit");
                });
    }

    @Test
    void unknownSubjectsAreRefusedWithTheCodeAndTheServedList() {
        assertThatThrownBy(() -> stub.queryMetrics(
                query().setMappingSubject("nope").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription())
                            .contains("[unknown-subject]")
                            .contains("orders");
                });
    }

    @Test
    void refusalStatusesSplitCallerErrorsFromMountPreconditions() {
        assertThatThrownBy(() -> stub.queryMetrics(
                query().clearMeasures().addMeasures("nope").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("[unknown-member]");
                });
        assertThatThrownBy(() -> stub.queryMetrics(
                query().setBackend(MetricBackend.METRIC_BACKEND_ICEBERG).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription()).contains("[unknown-backend]");
                });
    }

    @Test
    void aDistinctBoundRefusalMapsToFailedPrecondition() throws Exception {
        // The engine refuses mid-collection when a count_distinct passes
        // its bound; the door maps that to the mount-precondition status,
        // because the caller fixes it by picking the engine that spills.
        MetricExecutor bounded = new FakeExecutor() {
            @Override
            public Result execute(CompiledMetricQuery query) {
                throw new ai.pipestream.proto.metric.spi.MetricRefusal(
                        ai.pipestream.proto.metric.spi.MetricRefusal.DISTINCT_BOUND,
                        "count_distinct over 'revenue' passed this engine's bound",
                        List.of());
            }
        };
        try (MetricDoorServices boundedDoor = MetricDoorServices.build(Map.of(
                "orders", new ServedMetricSubject(
                        subjects.get("orders").mapping(),
                        Map.of(MetricBackend.METRIC_BACKEND_LUCENE, bounded))))) {
            String name = InProcessServerBuilder.generateName();
            boundedDoor.startInProcess(name);
            ManagedChannel boundedChannel = InProcessChannelBuilder.forName(name).build();
            try {
                assertThatThrownBy(() -> MetricServiceGrpc.newBlockingStub(boundedChannel)
                        .queryMetrics(query().build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.FAILED_PRECONDITION);
                            assertThat(e.getStatus().getDescription())
                                    .contains("[distinct-bound]");
                        });
            } finally {
                boundedChannel.shutdownNow();
            }
        }
    }

    /** A rollup sink that records the one replace it receives. */
    static final class FakeRollupSink implements ai.pipestream.proto.metric.spi.RollupSink {
        String table;
        List<String> dimensions;
        List<String> measures;
        List<MetricRow> rows;

        @Override
        public Written replace(String table, List<String> dimensions,
                List<String> measures, List<MetricRow> rows) {
            this.table = table;
            this.dimensions = dimensions;
            this.measures = measures;
            this.rows = rows;
            return new Written("protomolt." + table, rows.size(), 42L);
        }
    }

    private static ai.pipestream.proto.metric.RebuildRollupRequest.Builder rebuild() {
        return ai.pipestream.proto.metric.RebuildRollupRequest.newBuilder()
                .setMappingSubject("orders")
                .addMeasures("revenue")
                .addDimensions(ai.pipestream.proto.metric.MemberRef.newBuilder()
                        .setName("segment"))
                .setTable("revenue_by_segment");
    }

    @Test
    void rebuildRollupRunsTheQueryAndHandsTheAnswerToTheSink() throws Exception {
        FakeRollupSink rollups = new FakeRollupSink();
        try (MetricDoorServices rollupDoor = MetricDoorServices.build(subjects, rollups)) {
            String name = InProcessServerBuilder.generateName();
            rollupDoor.startInProcess(name);
            ManagedChannel rollupChannel = InProcessChannelBuilder.forName(name).build();
            try {
                ai.pipestream.proto.metric.RebuildRollupResponse written =
                        MetricServiceGrpc.newBlockingStub(rollupChannel)
                                .rebuildRollup(rebuild().build());
                assertThat(written.getTable()).isEqualTo("protomolt.revenue_by_segment");
                assertThat(written.getRowsWritten()).isEqualTo(1);
                assertThat(written.getSnapshotId()).isEqualTo(42L);
                assertThat(written.getPhysicalPlan()).isEqualTo("fake-plan");
                assertThat(written.getBackend())
                        .isEqualTo(MetricBackend.METRIC_BACKEND_LUCENE);
                assertThat(rollups.table).isEqualTo("revenue_by_segment");
                assertThat(rollups.dimensions).containsExactly("segment");
                assertThat(rollups.measures).containsExactly("revenue");
                assertThat(rollups.rows).hasSize(1);
            } finally {
                rollupChannel.shutdownNow();
            }
        }
    }

    @Test
    void rebuildRollupWithoutASinkRefusesWithMissingSink() {
        assertThatThrownBy(() -> stub.rebuildRollup(rebuild().build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription())
                            .contains("[missing-sink]");
                });
    }

    @Test
    void aRollupThatFillsTheGroupBudgetRefusesInsteadOfTruncating() throws Exception {
        // An engine answering exactly the budget cannot attest the rollup
        // complete, so nothing lands: exact or refused, never truncated.
        MetricExecutor wide = new FakeExecutor() {
            @Override
            public Result execute(CompiledMetricQuery query) {
                List<MetricRow> rows = new java.util.ArrayList<>();
                for (int i = 0; i < 1000; i++) {
                    rows.add(MetricRow.newBuilder()
                            .putDimensions("segment", "s" + i)
                            .putMeasures("revenue", (double) i).build());
                }
                return new Result(rows, "wide-plan");
            }
        };
        FakeRollupSink rollups = new FakeRollupSink();
        try (MetricDoorServices wideDoor = MetricDoorServices.build(Map.of(
                "orders", new ServedMetricSubject(
                        subjects.get("orders").mapping(),
                        Map.of(MetricBackend.METRIC_BACKEND_LUCENE, wide))), rollups)) {
            String name = InProcessServerBuilder.generateName();
            wideDoor.startInProcess(name);
            ManagedChannel wideChannel = InProcessChannelBuilder.forName(name).build();
            try {
                assertThatThrownBy(() -> MetricServiceGrpc.newBlockingStub(wideChannel)
                        .rebuildRollup(rebuild().build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.FAILED_PRECONDITION);
                            assertThat(e.getStatus().getDescription())
                                    .contains("[rollup-budget]");
                        });
                assertThat(rollups.table).as("nothing landed").isNull();
            } finally {
                wideChannel.shutdownNow();
            }
        }
    }

    @Test
    void describeAndQueryRoundTrip() {
        DescribeMappingResponse described = stub.describeMapping(
                DescribeMappingRequest.newBuilder().setMappingSubject("orders").build());
        assertThat(described.getMessageType()).isEqualTo("test.Order");
        assertThat(described.getMembersList()).hasSize(2);
        assertThat(described.getMembers(0).getDescription()).isEqualTo("Sales segment");
        assertThat(described.getMembers(0).getSensitivity()).isEqualTo("internal");
        assertThat(described.getBackendsList())
                .containsExactly(MetricBackend.METRIC_BACKEND_LUCENE);

        QueryMetricsResponse answered = stub.queryMetrics(query().build());
        assertThat(answered.getRowCount()).isEqualTo(1);
        assertThat(answered.getRows(0).getMeasuresOrThrow("revenue")).isEqualTo(180.0);
        assertThat(answered.getPhysicalPlan()).isEqualTo("fake-plan");
    }

    // ------------------------------------------------------------- the verbs

    @Test
    void theVerbsRoundTripProto3JsonWithRefusalCodesInDetails() throws Exception {
        List<ProtoAction> actions = MetricActions.over(subjects);
        assertThat(actions).extracting(ProtoAction::name)
                .containsExactly("describe-mapping", "query-metrics", "rebuild-rollup");
        ActionContext context = ActionContext.create();
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode describeInput = mapper.createObjectNode().put("mappingSubject", "orders");
        ObjectNode described = actions.get(0).execute(describeInput, context);
        assertThat(described.get("messageType").asText()).isEqualTo("test.Order");

        ObjectNode queryInput = mapper.createObjectNode();
        queryInput.putObject("request")
                .put("mappingSubject", "orders")
                .put("limit", 10)
                .putArray("measures").add("revenue");
        ObjectNode answered = actions.get(1).execute(queryInput, context);
        assertThat(answered.get("rows").get(0).get("measures").get("revenue").asDouble())
                .isEqualTo(180.0);

        ObjectNode unknown = mapper.createObjectNode().put("mappingSubject", "nope");
        assertThatThrownBy(() -> actions.get(0).execute(unknown, context))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("unknown-subject");
                    assertThat(e.details().orElseThrow().get("legal").get(0).asText())
                            .isEqualTo("orders");
                });

        ObjectNode badMember = mapper.createObjectNode();
        badMember.putObject("request")
                .put("mappingSubject", "orders")
                .put("limit", 10)
                .putArray("measures").add("nope");
        assertThatThrownBy(() -> actions.get(1).execute(badMember, context))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.code()).isEqualTo("unknown-member"));

        ObjectNode garbage = mapper.createObjectNode();
        garbage.putObject("request").put("limit", "not-a-number");
        assertThatThrownBy(() -> actions.get(1).execute(garbage, context))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void theRebuildRollupVerbRunsTheSharedRebuildAndSurfacesRefusalCodes()
            throws Exception {
        FakeRollupSink rollups = new FakeRollupSink();
        List<ProtoAction> actions = MetricActions.over(subjects, rollups);
        ProtoAction rebuild = actions.get(2);
        ActionContext context = ActionContext.create();
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode input = mapper.createObjectNode();
        ObjectNode request = input.putObject("request");
        request.put("mappingSubject", "orders");
        request.put("table", "revenue_by_segment");
        request.putArray("measures").add("revenue");
        request.putArray("dimensions").addObject().put("name", "segment");
        ObjectNode written = rebuild.execute(input, context);
        assertThat(written.get("table").asText()).isEqualTo("protomolt.revenue_by_segment");
        assertThat(written.get("rowsWritten").asText()).isEqualTo("1");
        assertThat(written.get("physicalPlan").asText()).isEqualTo("fake-plan");
        assertThat(rollups.table).isEqualTo("revenue_by_segment");
        assertThat(rollups.dimensions).containsExactly("segment");

        // Without a sink the verb refuses exactly like the RPC.
        ProtoAction sinkless = MetricActions.over(subjects).get(2);
        assertThatThrownBy(() -> sinkless.execute(input, context))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.code()).isEqualTo("missing-sink"));

        // The verb re-checks what the interceptor enforces on the wire.
        ObjectNode noTable = mapper.createObjectNode();
        noTable.putObject("request")
                .put("mappingSubject", "orders")
                .putArray("measures").add("revenue");
        assertThatThrownBy(() -> rebuild.execute(noTable, context))
                .isInstanceOfSatisfying(ActionException.class, e ->
                        assertThat(e.code()).isEqualTo("invalid-input"));
    }
}
