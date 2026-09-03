package ai.protomolt.proto.metric.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricServiceGrpc;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.RebuildRollupRequest;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricMapping.FieldKind;
import ai.protomolt.proto.metric.spi.MetricMapping.MetricMember;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The metric service under identity: describing and querying require metrics-query, the
 * rollup rebuild requires metrics-rebuild (the method override), the credential check runs
 * before validation, and the operator keeps everything.
 */
class ScopedMetricServiceTest {

    private static final String OPERATOR = "test-operator";
    private static final String ANALYST = "analyst-cred";
    private static final String REBUILDER = "rebuilder-cred";

    private static MetricServices service;
    private static ManagedChannel channel;

    @BeforeAll
    static void boot() throws Exception {
        Map<String, MetricMember> members = new LinkedHashMap<>();
        members.put("segment", new MetricMember("segment",
                MemberRole.MEMBER_ROLE_DIMENSION, Aggregate.AGGREGATE_UNSPECIFIED,
                "segment", "segment", FieldKind.KEYWORD, List.of(), "", List.of(),
                TimeGrain.TIME_GRAIN_UNSPECIFIED, "", ""));
        members.put("revenue", new MetricMember("revenue",
                MemberRole.MEMBER_ROLE_MEASURE, Aggregate.AGGREGATE_SUM,
                "amount_cents", "amount_cents", FieldKind.NUMERIC, List.of(), "", List.of(),
                TimeGrain.TIME_GRAIN_UNSPECIFIED, "", ""));
        MetricMapping mapping = new MetricMapping("orders", "test.Order", members);
        service = MetricServices.build(Map.of("orders", new ServedMetricSubject(mapping,
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE,
                        new MetricServicesTest.FakeExecutor()))));

        CallerResolver resolver = credential -> switch (credential) {
            case ANALYST -> Optional.of(Caller.scoped("analyst",
                    Set.of(Scopes.METRICS_QUERY)));
            case REBUILDER -> Optional.of(Caller.scoped("rebuilder",
                    Set.of(Scopes.METRICS_REBUILD)));
            default -> Optional.empty();
        };
        String name = InProcessServerBuilder.generateName();
        service.startInProcess(name, OPERATOR, resolver);
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterAll
    static void shutdown() {
        channel.shutdownNow();
        service.close();
    }

    private static MetricServiceGrpc.MetricServiceBlockingStub stub(String credential) {
        Metadata headers = new Metadata();
        if (credential != null) {
            headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                    credential);
        }
        return MetricServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static QueryMetricsRequest query() {
        return QueryMetricsRequest.newBuilder()
                .setMappingSubject("orders").addMeasures("revenue").setLimit(10).build();
    }

    @Test
    void anAnalystQueriesButCannotRebuild() {
        assertThat(stub(ANALYST).queryMetrics(query()).getRowsCount()).isEqualTo(1);

        assertThatThrownBy(() -> stub(ANALYST).rebuildRollup(
                RebuildRollupRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("analyst").contains(Scopes.METRICS_REBUILD);
                });
    }

    @Test
    void aRebuilderCannotQuery() {
        assertThatThrownBy(() -> stub(REBUILDER).queryMetrics(query()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("rebuilder").contains(Scopes.METRICS_QUERY);
                });
    }

    @Test
    void theCredentialCheckRunsBeforeValidation() {
        // limit 0 violates the request's declared rules, but with no credential the
        // refusal must be the authentication one, revealing nothing about the schema.
        assertThatThrownBy(() -> stub(null).queryMetrics(
                query().toBuilder().setLimit(0).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void theOperatorKeepsEverything() {
        assertThat(stub(OPERATOR).queryMetrics(query()).getRowsCount()).isEqualTo(1);
        assertThatThrownBy(() -> stub(OPERATOR).rebuildRollup(
                RebuildRollupRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isNotEqualTo(Status.Code.PERMISSION_DENIED));
    }

    @Test
    void anUnknownCredentialStaysUnauthenticated() {
        assertThatThrownBy(() -> stub("guessed").queryMetrics(query()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.UNAUTHENTICATED));
    }
}
