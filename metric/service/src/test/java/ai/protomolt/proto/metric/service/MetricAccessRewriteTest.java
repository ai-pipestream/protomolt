package ai.protomolt.proto.metric.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.MetricAccess;
import ai.protomolt.proto.authz.MetricMemberDeny;
import ai.protomolt.proto.authz.MetricRowFilter;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.DescribeMappingRequest;
import ai.protomolt.proto.metric.MemberRef;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The metric access rewrite behind the policy's {@code metric_access}: a restricted
 * principal's denied members disappear from descriptions and refuse queries by name, its
 * row filters reach the executor ANDed into the compiled query, rollup subjects and the
 * rebuild verb are fail-closed, a policy filter on an undeclared member is the operator's
 * loud error, an unrestricted principal and the operator pass untouched, and a swapped
 * policy re-scopes the running service.
 */
class MetricAccessRewriteTest {

    private static final String OPERATOR = "test-operator";
    private static final String RESTRICTED = "restricted-cred";
    private static final String ANALYST = "analyst-cred";
    private static final String MISCONFIGURED = "misconfigured-cred";

    private static final AtomicReference<AccessPolicy> policy = new AtomicReference<>();
    private static MetricServicesTest.FakeExecutor executor;
    private static MetricServices service;
    private static ManagedChannel channel;

    private static AccessPolicy policyOf(MetricAccess restricted,
            MetricAccess misconfigured) {
        return AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("restricted")
                        .addCredentialSha256("a".repeat(64))
                        .addScopes(Scopes.METRICS_QUERY)
                        .setMetricAccess(restricted))
                .addPrincipals(Principal.newBuilder()
                        .setName("misconfigured")
                        .addCredentialSha256("b".repeat(64))
                        .addScopes(Scopes.METRICS_QUERY)
                        .setMetricAccess(misconfigured))
                .build();
    }

    @BeforeAll
    static void boot() throws Exception {
        policy.set(policyOf(
                MetricAccess.newBuilder()
                        .addDeny(MetricMemberDeny.newBuilder()
                                .setMappingSubject("orders")
                                .addMembers("segment"))
                        .addRowFilters(MetricRowFilter.newBuilder()
                                .setMappingSubject("orders")
                                .setMember("segment")
                                .addEquals("smb"))
                        .build(),
                MetricAccess.newBuilder()
                        .addRowFilters(MetricRowFilter.newBuilder()
                                .setMappingSubject("orders")
                                .setMember("nonexistent")
                                .addEquals("x"))
                        .build()));

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
        executor = new MetricServicesTest.FakeExecutor();
        ServedMetricSubject orders = new ServedMetricSubject(mapping,
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor));
        service = MetricServices.build(Map.of("orders", orders), null,
                name -> "rollup:orders".equals(name)
                        ? new ai.protomolt.proto.metric.spi.MetricSubjectResolver
                                .Resolved(mapping, executor)
                        : null,
                policy::get);

        CallerResolver resolver = credential -> switch (credential) {
            case RESTRICTED -> Optional.of(Caller.scoped("restricted",
                    Set.of(Scopes.METRICS_QUERY, Scopes.METRICS_REBUILD)));
            case ANALYST -> Optional.of(Caller.scoped("analyst",
                    Set.of(Scopes.METRICS_QUERY)));
            case MISCONFIGURED -> Optional.of(Caller.scoped("misconfigured",
                    Set.of(Scopes.METRICS_QUERY)));
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
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER),
                credential);
        return MetricServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static QueryMetricsRequest revenue(String subject) {
        return QueryMetricsRequest.newBuilder()
                .setMappingSubject(subject).addMeasures("revenue").setLimit(10).build();
    }

    @Test
    void deniedMembersDisappearFromTheDescription() {
        assertThat(stub(RESTRICTED).describeMapping(DescribeMappingRequest.newBuilder()
                .setMappingSubject("orders").build()).getMembersList())
                .extracting(m -> m.getName())
                .containsExactly("revenue");
        assertThat(stub(ANALYST).describeMapping(DescribeMappingRequest.newBuilder()
                .setMappingSubject("orders").build()).getMembersCount()).isEqualTo(2);
        assertThat(stub(OPERATOR).describeMapping(DescribeMappingRequest.newBuilder()
                .setMappingSubject("orders").build()).getMembersCount()).isEqualTo(2);
    }

    @Test
    void rowFiltersReachTheExecutorAndDeniedMembersRefuseByName() {
        executor.executed = null;
        assertThat(stub(RESTRICTED).queryMetrics(revenue("orders")).getRowsCount())
                .isEqualTo(1);
        assertThat(executor.executed.filters())
                .anySatisfy(filter -> {
                    assertThat(filter.member()).isEqualTo("segment");
                    assertThat(filter.values()).containsExactly("smb");
                });

        assertThatThrownBy(() -> stub(RESTRICTED).queryMetrics(revenue("orders")
                .toBuilder()
                .addDimensions(MemberRef.newBuilder().setName("segment"))
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("restricted").contains("segment")
                            .contains("orders");
                });

        // An unrestricted principal's query carries no injected filter.
        executor.executed = null;
        assertThat(stub(ANALYST).queryMetrics(revenue("orders")).getRowsCount())
                .isEqualTo(1);
        assertThat(executor.executed.filters()).isEmpty();
    }

    @Test
    void rollupSubjectsAndTheRebuildVerbAreFailClosed() {
        assertThatThrownBy(() -> stub(RESTRICTED).queryMetrics(revenue("rollup:orders")))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("restricted").contains("rollup:orders");
                });
        assertThat(stub(ANALYST).queryMetrics(revenue("rollup:orders")).getRowsCount())
                .isEqualTo(1);

        assertThatThrownBy(() -> stub(RESTRICTED).rebuildRollup(
                RebuildRollupRequest.newBuilder()
                        .setMappingSubject("orders")
                        .addMeasures("revenue")
                        .setTable("orders_rollup")
                        .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("restricted").contains("metric access rules");
                });
    }

    @Test
    void aPolicyFilterOnAnUndeclaredMemberIsTheOperatorsLoudError() {
        assertThatThrownBy(() -> stub(MISCONFIGURED).queryMetrics(revenue("orders")))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode())
                            .isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(e.getStatus().getDescription())
                            .contains("access policy").contains("nonexistent");
                });
    }

    @Test
    void aSwappedPolicyRescopesTheRunningService() {
        AccessPolicy before = policy.get();
        try {
            policy.set(policyOf(
                    MetricAccess.newBuilder()
                            .addDeny(MetricMemberDeny.newBuilder()
                                    .setMappingSubject("orders")
                                    .addMembers("revenue"))
                            .build(),
                    MetricAccess.newBuilder()
                            .addRowFilters(MetricRowFilter.newBuilder()
                                    .setMappingSubject("orders")
                                    .setMember("segment")
                                    .addEquals("smb"))
                            .build()));
            assertThatThrownBy(() -> stub(RESTRICTED).queryMetrics(revenue("orders")))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.PERMISSION_DENIED);
                        assertThat(e.getStatus().getDescription()).contains("revenue");
                    });
        } finally {
            policy.set(before);
        }
    }
}
