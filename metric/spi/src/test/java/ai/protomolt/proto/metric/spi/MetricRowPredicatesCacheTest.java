package ai.protomolt.proto.metric.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricFilter;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.RowPredicate;
import ai.protomolt.proto.metric.spi.MetricMapping.FieldKind;
import ai.protomolt.proto.metric.spi.MetricMapping.MetricMember;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link MetricRowPredicates} reuses a single cached synthetic row-context
 * {@link FileDescriptor}, CEL environment, and compiled expression entry per
 * {@link MetricMapping} across member collection, access-policy validation, and query
 * compilation (#866), while isolating distinct descriptors and bounding cache growth.
 */
class MetricRowPredicatesCacheTest {

    @Test
    void oneQueryReusesCachedDescriptorEnvironmentAndCompiledPredicates() throws Exception {
        MetricMapping mapping = MetricQueriesTest.mapping();
        MetricQueriesTest.FakeExecutor lucene = MetricQueriesTest.FakeExecutor.lucene(
                List.of(MetricRow.newBuilder().putMeasures("revenue", 42.0).build()));
        Map<MetricBackend, MetricExecutor> executors =
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, lucene);

        String callerCel = "this.segment == 'smb' && this.revenue >= 100";
        String policyCel = "this.paying && this.segment != 'ent'";
        MetricFilter callerFilter = MetricFilter.newBuilder().setCel(callerCel).build();
        MetricFilter policyFilter = MetricFilter.newBuilder().setCel(policyCel).build();

        MetricRowPredicates.resetCacheAndStats();

        // 1. Access-policy member collection over the caller's CEL filter.
        Set<String> referenced = MetricRowPredicates.referencedMembers(mapping, callerCel);
        assertThat(referenced).containsExactlyInAnyOrder("segment", "revenue");
        assertThat(MetricRowPredicates.descriptorBuildCount()).isEqualTo(1);
        assertThat(MetricRowPredicates.celCompilationCount()).isEqualTo(1);

        // 2. Access-policy validation of the injected policy CEL filter.
        MetricQueries.validateFilter(mapping, policyFilter);
        assertThat(MetricRowPredicates.descriptorBuildCount()).isEqualTo(1);
        assertThat(MetricRowPredicates.celCompilationCount()).isEqualTo(2);

        // 3. Query compilation of the rewritten request containing both CEL filters.
        QueryMetricsRequest rewritten = MetricQueriesTest.request("revenue")
                .addFilters(callerFilter)
                .addFilters(policyFilter)
                .build();
        MetricQueries.query(mapping, executors, rewritten);

        assertThat(MetricRowPredicates.descriptorBuildCount()).isEqualTo(1);
        assertThat(MetricRowPredicates.celCompilationCount()).isEqualTo(2);
        assertThat(lucene.executed.predicates()).hasSize(2);

        // 4. Subsequent queries over the same mapping and expressions perform zero builds/compiles.
        MetricRowPredicates.referencedMembers(mapping, callerCel);
        MetricQueries.validateFilter(mapping, policyFilter);
        MetricQueries.query(mapping, executors, rewritten);

        assertThat(MetricRowPredicates.descriptorBuildCount()).isEqualTo(1);
        assertThat(MetricRowPredicates.celCompilationCount()).isEqualTo(2);
    }

    @Test
    void distinctDescriptorsSharingSubjectAndMessageTypeNeverCollide() throws Exception {
        Descriptor v1 = buildOrderDescriptor(false);
        Descriptor v2 = buildOrderDescriptor(true);
        Map<String, MetricMember> members = Map.of(
                "revenue", new MetricMember(
                        "revenue", MemberRole.MEMBER_ROLE_MEASURE, Aggregate.AGGREGATE_SUM,
                        "amount_cents", "amount_cents", FieldKind.NUMERIC,
                        List.of(), "", List.of(), TimeGrain.TIME_GRAIN_UNSPECIFIED, "", ""));

        MetricMapping mappingV1 = new MetricMapping("orders", "test.Order", members, v1);
        MetricMapping mappingV2 = new MetricMapping("orders", "test.Order", members, v2);

        assertThatThrownBy(() -> MetricRowPredicates.compile(mappingV1, "this.tier == 'gold'"))
                .isInstanceOfSatisfying(MetricRefusal.class,
                        r -> assertThat(r.code()).isEqualTo(MetricRefusal.UNKNOWN_MEMBER));

        RowPredicate v2Predicate = MetricRowPredicates.compile(mappingV2, "this.tier == 'gold'");
        assertThat(v2Predicate).isInstanceOf(RowPredicate.Terms.class);

        // Re-querying v1 must still refuse closed rather than seeing v2's descriptor.
        assertThatThrownBy(() -> MetricRowPredicates.compile(mappingV1, "this.tier == 'gold'"))
                .isInstanceOfSatisfying(MetricRefusal.class,
                        r -> assertThat(r.code()).isEqualTo(MetricRefusal.UNKNOWN_MEMBER));
    }

    @Test
    void typeInvalidAndSyntaxInvalidExpressionsPreserveMemberDetectionAndThrowFreshExceptions()
            throws Exception {
        MetricMapping mapping = MetricQueriesTest.mapping();
        MetricRowPredicates.resetCacheAndStats();

        // Syntax-valid, type-invalid expression still extracts referenced members for deny checks.
        String typeInvalid = "this.segment > 100";
        assertThat(MetricRowPredicates.referencedMembers(mapping, typeInvalid))
                .containsExactly("segment");
        MetricRefusal first = MetricQueriesTest.refusalOf(
                () -> MetricRowPredicates.compile(mapping, typeInvalid));
        MetricRefusal second = MetricQueriesTest.refusalOf(
                () -> MetricRowPredicates.compile(mapping, typeInvalid));
        assertThat(first.code()).isEqualTo(MetricRefusal.UNSUPPORTED_FILTER);
        assertThat(second.code()).isEqualTo(MetricRefusal.UNSUPPORTED_FILTER);
        assertThat(second).isNotSameAs(first);
        assertThat(MetricRowPredicates.celCompilationCount()).isEqualTo(1);

        // Syntax-invalid expression returns empty referenced members and refuses compile cleanly.
        String syntaxInvalid = "this.segment ==";
        assertThat(MetricRowPredicates.referencedMembers(mapping, syntaxInvalid)).isEmpty();
        MetricRefusal third = MetricQueriesTest.refusalOf(
                () -> MetricRowPredicates.compile(mapping, syntaxInvalid));
        assertThat(third.code()).isEqualTo(MetricRefusal.UNSUPPORTED_FILTER);
        assertThat(MetricRowPredicates.celCompilationCount()).isEqualTo(2);
    }

    @Test
    void schemaAndExpressionCachesStayBoundedUnderChurn() throws Exception {
        MetricRowPredicates.resetCacheAndStats();

        for (int i = 0; i < 320; i++) {
            MetricMapping ephemeral = new MetricMapping(
                    "subject_" + i,
                    "test.Ephemeral" + i,
                    Map.of("dim", new MetricMember(
                            "dim", MemberRole.MEMBER_ROLE_DIMENSION,
                            Aggregate.AGGREGATE_UNSPECIFIED, "dim", "dim",
                            FieldKind.KEYWORD, List.of(), "", List.of(),
                            TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "")));
            assertThat(MetricRowPredicates.compile(ephemeral, "this.dim == 'v" + i + "'"))
                    .isInstanceOf(RowPredicate.Terms.class);
        }
        assertThat(MetricRowPredicates.schemaCacheSize()).isLessThanOrEqualTo(256);

        MetricMapping steady = MetricQueriesTest.mapping();
        for (int i = 0; i < 600; i++) {
            assertThat(MetricRowPredicates.compile(steady, "this.revenue >= " + i))
                    .isInstanceOf(RowPredicate.NumericRange.class);
        }
        assertThat(MetricRowPredicates.expressionCacheSize(steady)).isLessThanOrEqualTo(512);
    }

    @Test
    void concurrentColdCallsBuildSchemaAndCompileExpressionOnce() throws Exception {
        MetricMapping mapping = new MetricMapping(
                "concurrent_orders",
                "test.ConcurrentOrders",
                Map.of("segment", new MetricMember(
                        "segment", MemberRole.MEMBER_ROLE_DIMENSION,
                        Aggregate.AGGREGATE_UNSPECIFIED, "segment", "segment",
                        FieldKind.KEYWORD, List.of(), "", List.of(),
                        TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "")));
        MetricRowPredicates.resetCacheAndStats();

        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Callable<RowPredicate>> tasks = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                tasks.add(() -> {
                    start.await();
                    MetricRowPredicates.referencedMembers(mapping, "this.segment == 'smb'");
                    return MetricRowPredicates.compile(mapping, "this.segment == 'smb'");
                });
            }
            List<Future<RowPredicate>> futures = new ArrayList<>(threads);
            for (Callable<RowPredicate> task : tasks) {
                futures.add(pool.submit(task));
            }
            start.countDown();
            RowPredicate expected = futures.get(0).get();
            for (Future<RowPredicate> future : futures) {
                assertThat(future.get()).isEqualTo(expected);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(MetricRowPredicates.descriptorBuildCount()).isEqualTo(1);
        assertThat(MetricRowPredicates.celCompilationCount()).isEqualTo(1);
    }

    private static Descriptor buildOrderDescriptor(boolean includeTier) throws Exception {
        DescriptorProto.Builder msg = DescriptorProto.newBuilder()
                .setName("Order")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("amount_cents")
                        .setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_INT64));
        if (includeTier) {
            msg.addField(FieldDescriptorProto.newBuilder()
                    .setName("tier")
                    .setNumber(2)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING));
        }
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("order.proto")
                .setPackage("test")
                .setSyntax("proto3")
                .addMessageType(msg.build())
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Order");
    }
}
