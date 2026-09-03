package ai.protomolt.proto.metric.lucene;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.composer.NodeContext;
import ai.protomolt.proto.composer.ServiceModule;
import ai.protomolt.proto.composer.ServiceMount;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.DescribeMappingResponse;
import ai.protomolt.proto.metric.DescribeMappingRequest;
import ai.protomolt.proto.metric.FieldMetric;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.MetricServiceGrpc;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.spi.CatalogMetricHintSource;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery;
import ai.protomolt.proto.metric.spi.MetricExecutor;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricMappings;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.protomolt.proto.repo.v1.GetDocumentResponse;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.service.RepoDocumentMapping;
import ai.protomolt.proto.search.service.SearchServiceModule;
import ai.protomolt.proto.search.v1.IndexDocumentRequest;
import ai.protomolt.proto.search.v1.SearchIndexServiceGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The metric service as a composed role: the search role indexes over its
 * real port, the metrics role answers an aggregate over the same live
 * index, and a node claiming metrics without search refuses to wire
 * instead of serving an empty corpus.
 */
class MetricServiceModuleTest {

    @TempDir
    Path work;

    /** A repo role stub: served documents behind the real wire contract. */
    static final class FakeRepoModule implements ServiceModule {

        private final Map<String, Document> documents;
        private Server server;

        FakeRepoModule(Map<String, Document> documents) {
            this.documents = documents;
        }

        @Override
        public String role() {
            return "repo";
        }

        @Override
        public ServiceMount wire(NodeContext context) throws Exception {
            String name = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(new DocumentServiceGrpc.DocumentServiceImplBase() {
                        @Override
                        public void getDocumentByReference(GetDocumentByReferenceRequest request,
                                StreamObserver<GetDocumentResponse> observer) {
                            observer.onNext(GetDocumentResponse.newBuilder()
                                    .setDocument(documents.get(
                                            request.getAddress().getDocId()))
                                    .build());
                            observer.onCompleted();
                        }
                    })
                    .build()
                    .start();
            context.channels().publishInProcess("repo", name);
            return ServiceMount.inert(() -> server.shutdownNow());
        }
    }

    static Document document(String docId, String title, String type, String processedAt) {
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle(title)
                        .setBody("A corpus the metrics role counts over the live index.")
                        .setDocumentType(type)
                        .setProcessedDate(Timestamp.newBuilder()
                                .setSeconds(Instant.parse(processedAt).getEpochSecond())))
                .build();
    }

    static MetricMapping documentsMapping() {
        return MetricMappings.build(
                RepoDocumentMapping.SUBJECT,
                Document.getDescriptor(),
                new CatalogMetricHintSource()
                        .put(Document.getDescriptor().getFullName(), "doc_id",
                                FieldMetric.newBuilder()
                                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                        .setAggregate(Aggregate.AGGREGATE_COUNT)
                                        .setName("documents")
                                        .build())
                        .put("ai.protomolt.proto.repo.v1.SearchMetadata", "document_type",
                                FieldMetric.newBuilder()
                                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                        .build())
                        .put("ai.protomolt.proto.repo.v1.SearchMetadata", "processed_date",
                                FieldMetric.newBuilder()
                                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                        .setDefaultGrain(TimeGrain.TIME_GRAIN_MONTH)
                                        .build()));
    }

    static MetricServiceModule metricsModule(int port) {
        return new MetricServiceModule(new MetricServiceModule.Config(
                port,
                Map.of(RepoDocumentMapping.SUBJECT, new MetricServiceModule.Subject(
                        documentsMapping(), RepoDocumentMapping.mapping()))));
    }

    @Test
    void aComposedMetricsNodeAggregatesTheSearchRolesLiveIndex() throws Exception {
        Map<String, Document> corpus = Map.of(
                "doc-1", document("doc-1", "First", "PDF", "2026-07-10T10:00:00Z"),
                "doc-2", document("doc-2", "Second", "HTML", "2026-08-05T10:00:00Z"),
                "doc-3", document("doc-3", "Third", "PDF", "2026-08-20T10:00:00Z"));
        SearchServiceModule search = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        MetricServiceModule metrics = metricsModule(0);
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new FakeRepoModule(corpus))
                .module(search)
                .module(metrics)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search", "metric"))) {
            ManagedChannel searchChannel = NettyChannelBuilder
                    .forAddress("127.0.0.1", search.grpcPort()).usePlaintext().build();
            ManagedChannel metricsChannel = NettyChannelBuilder
                    .forAddress("127.0.0.1", metrics.grpcPort()).usePlaintext().build();
            try {
                for (String docId : corpus.keySet()) {
                    SearchIndexServiceGrpc.newBlockingStub(searchChannel)
                            .indexDocument(IndexDocumentRequest.newBuilder()
                                    .setAddress(NodeAddress.newBuilder().setDocId(docId)
                                            .setGraphAddressId("ds").setAccountId("acct")
                                            .setGraphId("intake:acct"))
                                    .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                    .build());
                }

                MetricServiceGrpc.MetricServiceBlockingStub stub =
                        MetricServiceGrpc.newBlockingStub(metricsChannel);
                DescribeMappingResponse described = stub.describeMapping(
                        DescribeMappingRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build());
                assertThat(described.getMembersList())
                        .anySatisfy(member -> assertThat(member.getName())
                                .isEqualTo("documents"));

                QueryMetricsResponse answered = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(answered.getRowsList()).hasSize(1);
                assertThat(answered.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 3.0);

                // Group-by over a nested dimension: the member's flattened
                // field name lands on the doc values the service wrote.
                QueryMetricsResponse byType = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .addDimensions(MemberRef.newBuilder()
                                        .setName("document_type"))
                                .setLimit(10)
                                .build());
                assertThat(byType.getRowsList()).hasSize(2);
                assertThat(byType.getRows(0).getDimensionsMap())
                        .containsEntry("document_type", "HTML");
                assertThat(byType.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 1.0);
                assertThat(byType.getRows(1).getDimensionsMap())
                        .containsEntry("document_type", "PDF");
                assertThat(byType.getRows(1).getMeasuresMap())
                        .containsEntry("documents", 2.0);

                // The nested date dimension buckets under its default grain.
                QueryMetricsResponse byMonth = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .addDimensions(MemberRef.newBuilder()
                                        .setName("processed_date"))
                                .setLimit(10)
                                .build());
                assertThat(byMonth.getRowsList()).hasSize(2);
                assertThat(byMonth.getRows(0).getDimensionsMap())
                        .containsEntry("processed_date", "2026-07");
                assertThat(byMonth.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 1.0);
                assertThat(byMonth.getRows(1).getDimensionsMap())
                        .containsEntry("processed_date", "2026-08");
                assertThat(byMonth.getRows(1).getMeasuresMap())
                        .containsEntry("documents", 2.0);
            } finally {
                searchChannel.shutdownNow();
                metricsChannel.shutdownNow();
            }
        }
    }

    @Test
    void aMetricsNodeWithoutTheSearchRoleRefusesToWire() {
        assertThatThrownBy(() -> Composer.emptyBuilder()
                .module(metricsModule(0))
                .environment(Map.of())
                .build()
                .boot(List.of("metric")))
                .hasMessageContaining("mount the 'search' role");
    }

    @Test
    void aMetricSubjectTheSearchRoleDoesNotServeRefusesToWire() {
        SearchServiceModule search = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        MetricServiceModule metrics = new MetricServiceModule(new MetricServiceModule.Config(
                0,
                Map.of("elsewhere", new MetricServiceModule.Subject(
                        documentsMapping(), RepoDocumentMapping.mapping()))));
        assertThatThrownBy(() -> Composer.emptyBuilder()
                .module(new FakeRepoModule(Map.of()))
                .module(search)
                .module(metrics)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search", "metric")))
                .hasMessageContaining("metric subject 'elsewhere'")
                .hasMessageContaining(RepoDocumentMapping.SUBJECT);
    }

    /** A host-built lake engine: a fixed answer under the Iceberg backend. */
    static class FakeLakeExecutor implements MetricExecutor {

        @Override
        public MetricBackend backend() {
            return MetricBackend.METRIC_BACKEND_ICEBERG;
        }

        @Override
        public Capabilities capabilities() {
            return new Capabilities(
                    java.util.Set.of(Aggregate.AGGREGATE_COUNT), true, true);
        }

        @Override
        public Result execute(CompiledMetricQuery query) {
            return new Result(List.of(MetricRow.newBuilder()
                    .putMeasures("documents", 42.0).build()), "fake lake plan");
        }
    }

    @Test
    void anExtraEngineMountsBesideTheLuceneExecutorAndUnsetBackendRefuses() throws Exception {
        Map<String, Document> corpus = Map.of(
                "doc-1", document("doc-1", "Only", "PDF", "2026-07-10T10:00:00Z"));
        SearchServiceModule search = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        MetricServiceModule metrics = new MetricServiceModule(new MetricServiceModule.Config(
                0,
                Map.of(RepoDocumentMapping.SUBJECT, new MetricServiceModule.Subject(
                        documentsMapping(), RepoDocumentMapping.mapping(),
                        Map.of(MetricBackend.METRIC_BACKEND_ICEBERG,
                                new FakeLakeExecutor())))));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new FakeRepoModule(corpus))
                .module(search)
                .module(metrics)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search", "metric"))) {
            ManagedChannel searchChannel = NettyChannelBuilder
                    .forAddress("127.0.0.1", search.grpcPort()).usePlaintext().build();
            ManagedChannel metricsChannel = NettyChannelBuilder
                    .forAddress("127.0.0.1", metrics.grpcPort()).usePlaintext().build();
            try {
                SearchIndexServiceGrpc.newBlockingStub(searchChannel)
                        .indexDocument(IndexDocumentRequest.newBuilder()
                                .setAddress(NodeAddress.newBuilder().setDocId("doc-1")
                                        .setGraphAddressId("ds").setAccountId("acct")
                                        .setGraphId("intake:acct"))
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build());
                MetricServiceGrpc.MetricServiceBlockingStub stub =
                        MetricServiceGrpc.newBlockingStub(metricsChannel);

                DescribeMappingResponse described = stub.describeMapping(
                        DescribeMappingRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build());
                assertThat(described.getBackendsList()).containsExactlyInAnyOrder(
                        MetricBackend.METRIC_BACKEND_LUCENE,
                        MetricBackend.METRIC_BACKEND_ICEBERG);

                // Per the design: on a multi-engine mount an unset backend is
                // refused naming the mounted engines, never silently picked.
                assertThatThrownBy(() -> stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build()))
                        .isInstanceOf(StatusRuntimeException.class)
                        .hasMessageContaining("FAILED_PRECONDITION")
                        .hasMessageContaining("ambiguous-backend");

                QueryMetricsResponse fromIndex = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setBackend(MetricBackend.METRIC_BACKEND_LUCENE)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(fromIndex.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 1.0);

                QueryMetricsResponse fromLake = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setBackend(MetricBackend.METRIC_BACKEND_ICEBERG)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(fromLake.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 42.0);
                assertThat(fromLake.getPhysicalPlan()).isEqualTo("fake lake plan");
            } finally {
                searchChannel.shutdownNow();
                metricsChannel.shutdownNow();
            }
        }
    }

    @Test
    void aSubjectRefusesExtraExecutorsThatLieAboutTheirBackend() {
        assertThatThrownBy(() -> new MetricServiceModule.Subject(
                documentsMapping(), RepoDocumentMapping.mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, new FakeLakeExecutor())))
                .hasMessageContaining("builds the Lucene executor itself");
        assertThatThrownBy(() -> new MetricServiceModule.Subject(
                documentsMapping(), RepoDocumentMapping.mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_UNSPECIFIED, new FakeLakeExecutor())))
                .hasMessageContaining("own named backend");
        MetricExecutor liar = new FakeLakeExecutor() {
            @Override
            public MetricBackend backend() {
                return MetricBackend.METRIC_BACKEND_LUCENE;
            }
        };
        assertThatThrownBy(() -> new MetricServiceModule.Subject(
                documentsMapping(), RepoDocumentMapping.mapping(),
                Map.of(MetricBackend.METRIC_BACKEND_ICEBERG, liar)))
                .hasMessageContaining("must agree");
    }

    /** A registry role: the real store over a temp repository, inert mount. */
    static final class FakeRegistryModule implements ServiceModule, ServiceMount {

        final ai.protomolt.proto.registry.GitSchemaRegistryStore store;

        FakeRegistryModule(Path dir) {
            this.store = ai.protomolt.proto.registry.GitSchemaRegistryStore.builder()
                    .repositoryDir(dir)
                    .build();
        }

        @Override
        public String role() {
            return "registry";
        }

        @Override
        public ServiceMount wire(NodeContext context) {
            context.contributions().contribute(
                    ai.protomolt.proto.registry.GitSchemaRegistryStore.class, store);
            return this;
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    void theModuleThreadsItsRollupSinkIntoTheContributedVerb() throws Exception {
        // The rebuild verb the module contributes must carry the mount's
        // sink: a module that dropped it would refuse missing-sink in
        // production while the RPC works.
        final java.util.List<String> replaced = new java.util.ArrayList<>();
        ai.protomolt.proto.metric.spi.RollupSink sink =
                (sourceSubject, table, dimensions, measures, rows) -> {
                    replaced.add(table);
                    return new ai.protomolt.proto.metric.spi.RollupSink.Written(
                            "protomolt." + table, rows.size(), 7L);
                };
        SearchServiceModule search = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        MetricServiceModule metrics = new MetricServiceModule(new MetricServiceModule.Config(
                0,
                Map.of(RepoDocumentMapping.SUBJECT, new MetricServiceModule.Subject(
                        documentsMapping(), RepoDocumentMapping.mapping())),
                sink));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new FakeRepoModule(Map.of()))
                .module(search)
                .module(metrics)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search", "metric"))) {
            ai.protomolt.proto.actions.ProtoAction rebuild =
                    node.context().contributions()
                            .all(ai.protomolt.proto.actions.ProtoAction.class).stream()
                            .filter(action -> action.name().equals("rebuild-rollup"))
                            .findFirst().orElseThrow();
            // The envelope is the RebuildRollupRequest itself, not a wrapper around one.
            com.fasterxml.jackson.databind.node.ObjectNode input =
                    new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode();
            input.put("mappingSubject", RepoDocumentMapping.SUBJECT);
            input.put("table", "documents_total");
            input.putArray("measures").add("documents");
            com.fasterxml.jackson.databind.node.ObjectNode written =
                    ai.protomolt.proto.actions.ActionCatalog
                            .defaults(ai.protomolt.proto.actions.ActionContext.create())
                            .replace(rebuild)
                            .execute(rebuild.name(), input);
            assertThat(written.get("table").asText()).isEqualTo("protomolt.documents_total");
            assertThat(replaced).containsExactly("documents_total");
        }
    }

    @Test
    void aCoMountedRegistryReceivesTheRebuildRollupWorkflow() throws Exception {
        SearchServiceModule search = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        FakeRegistryModule registry = new FakeRegistryModule(work.resolve("registry"));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new FakeRepoModule(Map.of()))
                .module(registry)
                .module(search)
                .module(metricsModule(0))
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "registry", "search", "metric"))) {
            String envelope = registry.store
                    .workflow(ai.protomolt.proto.metric.service.MetricWorkflows
                            .REBUILD_ROLLUP_WORKFLOW)
                    .orElseThrow();
            assertThat(envelope)
                    .contains("ai.protomolt.proto.metric.v1.MetricService/RebuildRollup")
                    .contains("table = input.table");
        }
    }
}
