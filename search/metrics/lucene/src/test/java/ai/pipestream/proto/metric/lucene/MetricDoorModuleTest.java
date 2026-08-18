package ai.pipestream.proto.metric.lucene;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.composer.Composer;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.DescribeMappingResponse;
import ai.pipestream.proto.metric.DescribeMappingRequest;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.spi.CatalogMetricHintSource;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMappings;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.door.RepoDocumentMapping;
import ai.pipestream.proto.search.door.SearchDoorModule;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The metric door as a composed role: the search role indexes over its
 * real port, the metrics role answers an aggregate over the same live
 * index, and a node claiming metrics without search refuses to wire
 * instead of serving an empty corpus.
 */
class MetricDoorModuleTest {

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

    static Document document(String docId, String title) {
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle(title)
                        .setBody("A corpus the metrics role counts over the live index."))
                .build();
    }

    static MetricMapping documentsMapping() {
        return MetricMappings.build(
                RepoDocumentMapping.SUBJECT,
                Document.getDescriptor(),
                new CatalogMetricHintSource().put(
                        Document.getDescriptor().getFullName(), "doc_id",
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setName("documents")
                                .build()));
    }

    static MetricDoorModule metricsModule(int port) {
        return new MetricDoorModule(new MetricDoorModule.Config(
                port,
                Map.of(RepoDocumentMapping.SUBJECT, new MetricDoorModule.Subject(
                        documentsMapping(), RepoDocumentMapping.mapping()))));
    }

    @Test
    void aComposedMetricsNodeAggregatesTheSearchRolesLiveIndex() throws Exception {
        Map<String, Document> corpus = Map.of(
                "doc-1", document("doc-1", "First"),
                "doc-2", document("doc-2", "Second"));
        SearchDoorModule search = new SearchDoorModule(new SearchDoorModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        MetricDoorModule metrics = metricsModule(0);
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new FakeRepoModule(corpus))
                .module(search)
                .module(metrics)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search", "metrics"))) {
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
                .boot(List.of("metrics")))
                .hasMessageContaining("mount the 'search' role");
    }

    @Test
    void aMetricSubjectTheSearchRoleDoesNotServeRefusesToWire() {
        SearchDoorModule search = new SearchDoorModule(new SearchDoorModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        MetricDoorModule metrics = new MetricDoorModule(new MetricDoorModule.Config(
                0,
                Map.of("elsewhere", new MetricDoorModule.Subject(
                        documentsMapping(), RepoDocumentMapping.mapping()))));
        assertThatThrownBy(() -> Composer.emptyBuilder()
                .module(new FakeRepoModule(Map.of()))
                .module(search)
                .module(metrics)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search", "metrics")))
                .hasMessageContaining("metric subject 'elsewhere'")
                .hasMessageContaining(RepoDocumentMapping.SUBJECT);
    }
}
