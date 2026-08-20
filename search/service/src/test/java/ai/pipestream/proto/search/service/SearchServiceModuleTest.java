package ai.pipestream.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.composer.Composer;
import ai.pipestream.proto.composer.NodeContext;
import ai.pipestream.proto.composer.ServiceModule;
import ai.pipestream.proto.composer.ServiceMount;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.IndexDocumentResponse;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
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
 * The service as a composed role: a fake repo role serves one document, the
 * search role mounts over its channel, and the whole loop — index through
 * the workflow-facing RPC, query back — runs over the service's real TCP port.
 */
class SearchServiceModuleTest {

    @TempDir
    Path work;

    /** A repo role stub: one in-memory document behind the real wire contract. */
    static final class FakeRepoModule implements ServiceModule {

        private final Document document;
        private Server server;

        FakeRepoModule(Document document) {
            this.document = document;
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
                                    .setDocument(document)
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

    @Test
    void aComposedSearchNodeIndexesAndAnswersOverItsRealPort() throws Exception {
        Document document = Document.newBuilder()
                .setDocId("doc-9")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Composed Node Proof")
                        .setBody("The composed node serves search. Roles resolve through"
                                + " channels."))
                .build();
        SearchServiceModule search = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served())));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new FakeRepoModule(document))
                .module(search)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", search.grpcPort())
                    .usePlaintext()
                    .build();
            try {
                IndexDocumentResponse landed = SearchIndexServiceGrpc.newBlockingStub(channel)
                        .indexDocument(IndexDocumentRequest.newBuilder()
                                .setAddress(NodeAddress.newBuilder().setDocId("doc-9")
                                        .setGraphAddressId("ds").setAccountId("acct")
                                        .setGraphId("intake:acct"))
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build());
                assertThat(landed.getDocId()).isEqualTo("doc-9");

                SearchResponse hits = SearchServiceGrpc.newBlockingStub(channel)
                        .search(SearchRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setQuery("composed channels")
                                .setK(3)
                                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                                .build());
                assertThat(hits.getHitsList()).isNotEmpty();
                assertThat(hits.getHits(0).getDocId()).isEqualTo("doc-9");
            } finally {
                channel.shutdownNow();
            }
        }
    }
}
