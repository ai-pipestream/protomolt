package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.v1.IndexDocumentRequest;
import ai.protomolt.proto.search.v1.SearchIndexServiceGrpc;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SearchResponse;
import ai.protomolt.proto.search.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The read-only search node: it boots with no repo role at all, restores
 * the writer's snapshot, serves queries, mounts no write surface, and its
 * shutdown never writes to the blob store — the writer's snapshots are
 * not the reader's to overwrite or prune.
 */
class ReadOnlySearchNodeTest {

    @TempDir
    Path work;

    static Document document(String docId, String body) {
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Reader Node Proof")
                        .setBody(body))
                .build();
    }

    static void index(int port, String docId) {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress("127.0.0.1", port).usePlaintext().build();
        try {
            SearchIndexServiceGrpc.newBlockingStub(channel)
                    .indexDocument(IndexDocumentRequest.newBuilder()
                            .setAddress(NodeAddress.newBuilder().setDocId(docId)
                                    .setGraphAddressId("ds").setAccountId("acct")
                                    .setGraphId("intake:acct"))
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .build());
        } finally {
            channel.shutdownNow();
        }
    }

    static Composer.Node writerNode(SearchServiceModule writer, Map<String, Document> corpus) {
        return Composer.emptyBuilder()
                .module(new FakeCorpusRepoModule(corpus))
                .module(writer)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search"));
    }

    /** A corpus-backed repo stub (the single-document fake serves one). */
    static final class FakeCorpusRepoModule implements ai.protomolt.proto.composer.ServiceMount,
            ai.protomolt.proto.composer.ServiceModule {

        private final Map<String, Document> corpus;
        private io.grpc.Server server;

        FakeCorpusRepoModule(Map<String, Document> corpus) {
            this.corpus = corpus;
        }

        @Override
        public String role() {
            return "repo";
        }

        @Override
        public ai.protomolt.proto.composer.ServiceMount wire(
                ai.protomolt.proto.composer.NodeContext context) throws Exception {
            String name = io.grpc.inprocess.InProcessServerBuilder.generateName();
            server = io.grpc.inprocess.InProcessServerBuilder.forName(name)
                    .directExecutor()
                    .addService(new ai.protomolt.proto.repo.v1.DocumentServiceGrpc
                            .DocumentServiceImplBase() {
                        @Override
                        public void getDocumentByReference(
                                ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest request,
                                io.grpc.stub.StreamObserver<ai.protomolt.proto.repo.v1
                                        .GetDocumentResponse> observer) {
                            observer.onNext(ai.protomolt.proto.repo.v1.GetDocumentResponse
                                    .newBuilder()
                                    .setDocument(corpus.get(request.getAddress().getDocId()))
                                    .build());
                            observer.onCompleted();
                        }
                    })
                    .build()
                    .start();
            context.channels().publishInProcess("repo", name);
            return this;
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
            server.shutdownNow();
        }
    }

    @Test
    void aReaderServesTheWritersSnapshotAndNeverTouchesTheBucket() throws Exception {
        Map<String, Document> corpus = Map.of(
                "doc-7", document("doc-7", "The reader serves the writer's harvest."),
                "doc-8", document("doc-8", "The writer advances after the reader boots."));
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();

        // Writer run one: doc-7 lands, close snapshots it.
        SearchServiceModule writerOne = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("writer-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs)));
        try (Composer.Node node = writerNode(writerOne, corpus)) {
            index(writerOne.grpcPort(), "doc-7");
        }

        // The reader boots on the v1 snapshot: search alone, no repo
        // module anywhere, restore-only.
        SearchServiceModule reader = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("reader-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs, true),
                true));
        Composer.Node readerNode = Composer.emptyBuilder()
                .module(reader)
                .environment(Map.of())
                .build()
                .boot(List.of("search"));
        try {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", reader.grpcPort()).usePlaintext().build();
            try {
                SearchResponse hits = SearchServiceGrpc.newBlockingStub(channel)
                        .search(SearchRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setQuery("writer's harvest")
                                .setK(3)
                                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                                .build());
                assertThat(hits.getHitsList()).isNotEmpty();
                assertThat(hits.getHits(0).getDocId()).isEqualTo("doc-7");

                // The write surface does not exist on this node.
                assertThatThrownBy(() -> SearchIndexServiceGrpc.newBlockingStub(channel)
                        .indexDocument(IndexDocumentRequest.newBuilder()
                                .setAddress(NodeAddress.newBuilder().setDocId("doc-7")
                                        .setGraphAddressId("ds").setAccountId("acct")
                                        .setGraphId("intake:acct"))
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                                assertThat(e.getStatus().getCode())
                                        .isEqualTo(Status.Code.UNIMPLEMENTED));

                // Writer run two advances the bucket to v2 while the reader,
                // still holding v1, stays up.
                SearchServiceModule writerTwo = new SearchServiceModule(
                        new SearchServiceModule.Config(
                                0, work.resolve("writer-index"),
                                Map.of(RepoDocumentMapping.SUBJECT,
                                        RepoDocumentMapping.served()),
                                new IndexSnapshots(blobs)));
                try (Composer.Node node = writerNode(writerTwo, corpus)) {
                    index(writerTwo.grpcPort(), "doc-8");
                }
            } finally {
                channel.shutdownNow();
            }
        } finally {
            readerNode.close();
        }

        // The stale reader's shutdown neither re-uploaded its old commit
        // nor pruned the writer's newer blobs: v2 restores intact.
        SearchServiceModule verify = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("verify-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs, true),
                true));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(verify)
                .environment(Map.of())
                .build()
                .boot(List.of("search"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", verify.grpcPort()).usePlaintext().build();
            try {
                SearchResponse advanced = SearchServiceGrpc.newBlockingStub(channel)
                        .search(SearchRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setQuery("writer advances")
                                .setK(3)
                                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                                .build());
                assertThat(advanced.getHitsList()).isNotEmpty();
                assertThat(advanced.getHits(0).getDocId()).isEqualTo("doc-8");
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test
    void aReadOnlyNodeWithWritableSnapshotsRefuses() {
        assertThatThrownBy(() -> new SearchServiceModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(new IndexSnapshotsTest.FakeSnapshotStore()),
                true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read-only")
                .hasMessageContaining("IndexSnapshots read-only");
    }
}
