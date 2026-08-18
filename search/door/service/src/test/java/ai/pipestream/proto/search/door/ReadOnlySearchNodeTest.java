package ai.pipestream.proto.search.door;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.composer.Composer;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.v1.IndexDocumentRequest;
import ai.pipestream.proto.search.v1.SearchIndexServiceGrpc;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
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

    @Test
    void aReaderServesTheWritersSnapshotWithoutARepoOrAWriteSurface() throws Exception {
        Document document = Document.newBuilder()
                .setDocId("doc-7")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Reader Node Proof")
                        .setBody("The reader serves the writer's harvest."))
                .build();
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();

        SearchDoorModule writer = new SearchDoorModule(new SearchDoorModule.Config(
                0, work.resolve("writer-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs)));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new SearchDoorModuleTest.FakeRepoModule(document))
                .module(writer)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", writer.grpcPort()).usePlaintext().build();
            try {
                SearchIndexServiceGrpc.newBlockingStub(channel)
                        .indexDocument(IndexDocumentRequest.newBuilder()
                                .setAddress(NodeAddress.newBuilder().setDocId("doc-7")
                                        .setGraphAddressId("ds").setAccountId("acct")
                                        .setGraphId("intake:acct"))
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build());
            } finally {
                channel.shutdownNow();
            }
        }
        List<String> writerOperations = List.copyOf(blobs.operations);

        // The reader: search alone, no repo module anywhere, restore-only.
        SearchDoorModule reader = new SearchDoorModule(new SearchDoorModule.Config(
                0, work.resolve("reader-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs, true),
                true));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(reader)
                .environment(Map.of())
                .build()
                .boot(List.of("search"))) {
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
            } finally {
                channel.shutdownNow();
            }
        }

        // The reader's whole life wrote nothing to the store.
        assertThat(blobs.operations).isEqualTo(writerOperations);
    }

    @Test
    void aReadOnlyNodeWithWritableSnapshotsRefuses() {
        assertThatThrownBy(() -> new SearchDoorModule.Config(
                0, work.resolve("index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(new IndexSnapshotsTest.FakeSnapshotStore()),
                true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read-only")
                .hasMessageContaining("IndexSnapshots read-only");
    }
}
