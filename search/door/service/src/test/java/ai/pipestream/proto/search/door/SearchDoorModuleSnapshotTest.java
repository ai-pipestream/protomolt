package ai.pipestream.proto.search.door;

import static org.assertj.core.api.Assertions.assertThat;

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
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Snapshots threaded through the module's configuration: a composed
 * search node with a snapshot store indexes and closes, and a second node
 * over an EMPTY index directory restores the corpus from the store on
 * boot, so the same query answers without reindexing.
 */
class SearchDoorModuleSnapshotTest {

    @TempDir
    Path work;

    @Test
    void aRebootedNodeRestoresItsIndexFromTheSnapshotStore() throws Exception {
        Document document = Document.newBuilder()
                .setDocId("doc-42")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Snapshot Reboot Proof")
                        .setBody("The rebooted node answers from the restored snapshot."))
                .build();
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();

        SearchDoorModule first = new SearchDoorModule(new SearchDoorModule.Config(
                0, work.resolve("first-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs)));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new SearchDoorModuleTest.FakeRepoModule(document))
                .module(first)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", first.grpcPort()).usePlaintext().build();
            try {
                SearchIndexServiceGrpc.newBlockingStub(channel)
                        .indexDocument(IndexDocumentRequest.newBuilder()
                                .setAddress(NodeAddress.newBuilder().setDocId("doc-42")
                                        .setGraphAddressId("ds").setAccountId("acct")
                                        .setGraphId("intake:acct"))
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .build());
            } finally {
                channel.shutdownNow();
            }
        }
        assertThat(blobs.blobs).isNotEmpty();

        // A different node, a fresh empty directory, the same blob store.
        SearchDoorModule second = new SearchDoorModule(new SearchDoorModule.Config(
                0, work.resolve("second-index"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blobs)));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(new SearchDoorModuleTest.FakeRepoModule(document))
                .module(second)
                .environment(Map.of())
                .build()
                .boot(List.of("repo", "search"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", second.grpcPort()).usePlaintext().build();
            try {
                SearchResponse hits = SearchServiceGrpc.newBlockingStub(channel)
                        .search(SearchRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setQuery("restored snapshot")
                                .setK(3)
                                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                                .build());
                assertThat(hits.getHitsList()).isNotEmpty();
                assertThat(hits.getHits(0).getDocId()).isEqualTo("doc-42");
            } finally {
                channel.shutdownNow();
            }
        }
    }
}
