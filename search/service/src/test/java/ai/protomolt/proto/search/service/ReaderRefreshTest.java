package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SearchResponse;
import ai.protomolt.proto.search.v1.SearchServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.IndexSearcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The reader's live pull: a read-only store follows the writer's snapshots
 * without rebooting, never uploads or prunes a blob, keeps serving its
 * current commit when a pull fails half-way, and a reader born before the
 * first snapshot answers empty until one lands. The refresh interval on
 * the module runs the same pull on a timer.
 */
class ReaderRefreshTest {

    @TempDir
    Path work;

    static final Map<String, ServedMapping> SERVED =
            Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served());

    static Document document(String docId, String body) {
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Refresh Proof")
                        .setBody(body))
                .build();
    }

    /** One writer run: index the documents, close (commits and snapshots). */
    static void writerRun(Path indexDir, IndexSnapshotsTest.FakeSnapshotStore blobs,
            Document... documents) {
        try (LuceneSearchStore writer = new LuceneSearchStore(
                indexDir, SERVED, new IndexSnapshots(blobs))) {
            for (Document document : documents) {
                writer.index(RepoDocumentMapping.SUBJECT, document);
            }
        }
    }

    static List<String> hitIds(LuceneSearchStore store, String query) {
        return IndexSnapshotsTest.search(store, query).stream()
                .map(hit -> hit.getDocId())
                .toList();
    }

    @Test
    void aLiveReaderPullsTheWritersNewerSnapshotWithoutRebooting() {
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();
        writerRun(work.resolve("writer"), blobs,
                document("doc-1", "The alpha meadow rests."));

        try (LuceneSearchStore reader = new LuceneSearchStore(
                work.resolve("reader"), SERVED, new IndexSnapshots(blobs, true), true)) {
            assertThat(hitIds(reader, "meadow")).containsExactly("doc-1");

            writerRun(work.resolve("writer"), blobs,
                    document("doc-2", "The bravo orchard blooms."));
            List<String> operationsAfterWriter = List.copyOf(blobs.operations);

            assertThat(reader.refreshFromSnapshots()).isTrue();
            assertThat(hitIds(reader, "orchard")).containsExactly("doc-2");
            assertThat(hitIds(reader, "meadow")).containsExactly("doc-1");

            // Another pull with nothing new is a no-op.
            assertThat(reader.refreshFromSnapshots()).isFalse();

            // The reader's pulls added no store operation: it never
            // uploads a blob and never prunes one, so the writer's
            // snapshots are safe from it.
            assertThat(blobs.operations).isEqualTo(operationsAfterWriter);
        }
    }

    @Test
    void aReaderBornBeforeTheFirstSnapshotAnswersEmptyThenSwapsItIn() {
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();

        try (LuceneSearchStore reader = new LuceneSearchStore(
                work.resolve("reader"), SERVED, new IndexSnapshots(blobs, true), true)) {
            // No snapshot exists yet: the reader answers empty, not an error.
            assertThat(hitIds(reader, "anything")).isEmpty();
            assertThat(reader.refreshFromSnapshots()).isFalse();

            writerRun(work.resolve("writer"), blobs,
                    document("doc-1", "The alpha meadow rests."));

            assertThat(reader.refreshFromSnapshots()).isTrue();
            assertThat(hitIds(reader, "meadow")).containsExactly("doc-1");
            assertThat(blobs.operations)
                    .as("the reader's pulls never wrote to the store")
                    .allSatisfy(operation -> assertThat(operation).startsWith("put:"));
        }
    }

    @Test
    void aHalfPrunedPullLeavesTheServingCommitUntouched() {
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();
        writerRun(work.resolve("writer"), blobs,
                document("doc-1", "The alpha meadow rests."));
        List<String> versionOneKeys = List.copyOf(blobs.blobs.keySet());

        try (LuceneSearchStore reader = new LuceneSearchStore(
                work.resolve("reader"), SERVED, new IndexSnapshots(blobs, true), true)) {
            assertThat(hitIds(reader, "meadow")).containsExactly("doc-1");

            writerRun(work.resolve("writer"), blobs,
                    document("doc-2", "The bravo orchard blooms."));

            // A writer pruning mid-pull: one of the new commit's segment
            // files vanishes from the store before the reader fetches it.
            List<String> versionTwoOnly = new ArrayList<>(blobs.blobs.keySet());
            versionTwoOnly.removeAll(versionOneKeys);
            String vanished = versionTwoOnly.stream()
                    .filter(key -> !key.substring(key.lastIndexOf('/') + 1)
                            .startsWith("segments_"))
                    .findFirst().orElseThrow();
            byte[] bytes = blobs.blobs.remove(vanished);

            assertThat(reader.refreshFromSnapshots()).isFalse();
            assertThat(hitIds(reader, "meadow")).containsExactly("doc-1");
            assertThat(hitIds(reader, "orchard")).isEmpty();

            // The store heals (the writer's next snapshot re-uploads) and
            // the next pull lands the commit whole.
            blobs.blobs.put(vanished, bytes);
            assertThat(reader.refreshFromSnapshots()).isTrue();
            assertThat(hitIds(reader, "orchard")).containsExactly("doc-2");
        }
    }

    @Test
    void aReadOnlyStoreRefusesWritesByName() {
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();
        writerRun(work.resolve("writer"), blobs,
                document("doc-1", "The alpha meadow rests."));
        try (LuceneSearchStore reader = new LuceneSearchStore(
                work.resolve("reader"), SERVED, new IndexSnapshots(blobs, true), true)) {
            assertThatThrownBy(() -> reader.index(RepoDocumentMapping.SUBJECT,
                    document("doc-9", "A reader never indexes.")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("read-only");
            assertThatThrownBy(() -> reader.delete(RepoDocumentMapping.SUBJECT, "doc-1"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("read-only");
        }
    }

    @Test
    void refreshOnAWritableStoreRefuses() {
        try (LuceneSearchStore writer = new LuceneSearchStore(
                work.resolve("writer"), SERVED,
                new IndexSnapshots(new IndexSnapshotsTest.FakeSnapshotStore()))) {
            assertThatThrownBy(writer::refreshFromSnapshots)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reader's pull");
        }
    }

    @Test
    void theRefreshIntervalDemandsAReaderWithSnapshots() {
        assertThatThrownBy(() -> new SearchServiceModule.Config(
                0, work.resolve("index"), SERVED, null, false, 30L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reader's pull");
        assertThatThrownBy(() -> new SearchServiceModule.Config(
                0, work.resolve("index"), SERVED, null, true, 30L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs snapshots");
        assertThatThrownBy(() -> new SearchServiceModule.Config(
                0, work.resolve("index"), SERVED,
                new IndexSnapshots(new IndexSnapshotsTest.FakeSnapshotStore(), true),
                true, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("negative");
    }

    @Test
    void theModuleTimerPullsWithoutOperatorAction() throws Exception {
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();
        writerRun(work.resolve("writer"), blobs,
                document("doc-1", "The alpha meadow rests."));

        SearchServiceModule reader = new SearchServiceModule(new SearchServiceModule.Config(
                0, work.resolve("reader"), SERVED,
                new IndexSnapshots(blobs, true), true, 1L));
        try (Composer.Node node = Composer.emptyBuilder()
                .module(reader)
                .environment(Map.of())
                .build()
                .boot(List.of("search"))) {
            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("127.0.0.1", reader.grpcPort()).usePlaintext().build();
            try {
                SearchServiceGrpc.SearchServiceBlockingStub stub =
                        SearchServiceGrpc.newBlockingStub(channel);
                assertThat(hits(stub, "meadow")).isNotEmpty();

                writerRun(work.resolve("writer"), blobs,
                        document("doc-2", "The bravo orchard blooms."));

                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
                List<String> found = List.of();
                while (System.nanoTime() < deadline) {
                    found = hits(stub, "orchard");
                    if (!found.isEmpty()) {
                        break;
                    }
                    Thread.sleep(200);
                }
                assertThat(found).as("the timer pulled the newer snapshot")
                        .containsExactly("doc-2");
            } finally {
                channel.shutdownNow();
            }
        }
    }

    private static List<String> hits(
            SearchServiceGrpc.SearchServiceBlockingStub stub, String query) {
        SearchResponse response = stub.search(SearchRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .setQuery(query)
                .setK(5)
                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                .build());
        return response.getHitsList().stream().map(hit -> hit.getDocId()).toList();
    }

    /** The searcher seam a co-mounted metrics role reads through survives the swap. */
    @Test
    void withSearcherWorksAcrossThePlaceholderSwap() {
        IndexSnapshotsTest.FakeSnapshotStore blobs = new IndexSnapshotsTest.FakeSnapshotStore();
        try (LuceneSearchStore reader = new LuceneSearchStore(
                work.resolve("reader"), SERVED, new IndexSnapshots(blobs, true), true)) {
            int before = reader.withSearcher(RepoDocumentMapping.SUBJECT,
                    (IndexSearcher searcher) -> searcher.getIndexReader().numDocs());
            assertThat(before).isZero();

            writerRun(work.resolve("writer"), blobs,
                    document("doc-1", "The alpha meadow rests."));
            assertThat(reader.refreshFromSnapshots()).isTrue();

            int after = reader.withSearcher(RepoDocumentMapping.SUBJECT,
                    (IndexSearcher searcher) -> searcher.getIndexReader().numDocs());
            assertThat(after).isGreaterThan(0);
        }
    }
}
