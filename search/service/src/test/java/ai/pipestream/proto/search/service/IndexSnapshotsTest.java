package ai.pipestream.proto.search.service;

import ai.pipestream.proto.search.v1.SearchHit;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Commit-point snapshots to a blob store: close snapshots what boot
 * restores, the segments marker uploads last and uploads stay incremental
 * with stale blobs pruned, a changed mapping identity refuses the old
 * snapshot, and a corrupt snapshot falls through to an empty mount instead
 * of failing it — the repository stays the source of truth.
 */
class IndexSnapshotsTest {

    @TempDir
    Path work;

    /** An in-memory blob store recording every operation in order. */
    static final class FakeSnapshotStore implements SnapshotStore {

        final Map<String, byte[]> blobs = new LinkedHashMap<>();
        final List<String> operations = new ArrayList<>();

        @Override
        public synchronized List<String> list(String prefix) {
            return blobs.keySet().stream().filter(key -> key.startsWith(prefix)).toList();
        }

        @Override
        public synchronized void put(String key, Path file) {
            try {
                blobs.put(key, Files.readAllBytes(file));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            operations.add("put:" + key);
        }

        @Override
        public synchronized void download(String key, Path target) {
            byte[] bytes = blobs.get(key);
            if (bytes == null) {
                throw new IllegalStateException("no blob under " + key);
            }
            try {
                Files.write(target, bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public synchronized void delete(String key) {
            blobs.remove(key);
            operations.add("delete:" + key);
        }
    }

    static List<SearchHit> search(LuceneSearchStore store, String query) {
        return store.search(RepoDocumentMapping.SUBJECT, SearchRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .setQuery(query).setK(10)
                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                .build());
    }

    @Test
    void closeSnapshotsWhatBootRestores() {
        FakeSnapshotStore blob = new FakeSnapshotStore();
        try (LuceneSearchStore store = new LuceneSearchStore(work.resolve("writer"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    LuceneSearchStoreTest.document("doc-1", "the autumn ledger arrived"));
        }
        assertThat(blob.blobs).isNotEmpty();

        // A different machine: empty directory, same blob store.
        try (LuceneSearchStore reader = new LuceneSearchStore(work.resolve("reader"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            assertThat(search(reader, "autumn ledger"))
                    .extracting(SearchHit::getDocId)
                    .containsExactly("doc-1");
        }
    }

    @Test
    void theMarkerUploadsLastUploadsStayIncrementalAndStaleBlobsPrune() {
        FakeSnapshotStore blob = new FakeSnapshotStore();
        try (LuceneSearchStore store = new LuceneSearchStore(work.resolve("first"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    LuceneSearchStoreTest.document("doc-1", "first body"));
        }
        List<String> firstPuts = blob.operations.stream()
                .filter(op -> op.startsWith("put:")).toList();
        // The atomic marker that the snapshot exists is written last.
        assertThat(firstPuts.getLast()).contains("/segments_");
        assertThat(firstPuts.subList(0, firstPuts.size() - 1))
                .noneMatch(op -> op.contains("/segments_"));

        blob.operations.clear();
        try (LuceneSearchStore store = new LuceneSearchStore(work.resolve("second"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    LuceneSearchStoreTest.document("doc-2", "second body"));
        }
        // Segment immutability: files the store already has never re-upload.
        List<String> reUploads = blob.operations.stream()
                .filter(op -> op.startsWith("put:"))
                .filter(op -> firstPuts.contains(op))
                .toList();
        assertThat(reUploads).isEmpty();
        // The old marker pruned: exactly one segments_N remains, and both
        // documents answer from a fresh restore of it.
        assertThat(blob.blobs.keySet().stream()
                .filter(key -> key.contains("/segments_"))).hasSize(1);
        try (LuceneSearchStore reader = new LuceneSearchStore(work.resolve("third"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            assertThat(search(reader, "body")).hasSize(2);
        }
    }

    @Test
    void aChangedMappingIdentityRefusesTheOldSnapshot() {
        FakeSnapshotStore blob = new FakeSnapshotStore();
        try (LuceneSearchStore store = new LuceneSearchStore(work.resolve("writer"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    LuceneSearchStoreTest.document("doc-1", "the autumn ledger arrived"));
        }

        // A reshaped mapping (a chunk lane changes the identity's policy
        // digest): the old snapshot is not this mount's to restore.
        ServedMapping reshaped = RepoDocumentMapping.served(
                LuceneSearchStoreTest.policyNaming(SearchTestProvider.PROVIDER_ID));
        try (LuceneSearchStore reader = new LuceneSearchStore(work.resolve("reader"),
                Map.of(RepoDocumentMapping.SUBJECT, reshaped),
                new IndexSnapshots(blob))) {
            assertThat(search(reader, "autumn ledger")).isEmpty();
        }
    }

    @Test
    void aCorruptSnapshotFallsThroughToAnEmptyMount() throws Exception {
        FakeSnapshotStore blob = new FakeSnapshotStore();
        try (LuceneSearchStore store = new LuceneSearchStore(work.resolve("writer"),
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    LuceneSearchStoreTest.document("doc-1", "the autumn ledger arrived"));
        }
        // Truncate every non-marker blob: the restore downloads, fails
        // verification, wipes, and the mount proceeds empty.
        for (Map.Entry<String, byte[]> entry : blob.blobs.entrySet()) {
            if (!entry.getKey().contains("/segments_")) {
                entry.setValue(new byte[] {1, 2, 3});
            }
        }
        Path readerDir = work.resolve("reader");
        try (LuceneSearchStore reader = new LuceneSearchStore(readerDir,
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()),
                new IndexSnapshots(blob))) {
            assertThat(search(reader, "autumn ledger")).isEmpty();
            // Still writable after the wipe: replay would re-derive here.
            store(reader);
        }
    }

    private static void store(LuceneSearchStore reader) {
        reader.index(RepoDocumentMapping.SUBJECT,
                LuceneSearchStoreTest.document("doc-2", "a fresh document lands"));
        assertThat(search(reader, "fresh document"))
                .extracting(SearchHit::getDocId)
                .containsExactly("doc-2");
    }
}
