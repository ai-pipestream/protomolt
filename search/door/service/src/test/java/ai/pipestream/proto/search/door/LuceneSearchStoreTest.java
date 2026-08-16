package ai.pipestream.proto.search.door;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.chunk.SentencePackedChunker;
import ai.pipestream.proto.index.spi.ChunkingPolicy;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.v1.SearchHit;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The store's own guarantees: a failed mount releases every subject it
 * already opened, and concurrent replaces of one document converge to a
 * single block — the "replays never duplicate" contract under parallelism.
 */
class LuceneSearchStoreTest {

    @TempDir
    Path work;

    static ChunkingPolicy policyNaming(String providerId) {
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        12, 0, 2, 30, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        providerId, DoorTestProvider.DIMENSION,
                        VectorSimilarity.COSINE, true),
                "", true);
    }

    static Document document(String docId, String body) {
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Title of " + docId)
                        .setBody(body))
                .build();
    }

    @Test
    void aFailedMountReleasesTheSubjectsItAlreadyOpened() {
        // Mount order matters: the sound subject opens first, the subject
        // naming an absent embedding provider fails the mount second.
        Map<String, ServedMapping> subjects = new LinkedHashMap<>();
        subjects.put("sound", RepoDocumentMapping.served());
        subjects.put("broken", RepoDocumentMapping.served(policyNaming("no-such-provider")));
        assertThatThrownBy(() -> new LuceneSearchStore(work, subjects))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no-such-provider");

        // The sound subject's writer did not leak its write lock: the same
        // directory mounts cleanly on the next attempt.
        try (LuceneSearchStore store =
                new LuceneSearchStore(work, Map.of("sound", RepoDocumentMapping.served()))) {
            assertThat(store.subjectNames()).containsExactly("sound");
        }
    }

    @Test
    void deletingADocumentRemovesItsWholeBlockAndLeavesOthers() {
        try (LuceneSearchStore store = new LuceneSearchStore(work, Map.of(
                RepoDocumentMapping.SUBJECT,
                RepoDocumentMapping.served(policyNaming(DoorTestProvider.PROVIDER_ID))))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    document("doc-keep", "The evergreen anchor stays behind."));
            LuceneSearchStore.IndexResult landed = store.index(RepoDocumentMapping.SUBJECT,
                    document("doc-gone", "The evergreen anchor leaves town."));
            assertThat(store.indexedDocIds(RepoDocumentMapping.SUBJECT))
                    .containsExactlyInAnyOrder("doc-keep", "doc-gone");

            // The delete reports exactly the chunk children the index call
            // landed, so a caller can tell a real removal from a no-op.
            assertThat(store.delete(RepoDocumentMapping.SUBJECT, "doc-gone"))
                    .isEqualTo(landed.chunksIndexed());

            // The enumeration respects live docs: the deleted block is out
            // even though no merge has reclaimed its terms yet.
            assertThat(store.indexedDocIds(RepoDocumentMapping.SUBJECT))
                    .containsExactly("doc-keep");

            List<SearchHit> lexical = store.search(RepoDocumentMapping.SUBJECT,
                    SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("evergreen anchor")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            assertThat(lexical).extracting(SearchHit::getDocId).containsExactly("doc-keep");

            // The chunk children went with the parent: the deleted
            // document's own sentence no longer answers the vector lane.
            List<SearchHit> vector = store.search(RepoDocumentMapping.SUBJECT,
                    SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("The evergreen anchor leaves town.")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_VECTOR)
                            .build());
            assertThat(vector).extracting(SearchHit::getDocId)
                    .doesNotContain("doc-gone");
        }
    }

    @Test
    void writesAreVisibleImmediatelyAndDurableAcrossReopen() {
        // Fewer writes than a commit batch: visibility rides the
        // near-real-time searcher, durability rides the close commit.
        SearchRequest query = SearchRequest.newBuilder()
                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                .setQuery("durable phrase")
                .setK(3)
                .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                .build();
        try (LuceneSearchStore store = new LuceneSearchStore(work,
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            store.index(RepoDocumentMapping.SUBJECT,
                    document("doc-d", "the durable phrase survives"));
            assertThat(store.search(RepoDocumentMapping.SUBJECT, query)).hasSize(1);
        }
        try (LuceneSearchStore reopened = new LuceneSearchStore(work,
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            assertThat(reopened.search(RepoDocumentMapping.SUBJECT, query)).hasSize(1);
        }
    }

    @Test
    void deletingAnAbsentIdSucceedsAndRefusalsNameTheProblem() {
        try (LuceneSearchStore store = new LuceneSearchStore(work,
                Map.of(RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            // Idempotent: an id the index does not hold is already gone,
            // and the count says nothing was removed.
            assertThat(store.delete(RepoDocumentMapping.SUBJECT, "never-indexed")).isZero();

            assertThatThrownBy(() -> store.delete("no-such-subject", "doc-x"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no-such-subject")
                    .hasMessageContaining(RepoDocumentMapping.SUBJECT);
            assertThatThrownBy(() -> store.delete(RepoDocumentMapping.SUBJECT, "  "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("doc_id");
        }
    }

    @Test
    void concurrentReplacesOfOneDocumentNeverDuplicate() throws Exception {
        try (LuceneSearchStore store = new LuceneSearchStore(work, Map.of(
                RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served()))) {
            int workers = 8;
            int rounds = 20;
            List<Future<?>> writes = new ArrayList<>();
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int w = 0; w < workers; w++) {
                    int worker = w;
                    writes.add(pool.submit(() -> {
                        for (int round = 0; round < rounds; round++) {
                            store.index(RepoDocumentMapping.SUBJECT,
                                    document("doc-c", "the shared anchor phrase, revision "
                                            + worker + "." + round));
                        }
                    }));
                }
                for (Future<?> write : writes) {
                    write.get();
                }
            }

            List<SearchHit> hits = store.search(RepoDocumentMapping.SUBJECT,
                    SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("shared anchor")
                            .setK(workers * rounds)
                            .setLane(SearchLane.SEARCH_LANE_LEXICAL)
                            .build());
            // However the replaces interleaved, exactly one block survives.
            assertThat(hits).hasSize(1);
            assertThat(hits.getFirst().getDocId()).isEqualTo("doc-c");
        }
    }
}
