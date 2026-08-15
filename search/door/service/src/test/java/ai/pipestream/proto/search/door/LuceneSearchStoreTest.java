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
