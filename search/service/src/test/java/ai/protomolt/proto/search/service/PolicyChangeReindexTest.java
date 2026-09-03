package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.search.chunk.SentencePackedChunker;
import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.search.v1.SearchHit;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A changed chunking policy is a data change: replaying the same document
 * atomically replaces its chunk generation — every surviving chunk carries
 * the new policy digest, and nothing duplicates.
 */
class PolicyChangeReindexTest {

    @TempDir
    Path work;

    static ChunkingPolicy policy(int targetTokens) {
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                        targetTokens, 0, 2, 40, SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec(
                        SearchTestProvider.PROVIDER_ID, SearchTestProvider.DIMENSION,
                        VectorSimilarity.COSINE, true),
                "", true);
    }

    @Test
    void replayUnderAChangedPolicyReplacesTheChunkGeneration() {
        Document document = Document.newBuilder()
                .setDocId("doc-r1")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Generations")
                        .setBody("Replay is an operation. Policies are data. Digests are"
                                + " generations. Chunks never duplicate. The corpus re-derives"
                                + " on demand."))
                .build();

        ChunkingPolicy first = policy(8);
        try (LuceneSearchStore store = new LuceneSearchStore(work, Map.of(
                RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(first)),
                null, false, RepoDocumentMapping.laneVectorization())) {
            LuceneSearchStore.IndexResult landed =
                    store.index(RepoDocumentMapping.SUBJECT, document);
            assertThat(landed.policyDigest()).isEqualTo(first.digest().substring(0, 12));
            assertThat(landed.chunksIndexed()).isGreaterThan(1);
        }

        // The same index directory reopens under a changed policy — a
        // different digest, a different chunk geometry — and replays.
        ChunkingPolicy second = policy(20);
        assertThat(second.digest()).isNotEqualTo(first.digest());
        try (LuceneSearchStore store = new LuceneSearchStore(work, Map.of(
                RepoDocumentMapping.SUBJECT, RepoDocumentMapping.served(second)),
                null, false, RepoDocumentMapping.laneVectorization())) {
            LuceneSearchStore.IndexResult replayed =
                    store.index(RepoDocumentMapping.SUBJECT, document);
            assertThat(replayed.policyDigest()).isEqualTo(second.digest().substring(0, 12));

            List<SearchHit> hits = store.search(RepoDocumentMapping.SUBJECT,
                    SearchRequest.newBuilder()
                            .setMappingSubject(RepoDocumentMapping.SUBJECT)
                            .setQuery("Chunks never duplicate.")
                            .setK(10)
                            .setLane(SearchLane.SEARCH_LANE_VECTOR)
                            .build());
            assertThat(hits).isNotEmpty();
            // Every chunk in the index belongs to the new generation: the
            // old generation was atomically replaced, not accumulated.
            assertThat(hits)
                    .allSatisfy(hit -> assertThat(hit.getChunkId())
                            .startsWith("doc-r1#" + second.digest().substring(0, 12) + "#"));
        }
    }
}
