package ai.protomolt.proto.search.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import ai.protomolt.proto.search.v1.SearchRequest;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.junit.jupiter.api.Test;

class SearchEmbeddingPurposeTest {
    @Test
    void vectorSearchUsesQueryPurpose() {
        EmbeddingProvider provider = new EmbeddingProvider() {
            public String providerId() { return "search-test"; }
            public int dimension() { return 8; }
            public float[] embed(String text) { throw new AssertionError("query purpose missing"); }
            public float[] embedQuery(String text) { return new float[] {1, 0, 0, 0, 0, 0, 0, 0}; }
        };
        var served = RepoDocumentMapping.served(LuceneSearchStoreTest.policyNaming("search-test"));
        var query = (KnnFloatVectorQuery) SearchQueries.vector(served, provider,
                SearchRequest.newBuilder().setQuery("question").setK(1).build());
        assertThat(query.getTargetCopy()).containsExactly(1, 0, 0, 0, 0, 0, 0, 0);
    }
}
