package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.rerank.RerankProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Semantic search with a rerank head: embeds the query, recalls a deep candidate set with a
 * kNN query, scores the candidates with a cross-encoder rerank provider, and answers the
 * reordered top-k.
 *
 * <p>The two passes divide the work: the kNN list is recall (cheap over the whole index,
 * approximate), the cross-encoder is precision (expensive per candidate, so it only sees the
 * recalled set). {@code candidates} should therefore be comfortably larger than {@code k}: the
 * reranker can only promote documents the kNN pass recalled, so the candidate depth bounds how
 * much the reranker can reorder.
 *
 * <p>Every {@link RankedHit} carries both scores side by side: the kNN similarity and the
 * reranker's relevance score are not commensurable, so neither may stand in for the other.
 */
public final class RerankedSemanticSearch {

    private final OpenSearchSearch search;
    private final EmbeddingProvider embedder;
    private final RerankProvider reranker;

    /**
     * @param search the OpenSearch read side
     * @param embedder embeds the query into the same vector space the index was built with
     * @param reranker scores the recalled candidates against the query
     */
    public RerankedSemanticSearch(OpenSearchSearch search, EmbeddingProvider embedder,
                                  RerankProvider reranker) {
        this.search = Objects.requireNonNull(search, "search");
        this.embedder = Objects.requireNonNull(embedder, "embedder");
        this.reranker = Objects.requireNonNull(reranker, "reranker");
    }

    /**
     * Searches {@code index} for documents semantically matching {@code query}.
     *
     * @param index index name
     * @param vectorField name of the {@code knn_vector} field the query vector runs against
     * @param textField name of the {@code _source} field holding each candidate's text; the
     *        reranker scores these texts
     * @param query the query text, embedded with {@code embedder}
     * @param k how many hits to answer
     * @param candidates how deep the kNN recall list runs; must be at least {@code k}
     * @return up to {@code k} hits ordered by descending rerank score
     * @throws IllegalArgumentException when {@code candidates} is smaller than {@code k}
     * @throws IOException when the engine cannot be reached or refuses the query
     * @throws IllegalStateException when a recalled hit lacks a String {@code textField} (the
     *         message names the hit id), or when the reranker returns a score list whose size
     *         differs from the candidate count (the message names the provider id)
     */
    public List<RankedHit> search(String index, String vectorField, String textField,
                                  String query, int k, int candidates) throws IOException {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(vectorField, "vectorField");
        Objects.requireNonNull(textField, "textField");
        Objects.requireNonNull(query, "query");
        if (candidates < k) {
            throw new IllegalArgumentException("candidates (" + candidates
                    + ") must be at least k (" + k
                    + "); the reranker can only reorder what the kNN pass recalled");
        }
        float[] embedding = embedder.embed(query);
        List<Float> vector = new ArrayList<>(embedding.length);
        for (float component : embedding) {
            vector.add(component);
        }
        List<OpenSearchHit> hits = search.knn(index, vectorField, vector, candidates);
        List<String> texts = new ArrayList<>(hits.size());
        for (OpenSearchHit hit : hits) {
            Object value = hit.source().get(textField);
            // A silent drop would corrupt ranking positions, so a missing or non-String
            // text field fails the search and names the hit.
            if (!(value instanceof String text)) {
                throw new IllegalStateException("Hit '" + hit.id() + "' has no String field '"
                        + textField + "' in its _source; found "
                        + (value == null ? "nothing" : value.getClass().getName()));
            }
            texts.add(text);
        }
        List<Double> scores = reranker.score(query, texts);
        if (scores.size() != hits.size()) {
            throw new IllegalStateException("Rerank provider '" + reranker.providerId()
                    + "' returned " + scores.size() + " scores for " + hits.size()
                    + " candidates");
        }
        List<RankedHit> ranked = new ArrayList<>(hits.size());
        for (int i = 0; i < hits.size(); i++) {
            ranked.add(new RankedHit(hits.get(i).id(), texts.get(i),
                    scores.get(i), hits.get(i).score()));
        }
        ranked.sort(Comparator.comparingDouble(RankedHit::relevanceScore).reversed());
        if (k >= ranked.size()) {
            return ranked;
        }
        return new ArrayList<>(ranked.subList(0, k));
    }
}
