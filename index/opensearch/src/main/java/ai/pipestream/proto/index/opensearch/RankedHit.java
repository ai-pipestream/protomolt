package ai.pipestream.proto.index.opensearch;

/**
 * One hit of a {@link RerankedSemanticSearch#search} result: the document id, the text the
 * reranker scored, the cross-encoder relevance score that ordered the result, and the engine's
 * kNN score the candidate was recalled with.
 *
 * @param id the {@code _id} the document was indexed under
 * @param text the candidate text pulled from the document's text field
 * @param relevanceScore the rerank provider's relevance score, higher meaning more relevant
 * @param knnScore the engine's kNN similarity score from the recall pass
 */
public record RankedHit(String id, String text, double relevanceScore, double knnScore) {
}
