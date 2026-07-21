package ai.pipestream.proto.index.opensearch;

import java.util.Map;

/**
 * One hit of an {@link OpenSearchSearch#knn} result: the document id, the engine's kNN score,
 * and the {@code _source} document map as stored in the index.
 *
 * @param id the {@code _id} the document was indexed under
 * @param score the engine's kNN similarity score, higher meaning nearer the query vector
 * @param source the {@code _source} document map
 */
public record OpenSearchHit(String id, double score, Map<String, Object> source) {
}
