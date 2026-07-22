package ai.pipestream.proto.rerank.harness;

import java.util.List;
import java.util.Objects;

/**
 * One rerank comparison case: a query and the candidate documents both providers score
 * against it.
 *
 * @param query the query text
 * @param documents the candidate documents, at least two: a ranking of one document carries
 *        no order to compare
 */
public record QueryDocuments(String query, List<String> documents) {

    public QueryDocuments {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(documents, "documents");
        if (documents.size() < 2) {
            throw new IllegalArgumentException(
                    "A ranking comparison needs at least two documents, got "
                            + documents.size());
        }
    }
}
