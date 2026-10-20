package ai.protomolt.proto.search.service;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.v1.SearchHit;
import ai.protomolt.proto.search.v1.SearchRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * Package-private search query and fusion construction for {@link LuceneSearchStore}.
 *
 * <p>Separates building lexical and vector queries (and fusing their hit
 * lists) from the store's writer/searcher acquisition, snapshot refresh,
 * and close lifecycle. Construction is stateless: the mapping, lane, and
 * analyzer arrive as parameters on every call.
 */
final class SearchQueries {

    /** Reciprocal-rank fusion constant for the hybrid lane. */
    private static final int RRF_K = 60;

    private SearchQueries() {}

    /** The lexical lane's query: analyzed request terms OR-ed over the text fields. */
    static Query lexical(IndexMapping mapping, Analyzer analyzer, SearchRequest request) {
        Set<String> textFields = textFields(mapping);
        List<String> fields;
        if (request.getFieldsList().isEmpty()) {
            fields = List.copyOf(textFields);
        } else {
            for (String field : request.getFieldsList()) {
                if (!textFields.contains(field)) {
                    throw new IllegalArgumentException("field '" + field
                            + "' is not a text field of this subject's mapping;"
                            + " text fields: " + String.join(", ", textFields));
                }
            }
            fields = request.getFieldsList();
        }
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        for (String field : fields) {
            for (String token : analyze(analyzer, field, request.getQuery())) {
                query.add(new TermQuery(new Term(field, token)), BooleanClause.Occur.SHOULD);
            }
        }
        return query.build();
    }

    /** The vector lane's kNN query over the chunk lane's vector field. */
    static Query vector(ServedMapping served, EmbeddingProvider embedder, SearchRequest request) {
        ServedMapping.ChunkLane lane = served.chunkLane();
        if (lane == null) {
            throw new IllegalStateException(
                    "this subject serves no vector lane: it has no chunking policy");
        }
        return new KnnFloatVectorQuery(
                lane.vectorField(),
                embedder.embed(request.getQuery()),
                request.getK());
    }

    /** Reciprocal-rank fusion of the lexical and vector lanes' hit lists. */
    static List<SearchHit> fuse(List<SearchHit> lexical, List<SearchHit> vector, int k) {
        Map<String, SearchHit> byKey = new LinkedHashMap<>();
        Map<String, Double> scores = new LinkedHashMap<>();
        for (List<SearchHit> lane : List.of(lexical, vector)) {
            for (int rank = 0; rank < lane.size(); rank++) {
                SearchHit hit = lane.get(rank);
                String key = hit.getChunkId().isEmpty() ? hit.getDocId() : hit.getChunkId();
                byKey.putIfAbsent(key, hit);
                scores.merge(key, 1.0 / (RRF_K + rank + 1), Double::sum);
            }
        }
        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(k)
                .map(fused -> byKey.get(fused.getKey()).toBuilder()
                        .setScore(fused.getValue().floatValue())
                        .build())
                .toList();
    }

    /** The mapping's text-kind index field names, in declaration order. */
    static Set<String> textFields(IndexMapping mapping) {
        Set<String> fields = new LinkedHashSet<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            if (field.type() == IndexFieldKind.TEXT) {
                fields.add(field.fieldName());
            }
        }
        return fields;
    }

    private static List<String> analyze(Analyzer analyzer, String field, String query) {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(field, query)) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(term.toString());
            }
            stream.end();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot analyze the query", e);
        }
        return tokens;
    }
}
