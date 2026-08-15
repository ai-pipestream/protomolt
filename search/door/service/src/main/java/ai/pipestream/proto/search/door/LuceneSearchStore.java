package ai.pipestream.proto.search.door;

import ai.pipestream.proto.chunk.PolicyDerivation;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.embeddings.EmbeddingProviders;
import ai.pipestream.proto.index.lucene.LuceneFieldSpecs;
import ai.pipestream.proto.index.lucene.ProtoLuceneMapper;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.search.v1.SearchHit;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SubjectInfo;
import com.google.protobuf.Message;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The door's Lucene state: one index per served mapping subject, written by
 * the indexing RPC and read by the query RPC. Indexing is idempotent — a
 * document's whole block (chunk children first, parent last) atomically
 * replaces its previous state by identity term. Chunk-lane embedding
 * providers resolve when the store is built, so a subject naming an absent
 * or misconfigured model fails at mount, not on the first document.
 *
 * <p>Validation errors throw {@link IllegalArgumentException} (the request
 * named something outside the served surface) or
 * {@link IllegalStateException} (the request needs a lane the subject does
 * not serve); the gRPC layer maps them onto statuses.
 */
public final class LuceneSearchStore implements Closeable {

    /** Reciprocal-rank fusion constant for the hybrid lane. */
    private static final int RRF_K = 60;

    private static final Logger LOG = LoggerFactory.getLogger(LuceneSearchStore.class);

    /** Index field name of chunk identity on chunk children: {@value}. */
    public static final String CHUNK_ID_FIELD = "chunk_id";

    /** Index field name of stored chunk text on chunk children: {@value}. */
    public static final String CHUNK_TEXT_FIELD = "chunk_text";

    private record Subject(
            ServedMapping served,
            EmbeddingProvider embedder,
            IndexWriter writer,
            SearcherManager searchers) {
    }

    /**
     * What one index call landed.
     *
     * @param docId the indexed document's identity
     * @param chunksIndexed chunk children written
     * @param policyDigest the policy digest chunks derive under; empty when
     *        no chunks were written
     */
    public record IndexResult(String docId, int chunksIndexed, String policyDigest) {
    }

    private final Map<String, Subject> subjects = new LinkedHashMap<>();
    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));
    private final Analyzer analyzer = new StandardAnalyzer();

    /**
     * Opens one index per subject under {@code indexDir} and resolves every
     * chunk lane's embedding provider.
     *
     * @param indexDir the root directory; each subject indexes in its own
     *        subdirectory
     * @param served the subjects to serve, keyed by subject name
     */
    public LuceneSearchStore(Path indexDir, Map<String, ServedMapping> served) {
        if (indexDir == null) {
            throw new IllegalArgumentException("indexDir must not be null");
        }
        if (served == null || served.isEmpty()) {
            throw new IllegalArgumentException("at least one served mapping subject is required");
        }
        try {
            for (Map.Entry<String, ServedMapping> subject : served.entrySet()) {
                EmbeddingProvider embedder = subject.getValue().chunkLane() == null
                        ? null
                        : EmbeddingProviders.forSpec(
                                subject.getValue().chunkLane().policy().embedding());
                IndexWriter writer = new IndexWriter(
                        FSDirectory.open(indexDir.resolve(subject.getKey())),
                        new IndexWriterConfig(analyzer));
                subjects.put(subject.getKey(), new Subject(
                        subject.getValue(), embedder, writer,
                        new SearcherManager(writer, null)));
            }
        } catch (IOException e) {
            close();
            throw new UncheckedIOException("cannot open the search index under " + indexDir, e);
        } catch (RuntimeException e) {
            // A misconfigured chunk lane (an absent embedding provider, a
            // dims mismatch) fails the mount the same way; the subjects
            // already opened must not leak their write locks.
            close();
            throw e;
        }
    }

    /** The served subject names, in configuration order. */
    public Set<String> subjectNames() {
        return Set.copyOf(subjects.keySet());
    }

    /** The served surface described for callers, in configuration order. */
    public List<SubjectInfo> describeSubjects() {
        List<SubjectInfo> described = new ArrayList<>();
        for (Map.Entry<String, Subject> entry : subjects.entrySet()) {
            ServedMapping served = entry.getValue().served();
            SubjectInfo.Builder info = SubjectInfo.newBuilder()
                    .setSubject(entry.getKey())
                    .setDocIdField(served.docIdField())
                    .addAllTextFields(textFields(served.mapping()));
            if (served.chunkLane() != null) {
                info.setHasVectorLane(true)
                        .setPolicyDigest(served.chunkLane().policy().digest());
            }
            described.add(info.build());
        }
        return List.copyOf(described);
    }

    /**
     * (Re-)indexes one document under a subject: the parent under the
     * subject's mapping, chunk children from the chunk lane when present,
     * replacing the document's previous block atomically by identity.
     *
     * @param subjectName the mapping subject
     * @param document the source message
     * @return what landed
     */
    public IndexResult index(String subjectName, Message document) {
        Subject subject = subject(subjectName);
        ServedMapping served = subject.served();
        String docId = served.docId().apply(document);
        if (docId == null || docId.isBlank()) {
            throw new IllegalArgumentException(
                    "the document carries a blank identity; cannot index it under '"
                            + subjectName + "'");
        }
        List<org.apache.lucene.document.Document> block = new ArrayList<>();
        int chunks = 0;
        String digest = "";
        ServedMapping.ChunkLane lane = served.chunkLane();
        if (lane != null) {
            String text = lane.sourceText().apply(document);
            if (text != null && !text.isBlank()) {
                List<PolicyDerivation.DerivedChunk> derived =
                        new PolicyDerivation(subject.embedder())
                                .derive(text, lane.policy());
                digest = derived.isEmpty() ? "" : lane.policy().digest().substring(0, 12);
                for (PolicyDerivation.DerivedChunk chunk : derived) {
                    org.apache.lucene.document.Document child =
                            new org.apache.lucene.document.Document();
                    child.add(new StringField(CHUNK_ID_FIELD,
                            docId + "#" + digest + "#" + chunk.chunk().ordinal(),
                            Field.Store.YES));
                    child.add(new StringField(served.docIdField(), docId, Field.Store.YES));
                    if (lane.policy().storeChunkText()) {
                        child.add(new StoredField(CHUNK_TEXT_FIELD, chunk.chunk().text()));
                    }
                    child.add(new KnnFloatVectorField(
                            lane.vectorField(),
                            chunk.vector(),
                            LuceneFieldSpecs.similarityFunction(
                                    lane.policy().embedding().similarity())));
                    block.add(child);
                }
                chunks = block.size();
            }
        }
        try {
            block.add(mapper.map(document, served.mapping()));
        } catch (Exception e) {
            // The proto-to-document mapping refused the message; the mapping
            // and the message disagree, which is a caller problem.
            throw new IllegalArgumentException("cannot map '" + docId + "' under '"
                    + subjectName + "': " + e.getMessage(), e);
        }
        try {
            // Children first, parent last: Lucene's block contract. The
            // update deletes every previous block member carrying the
            // identity term and adds the new block in one atomic step.
            subject.writer().updateDocuments(
                    new Term(served.docIdField(), docId), block);
            subject.writer().commit();
            subject.searchers().maybeRefresh();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "cannot index '" + docId + "' under '" + subjectName + "'", e);
        }
        return new IndexResult(docId, chunks, digest);
    }

    /**
     * Runs one query against a subject.
     *
     * @param subjectName the mapping subject
     * @param request the query; its subject field is not consulted here
     * @return hits, best first
     */
    public List<SearchHit> search(String subjectName, SearchRequest request) {
        Subject subject = subject(subjectName);
        if (request.getQuery().isBlank()) {
            throw new IllegalArgumentException("query must not be blank");
        }
        if (request.getK() <= 0) {
            throw new IllegalArgumentException("k must be positive, got " + request.getK());
        }
        return switch (request.getLane()) {
            case SEARCH_LANE_LEXICAL -> hits(subject, lexical(subject, request), request.getK());
            case SEARCH_LANE_VECTOR -> hits(subject, vector(subject, request), request.getK());
            case SEARCH_LANE_HYBRID -> fuse(subject, request);
            default -> throw new IllegalArgumentException(
                    "lane must be one of SEARCH_LANE_LEXICAL, SEARCH_LANE_VECTOR,"
                            + " SEARCH_LANE_HYBRID; got " + request.getLane());
        };
    }

    private Query lexical(Subject subject, SearchRequest request) {
        Set<String> textFields = textFields(subject.served().mapping());
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
            for (String token : analyze(field, request.getQuery())) {
                query.add(new TermQuery(new Term(field, token)), BooleanClause.Occur.SHOULD);
            }
        }
        return query.build();
    }

    private Query vector(Subject subject, SearchRequest request) {
        ServedMapping.ChunkLane lane = subject.served().chunkLane();
        if (lane == null) {
            throw new IllegalStateException(
                    "this subject serves no vector lane: it has no chunking policy");
        }
        return new KnnFloatVectorQuery(
                lane.vectorField(),
                subject.embedder().embed(request.getQuery()),
                request.getK());
    }

    /** Reciprocal-rank fusion of the lexical and vector lanes. */
    private List<SearchHit> fuse(Subject subject, SearchRequest request) {
        List<SearchHit> lexical = hits(subject, lexical(subject, request), request.getK());
        List<SearchHit> vector = hits(subject, vector(subject, request), request.getK());
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
                .limit(request.getK())
                .map(fused -> byKey.get(fused.getKey()).toBuilder()
                        .setScore(fused.getValue().floatValue())
                        .build())
                .toList();
    }

    private List<SearchHit> hits(Subject subject, Query query, int k) {
        IndexSearcher searcher = null;
        try {
            searcher = subject.searchers().acquire();
            List<SearchHit> hits = new ArrayList<>();
            for (ScoreDoc scored : searcher.search(query, k).scoreDocs) {
                org.apache.lucene.document.Document doc =
                        searcher.storedFields().document(scored.doc);
                SearchHit.Builder hit = SearchHit.newBuilder().setScore(scored.score);
                for (IndexableField field : doc.getFields()) {
                    if (field.stringValue() != null) {
                        hit.putStored(field.name(), field.stringValue());
                    }
                }
                String docId = doc.get(subject.served().docIdField());
                hit.setDocId(docId == null ? "" : docId);
                String chunkId = doc.get(CHUNK_ID_FIELD);
                if (chunkId != null) {
                    hit.setChunkId(chunkId);
                }
                hits.add(hit.build());
            }
            return hits;
        } catch (IOException e) {
            throw new UncheckedIOException("search failed", e);
        } finally {
            release(subject, searcher);
        }
    }

    private void release(Subject subject, IndexSearcher searcher) {
        if (searcher != null) {
            try {
                subject.searchers().release(searcher);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot release the searcher", e);
            }
        }
    }

    private List<String> analyze(String field, String query) {
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

    private static Set<String> textFields(IndexMapping mapping) {
        Set<String> fields = new LinkedHashSet<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            if (field.type() == IndexFieldKind.TEXT) {
                fields.add(field.fieldName());
            }
        }
        return fields;
    }

    private Subject subject(String subjectName) {
        Subject subject = subjects.get(subjectName);
        if (subject == null) {
            throw new IllegalArgumentException("unknown mapping subject '" + subjectName
                    + "'; served subjects: " + String.join(", ", subjects.keySet()));
        }
        return subject;
    }

    @Override
    public void close() {
        UncheckedIOException failure = null;
        for (Subject subject : subjects.values()) {
            try {
                subject.searchers().close();
            } catch (IOException e) {
                // Closing continues; the writer close below is the one that
                // persists.
                LOG.warn("cannot close the searcher manager", e);
            }
            try {
                subject.writer().close();
            } catch (IOException e) {
                // Every subject still gets its close; the first failure is
                // reported after the loop with the rest suppressed.
                UncheckedIOException wrapped =
                        new UncheckedIOException("cannot close the search index", e);
                if (failure == null) {
                    failure = wrapped;
                } else {
                    failure.addSuppressed(wrapped);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
