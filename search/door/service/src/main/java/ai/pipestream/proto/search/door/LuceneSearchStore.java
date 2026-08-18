package ai.pipestream.proto.search.door;

import ai.pipestream.proto.chunk.PolicyDerivation;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.embeddings.EmbeddingProviders;
import ai.pipestream.proto.index.lucene.LuceneFieldSpecs;
import ai.pipestream.proto.index.lucene.ProtoLuceneMapper;
import ai.pipestream.proto.index.spi.DateResolution;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.search.v1.SearchHit;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.StoredValue;
import ai.pipestream.proto.search.v1.SubjectInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
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
 * <p>Visibility and durability are decoupled: every write refreshes the
 * near-real-time searcher, while durability commits batch (per
 * {@link #COMMIT_MAX_PENDING} writes, a periodic flush, and close). A
 * crash loses at most the last interval's writes, which a replay
 * re-derives.
 *
 * <p>Validation errors throw {@link IllegalArgumentException} (the request
 * named something outside the served surface) or
 * {@link IllegalStateException} (the request needs a lane the subject does
 * not serve); the gRPC layer maps them onto statuses.
 */
public final class LuceneSearchStore implements SubjectIndex, Closeable {

    /** Reciprocal-rank fusion constant for the hybrid lane. */
    private static final int RRF_K = 60;

    /** The most hits one query may ask for; larger k is refused by name. */
    public static final int MAX_K = 1000;

    /** Writes accumulated before a durability commit forces. */
    private static final int COMMIT_MAX_PENDING = 64;

    /** The longest a pending write waits for its durability commit. */
    private static final Duration COMMIT_INTERVAL = Duration.ofSeconds(2);

    private static final Logger LOG = LoggerFactory.getLogger(LuceneSearchStore.class);

    /** Index field name of chunk identity on chunk children: {@value}. */
    public static final String CHUNK_ID_FIELD = "chunk_id";

    /** Index field name of stored chunk text on chunk children: {@value}. */
    public static final String CHUNK_TEXT_FIELD = "chunk_text";

    /** One subject's mounted state plus its durability-commit bookkeeping. */
    private static final class Subject {

        final ServedMapping served;
        final EmbeddingProvider embedder;
        final IndexWriter writer;
        final SnapshotDeletionPolicy snapshotPolicy;
        // A reader's searcher manager swaps on refresh (placeholder to real
        // index); readers of these fields capture the manager once per read.
        volatile SearcherManager searchers;
        volatile Directory searcherDir;
        volatile boolean placeholder;
        int pendingWrites;

        Subject(ServedMapping served, EmbeddingProvider embedder,
                IndexWriter writer, SearcherManager searchers,
                SnapshotDeletionPolicy snapshotPolicy) {
            this(served, embedder, writer, searchers, snapshotPolicy, null, false);
        }

        Subject(ServedMapping served, EmbeddingProvider embedder,
                IndexWriter writer, SearcherManager searchers,
                SnapshotDeletionPolicy snapshotPolicy,
                Directory searcherDir, boolean placeholder) {
            this.served = served;
            this.embedder = embedder;
            this.writer = writer;
            this.searchers = searchers;
            this.snapshotPolicy = snapshotPolicy;
            this.searcherDir = searcherDir;
            this.placeholder = placeholder;
        }

        ServedMapping served() {
            return served;
        }

        EmbeddingProvider embedder() {
            return embedder;
        }

        IndexWriter writer() {
            return writer;
        }

        SearcherManager searchers() {
            return searchers;
        }
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
    private final Path indexDir;
    private final IndexSnapshots snapshots;
    private final boolean readOnly;
    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));
    private final Analyzer analyzer = new StandardAnalyzer();
    private final ScheduledExecutorService committer;

    /**
     * Opens one index per subject under {@code indexDir} and resolves every
     * chunk lane's embedding provider.
     *
     * @param indexDir the root directory; each subject indexes in its own
     *        subdirectory
     * @param served the subjects to serve, keyed by subject name
     */
    public LuceneSearchStore(Path indexDir, Map<String, ServedMapping> served) {
        this(indexDir, served, null);
    }

    /**
     * As {@link #LuceneSearchStore(Path, Map)}, with index snapshots to a
     * blob store: each subject restores its latest snapshot on boot (when
     * its local directory is empty and the snapshot's identity matches) and
     * snapshots on the durability-commit cadence and on close.
     *
     * @param indexDir the root directory; each subject indexes in its own
     *        subdirectory
     * @param served the subjects to serve, keyed by subject name
     * @param snapshots the snapshot component; null disables snapshots
     */
    public LuceneSearchStore(
            Path indexDir, Map<String, ServedMapping> served, IndexSnapshots snapshots) {
        this(indexDir, served, snapshots, false);
    }

    /**
     * As {@link #LuceneSearchStore(Path, Map, IndexSnapshots)}, optionally
     * read-only: a reader opens no {@link IndexWriter} (no write lock, no
     * commits, no uploads), serves whatever its directory restores, and
     * pulls newer snapshots through {@link #refreshFromSnapshots()}. Until
     * a first snapshot exists a reader serves an empty in-memory index,
     * swapped for the real one when a restore lands.
     *
     * @param indexDir the root directory; each subject in its own
     *        subdirectory
     * @param served the subjects to serve, keyed by subject name
     * @param snapshots the snapshot component; null disables snapshots
     * @param readOnly whether this store is a reader
     */
    public LuceneSearchStore(Path indexDir, Map<String, ServedMapping> served,
            IndexSnapshots snapshots, boolean readOnly) {
        if (indexDir == null) {
            throw new IllegalArgumentException("indexDir must not be null");
        }
        if (served == null || served.isEmpty()) {
            throw new IllegalArgumentException("at least one served mapping subject is required");
        }
        if (readOnly && snapshots != null && !snapshots.readOnly()) {
            throw new IllegalArgumentException("a read-only store must not write snapshots:"
                    + " construct its IndexSnapshots read-only");
        }
        this.indexDir = indexDir;
        this.snapshots = snapshots;
        this.readOnly = readOnly;
        committer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "search-door-committer");
            thread.setDaemon(true);
            return thread;
        });
        try {
            for (Map.Entry<String, ServedMapping> subject : served.entrySet()) {
                EmbeddingProvider embedder = subject.getValue().chunkLane() == null
                        ? null
                        : EmbeddingProviders.forSpec(
                                subject.getValue().chunkLane().policy().embedding());
                Path subjectDir = indexDir.resolve(subject.getKey());
                if (readOnly) {
                    if (snapshots != null) {
                        snapshots.restoreInto(subjectDir, subject.getKey(), subject.getValue());
                    }
                    subjects.put(subject.getKey(),
                            openReader(subject.getValue(), embedder, subjectDir));
                    continue;
                }
                SnapshotDeletionPolicy snapshotPolicy = null;
                IndexWriterConfig config = new IndexWriterConfig(analyzer);
                if (snapshots != null) {
                    snapshots.restoreInto(subjectDir, subject.getKey(), subject.getValue());
                    snapshotPolicy = new SnapshotDeletionPolicy(
                            new KeepOnlyLastCommitDeletionPolicy());
                    config.setIndexDeletionPolicy(snapshotPolicy);
                }
                IndexWriter writer = new IndexWriter(FSDirectory.open(subjectDir), config);
                subjects.put(subject.getKey(), new Subject(
                        subject.getValue(), embedder, writer,
                        new SearcherManager(writer, null), snapshotPolicy));
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
        if (!readOnly) {
            committer.scheduleWithFixedDelay(this::commitPending,
                    COMMIT_INTERVAL.toMillis(), COMMIT_INTERVAL.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Opens a reader-mode subject: a searcher manager over the restored
     * directory when it holds a commit, an empty in-memory placeholder
     * otherwise (a snapshot may not exist yet; {@link #refreshFromSnapshots()}
     * swaps the real index in when one lands).
     */
    private Subject openReader(ServedMapping served, EmbeddingProvider embedder,
            Path subjectDir) throws IOException {
        Files.createDirectories(subjectDir);
        Directory directory = FSDirectory.open(subjectDir);
        if (DirectoryReader.indexExists(directory)) {
            return new Subject(served, embedder, null,
                    new SearcherManager(directory, null), null, directory, false);
        }
        directory.close();
        Directory placeholder = new ByteBuffersDirectory();
        // An empty commit, so the manager has something to open: a reader
        // with no snapshot yet answers empty instead of failing to mount.
        new IndexWriter(placeholder, new IndexWriterConfig(analyzer)).close();
        return new Subject(served, embedder, null,
                new SearcherManager(placeholder, null), null, placeholder, true);
    }

    /**
     * A reader's pull: for each subject, restore the first snapshot (when
     * the subject still serves its empty placeholder) or pull a newer
     * commit into the live directory and refresh the searchers. Never
     * uploads or prunes anything, locally or in the store; a failed pull
     * leaves the serving commit untouched.
     *
     * @return whether any subject advanced
     */
    public boolean refreshFromSnapshots() {
        if (!readOnly || snapshots == null) {
            throw new IllegalStateException(readOnly
                    ? "this reader has no snapshot store to refresh from"
                    : "refresh is the reader's pull; the writer publishes snapshots"
                            + " on its commit cadence");
        }
        boolean advanced = false;
        for (Map.Entry<String, Subject> entry : subjects.entrySet()) {
            String subjectName = entry.getKey();
            Subject subject = entry.getValue();
            Path subjectDir = indexDir.resolve(subjectName);
            try {
                if (subject.placeholder) {
                    snapshots.restoreInto(subjectDir, subjectName, subject.served());
                    Directory directory = FSDirectory.open(subjectDir);
                    if (!DirectoryReader.indexExists(directory)) {
                        directory.close();
                        continue;
                    }
                    SearcherManager fresh = new SearcherManager(directory, null);
                    SearcherManager retired = subject.searchers;
                    Directory retiredDir = subject.searcherDir;
                    subject.searchers = fresh;
                    subject.searcherDir = directory;
                    subject.placeholder = false;
                    retired.close();
                    retiredDir.close();
                    advanced = true;
                } else if (snapshots.refreshInto(subjectDir, subjectName, subject.served())) {
                    subject.searchers().maybeRefresh();
                    advanced = true;
                }
            } catch (IOException | RuntimeException e) {
                LOG.warn("cannot refresh '{}'; the previous state keeps serving",
                        subjectName, e);
            }
        }
        return advanced;
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
        Subject subject = writable(subjectName);
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
            afterWrite(subject);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "cannot index '" + docId + "' under '" + subjectName + "'", e);
        }
        return new IndexResult(docId, chunks, digest);
    }

    /**
     * Post-write bookkeeping. Visibility is near-real-time and needs no
     * commit, so the searcher refreshes per write; durability commits
     * (fsyncs) once per {@link #COMMIT_MAX_PENDING}-write batch, with the
     * periodic committer flushing partial batches every
     * {@link #COMMIT_INTERVAL} and close committing whatever remains. A
     * crash therefore loses at most the last interval's writes — replay
     * re-derives them — while a busy subject fsyncs per batch instead of
     * per document.
     */
    private void afterWrite(Subject subject) throws IOException {
        subject.searchers().maybeRefresh();
        boolean commit = false;
        synchronized (subject) {
            subject.pendingWrites++;
            if (subject.pendingWrites >= COMMIT_MAX_PENDING) {
                subject.pendingWrites = 0;
                commit = true;
            }
        }
        if (commit) {
            subject.writer().commit();
        }
    }

    /** The periodic committer's tick: fsync every subject left dirty. */
    private void commitPending() {
        for (Map.Entry<String, Subject> entry : subjects.entrySet()) {
            Subject subject = entry.getValue();
            try {
                if (subject.writer().hasUncommittedChanges()) {
                    subject.writer().commit();
                    synchronized (subject) {
                        subject.pendingWrites = 0;
                    }
                    snapshotIfConfigured(entry.getKey(), subject);
                }
            } catch (IOException | RuntimeException e) {
                // The next tick retries; writes keep accumulating in the
                // writer either way, and close still commits.
                LOG.warn("periodic commit of '{}' failed", entry.getKey(), e);
            }
        }
    }

    /** Uploads the latest commit when snapshots are configured; never throws. */
    private void snapshotIfConfigured(String subjectName, Subject subject) {
        if (snapshots == null || subject.snapshotPolicy == null) {
            return;
        }
        snapshots.snapshot(subjectName, subject.served(), subject.snapshotPolicy,
                indexDir.resolve(subjectName));
    }

    /**
     * The doc ids a subject currently serves, walked from the identity
     * field's term dictionary with deleted-but-unmerged blocks excluded.
     *
     * @param subjectName the mapping subject
     * @return the indexed doc ids
     */
    @Override
    public Set<String> indexedDocIds(String subjectName) {
        Subject subject = subject(subjectName);
        SearcherManager searchers = subject.searchers();
        try {
            IndexSearcher searcher = searchers.acquire();
            try {
                Set<String> ids = new LinkedHashSet<>();
                for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                    Terms terms = leaf.reader().terms(subject.served().docIdField());
                    if (terms == null) {
                        continue;
                    }
                    // The term dictionary still lists deleted blocks until a
                    // merge reclaims them; only a term with a live posting
                    // counts as indexed.
                    Bits live = leaf.reader().getLiveDocs();
                    TermsEnum iterator = terms.iterator();
                    PostingsEnum postings = null;
                    BytesRef term;
                    while ((term = iterator.next()) != null) {
                        postings = iterator.postings(postings, PostingsEnum.NONE);
                        int doc;
                        while ((doc = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                            if (live == null || live.get(doc)) {
                                ids.add(term.utf8ToString());
                                break;
                            }
                        }
                    }
                }
                return ids;
            } finally {
                searchers.release(searcher);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "cannot enumerate '" + subjectName + "'", e);
        }
    }

    /**
     * Removes one document's block (parent and chunk children) from a
     * subject's index. Idempotent: deleting an id the index does not hold
     * succeeds — the id is not searchable either way.
     *
     * @param subjectName the mapping subject
     * @param docId the document identity to remove
     * @return the number of chunk children removed, counted across every
     *         policy digest, so a caller can tell a real removal from a
     *         no-op
     */
    @Override
    public int delete(String subjectName, String docId) {
        Subject subject = writable(subjectName);
        if (docId == null || docId.isBlank()) {
            throw new IllegalArgumentException(
                    "doc_id is required; cannot delete from '" + subjectName + "'");
        }
        SearcherManager searchers = subject.searchers();
        IndexSearcher searcher = null;
        try {
            // Chunk identity is <doc_id>#<digest12>#<ordinal>: the prefix
            // counts this document's chunk children whatever digest they
            // were derived under.
            searcher = searchers.acquire();
            int chunks = searcher.count(
                    new PrefixQuery(new Term(CHUNK_ID_FIELD, docId + "#")));
            // The parent and its chunk children all carry the identity
            // term, so one term delete removes the whole block.
            subject.writer().deleteDocuments(
                    new Term(subject.served().docIdField(), docId));
            afterWrite(subject);
            return chunks;
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "cannot delete '" + docId + "' from '" + subjectName + "'", e);
        } finally {
            release(searchers, searcher);
        }
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
        if (request.getK() > MAX_K) {
            throw new IllegalArgumentException(
                    "k must be at most " + MAX_K + ", got " + request.getK());
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
        SearcherManager searchers = subject.searchers();
        IndexSearcher searcher = null;
        try {
            searcher = searchers.acquire();
            Map<String, ResolvedFieldHint> hints = storedHints(subject.served().mapping());
            List<SearchHit> hits = new ArrayList<>();
            for (ScoreDoc scored : searcher.search(query, k).scoreDocs) {
                org.apache.lucene.document.Document doc =
                        searcher.storedFields().document(scored.doc);
                SearchHit.Builder hit = SearchHit.newBuilder().setScore(scored.score);
                for (IndexableField field : doc.getFields()) {
                    StoredValue value = storedValue(hints.get(field.name()), field);
                    if (value != null) {
                        hit.putStored(field.name(), value);
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
            release(searchers, searcher);
        }
    }

    /** The subject as a write target; a reader-mode subject refuses. */
    private Subject writable(String subjectName) {
        Subject subject = subject(subjectName);
        if (subject.writer() == null) {
            throw new IllegalStateException("subject '" + subjectName + "' is read-only"
                    + " on this node: the writer indexes, a reader only refreshes");
        }
        return subject;
    }

    private void release(SearcherManager searchers, IndexSearcher searcher) {
        if (searcher != null) {
            try {
                searchers.release(searcher);
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

    /** The mapping's field hints by index field name, for typing stored values. */
    private static Map<String, ResolvedFieldHint> storedHints(IndexMapping mapping) {
        Map<String, ResolvedFieldHint> hints = new LinkedHashMap<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            hints.put(field.fieldName(), field.hint());
        }
        return hints;
    }

    /**
     * One stored Lucene field as a typed cell. The mapping's declared kind
     * decides the arm; a field the mapping does not declare (chunk text,
     * chunk identity) or whose stored form disagrees with its kind falls
     * back to the runtime form. Returns {@code null} only when the field
     * carries nothing storable.
     */
    private static StoredValue storedValue(ResolvedFieldHint hint, IndexableField field) {
        Number number = field.numericValue();
        String string = field.stringValue();
        BytesRef binary = field.binaryValue();
        if (hint != null) {
            switch (hint.type()) {
                case DATE -> {
                    if (number != null) {
                        long epoch = number.longValue();
                        long millis = hint.dateResolution() == DateResolution.SECONDS
                                ? epoch * 1000L
                                : epoch;
                        return StoredValue.newBuilder()
                                .setTimestampValue(Timestamp.newBuilder()
                                        .setSeconds(Math.floorDiv(millis, 1000L))
                                        .setNanos((int) Math.floorMod(millis, 1000L) * 1_000_000))
                                .build();
                    }
                }
                case INT32, INT64 -> {
                    if (number != null) {
                        return StoredValue.newBuilder()
                                .setInt64Value(number.longValue()).build();
                    }
                }
                case FLOAT, DOUBLE -> {
                    if (number != null) {
                        return StoredValue.newBuilder()
                                .setDoubleValue(number.doubleValue()).build();
                    }
                }
                case BOOLEAN -> {
                    if (string != null) {
                        return StoredValue.newBuilder()
                                .setBoolValue(Boolean.parseBoolean(string)).build();
                    }
                }
                case BINARY -> {
                    if (binary != null) {
                        return StoredValue.newBuilder()
                                .setBytesValue(ByteString.copyFrom(
                                        binary.bytes, binary.offset, binary.length))
                                .build();
                    }
                }
                default -> {
                    // TEXT, KEYWORD, and JSON-folded fields land below.
                }
            }
        }
        if (string != null) {
            return StoredValue.newBuilder().setStringValue(string).build();
        }
        if (number instanceof Float || number instanceof Double) {
            return StoredValue.newBuilder().setDoubleValue(number.doubleValue()).build();
        }
        if (number != null) {
            return StoredValue.newBuilder().setInt64Value(number.longValue()).build();
        }
        if (binary != null) {
            return StoredValue.newBuilder()
                    .setBytesValue(ByteString.copyFrom(binary.bytes, binary.offset, binary.length))
                    .build();
        }
        return null;
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

    /**
     * Borrows the subject's live near-real-time searcher for one read. The
     * acquire/release pair stays inside the store so a borrower can never
     * leak the manager's reference count; the searcher is valid only within
     * {@code read}. This is the metric executors' read seam: aggregation is
     * a read path over the same index the door serves.
     *
     * @param subjectName the mapping subject
     * @param read the work to run against the searcher
     * @param <T> the read's result type
     * @return what {@code read} returned
     */
    public <T> T withSearcher(String subjectName, SearcherRead<T> read) {
        Subject subject = subject(subjectName);
        SearcherManager searchers = subject.searchers();
        IndexSearcher searcher;
        try {
            searcher = searchers.acquire();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot acquire a searcher", e);
        }
        try {
            return read.apply(searcher);
        } catch (IOException e) {
            throw new UncheckedIOException("searcher read failed", e);
        } finally {
            release(searchers, searcher);
        }
    }

    /** One read over a borrowed {@link IndexSearcher}. */
    @FunctionalInterface
    public interface SearcherRead<T> {
        T apply(IndexSearcher searcher) throws IOException;
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
        // The committer stops (and its in-flight tick drains) before any
        // writer closes, so a tick never commits against a closed writer.
        // A graceful shutdown, never shutdownNow: interrupting a thread
        // inside writer.commit() is a tragic event that closes the writer
        // under us, and the closes below then throw AlreadyClosed.
        committer.shutdown();
        try {
            committer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        UncheckedIOException failure = null;
        for (Map.Entry<String, Subject> entry : subjects.entrySet()) {
            Subject subject = entry.getValue();
            if (subject.writer() == null) {
                continue;
            }
            try {
                if (subject.writer().isOpen() && subject.writer().hasUncommittedChanges()) {
                    subject.writer().commit();
                }
            } catch (IOException | RuntimeException e) {
                // The writer close below still commits what it can.
                LOG.warn("final commit of '{}' failed", entry.getKey(), e);
            }
            snapshotIfConfigured(entry.getKey(), subject);
        }
        for (Subject subject : subjects.values()) {
            try {
                subject.searchers().close();
            } catch (IOException e) {
                // Closing continues; the writer close below is the one that
                // persists.
                LOG.warn("cannot close the searcher manager", e);
            }
            try {
                if (subject.searcherDir != null) {
                    subject.searcherDir.close();
                }
                if (subject.writer() != null) {
                    subject.writer().close();
                }
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
