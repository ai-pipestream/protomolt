package ai.protomolt.proto.samples;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.embedding.model2vec.Model2VecEmbeddingProvider;
import ai.protomolt.proto.search.index.lucene.LuceneIndexWriter;
import ai.protomolt.proto.search.index.lucene.ProtoLuceneMapper;
import ai.protomolt.proto.search.index.ndjson.ProtoNdjsonWriter;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * End-to-end demo: CourtListener opinion JSONL → {@link Document} (the platform's generic
 * document type) → projected NDJSON → Lucene (text + HNSW float vectors), followed by one
 * demo kNN query and one demo text query against the index just built.
 *
 * <p>Embeddings come from a {@link Model2VecEmbeddingProvider}: a real Model2Vec directory
 * named by {@code -Dprotomolt.embeddings.model2vec.path} / {@code PROTOMOLT_MODEL2VEC_PATH}
 * when set, otherwise a small deterministic sample model the demo writes itself
 * ({@link CourtSampleModel}), mirroring the provider module's own tests.
 *
 * <p>Usage: {@code ./gradlew :samples:runCourtDocIndex [-Pfixtures=...] [-Pout=...] [-Plimit=...]
 * [-Pmodel2vec=...]}
 */
public final class CourtDocumentIndexSample {

    private static final String FIXTURE_RESOURCE = "/fixtures/court/opinions_sample.jsonl";
    private static final int EMBED_BODY_CHARS = 2000;
    private static final String TEXT_QUERY_TERM = "habeas";

    private static final ObjectMapper JSON = new ObjectMapper();

    private CourtDocumentIndexSample() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseArgs(args);
        String fixtures = options.get("fixtures"); // unset: the bundled classpath fixture
        Path outDir = Path.of(options.getOrDefault("out", "build/court-index-out"));
        int limit = Integer.parseInt(options.getOrDefault("limit", "25"));

        Files.createDirectories(outDir);
        Path ndjsonPath = outDir.resolve("documents.ndjson");
        Path lucenePath = outDir.resolve("lucene");
        Files.createDirectories(lucenePath);

        Model2VecEmbeddingProvider provider = embeddingProvider(outDir);
        System.out.printf(Locale.ROOT, "Embedding provider '%s' ready (%d dims)%n",
                provider.providerId(), provider.dimension());

        IndexMapping mapping = IndexMappingFactory.defaults(documentCatalog())
                .create(Document.getDescriptor());
        ProtoLuceneMapper luceneMapper =
                new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));
        ProtoNdjsonWriter ndjson = new ProtoNdjsonWriter();

        int indexed = 0;
        int withVectors = 0;
        float[] firstVector = null;
        try (BufferedReader lines = fixtureReader(fixtures);
                BufferedWriter ndjsonOut = Files.newBufferedWriter(ndjsonPath);
                LuceneIndexWriter lucene = new LuceneIndexWriter(lucenePath)) {
            String line;
            while ((line = lines.readLine()) != null && indexed < limit) {
                if (line.isBlank()) {
                    continue;
                }
                Document document = CourtDocuments.toDocument(JSON.readTree(line));

                float[] vector = provider.embed(embedText(document));
                if (indexed == 0) {
                    System.out.printf(Locale.ROOT, "Embedded first document: %d vector dims%n",
                            vector.length);
                }
                document = CourtDocuments.withEmbedding(document, vector, provider.providerId());

                ndjson.writeLine(ndjsonOut, project(document, vector));

                org.apache.lucene.document.Document luceneDoc = luceneMapper.map(document, mapping);
                // Document-level embedding lives under repeated semantic_results; catalog
                // paths cannot index into repeated fields yet — attach explicitly.
                if (isNonZero(vector)) {
                    luceneDoc.add(new KnnFloatVectorField(
                            "embedding", vector, VectorSimilarityFunction.COSINE));
                    luceneDoc.add(new StoredField("embedding_dims", vector.length));
                    withVectors++;
                    if (firstVector == null) {
                        firstVector = vector;
                    }
                }
                lucene.add(luceneDoc);
                indexed++;
            }
            lucene.commit();
            System.out.printf(Locale.ROOT,
                    "Indexed %d Documents (%d with doc-level vectors) →%n  NDJSON: %s%n"
                            + "  Lucene: %s (%d docs)%n",
                    indexed, withVectors,
                    ndjsonPath.toAbsolutePath(), lucenePath.toAbsolutePath(), lucene.numDocs());
        }

        demoQueries(lucenePath, firstVector);
    }

    /** One kNN query (nearest neighbours of the first document) and one text query. */
    private static void demoQueries(Path lucenePath, float[] firstVector) throws Exception {
        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(lucenePath))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            System.out.printf(Locale.ROOT, "Verified Lucene reader numDocs=%d%n", reader.numDocs());

            if (firstVector != null) {
                System.out.printf(Locale.ROOT, "%nKNN demo: 5 nearest neighbours of the first"
                        + " document's vector (cosine, HNSW)%n");
                TopDocs hits = searcher.search(
                        new KnnFloatVectorQuery("embedding", firstVector, 5), 5);
                printHits(searcher, hits);
            }

            System.out.printf(Locale.ROOT, "%nText demo: body contains \"%s\"%n", TEXT_QUERY_TERM);
            TopDocs hits = searcher.search(
                    new TermQuery(new Term("body", TEXT_QUERY_TERM)), 5);
            printHits(searcher, hits);
        }
    }

    private static void printHits(IndexSearcher searcher, TopDocs hits) throws Exception {
        for (int i = 0; i < hits.scoreDocs.length; i++) {
            var stored = searcher.storedFields().document(hits.scoreDocs[i].doc);
            System.out.printf(Locale.ROOT, "  %d. %-38s %s (score %.4f)%n",
                    i + 1, stored.get("doc_id"), stored.get("title"), hits.scoreDocs[i].score);
        }
    }

    /** The embedding input: title plus the first {@value #EMBED_BODY_CHARS} chars of the body. */
    static String embedText(Document document) {
        SearchMetadata metadata = document.getSearchMetadata();
        String body = metadata.getBody();
        if (body.length() > EMBED_BODY_CHARS) {
            body = body.substring(0, EMBED_BODY_CHARS);
        }
        return metadata.getTitle() + "\n" + body;
    }

    /** Flat NDJSON projection: the searchable fields plus the document embedding. */
    static Struct project(Document document, float[] vector) {
        SearchMetadata sm = document.getSearchMetadata();
        Struct.Builder projection = Struct.newBuilder()
                .putFields("doc_id", str(document.getDocId()))
                .putFields("title", str(sm.getTitle()))
                .putFields("body", str(sm.getBody()))
                .putFields("language", str(sm.getLanguage()))
                .putFields("source_uri", str(sm.getSourceUri()))
                .putFields("document_type", str(sm.getDocumentType()))
                .putFields("author", str(sm.getAuthor()));
        if (vector.length > 0) {
            ListValue.Builder list = ListValue.newBuilder();
            for (float component : vector) {
                list.addValues(Value.newBuilder().setNumberValue(component).build());
            }
            projection.putFields("embedding", Value.newBuilder().setListValue(list).build());
            projection.putFields("embedding_dims",
                    Value.newBuilder().setNumberValue(vector.length).build());
        }
        return projection.build();
    }

    /**
     * Hint catalog for {@link Document}/{@link SearchMetadata}: ids and classifications as
     * keywords, title and body as full text, everything else skipped (the embedding is a
     * repeated-field path and is attached explicitly at index time).
     */
    static CatalogIndexingHintSource documentCatalog() {
        String document = Document.getDescriptor().getFullName();
        String metadata = SearchMetadata.getDescriptor().getFullName();
        return new CatalogIndexingHintSource()
                .put(document, "doc_id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))
                // TEXT on the intermediate message expands it into dotted paths
                .put(document, "search_metadata", ResolvedFieldHint.of(IndexFieldKind.TEXT))
                .put(metadata, "title", named(IndexFieldKind.TEXT, "title"))
                .put(metadata, "body", named(IndexFieldKind.TEXT, "body"))
                .put(metadata, "language", named(IndexFieldKind.KEYWORD, "language"))
                .put(metadata, "source_uri", named(IndexFieldKind.KEYWORD, "source_uri"))
                .put(metadata, "document_type", named(IndexFieldKind.KEYWORD, "document_type"))
                .put(metadata, "author", named(IndexFieldKind.KEYWORD, "author"))
                .put(metadata, "source_mime_type", ResolvedFieldHint.skipped())
                .put(metadata, "creation_date", ResolvedFieldHint.skipped())
                .put(metadata, "last_modified_date", ResolvedFieldHint.skipped())
                .put(metadata, "processed_date", ResolvedFieldHint.skipped())
                .put(metadata, "category", ResolvedFieldHint.skipped())
                .put(metadata, "metadata", ResolvedFieldHint.skipped())
                .put(metadata, "semantic_results", ResolvedFieldHint.skipped())
                .put(metadata, "custom_fields", ResolvedFieldHint.skipped())
                .put(metadata, "source_path", ResolvedFieldHint.skipped())
                .put(document, "blob_bag", ResolvedFieldHint.skipped())
                .put(document, "structured_data", ResolvedFieldHint.skipped())
                .put(document, "parser_results", ResolvedFieldHint.skipped())
                .put(document, "ownership", ResolvedFieldHint.skipped())
                .put(document, "doc_id_derivation", ResolvedFieldHint.skipped());
    }

    private static ResolvedFieldHint named(IndexFieldKind kind, String name) {
        return ResolvedFieldHint.builder(kind).name(name).build();
    }

    private static Model2VecEmbeddingProvider embeddingProvider(Path outDir) throws Exception {
        String configured = System.getProperty(Model2VecEmbeddingProvider.PATH_PROPERTY);
        if (configured == null) {
            configured = System.getenv(Model2VecEmbeddingProvider.PATH_ENVIRONMENT_VARIABLE);
        }
        if (configured != null) {
            return new Model2VecEmbeddingProvider(Path.of(configured));
        }
        // Mirror the provider module's own tests: write a small genuine model directory.
        Path modelDir = outDir.resolve("model2vec");
        if (!Files.exists(modelDir.resolve("model.safetensors"))) {
            CourtSampleModel.write(modelDir);
        }
        System.out.printf(Locale.ROOT, "No %s/%s configured; wrote sample Model2Vec model to %s%n",
                Model2VecEmbeddingProvider.PATH_PROPERTY,
                Model2VecEmbeddingProvider.PATH_ENVIRONMENT_VARIABLE,
                modelDir.toAbsolutePath());
        return new Model2VecEmbeddingProvider(modelDir);
    }

    private static BufferedReader fixtureReader(String fixtures) throws Exception {
        if (fixtures != null) {
            return Files.newBufferedReader(Path.of(fixtures));
        }
        InputStream resource = CourtDocumentIndexSample.class.getResourceAsStream(FIXTURE_RESOURCE);
        if (resource == null) {
            throw new IllegalStateException("Bundled fixture " + FIXTURE_RESOURCE
                    + " is not on the classpath");
        }
        return new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8));
    }

    private static boolean isNonZero(float[] vector) {
        for (float component : vector) {
            if (component != 0f) {
                return true;
            }
        }
        return false;
    }

    private static Value str(String value) {
        return Value.newBuilder().setStringValue(value == null ? "" : value).build();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (args[i].startsWith("--")) {
                options.put(args[i].substring(2), args[i + 1]);
            }
        }
        return options;
    }
}
